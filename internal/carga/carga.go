// Package carga é o gerador de carga do quiz: abre milhares de conexões com
// o broker, dispara perguntas, faz os jogadores responderem em rajada e põe
// um grupo de "clientes barulhentos" mandando spam de reações.
//
// O teste roda em duas fases com as mesmas conexões:
//
//  1. sem_barulho: só perguntas e rajadas de respostas (linha de base);
//  2. com_barulho: o mesmo, com os barulhentos mandando reações sem parar.
//
// Comparar as duas mostra o impacto dos clientes barulhentos sobre os demais
// (PDF, seção 6). As latências são medidas dentro deste processo, com o
// relógio monotônico, porque quem publica e quem recebe são clientes daqui.
package carga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/quiz"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

// Config define o cenário de carga.
type Config struct {
	Broker string
	Sala   string

	// Jogadores são os clientes normais: recebem perguntas e respondem.
	Jogadores int
	// Barulhentos são clientes que só mandam reações, na taxa
	// ReacoesPorBarulhento (por segundo, cada um), durante a fase com barulho.
	Barulhentos          int
	ReacoesPorBarulhento float64
	// ReacoesPorJogador é a taxa de reações de cada jogador normal nas duas
	// fases (o "ruído de fundo" de uma live). Zero desliga.
	ReacoesPorJogador float64

	// PerguntasPorFase e IntervaloPergunta definem o ritmo do quiz.
	PerguntasPorFase  int
	IntervaloPergunta time.Duration
	// JanelaRajada é o intervalo em que todos os jogadores respondem depois
	// de receber a pergunta (atraso sorteado entre 0 e JanelaRajada).
	JanelaRajada time.Duration
	// Aquecimento é quanto tempo os barulhentos mandam spam antes da primeira
	// pergunta da fase com barulho, para o rate limit já estar reagindo.
	Aquecimento time.Duration

	// ConexoesParalelas limita quantas conexões são abertas ao mesmo tempo.
	ConexoesParalelas int
	// Heartbeat é o intervalo de heartbeat de cada cliente (0 = padrão da SDK).
	Heartbeat time.Duration

	// MetricasBroker, se definido, lê as métricas do broker antes e depois de
	// cada fase, para o relatório mostrar os descartes do lado do broker.
	MetricasBroker func() (*metrics.Snapshot, error)
	// Log recebe o progresso do teste (nil = silencioso).
	Log io.Writer
}

// DefaultConfig é um cenário de 1000 jogadores e 50 barulhentos.
func DefaultConfig() Config {
	return Config{
		Broker:               "localhost:8080",
		Sala:                 "carga",
		Jogadores:            1000,
		Barulhentos:          50,
		ReacoesPorBarulhento: 100,
		ReacoesPorJogador:    0.2,
		PerguntasPorFase:     5,
		IntervaloPergunta:    2 * time.Second,
		JanelaRajada:         500 * time.Millisecond,
		Aquecimento:          2 * time.Second,
		ConexoesParalelas:    100,
	}
}

// Fase é o resultado de uma fase do teste.
type Fase struct {
	Nome string `json:"nome"`

	PerguntasEnviadas  int64                   `json:"perguntas_enviadas"`
	PerguntasEsperadas int64                   `json:"perguntas_esperadas"`
	PerguntasRecebidas int64                   `json:"perguntas_recebidas"`
	EntregaPerguntas   float64                 `json:"entrega_perguntas"`
	LatenciaPergunta   metrics.LatencySnapshot `json:"latencia_pergunta"`

	RespostasEnviadas  int64                   `json:"respostas_enviadas"`
	RespostasRecebidas int64                   `json:"respostas_recebidas"`
	EntregaRespostas   float64                 `json:"entrega_respostas"`
	LatenciaResposta   metrics.LatencySnapshot `json:"latencia_resposta"`
	// PicoRespostasPorSeg é a maior vazão de respostas que chegou ao
	// apurador numa rajada (respostas da pergunta / duração da rajada).
	PicoRespostasPorSeg float64 `json:"pico_respostas_por_s"`

	ReacoesBarulhentos int64 `json:"reacoes_barulhentos"`
	ReacoesJogadores   int64 `json:"reacoes_jogadores"`
	// ReacoesContadas é a soma dos resumos agregados que chegaram ao telão.
	ReacoesContadas int64 `json:"reacoes_contadas"`
	Resumos         int64 `json:"resumos"`

	// Descartes no broker durante a fase, por tópico e motivo (só com
	// Config.MetricasBroker).
	DescartesBroker map[string]map[string]int64 `json:"descartes_broker,omitempty"`
}

// Relatorio é o resultado completo do teste.
type Relatorio struct {
	Config         map[string]string        `json:"configuracao"`
	Conexoes       int                      `json:"conexoes"`
	FalhasConexao  int                      `json:"falhas_conexao"`
	TempoConexao   string                   `json:"tempo_conexao"`
	ConexoesPorSeg float64                  `json:"conexoes_por_s"`
	Fases          []Fase                   `json:"fases"`
	Cliente        client.Stats             `json:"biblioteca_cliente"`
	Gerador        metrics.ResourceSnapshot `json:"recursos_gerador"`
	ErroBroker     string                   `json:"erro_metricas_broker,omitempty"`
}

// Impacto compara a fase com barulho com a linha de base.
type Impacto struct {
	P50Ms, P95Ms, P99Ms float64 // aumento da latência das perguntas
	EntregaPerguntas    float64 // variação da entrega de perguntas (pontos percentuais)
	EntregaRespostas    float64 // variação da entrega de respostas (pontos percentuais)
}

// Impacto calcula a diferença entre as duas fases.
func (r *Relatorio) Impacto() (Impacto, bool) {
	if len(r.Fases) < 2 {
		return Impacto{}, false
	}
	a, b := r.Fases[0], r.Fases[1]
	return Impacto{
		P50Ms:            b.LatenciaPergunta.P50Ms - a.LatenciaPergunta.P50Ms,
		P95Ms:            b.LatenciaPergunta.P95Ms - a.LatenciaPergunta.P95Ms,
		P99Ms:            b.LatenciaPergunta.P99Ms - a.LatenciaPergunta.P99Ms,
		EntregaPerguntas: 100 * (b.EntregaPerguntas - a.EntregaPerguntas),
		EntregaRespostas: 100 * (b.EntregaRespostas - a.EntregaRespostas),
	}, true
}

// Nomes das fases.
const (
	FaseSemBarulho = "sem_barulho"
	FaseComBarulho = "com_barulho"
)

// faseAtiva acumula os contadores de uma fase enquanto ela roda.
type faseAtiva struct {
	nome                        string
	latPergunta, latResposta    *metrics.Latency
	perguntasEnviadas           atomic.Int64
	perguntasRecebidas          atomic.Int64
	respostasEnviadas           atomic.Int64
	respostasRecebidas          atomic.Int64
	reacoesBarulho, reacoesJogo atomic.Int64
	reacoesContadas, resumos    atomic.Int64
	mu                          sync.Mutex
	rajadas                     map[int]*rajada // pergunta -> respostas recebidas
}

type rajada struct {
	primeira, ultima int64
	n                int64
}

func novaFase(nome string) *faseAtiva {
	return &faseAtiva{
		nome:        nome,
		latPergunta: metrics.NewLatency(metrics.DefaultLatencySamples),
		latResposta: metrics.NewLatency(metrics.DefaultLatencySamples),
		rajadas:     make(map[int]*rajada),
	}
}

// execucao é o estado de uma execução do teste.
type execucao struct {
	cfg     Config
	topicos quiz.Topicos
	fases   []*faseAtiva
	atual   atomic.Int32 // índice da fase em curso (para os resumos de reações)
	pend    sync.WaitGroup
}

// Executar roda o teste completo e devolve o relatório.
func Executar(ctx context.Context, cfg Config) (*Relatorio, error) {
	if cfg.Jogadores <= 0 {
		return nil, errors.New("carga: precisa de pelo menos 1 jogador")
	}
	cfg.ConexoesParalelas = max(cfg.ConexoesParalelas, 1)
	e := &execucao{
		cfg:     cfg,
		topicos: quiz.TopicosDaSala(cfg.Sala),
		fases:   []*faseAtiva{novaFase(FaseSemBarulho), novaFase(FaseComBarulho)},
	}
	recursos := metrics.NewResources()
	pararRecursos := amostrar(recursos)
	defer pararRecursos()

	ccfg := client.DefaultConfig()
	ccfg.InboxSize = 64 // milhares de clientes: fila local pequena
	if cfg.Heartbeat != 0 {
		ccfg.HeartbeatInterval = cfg.Heartbeat
		ccfg.HeartbeatTimeout = 0 // 4x o intervalo
	}

	// Apresentador, apurador (recebe as respostas) e telão (recebe os resumos).
	host, err := client.Connect(cfg.Broker, ccfg)
	if err != nil {
		return nil, fmt.Errorf("apresentador: %w", err)
	}
	defer host.Disconnect()
	for _, t := range e.topicos.Todos() {
		host.CreateTopic(t) // "ja existe" numa segunda execução é esperado
	}
	defer func() {
		for _, t := range e.topicos.Todos() {
			host.RemoveTopic(t)
		}
	}()
	apurador, err := client.Connect(cfg.Broker, ccfg)
	if err != nil {
		return nil, fmt.Errorf("apurador: %w", err)
	}
	defer apurador.Disconnect()
	if err := apurador.Subscribe(e.topicos.Respostas, e.onResposta); err != nil {
		return nil, fmt.Errorf("apurador: %w", err)
	}
	telao, err := client.Connect(cfg.Broker, ccfg)
	if err != nil {
		return nil, fmt.Errorf("telao: %w", err)
	}
	defer telao.Disconnect()
	if err := telao.Subscribe(e.topicos.Reacoes, e.onResumo); err != nil {
		return nil, fmt.Errorf("telao: %w", err)
	}

	e.logf("Abrindo %d conexoes de jogadores e %d de barulhentos (%d em paralelo)...\n",
		cfg.Jogadores, cfg.Barulhentos, cfg.ConexoesParalelas)
	inicio := time.Now()
	jogadores, falhasJ := conectarVarios(ctx, cfg.Jogadores, cfg.ConexoesParalelas, func() (*client.Client, error) {
		return e.conectarJogador(ccfg)
	})
	barulhentos, falhasB := conectarVarios(ctx, cfg.Barulhentos, cfg.ConexoesParalelas, func() (*client.Client, error) {
		return client.Connect(cfg.Broker, ccfg)
	})
	tempoConexao := time.Since(inicio)
	defer desconectarVarios(append(jogadores, barulhentos...), cfg.ConexoesParalelas)
	if len(jogadores) == 0 {
		return nil, fmt.Errorf("carga: nenhum jogador conseguiu conectar em %s", cfg.Broker)
	}
	e.logf("%d conexoes abertas em %v (%d falhas)\n", len(jogadores)+len(barulhentos),
		tempoConexao.Round(time.Millisecond), falhasJ+falhasB)

	rel := &Relatorio{
		Config:        e.configTexto(),
		Conexoes:      len(jogadores) + len(barulhentos),
		FalhasConexao: falhasJ + falhasB,
		TempoConexao:  tempoConexao.Round(time.Millisecond).String(),
	}
	if s := tempoConexao.Seconds(); s > 0 {
		rel.ConexoesPorSeg = float64(rel.Conexoes) / s
	}

	for i, f := range e.fases {
		e.atual.Store(int32(i))
		antes, errAntes := e.metricasBroker()
		comBarulho := f.nome == FaseComBarulho

		parar := make(chan struct{})
		var spam sync.WaitGroup
		spam.Add(1)
		go func() {
			defer spam.Done()
			reagir(jogadores, cfg.ReacoesPorJogador, parar, &f.reacoesJogo, e.topicos.Reacoes)
		}()
		if comBarulho && len(barulhentos) > 0 && cfg.ReacoesPorBarulhento > 0 {
			spam.Add(1)
			go func() {
				defer spam.Done()
				reagir(barulhentos, cfg.ReacoesPorBarulhento, parar, &f.reacoesBarulho, e.topicos.Reacoes)
			}()
			e.logf("Fase %s: barulhentos aquecendo por %v...\n", f.nome, cfg.Aquecimento)
			if err := esperar(ctx, cfg.Aquecimento); err != nil {
				close(parar)
				return nil, err
			}
		}

		e.logf("Fase %s: %d perguntas a cada %v, respostas em rajada de %v\n",
			f.nome, cfg.PerguntasPorFase, cfg.IntervaloPergunta, cfg.JanelaRajada)
		for q := 1; q <= cfg.PerguntasPorFase; q++ {
			payload := strconv.Itoa(i) + "|" + strconv.Itoa(q) + "|" + strconv.FormatInt(metrics.Nanotime(), 10)
			if host.Publish(e.topicos.Perguntas, []byte(payload)) == nil {
				f.perguntasEnviadas.Add(1)
			}
			if err := esperar(ctx, cfg.IntervaloPergunta); err != nil {
				close(parar)
				return nil, err
			}
		}
		close(parar)
		spam.Wait()

		// Respostas ainda agendadas e o último resumo de reações (1 s).
		e.pend.Wait()
		if err := esperar(ctx, 1500*time.Millisecond); err != nil {
			return nil, err
		}

		res := e.resultado(f, int64(len(jogadores)))
		if errAntes == nil {
			if depois, err := e.metricasBroker(); err == nil {
				res.DescartesBroker = descartes(antes, depois, e.topicos)
			} else {
				rel.ErroBroker = err.Error()
			}
		} else if !errors.Is(errAntes, errSemMetricas) {
			rel.ErroBroker = errAntes.Error()
		}
		rel.Fases = append(rel.Fases, res)
	}

	for _, c := range append(append(jogadores, barulhentos...), host, apurador, telao) {
		rel.Cliente = somarStats(rel.Cliente, c.Stats())
	}
	recursos.Sample()
	rel.Gerador = recursos.Snapshot()
	return rel, nil
}

func (e *execucao) conectarJogador(ccfg client.Config) (*client.Client, error) {
	c, err := client.Connect(e.cfg.Broker, ccfg)
	if err != nil {
		return nil, err
	}
	if err := c.Subscribe(e.topicos.Perguntas, func(m client.Message) { e.onPergunta(c, m) }); err != nil {
		c.Disconnect()
		return nil, err
	}
	return c, nil
}

// onPergunta mede a latência da pergunta e agenda a resposta do jogador
// dentro da janela da rajada.
func (e *execucao) onPergunta(c *client.Client, m client.Message) {
	f, q, enviada, ok := lerCarimbo(m.Payload)
	if !ok || f >= len(e.fases) {
		return
	}
	fase := e.fases[f]
	fase.latPergunta.Observe(time.Duration(metrics.Nanotime() - enviada))
	fase.perguntasRecebidas.Add(1)

	atraso := time.Duration(0)
	if e.cfg.JanelaRajada > 0 {
		atraso = rand.N(e.cfg.JanelaRajada)
	}
	e.pend.Add(1)
	time.AfterFunc(atraso, func() {
		defer e.pend.Done()
		payload := strconv.Itoa(f) + "|" + strconv.Itoa(q) + "|" + strconv.FormatInt(metrics.Nanotime(), 10)
		if c.Publish(e.topicos.Respostas, []byte(payload)) == nil {
			fase.respostasEnviadas.Add(1)
		}
	})
}

func (e *execucao) onResposta(m client.Message) {
	f, q, enviada, ok := lerCarimbo(m.Payload)
	if !ok || f >= len(e.fases) {
		return
	}
	fase := e.fases[f]
	agora := metrics.Nanotime()
	fase.latResposta.Observe(time.Duration(agora - enviada))
	fase.respostasRecebidas.Add(1)

	fase.mu.Lock()
	r := fase.rajadas[q]
	if r == nil {
		r = &rajada{primeira: agora}
		fase.rajadas[q] = r
	}
	r.ultima = agora
	r.n++
	fase.mu.Unlock()
}

func (e *execucao) onResumo(m client.Message) {
	var s aggregator.Summary
	if err := json.Unmarshal(m.Payload, &s); err != nil {
		return
	}
	f := e.fases[e.atual.Load()]
	f.resumos.Add(1)
	f.reacoesContadas.Add(int64(s.Total))
}

func (e *execucao) resultado(f *faseAtiva, jogadores int64) Fase {
	res := Fase{
		Nome:               f.nome,
		PerguntasEnviadas:  f.perguntasEnviadas.Load(),
		PerguntasRecebidas: f.perguntasRecebidas.Load(),
		LatenciaPergunta:   f.latPergunta.Snapshot(),
		RespostasEnviadas:  f.respostasEnviadas.Load(),
		RespostasRecebidas: f.respostasRecebidas.Load(),
		LatenciaResposta:   f.latResposta.Snapshot(),
		ReacoesBarulhentos: f.reacoesBarulho.Load(),
		ReacoesJogadores:   f.reacoesJogo.Load(),
		ReacoesContadas:    f.reacoesContadas.Load(),
		Resumos:            f.resumos.Load(),
	}
	res.PerguntasEsperadas = res.PerguntasEnviadas * jogadores
	res.EntregaPerguntas = razao(res.PerguntasRecebidas, res.PerguntasEsperadas)
	res.EntregaRespostas = razao(res.RespostasRecebidas, res.RespostasEnviadas)

	f.mu.Lock()
	for _, r := range f.rajadas {
		// Rajadas com poucas respostas ou instantâneas não dizem nada da vazão.
		if d := time.Duration(r.ultima - r.primeira); r.n > 1 && d >= time.Millisecond {
			res.PicoRespostasPorSeg = max(res.PicoRespostasPorSeg, float64(r.n)/d.Seconds())
		}
	}
	f.mu.Unlock()
	return res
}

var errSemMetricas = errors.New("sem metricas do broker")

func (e *execucao) metricasBroker() (*metrics.Snapshot, error) {
	if e.cfg.MetricasBroker == nil {
		return nil, errSemMetricas
	}
	return e.cfg.MetricasBroker()
}

// descartes calcula os descartes de publicação de cada tópico da sala
// durante a fase (depois - antes).
func descartes(antes, depois *metrics.Snapshot, t quiz.Topicos) map[string]map[string]int64 {
	out := make(map[string]map[string]int64)
	for _, topico := range t.Todos() {
		d := depois.Topics[topico]
		a := antes.Topics[topico]
		for motivo, n := range d.PublishDrops {
			if delta := n - a.PublishDrops[motivo]; delta > 0 {
				if out[topico] == nil {
					out[topico] = make(map[string]int64)
				}
				out[topico][motivo] = delta
			}
		}
		for motivo, n := range d.DeliveryDrops {
			if delta := n - a.DeliveryDrops[motivo]; delta > 0 {
				if out[topico] == nil {
					out[topico] = make(map[string]int64)
				}
				out[topico]["entrega_"+motivo] = delta
			}
		}
	}
	return out
}

// conectarVarios abre n conexões com no máximo paralelas ao mesmo tempo.
func conectarVarios(ctx context.Context, n, paralelas int, conectar func() (*client.Client, error)) ([]*client.Client, int) {
	clientes := make([]*client.Client, n)
	sem := make(chan struct{}, paralelas)
	var wg sync.WaitGroup
	for i := 0; i < n && ctx.Err() == nil; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			if c, err := conectar(); err == nil {
				clientes[i] = c
			}
		}(i)
	}
	wg.Wait()

	ok := clientes[:0]
	for _, c := range clientes {
		if c != nil {
			ok = append(ok, c)
		}
	}
	return ok, n - len(ok)
}

func desconectarVarios(clientes []*client.Client, paralelas int) {
	sem := make(chan struct{}, paralelas)
	var wg sync.WaitGroup
	for _, c := range clientes {
		sem <- struct{}{}
		wg.Add(1)
		go func(c *client.Client) {
			defer func() { <-sem; wg.Done() }()
			c.Disconnect()
		}(c)
	}
	wg.Wait()
}

var emojis = [][]byte{[]byte("👏"), []byte("😂"), []byte("🔥"), []byte("😮"), []byte("❤️")}

// reagir faz cada cliente publicar reações na taxa dada (por segundo, por
// cliente) até parar ser fechado. Uma única goroutine atende todos os
// clientes, com um acumulador por cliente, para não criar milhares de tickers.
func reagir(clientes []*client.Client, porSegundo float64, parar <-chan struct{}, enviadas *atomic.Int64, topico string) {
	if porSegundo <= 0 || len(clientes) == 0 {
		return
	}
	const passo = 10 * time.Millisecond
	acumulado := make([]float64, len(clientes))
	for i := range acumulado {
		acumulado[i] = rand.Float64() // defasagem: nem todos reagem no mesmo tique
	}
	t := time.NewTicker(passo)
	defer t.Stop()
	ultimo := time.Now()
	for {
		select {
		case <-parar:
			return
		case agora := <-t.C:
			dt := agora.Sub(ultimo).Seconds()
			ultimo = agora
			for i, c := range clientes {
				acumulado[i] += porSegundo * dt
				for acumulado[i] >= 1 {
					acumulado[i]--
					if c.Publish(topico, emojis[rand.N(len(emojis))]) == nil {
						enviadas.Add(1)
					}
				}
			}
		}
	}
}

// lerCarimbo lê "fase|pergunta|nanotime".
func lerCarimbo(p []byte) (fase, pergunta int, enviada int64, ok bool) {
	partes := strings.Split(string(p), "|")
	if len(partes) != 3 {
		return 0, 0, 0, false
	}
	f, err1 := strconv.Atoi(partes[0])
	q, err2 := strconv.Atoi(partes[1])
	ns, err3 := strconv.ParseInt(partes[2], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || f < 0 {
		return 0, 0, 0, false
	}
	return f, q, ns, true
}

func (e *execucao) configTexto() map[string]string {
	c := e.cfg
	return map[string]string{
		"broker":                 c.Broker,
		"jogadores":              strconv.Itoa(c.Jogadores),
		"barulhentos":            strconv.Itoa(c.Barulhentos),
		"reacoes_por_barulhento": strconv.FormatFloat(c.ReacoesPorBarulhento, 'f', -1, 64),
		"reacoes_por_jogador":    strconv.FormatFloat(c.ReacoesPorJogador, 'f', -1, 64),
		"perguntas_por_fase":     strconv.Itoa(c.PerguntasPorFase),
		"intervalo_pergunta":     c.IntervaloPergunta.String(),
		"janela_rajada":          c.JanelaRajada.String(),
	}
}

func (e *execucao) logf(format string, args ...any) {
	if e.cfg.Log != nil {
		fmt.Fprintf(e.cfg.Log, format, args...)
	}
}

func amostrar(r *metrics.Resources) (parar func()) {
	fim := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-fim:
				return
			case <-t.C:
				r.Sample()
			}
		}
	}()
	return func() { close(fim) }
}

func esperar(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func razao(parte, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(parte) / float64(total)
}

func somarStats(a, b client.Stats) client.Stats {
	a.Published += b.Published
	a.PublishNacks += b.PublishNacks
	a.PublishDropped += b.PublishDropped
	a.Received += b.Received
	a.InboxDropped += b.InboxDropped
	a.Unroutable += b.Unroutable
	a.Retransmitted += b.Retransmitted
	a.Failovers += b.Failovers
	a.RestoreFailed += b.RestoreFailed
	return a
}
