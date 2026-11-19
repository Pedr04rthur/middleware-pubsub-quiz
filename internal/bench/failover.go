package bench

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

// ConfigFailover define o cenário de queda do broker primário.
type ConfigFailover struct {
	Nome      string
	Jogadores int
	// IntervaloPergunta é o ritmo das publicações; quanto menor, mais fina a
	// medição da interrupção. Deve ficar abaixo do rate limit por cliente do
	// broker (10 msg/s), senão o próprio apresentador é limitado.
	IntervaloPergunta time.Duration
	Antes, Depois     time.Duration // tempo antes e depois da queda
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	Paralelas         int
}

// DefaultFailover são as duas configurações comparadas: o heartbeat padrão da
// SDK e um mais agressivo.
func DefaultFailover() []ConfigFailover {
	base := ConfigFailover{
		Jogadores:         1000,
		IntervaloPergunta: 125 * time.Millisecond,
		Antes:             3 * time.Second,
		Depois:            5 * time.Second,
		Paralelas:         100,
	}
	padrao, rapido := base, base
	padrao.Nome, padrao.HeartbeatInterval, padrao.HeartbeatTimeout = "padrao", 250*time.Millisecond, time.Second
	rapido.Nome, rapido.HeartbeatInterval, rapido.HeartbeatTimeout = "rapido", 100*time.Millisecond, 400*time.Millisecond
	return []ConfigFailover{padrao, rapido}
}

// ResultadoFailover descreve o que os clientes sentiram com a queda.
type ResultadoFailover struct {
	Nome              string `json:"nome"`
	Jogadores         int    `json:"jogadores"`
	HeartbeatInterval string `json:"heartbeat_intervalo"`
	HeartbeatTimeout  string `json:"heartbeat_timeout"`

	// Migraram é quantos clientes (jogadores e apresentador) trocaram de broker.
	Migraram int `json:"migraram"`
	// SemRecuperar é quantos jogadores não receberam nada depois da queda.
	SemRecuperar int `json:"sem_recuperar"`

	// Interrupcao: para cada jogador, do último datagrama antes da queda ao
	// primeiro depois dela (o "buraco" que o jogador viu).
	Interrupcao metrics.LatencySnapshot `json:"interrupcao"`
	// Deteccao + restauração medidas pela SDK (FailoverEvent).
	Downtime metrics.LatencySnapshot `json:"downtime_sdk"`
	Recovery metrics.LatencySnapshot `json:"restauracao_sdk"`

	PerguntasPublicadas int64   `json:"perguntas_publicadas"`
	PerguntasEsperadas  int64   `json:"perguntas_esperadas"`
	PerguntasRecebidas  int64   `json:"perguntas_recebidas"`
	Perda               float64 `json:"perda"`
	// PerdidasPorJogador é a média de perguntas que cada jogador deixou de
	// receber; EsperadoPelaInterrupcao é quantas caberiam na interrupção p50.
	PerdidasPorJogador      float64 `json:"perdidas_por_jogador"`
	EsperadoPelaInterrupcao float64 `json:"esperado_pela_interrupcao"`
	IntervaloPergunta       string  `json:"intervalo_pergunta"`
	// PerdidasNoApresentador são publicações descartadas pela SDK do
	// apresentador enquanto ele mesmo fazia failover.
	PerdidasNoApresentador uint64 `json:"perdidas_no_apresentador"`

	LatenciaAntes  metrics.LatencySnapshot `json:"latencia_antes"`
	LatenciaDepois metrics.LatencySnapshot `json:"latencia_depois"`
}

type jogadorFailover struct {
	ultimoAntes, primeiroDepois atomic.Int64
}

// Failover derruba o broker primário no meio de uma partida e mede a
// interrupção e as perdas.
func Failover(ctx context.Context, cfg ConfigFailover, log io.Writer) (*ResultadoFailover, error) {
	primario, pararPrimario, err := subirBroker()
	if err != nil {
		return nil, err
	}
	defer pararPrimario()
	reserva, pararReserva, err := subirBroker()
	if err != nil {
		return nil, err
	}
	defer pararReserva()

	res := &ResultadoFailover{
		Nome:              cfg.Nome,
		Jogadores:         cfg.Jogadores,
		HeartbeatInterval: cfg.HeartbeatInterval.String(),
		HeartbeatTimeout:  cfg.HeartbeatTimeout.String(),
	}
	latDowntime := metrics.NewLatency(metrics.DefaultLatencySamples)
	latRecovery := metrics.NewLatency(metrics.DefaultLatencySamples)
	latAntes := metrics.NewLatency(metrics.DefaultLatencySamples)
	latDepois := metrics.NewLatency(metrics.DefaultLatencySamples)
	var migraram atomic.Int64
	var queda atomic.Int64 // Nanotime da queda; 0 antes dela
	var recebidas atomic.Int64

	ccfg := client.DefaultConfig()
	ccfg.InboxSize = 64
	ccfg.Fallbacks = []string{reserva.Addr()}
	ccfg.HeartbeatInterval = cfg.HeartbeatInterval
	ccfg.HeartbeatTimeout = cfg.HeartbeatTimeout
	ccfg.OnFailover = func(ev client.FailoverEvent) {
		migraram.Add(1)
		latDowntime.Observe(ev.Downtime)
		latRecovery.Observe(ev.Recovery)
	}

	const topico = "quiz/failover/perguntas"
	host, err := client.Connect(primario.Addr(), ccfg)
	if err != nil {
		return nil, fmt.Errorf("apresentador: %w", err)
	}
	defer host.Disconnect()
	if err := host.CreateTopic(topico); err != nil {
		return nil, err
	}

	estado := make([]*jogadorFailover, cfg.Jogadores)
	logf(log, "[failover/%s] conectando %d jogadores...\n", cfg.Nome, cfg.Jogadores)
	jogadores, falhas, _ := conectarVarios(ctx, cfg.Jogadores, cfg.Paralelas, primario.Addr(), ccfg,
		func(i int, c *client.Client) error {
			j := &jogadorFailover{}
			estado[i] = j
			return c.Subscribe(topico, func(m client.Message) {
				agora := metrics.Nanotime()
				recebidas.Add(1)
				_, enviado, ok := lerCarimbo(m.Payload)
				q := queda.Load()
				if q == 0 || agora < q {
					j.ultimoAntes.Store(agora)
				} else {
					j.primeiroDepois.CompareAndSwap(0, agora)
				}
				if ok {
					if q == 0 || enviado < q {
						latAntes.Observe(time.Duration(agora - enviado))
					} else {
						latDepois.Observe(time.Duration(agora - enviado))
					}
				}
			})
		})
	defer fecharVarios(jogadores)
	if falhas > 0 {
		return nil, fmt.Errorf("%d jogadores nao conectaram", falhas)
	}

	// Publica perguntas num ritmo fixo durante todo o teste.
	var seq atomic.Int64
	pararPublicacao := make(chan struct{})
	publicacaoParou := make(chan struct{})
	go func() {
		defer close(publicacaoParou)
		t := time.NewTicker(cfg.IntervaloPergunta)
		defer t.Stop()
		for {
			select {
			case <-pararPublicacao:
				return
			case <-t.C:
				host.Publish(topico, carimbo(int(seq.Add(1))))
			}
		}
	}()

	logf(log, "[failover/%s] partida rodando; derrubando o broker primario em %v...\n", cfg.Nome, cfg.Antes)
	if err := esperar(ctx, cfg.Antes); err != nil {
		return nil, err
	}
	queda.Store(metrics.Nanotime())
	pararPrimario()
	logf(log, "[failover/%s] broker primario derrubado; observando por %v...\n", cfg.Nome, cfg.Depois)
	if err := esperar(ctx, cfg.Depois); err != nil {
		return nil, err
	}
	close(pararPublicacao)
	<-publicacaoParou
	esperar(ctx, 500*time.Millisecond) // mensagens em trânsito

	interrupcao := metrics.NewLatency(metrics.DefaultLatencySamples)
	for _, j := range estado {
		if j == nil {
			continue
		}
		depois := j.primeiroDepois.Load()
		if depois == 0 {
			res.SemRecuperar++
			continue
		}
		interrupcao.Observe(time.Duration(depois - j.ultimoAntes.Load()))
	}

	hs := host.Stats()
	res.Migraram = int(migraram.Load())
	res.Interrupcao = interrupcao.Snapshot()
	res.Downtime = latDowntime.Snapshot()
	res.Recovery = latRecovery.Snapshot()
	res.PerguntasPublicadas = seq.Load()
	res.PerguntasEsperadas = res.PerguntasPublicadas * int64(len(jogadores))
	res.PerguntasRecebidas = recebidas.Load()
	if res.PerguntasEsperadas > 0 {
		res.Perda = 1 - float64(res.PerguntasRecebidas)/float64(res.PerguntasEsperadas)
		res.PerdidasPorJogador = float64(res.PerguntasEsperadas-res.PerguntasRecebidas) / float64(len(jogadores))
	}
	res.IntervaloPergunta = cfg.IntervaloPergunta.String()
	res.EsperadoPelaInterrupcao = res.Interrupcao.P50Ms / float64(cfg.IntervaloPergunta.Milliseconds())
	res.PerdidasNoApresentador = hs.PublishDropped
	res.LatenciaAntes = latAntes.Snapshot()
	res.LatenciaDepois = latDepois.Snapshot()
	return res, nil
}

// TabelaFailover formata os resultados em Markdown.
func TabelaFailover(rs []*ResultadoFailover) string {
	var b strings.Builder
	b.WriteString("| Heartbeat (intervalo / timeout) | Clientes migrados | Interrupção p50 | p95 | máx | Restauração na reserva (p95) | Perguntas perdidas por jogador | p99 antes | p99 depois |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rs {
		fmt.Fprintf(&b, "| %s / %s | %d de %d | %.0f ms | %.0f ms | %.0f ms | %.1f ms | %.1f de %d (%.1f cabem na interrupção) | %.1f ms | %.1f ms |\n",
			r.HeartbeatInterval, r.HeartbeatTimeout, r.Migraram, r.Jogadores+1,
			r.Interrupcao.P50Ms, r.Interrupcao.P95Ms, r.Interrupcao.MaxMs, r.Recovery.P95Ms,
			r.PerdidasPorJogador, r.PerguntasPublicadas, r.EsperadoPelaInterrupcao,
			r.LatenciaAntes.P99Ms, r.LatenciaDepois.P99Ms)
	}
	return b.String()
}
