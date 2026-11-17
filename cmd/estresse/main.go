// Comando estresse simula uma partida de quiz com muitos jogadores contra um
// broker e mede latência (p50/p95/p99), vazão de respostas, perdas por tópico
// e consumo de recursos. Rode o broker com as métricas ligadas e sem log por
// mensagem para que o relatório inclua também o lado do broker:
//
//	go run ./cmd/broker -silencioso -metricas :9100
//	go run ./cmd/estresse -jogadores 100 -duracao 30s -metricas http://localhost:9100/metrics
//
// As latências de ponta a ponta são medidas dentro deste processo (quem
// publica e quem recebe são clientes daqui), então não dependem de relógios
// sincronizados entre máquinas.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

type config struct {
	broker           string
	metricsURL       string
	room             string
	players          int
	duration         time.Duration
	questionInterval time.Duration
	reactionsPerSec  float64
	maxAnswerDelay   time.Duration
	output           string
}

// Nomes das métricas medidas pelos clientes.
const (
	latQuestion = "fim_a_fim_pergunta"
	latScore    = "fim_a_fim_placar"
	latAnswer   = "fim_a_fim_resposta"

	meterAnswersReceived = "respostas_recebidas"
	meterAnswersSent     = "respostas_enviadas"
	meterReactionsSent   = "reacoes_enviadas"
)

// topicTally compara o que foi enviado com o que chegou em um tópico.
type topicTally struct {
	Sent     int64   `json:"enviadas"`
	Expected int64   `json:"esperadas"`
	Received int64   `json:"recebidas"`
	LossRate float64 `json:"taxa_perda"`
}

type report struct {
	Config      map[string]string     `json:"configuracao"`
	Clients     metrics.Snapshot      `json:"clientes"`
	Topics      map[string]topicTally `json:"entrega_por_topico"`
	ClientStats client.Stats          `json:"biblioteca_cliente"`
	Broker      *metrics.Snapshot     `json:"broker,omitempty"`
	BrokerError string                `json:"erro_broker,omitempty"`
}

type run struct {
	cfg config
	reg *metrics.Registry

	topicQuestions, topicScore, topicAnswers, topicReactions string

	questionsSent, scoresSent          atomic.Int64
	questionsReceived, scoresReceived  atomic.Int64
	answersSent, answersReceived       atomic.Int64
	reactionsSent, reactionsSummarized atomic.Int64
	summariesReceived                  atomic.Int64
	answering                          sync.WaitGroup
}

func main() {
	var cfg config
	flag.StringVar(&cfg.broker, "broker", "localhost:8080", "endereco UDP do broker")
	flag.StringVar(&cfg.metricsURL, "metricas", "", "URL das metricas do broker (ex.: http://localhost:9100/metrics)")
	flag.StringVar(&cfg.room, "sala", "estresse", "sala do quiz usada no teste")
	flag.IntVar(&cfg.players, "jogadores", 50, "quantidade de jogadores simulados")
	flag.DurationVar(&cfg.duration, "duracao", 20*time.Second, "duracao do teste")
	flag.DurationVar(&cfg.questionInterval, "intervalo-pergunta", 2*time.Second, "intervalo entre perguntas")
	flag.Float64Var(&cfg.reactionsPerSec, "reacoes", 3, "reacoes por segundo de cada jogador (0 desliga)")
	flag.DurationVar(&cfg.maxAnswerDelay, "atraso-resposta", time.Second, "atraso maximo (aleatorio) de cada jogador para responder")
	flag.StringVar(&cfg.output, "saida", "", "arquivo para salvar o relatorio em JSON")
	flag.Parse()

	r := &run{
		cfg:            cfg,
		reg:            metrics.NewRegistry(),
		topicQuestions: "quiz/" + cfg.room + "/perguntas",
		topicScore:     "quiz/" + cfg.room + "/placar",
		topicAnswers:   "quiz/" + cfg.room + "/respostas",
		topicReactions: "quiz/" + cfg.room + "/reacoes",
	}
	rep, err := r.execute()
	if err != nil {
		log.Fatal(err)
	}
	r.print(rep)
	if cfg.output != "" {
		data, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(cfg.output, data, 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Relatorio JSON salvo em", cfg.output)
	}
}

func (r *run) execute() (*report, error) {
	cfg := r.cfg
	stopMetrics := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		last := time.Now()
		for {
			select {
			case <-stopMetrics:
				return
			case now := <-t.C:
				r.reg.Tick(now.Sub(last))
				last = now
			}
		}
	}()
	defer close(stopMetrics)

	ccfg := client.DefaultConfig()
	host, err := client.Connect(cfg.broker, ccfg)
	if err != nil {
		return nil, fmt.Errorf("apresentador: %w", err)
	}
	defer host.Disconnect()
	for _, t := range []string{r.topicQuestions, r.topicScore, r.topicAnswers, r.topicReactions} {
		host.CreateTopic(t) // "topico ja existe" numa segunda rodada é esperado
	}

	scorer, err := client.Connect(cfg.broker, ccfg)
	if err != nil {
		return nil, fmt.Errorf("apurador: %w", err)
	}
	defer scorer.Disconnect()
	if err := scorer.Subscribe(r.topicAnswers, r.onAnswer); err != nil {
		return nil, fmt.Errorf("apurador: %w", err)
	}

	screen, err := client.Connect(cfg.broker, ccfg)
	if err != nil {
		return nil, fmt.Errorf("telao: %w", err)
	}
	defer screen.Disconnect()
	if err := screen.Subscribe(r.topicReactions, r.onSummary); err != nil {
		return nil, fmt.Errorf("telao: %w", err)
	}

	fmt.Printf("Conectando %d jogadores em %s...\n", cfg.players, cfg.broker)
	players := make([]*client.Client, 0, cfg.players)
	for i := 0; i < cfg.players; i++ {
		p, err := r.connectPlayer(i)
		if err != nil {
			return nil, fmt.Errorf("jogador %d: %w", i, err)
		}
		players = append(players, p)
	}
	defer func() {
		for _, p := range players {
			p.Disconnect()
		}
	}()

	fmt.Printf("Teste rodando por %v...\n", cfg.duration)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	if cfg.reactionsPerSec > 0 {
		for _, p := range players {
			wg.Add(1)
			go func(p *client.Client) {
				defer wg.Done()
				r.react(p, stop)
			}(p)
		}
	}

	end := time.Now().Add(cfg.duration)
	for q := 1; time.Now().Before(end); q++ {
		now := strconv.FormatInt(metrics.Nanotime(), 10)
		if host.Publish(r.topicQuestions, []byte(strconv.Itoa(q)+"|"+now)) == nil {
			r.questionsSent.Add(1)
		}
		if host.Publish(r.topicScore, []byte(strconv.Itoa(q)+"|"+now)) == nil {
			r.scoresSent.Add(1)
		}
		time.Sleep(min(cfg.questionInterval, time.Until(end)))
	}
	close(stop)
	wg.Wait()

	fmt.Println("Aguardando respostas e resumos em transito...")
	r.answering.Wait()
	time.Sleep(2 * time.Second)

	rep := &report{
		Config: map[string]string{
			"broker":             cfg.broker,
			"jogadores":          strconv.Itoa(cfg.players),
			"duracao":            cfg.duration.String(),
			"intervalo_pergunta": cfg.questionInterval.String(),
			"reacoes_por_s":      strconv.FormatFloat(cfg.reactionsPerSec, 'f', -1, 64),
			"atraso_resposta":    cfg.maxAnswerDelay.String(),
		},
		Clients: r.reg.Snapshot(),
		Topics:  r.tallies(),
	}
	for _, c := range append(players, host, scorer, screen) {
		rep.ClientStats = addStats(rep.ClientStats, c.Stats())
	}
	if cfg.metricsURL != "" {
		snap, err := fetchBrokerMetrics(cfg.metricsURL)
		if err != nil {
			rep.BrokerError = err.Error()
		} else {
			rep.Broker = snap
		}
	}
	return rep, nil
}

func (r *run) connectPlayer(i int) (*client.Client, error) {
	ccfg := client.DefaultConfig()
	p, err := client.Connect(r.cfg.broker, ccfg)
	if err != nil {
		return nil, err
	}
	id := strconv.Itoa(i)
	if err := p.Subscribe(r.topicQuestions, func(m client.Message) { r.onQuestion(p, id, m) }); err != nil {
		return nil, err
	}
	if err := p.Subscribe(r.topicScore, r.onScore); err != nil {
		return nil, err
	}
	return p, nil
}

// onQuestion mede a latência da pergunta e agenda a resposta do jogador.
func (r *run) onQuestion(p *client.Client, id string, m client.Message) {
	q, sent, ok := parseStamp(m.Payload)
	if !ok {
		return
	}
	r.reg.Latency(latQuestion).Observe(metrics.Since(sent))
	r.questionsReceived.Add(1)

	delay := time.Duration(0)
	if r.cfg.maxAnswerDelay > 0 {
		delay = rand.N(r.cfg.maxAnswerDelay)
	}
	r.answering.Add(1)
	time.AfterFunc(delay, func() {
		defer r.answering.Done()
		payload := q + "|" + strconv.FormatInt(metrics.Nanotime(), 10) + "|" + id
		if p.Publish(r.topicAnswers, []byte(payload)) == nil {
			r.answersSent.Add(1)
			r.reg.Meter(meterAnswersSent).Mark(1)
		}
	})
}

func (r *run) onScore(m client.Message) {
	if _, sent, ok := parseStamp(m.Payload); ok {
		r.reg.Latency(latScore).Observe(metrics.Since(sent))
		r.scoresReceived.Add(1)
	}
}

func (r *run) onAnswer(m client.Message) {
	if _, sent, ok := parseStamp(m.Payload); ok {
		r.reg.Latency(latAnswer).Observe(metrics.Since(sent))
		r.answersReceived.Add(1)
		r.reg.Meter(meterAnswersReceived).Mark(1)
	}
}

func (r *run) onSummary(m client.Message) {
	var s aggregator.Summary
	if json.Unmarshal(m.Payload, &s) == nil {
		r.summariesReceived.Add(1)
		r.reactionsSummarized.Add(int64(s.Total))
	}
}

var emojis = [][]byte{[]byte("👏"), []byte("😂"), []byte("🔥"), []byte("😮"), []byte("❤️")}

func (r *run) react(p *client.Client, stop <-chan struct{}) {
	interval := time.Duration(float64(time.Second) / r.cfg.reactionsPerSec)
	// Defasagem aleatória para os jogadores não reagirem todos no mesmo instante.
	time.Sleep(rand.N(interval))
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if p.Publish(r.topicReactions, emojis[rand.N(len(emojis))]) == nil {
				r.reactionsSent.Add(1)
				r.reg.Meter(meterReactionsSent).Mark(1)
			}
		}
	}
}

// parseStamp lê "id|nanotime[|...]".
func parseStamp(p []byte) (id string, sent int64, ok bool) {
	parts := strings.Split(string(p), "|")
	if len(parts) < 2 {
		return "", 0, false
	}
	sent, err := strconv.ParseInt(parts[1], 10, 64)
	return parts[0], sent, err == nil
}

func (r *run) tallies() map[string]topicTally {
	n := int64(r.cfg.players)
	t := map[string]topicTally{
		r.topicQuestions: tally(r.questionsSent.Load(), r.questionsSent.Load()*n, r.questionsReceived.Load()),
		r.topicScore:     tally(r.scoresSent.Load(), r.scoresSent.Load()*n, r.scoresReceived.Load()),
		r.topicAnswers:   tally(r.answersSent.Load(), r.answersSent.Load(), r.answersReceived.Load()),
		r.topicReactions: tally(r.reactionsSent.Load(), r.reactionsSent.Load(), r.reactionsSummarized.Load()),
	}
	return t
}

func tally(sent, expected, received int64) topicTally {
	t := topicTally{Sent: sent, Expected: expected, Received: received}
	if expected > 0 {
		t.LossRate = max(0, float64(expected-received)/float64(expected))
	}
	return t
}

func addStats(a, b client.Stats) client.Stats {
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

func fetchBrokerMetrics(url string) (*metrics.Snapshot, error) {
	c := http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metricas do broker: HTTP %s", resp.Status)
	}
	var s metrics.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (r *run) print(rep *report) {
	fmt.Println()
	fmt.Println("================ RELATORIO DO TESTE DE ESTRESSE ================")
	for _, k := range []string{"broker", "jogadores", "duracao", "intervalo_pergunta", "reacoes_por_s", "atraso_resposta"} {
		fmt.Printf("%-20s %s\n", k, rep.Config[k])
	}

	fmt.Println("\n---------------- Clientes (medido de ponta a ponta) ----------------")
	rep.Clients.WriteText(os.Stdout)

	fmt.Println("\nENTREGA POR TOPICO (lado do cliente)")
	fmt.Printf("%-28s %10s %10s %10s %8s\n", "topico", "enviadas", "esperadas", "recebidas", "perda")
	for _, k := range []string{r.topicQuestions, r.topicScore, r.topicAnswers, r.topicReactions} {
		t := rep.Topics[k]
		fmt.Printf("%-28s %10d %10d %10d %7.2f%%\n", k, t.Sent, t.Expected, t.Received, 100*t.LossRate)
	}
	fmt.Printf("(reacoes: \"recebidas\" e a soma dos totais dos %d resumos que chegaram ao telao)\n", r.summariesReceived.Load())
	s := rep.ClientStats
	fmt.Printf("biblioteca cliente: publicadas=%d nacks=%d perdidas_sem_broker=%d fila_local_cheia=%d reenvios=%d failovers=%d\n",
		s.Published, s.PublishNacks, s.PublishDropped, s.InboxDropped, s.Retransmitted, s.Failovers)

	fmt.Println("\n---------------- Broker ----------------")
	switch {
	case rep.Broker != nil:
		rep.Broker.WriteText(os.Stdout)
	case rep.BrokerError != "":
		fmt.Println("nao foi possivel ler as metricas do broker:", rep.BrokerError)
	default:
		fmt.Println("(use -metricas para incluir as metricas do broker)")
	}
}
