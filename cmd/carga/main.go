// Comando carga abre milhares de conexões com o broker, simula rajadas de
// respostas e um grupo de clientes barulhentos mandando spam de reações, e
// compara a latência das perguntas com e sem o barulho.
//
//	go run ./cmd/broker -silencioso -metricas :9100
//	go run ./cmd/carga -jogadores 2000 -barulhentos 100 -metricas http://localhost:9100/metrics
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/carga"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
)

func main() {
	cfg := carga.DefaultConfig()
	flag.StringVar(&cfg.Broker, "broker", cfg.Broker, "endereco UDP do broker")
	flag.StringVar(&cfg.Sala, "sala", cfg.Sala, "sala do quiz usada no teste")
	flag.IntVar(&cfg.Jogadores, "jogadores", cfg.Jogadores, "jogadores normais (recebem perguntas e respondem em rajada)")
	flag.IntVar(&cfg.Barulhentos, "barulhentos", cfg.Barulhentos, "clientes que mandam spam de reacoes na segunda fase")
	flag.Float64Var(&cfg.ReacoesPorBarulhento, "spam", cfg.ReacoesPorBarulhento, "reacoes por segundo de cada barulhento")
	flag.Float64Var(&cfg.ReacoesPorJogador, "reacoes", cfg.ReacoesPorJogador, "reacoes por segundo de cada jogador normal (0 desliga)")
	flag.IntVar(&cfg.PerguntasPorFase, "perguntas", cfg.PerguntasPorFase, "perguntas em cada fase")
	flag.DurationVar(&cfg.IntervaloPergunta, "intervalo", cfg.IntervaloPergunta, "intervalo entre perguntas")
	flag.DurationVar(&cfg.JanelaRajada, "rajada", cfg.JanelaRajada, "janela em que todos os jogadores respondem")
	flag.DurationVar(&cfg.Aquecimento, "aquecimento", cfg.Aquecimento, "spam antes da primeira pergunta da fase com barulho")
	flag.IntVar(&cfg.ConexoesParalelas, "paralelas", cfg.ConexoesParalelas, "conexoes abertas ao mesmo tempo")
	flag.DurationVar(&cfg.Heartbeat, "heartbeat", cfg.Heartbeat, "intervalo de heartbeat de cada cliente (0 = padrao da SDK, 250ms)")
	metricasURL := flag.String("metricas", "", "URL das metricas do broker (ex.: http://localhost:9100/metrics)")
	saida := flag.String("saida", "", "arquivo para salvar o relatorio em JSON")
	flag.Parse()

	if *metricasURL != "" {
		url := *metricasURL
		cfg.MetricasBroker = func() (*metrics.Snapshot, error) { return lerMetricas(url) }
	}
	cfg.Log = os.Stdout

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	rel, err := carga.Executar(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	rel.Escrever(os.Stdout)

	if *saida != "" {
		dados, _ := json.MarshalIndent(rel, "", "  ")
		if err := os.WriteFile(*saida, dados, 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Relatorio JSON salvo em", *saida)
	}
}

func lerMetricas(url string) (*metrics.Snapshot, error) {
	c := http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %s", resp.Status)
	}
	var s metrics.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}
