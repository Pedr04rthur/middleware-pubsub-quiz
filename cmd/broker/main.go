package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/broker"
)

func main() {
	addr := flag.String("addr", ":8080", "endereco UDP do broker (ex.: :8081 para a instancia reserva)")
	metricasAddr := flag.String("metricas", "", "endereco HTTP para expor as metricas em /metrics (ex.: :9100); vazio desativa")
	silencioso := flag.Bool("silencioso", false, "nao imprime uma linha por mensagem (use nos testes de estresse)")
	flag.Parse()

	b, err := broker.New(*addr)
	if err != nil {
		log.Fatal("Erro ao iniciar broker:", err)
	}
	b.SetQuiet(*silencioso)

	if *metricasAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", b.Metrics().Handler())
		go func() {
			if err := http.ListenAndServe(*metricasAddr, mux); err != nil {
				log.Println("Erro no servidor de metricas:", err)
			}
		}()
		fmt.Printf("Metricas HTTP em %s (rota /metrics)\n", *metricasAddr)
	}

	sinal := make(chan os.Signal, 1)
	signal.Notify(sinal, os.Interrupt)
	go func() {
		<-sinal
		fmt.Println("\n=== Metricas do broker ===")
		b.Metrics().Snapshot().WriteText(os.Stdout)
		b.Close()
	}()

	b.Run()
}
