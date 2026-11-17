package main

import (
	"flag"
	"log"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/broker"
)

func main() {
	addr := flag.String("addr", ":8080", "endereco UDP do broker (ex.: :8081 para a instancia reserva)")
	flag.Parse()

	b, err := broker.New(*addr)
	if err != nil {
		log.Fatal("Erro ao iniciar broker:", err)
	}
	defer b.Close()

	b.Run()
}
