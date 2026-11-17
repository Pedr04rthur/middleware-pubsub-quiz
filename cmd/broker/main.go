package main

import (
	"log"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/broker"
)

func main() {
	b, err := broker.New(":8080")
	if err != nil {
		log.Fatal("Erro ao iniciar broker:", err)
	}
	defer b.Close()

	b.Run()
}
