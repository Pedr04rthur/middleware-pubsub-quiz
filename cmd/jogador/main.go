// Comando jogador demonstra a biblioteca cliente (pkg/client) contra o broker:
// cria um tópico, se inscreve, publica, recebe e desconecta.
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:8080", "endereco UDP do broker")
	sala := flag.String("sala", "sala1", "nome da sala do quiz")
	flag.Parse()

	topico := fmt.Sprintf("quiz/%s/perguntas", *sala)

	cfg := client.DefaultConfig()
	cfg.OnPublishNack = func(topico, motivo string) {
		fmt.Printf("[AVISO] publicacao em %s descartada: %s\n", topico, motivo)
	}

	fmt.Println("1. Conectar")
	c, err := client.Connect(*brokerAddr, cfg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   conectado como %s\n", c.LocalAddr())

	fmt.Println("2. Criar topico")
	if err := c.CreateTopic(topico); err != nil {
		fmt.Printf("   %v\n", err) // ex.: topico ja existe
	}

	fmt.Println("3. Se inscrever")
	recebida := make(chan struct{})
	err = c.Subscribe(topico, func(m client.Message) {
		fmt.Printf("   [RECEBIDO] %s: %s\n", m.Topic, m.Payload)
		close(recebida)
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("4. Publicar (deve entregar para mim mesmo)")
	if err := c.Publish(topico, []byte("Qual a capital da Paraiba?")); err != nil {
		log.Fatal(err)
	}
	select {
	case <-recebida:
	case <-time.After(2 * time.Second):
		fmt.Println("   mensagem nao chegou (perda e aceitavel no at-most-once)")
	}

	fmt.Println("5. Desconectar")
	if err := c.Disconnect(); err != nil {
		fmt.Printf("   %v\n", err)
	}
	fmt.Printf("   stats: %+v\n", c.Stats())
}
