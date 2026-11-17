// Comando jogador demonstra a biblioteca cliente (pkg/client) contra o broker:
// cria um tópico, se inscreve e publica uma pergunta a cada intervalo,
// recebendo as próprias mensagens. Derrube o broker primário no meio da
// execução para ver o failover para a instância reserva.
//
//	go run ./cmd/broker -addr :8080
//	go run ./cmd/broker -addr :8081
//	go run ./cmd/jogador -fallback localhost:8081 -duracao 10s
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:8080", "endereco UDP do broker primario")
	fallbacks := flag.String("fallback", "", "instancias reserva separadas por virgula (ex.: localhost:8081)")
	sala := flag.String("sala", "sala1", "nome da sala do quiz")
	duracao := flag.Duration("duracao", 3*time.Second, "quanto tempo ficar publicando")
	intervalo := flag.Duration("intervalo", 500*time.Millisecond, "intervalo entre publicacoes")
	flag.Parse()

	topico := fmt.Sprintf("quiz/%s/perguntas", *sala)

	cfg := client.DefaultConfig()
	if *fallbacks != "" {
		cfg.Fallbacks = strings.Split(*fallbacks, ",")
	}
	cfg.OnPublishNack = func(topico, motivo string) {
		fmt.Printf("[AVISO] publicacao em %s descartada: %s\n", topico, motivo)
	}
	cfg.OnFailover = func(ev client.FailoverEvent) {
		fmt.Printf("[FAILOVER] %s -> %s | sem broker por %v, recuperado em %v | inscricoes refeitas: %d\n",
			ev.From, ev.To, ev.Downtime.Round(time.Millisecond), ev.Recovery.Round(time.Microsecond), ev.Restored)
	}

	c, err := client.Connect(*brokerAddr, cfg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("conectado em %s como %s\n", c.BrokerAddr(), c.LocalAddr())

	if err := c.CreateTopic(topico); err != nil {
		fmt.Printf("CreateTopic: %v\n", err) // ex.: topico ja existe
	}
	err = c.Subscribe(topico, func(m client.Message) {
		fmt.Printf("  [RECEBIDO] %s: %s\n", m.Topic, m.Payload)
	})
	if err != nil {
		log.Fatal(err)
	}

	fim := time.Now().Add(*duracao)
	for i := 1; time.Now().Before(fim); i++ {
		if err := c.Publish(topico, []byte(fmt.Sprintf("pergunta %d", i))); err != nil {
			log.Fatal(err)
		}
		time.Sleep(*intervalo)
	}

	if err := c.Disconnect(); err != nil {
		fmt.Printf("Disconnect: %v\n", err)
	}
	fmt.Printf("stats: %+v\n", c.Stats())
}
