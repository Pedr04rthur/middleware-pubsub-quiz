package client_test

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/broker"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

var (
	brokerOnce sync.Once
	brokerAddr string
)

// startBroker sobe o broker real uma vez por execução dos testes. Ele não é
// encerrado porque broker.Run não termina depois de Close.
func startBroker(t *testing.T) string {
	t.Helper()
	brokerOnce.Do(func() {
		l, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		addr := l.LocalAddr().String()
		l.Close()

		b, err := broker.New(addr)
		if err != nil {
			t.Fatal(err)
		}
		go b.Run()
		brokerAddr = addr
	})
	if brokerAddr == "" {
		t.Fatal("broker não iniciou")
	}
	return brokerAddr
}

func connect(t *testing.T, addr string, cfg client.Config) *client.Client {
	t.Helper()
	c, err := client.Connect(addr, cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { c.Disconnect() })
	return c
}

// Fluxo do quiz contra o broker real: apresentador cria o tópico, jogador se
// inscreve, apresentador publica, jogador recebe; após o jogador sair, a
// publicação é descartada e o apresentador é avisado.
func TestEndToEndWithRealBroker(t *testing.T) {
	addr := startBroker(t)
	const topic = "quiz/e2e/perguntas"

	nacks := make(chan string, 4)
	cfgApresentador := client.DefaultConfig()
	cfgApresentador.OnPublishNack = func(topic, reason string) { nacks <- topic }
	apresentador := connect(t, addr, cfgApresentador)
	jogador, err := client.Connect(addr, client.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	if err := apresentador.CreateTopic(topic); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	var nack *client.NackError
	if err := apresentador.CreateTopic(topic); !errors.As(err, &nack) {
		t.Fatalf("CreateTopic duplicado deveria dar Nack, veio %v", err)
	}

	got := make(chan client.Message, 1)
	if err := jogador.Subscribe(topic, func(m client.Message) { got <- m }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := apresentador.Publish(topic, []byte("Qual a capital da Paraiba?")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case m := <-got:
		if string(m.Payload) != "Qual a capital da Paraiba?" {
			t.Fatalf("payload = %q", m.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("jogador não recebeu a pergunta")
	}

	if err := jogador.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := apresentador.Publish(topic, []byte("ninguem ouvindo")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case tp := <-nacks:
		if tp != topic {
			t.Fatalf("Nack para tópico %q", tp)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("apresentador não foi avisado do descarte")
	}
}

func TestSubscribeToMissingTopicWithRealBroker(t *testing.T) {
	addr := startBroker(t)
	c := connect(t, addr, client.DefaultConfig())

	err := c.Subscribe("quiz/nao-existe/perguntas", func(client.Message) {})
	var nack *client.NackError
	if !errors.As(err, &nack) {
		t.Fatalf("erro = %v, quer *NackError", err)
	}
}
