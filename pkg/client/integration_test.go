package client_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/broker"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

// newBroker sobe uma instância real do broker em uma porta livre.
func newBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b, err := broker.New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		b.Run()
		close(done)
	}()
	t.Cleanup(func() {
		b.Close()
		<-done
	})
	return b
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
	addr := newBroker(t).Addr()
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
	c := connect(t, newBroker(t).Addr(), client.DefaultConfig())

	err := c.Subscribe("quiz/nao-existe/perguntas", func(client.Message) {})
	var nack *client.NackError
	if !errors.As(err, &nack) {
		t.Fatalf("erro = %v, quer *NackError", err)
	}
}

// Duas instâncias reais: a primária cai no meio da partida e apresentador e
// jogador passam sozinhos para a reserva, onde a pergunta seguinte chega.
func TestFailoverBetweenRealBrokers(t *testing.T) {
	primary := newBroker(t)
	fallback := newBroker(t)
	const topic = "quiz/failover/perguntas"

	var wg sync.WaitGroup
	wg.Add(2)
	var mu sync.Mutex
	var events []client.FailoverEvent
	cfg := client.DefaultConfig()
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.HeartbeatTimeout = 200 * time.Millisecond
	cfg.Fallbacks = []string{fallback.Addr()}
	cfg.OnFailover = func(ev client.FailoverEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
		wg.Done()
	}

	apresentador := connect(t, primary.Addr(), cfg)
	jogador := connect(t, primary.Addr(), cfg)
	if err := apresentador.CreateTopic(topic); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 4)
	if err := jogador.Subscribe(topic, func(m client.Message) { got <- string(m.Payload) }); err != nil {
		t.Fatal(err)
	}
	apresentador.Publish(topic, []byte("pergunta 1"))
	if p := <-got; p != "pergunta 1" {
		t.Fatalf("recebeu %q", p)
	}

	primary.Close() // instância primária cai

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("os clientes não fizeram failover")
	}
	mu.Lock()
	for _, ev := range events {
		if ev.To != fallback.Addr() {
			t.Fatalf("failover para %s, quer %s", ev.To, fallback.Addr())
		}
		t.Logf("failover %s -> %s: queda percebida em %v, recuperação em %v", ev.From, ev.To, ev.Downtime, ev.Recovery)
	}
	mu.Unlock()

	// A aplicação não fez nada: o mesmo handler recebe a próxima pergunta
	// pela instância reserva.
	deadline := time.After(2 * time.Second)
	for {
		apresentador.Publish(topic, []byte("pergunta 2"))
		select {
		case p := <-got:
			if p != "pergunta 2" {
				t.Fatalf("recebeu %q", p)
			}
			return
		case <-time.After(100 * time.Millisecond):
			// O apresentador pode ter recriado o tópico depois que o jogador
			// se reinscreveu; tenta de novo.
		case <-deadline:
			t.Fatal("pergunta 2 não chegou pela instância reserva")
		}
	}
}

// Um jogador barulhento estoura o rate limit com reações, mas seus heartbeats
// não passam pelo limite, então ele não faz failover à toa. Os Acks das
// publicações aceitas também contam como sinal de vida, por isso o teste dura
// o suficiente para a penalidade do rate limit derrubar a taxa do jogador
// (10 -> 5 -> 2,5 msg/s...) abaixo de uma resposta por HeartbeatTimeout.
func TestNoisyPublisherDoesNotTriggerFalseFailover(t *testing.T) {
	b := newBroker(t)
	cfg := client.DefaultConfig()
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.HeartbeatTimeout = 200 * time.Millisecond
	c := connect(t, b.Addr(), cfg)

	stop := time.After(3 * time.Second)
	for sent := 0; ; sent++ {
		select {
		case <-stop:
			if st := c.Stats(); st.Failovers != 0 {
				t.Fatalf("failover falso com %d publicações: %+v", sent, st)
			}
			return
		default:
		}
		c.Publish("quiz/barulho/reacoes", []byte("👏"))
		time.Sleep(time.Millisecond) // ~1000 msg/s, 100x o limite por cliente
	}
}
