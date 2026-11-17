package client

import (
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

// fakeBroker é um broker UDP mínimo cujas respostas o teste controla.
type fakeBroker struct {
	conn   *net.UDPConn
	reply  func(fb *fakeBroker, msg *protocol.Message, from *net.UDPAddr) // nil = responde Ack
	silent atomic.Bool                                                    // simula broker fora do ar

	mu       sync.Mutex
	received []byte // comandos recebidos, em ordem
}

func newFakeBroker(t *testing.T, reply func(*fakeBroker, *protocol.Message, *net.UDPAddr)) *fakeBroker {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	fb := &fakeBroker{conn: conn, reply: reply}
	go fb.loop()
	t.Cleanup(func() { conn.Close() })
	return fb
}

func (fb *fakeBroker) addr() string { return fb.conn.LocalAddr().String() }

func (fb *fakeBroker) loop() {
	buf := make([]byte, 64*1024)
	for {
		n, from, err := fb.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		msg, err := protocol.Deserialize(buf[:n])
		if err != nil {
			continue
		}
		fb.mu.Lock()
		fb.received = append(fb.received, msg.Command)
		fb.mu.Unlock()
		if fb.silent.Load() {
			continue
		}
		if msg.Command == protocol.CmdHeartbeat {
			fb.send(from, protocol.CmdHeartbeat, "", "")
			continue
		}
		if fb.reply != nil {
			fb.reply(fb, msg, from)
		} else {
			fb.send(from, protocol.CmdAck, msg.Topic, "")
		}
	}
}

func (fb *fakeBroker) send(to *net.UDPAddr, cmd byte, topic, payload string) {
	m := &protocol.Message{Command: cmd, Topic: topic, Payload: []byte(payload)}
	fb.conn.WriteToUDP(m.Serialize(), to)
}

func (fb *fakeBroker) count(cmd byte) int {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	n := 0
	for _, c := range fb.received {
		if c == cmd {
			n++
		}
	}
	return n
}

func fastConfig() Config {
	cfg := DefaultConfig()
	cfg.RequestTimeout = 50 * time.Millisecond
	cfg.Retries = 2
	cfg.HeartbeatInterval = 20 * time.Millisecond
	cfg.HeartbeatTimeout = 100 * time.Millisecond
	cfg.ReconnectBackoff = 20 * time.Millisecond
	return cfg
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("tempo esgotado esperando: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func mustConnect(t *testing.T, addr string, cfg Config) *Client {
	t.Helper()
	c, err := Connect(addr, cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { c.shutdown() })
	return c
}

func TestConnectSendsRegistration(t *testing.T) {
	fb := newFakeBroker(t, nil)
	mustConnect(t, fb.addr(), fastConfig())
	if got := fb.count(protocol.CmdConnect); got != 1 {
		t.Fatalf("broker recebeu %d CONNECT, quer 1", got)
	}
}

func TestConnectTimesOutWhenBrokerIsSilent(t *testing.T) {
	fb := newFakeBroker(t, nil)
	fb.silent.Store(true)
	cfg := fastConfig()

	start := time.Now()
	_, err := Connect(fb.addr(), cfg)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("erro = %v, quer ErrTimeout", err)
	}
	if got, want := fb.count(protocol.CmdConnect), cfg.Retries+1; got != want {
		t.Fatalf("CONNECT enviado %d vezes, quer %d", got, want)
	}
	if elapsed := time.Since(start); elapsed < time.Duration(cfg.Retries+1)*cfg.RequestTimeout {
		t.Fatalf("desistiu cedo demais: %v", elapsed)
	}
}

func TestConnectTimesOutWhenNoBrokerListening(t *testing.T) {
	// Porta sem ninguém escutando: o SO devolve ICMP "connection refused",
	// que deve ser tratado como perda, não como pânico.
	l, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	addr := l.LocalAddr().String()
	l.Close()

	if _, err := Connect(addr, fastConfig()); !errors.Is(err, ErrTimeout) {
		t.Fatalf("erro = %v, quer ErrTimeout", err)
	}
}

func TestControlRequestIsRetransmittedAfterLoss(t *testing.T) {
	var subs atomic.Int32
	fb := newFakeBroker(t, func(fb *fakeBroker, m *protocol.Message, from *net.UDPAddr) {
		// Perde o primeiro SUBSCRIBE, como faria a rede ou o rate limit.
		if m.Command == protocol.CmdSubscribe && subs.Add(1) == 1 {
			return
		}
		fb.send(from, protocol.CmdAck, m.Topic, "")
	})
	c := mustConnect(t, fb.addr(), fastConfig())

	if err := c.Subscribe("quiz/s1/perguntas", func(Message) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := fb.count(protocol.CmdSubscribe); got != 2 {
		t.Fatalf("SUBSCRIBE enviado %d vezes, quer 2", got)
	}
	if got := c.Stats().Retransmitted; got != 1 {
		t.Fatalf("Retransmitted = %d, quer 1", got)
	}
}

func TestSubscribeNackReturnsNackError(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, m *protocol.Message, from *net.UDPAddr) {
		if m.Command == protocol.CmdSubscribe {
			fb.send(from, protocol.CmdNack, m.Topic, "topico nao existe")
			return
		}
		fb.send(from, protocol.CmdAck, m.Topic, "")
	})
	c := mustConnect(t, fb.addr(), fastConfig())

	err := c.Subscribe("quiz/s1/perguntas", func(Message) {})
	var nack *NackError
	if !errors.As(err, &nack) {
		t.Fatalf("erro = %v, quer *NackError", err)
	}
	if nack.Reason != "topico nao existe" || nack.Topic != "quiz/s1/perguntas" {
		t.Fatalf("NackError inesperado: %+v", nack)
	}

	// O handler não pode ficar registrado após a falha.
	c.mu.Lock()
	_, registered := c.handlers["quiz/s1/perguntas"]
	c.mu.Unlock()
	if registered {
		t.Fatal("handler continuou registrado após Nack")
	}
}

func TestPublishIsFireAndForgetAndReportsNackAsync(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, m *protocol.Message, from *net.UDPAddr) {
		if m.Command == protocol.CmdPublish {
			// Responde depois de um tempo para provar que Publish não espera.
			go func() {
				time.Sleep(100 * time.Millisecond)
				fb.send(from, protocol.CmdNack, m.Topic, reasonNoSubscribers)
			}()
			return
		}
		fb.send(from, protocol.CmdAck, m.Topic, "")
	})

	nacks := make(chan string, 1)
	cfg := fastConfig()
	cfg.OnPublishNack = func(topic, reason string) { nacks <- topic + "|" + reason }
	c := mustConnect(t, fb.addr(), cfg)

	start := time.Now()
	if err := c.Publish("quiz/s1/reacoes", []byte("👏")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("Publish bloqueou por %v", elapsed)
	}

	select {
	case got := <-nacks:
		if got != "quiz/s1/reacoes|"+reasonNoSubscribers {
			t.Fatalf("OnPublishNack recebeu %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnPublishNack não foi chamado")
	}
	st := c.Stats()
	if st.Published != 1 || st.PublishNacks != 1 {
		t.Fatalf("Stats = %+v", st)
	}
}

func TestPublishNackDoesNotAnswerPendingControlRequest(t *testing.T) {
	// Um Nack "sem inscritos" no mesmo tópico de uma operação pendente não
	// pode ser confundido com a resposta dela.
	fb := newFakeBroker(t, func(fb *fakeBroker, m *protocol.Message, from *net.UDPAddr) {
		if m.Command == protocol.CmdSubscribe {
			fb.send(from, protocol.CmdNack, m.Topic, reasonNoSubscribers)
			time.Sleep(10 * time.Millisecond)
		}
		fb.send(from, protocol.CmdAck, m.Topic, "")
	})
	c := mustConnect(t, fb.addr(), fastConfig())
	if err := c.Subscribe("quiz/s1/respostas", func(Message) {}); err != nil {
		t.Fatalf("Subscribe deveria ter sucesso, veio %v", err)
	}
}

func TestReceivesMessagesOnSubscribedTopic(t *testing.T) {
	fb := newFakeBroker(t, nil)
	c := mustConnect(t, fb.addr(), fastConfig())

	got := make(chan Message, 1)
	if err := c.Subscribe("quiz/s1/perguntas", func(m Message) { got <- m }); err != nil {
		t.Fatal(err)
	}

	clientAddr, _ := net.ResolveUDPAddr("udp", c.LocalAddr())
	fb.send(clientAddr, protocol.CmdPublish, "quiz/s1/outro", "ignorada")
	fb.send(clientAddr, protocol.CmdPublish, "quiz/s1/perguntas", "Qual a capital?")

	select {
	case m := <-got:
		if m.Topic != "quiz/s1/perguntas" || string(m.Payload) != "Qual a capital?" {
			t.Fatalf("mensagem inesperada: %+v", m)
		}
		if m.ReceivedAt.IsZero() {
			t.Fatal("ReceivedAt vazio")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mensagem não entregue ao handler")
	}
	waitFor(t, "contador de mensagens sem handler", func() bool { return c.Stats().Unroutable == 1 })
}

func TestSlowHandlerDoesNotBlockControlOps(t *testing.T) {
	fb := newFakeBroker(t, nil)
	c := mustConnect(t, fb.addr(), fastConfig())

	release := make(chan struct{})
	defer close(release)
	if err := c.Subscribe("quiz/s1/reacoes", func(Message) { <-release }); err != nil {
		t.Fatal(err)
	}
	clientAddr, _ := net.ResolveUDPAddr("udp", c.LocalAddr())
	fb.send(clientAddr, protocol.CmdPublish, "quiz/s1/reacoes", "trava o handler")
	waitFor(t, "handler travado", func() bool { return c.Stats().Received == 1 })

	// Com o handler travado, operações de controle ainda recebem resposta.
	if err := c.CreateTopic("quiz/s1/placar"); err != nil {
		t.Fatalf("CreateTopic com handler lento: %v", err)
	}
}

func TestInboxOverflowDropsOldest(t *testing.T) {
	fb := newFakeBroker(t, nil)
	cfg := fastConfig()
	cfg.InboxSize = 2
	c := mustConnect(t, fb.addr(), cfg)

	release := make(chan struct{})
	var mu sync.Mutex
	var seen []string
	if err := c.Subscribe("t", func(m Message) {
		<-release
		mu.Lock()
		seen = append(seen, string(m.Payload))
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}

	clientAddr, _ := net.ResolveUDPAddr("udp", c.LocalAddr())
	fb.send(clientAddr, protocol.CmdPublish, "t", "m0") // fica preso no handler
	waitFor(t, "m0 no handler", func() bool { return len(c.inbox) == 0 && c.Stats().Received == 1 })
	for _, p := range []string{"m1", "m2", "m3", "m4"} {
		fb.send(clientAddr, protocol.CmdPublish, "t", p)
	}
	waitFor(t, "4 mensagens recebidas", func() bool { return c.Stats().Received == 5 })
	close(release)

	waitFor(t, "fila esvaziada", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(seen, ",") != "m0,m3,m4" {
		t.Fatalf("entregues = %v, quer as mais recentes [m0 m3 m4]", seen)
	}
	if got := c.Stats().InboxDropped; got != 2 {
		t.Fatalf("InboxDropped = %d, quer 2", got)
	}
}

func TestPublishValidation(t *testing.T) {
	fb := newFakeBroker(t, nil)
	cfg := fastConfig()
	cfg.MaxPacketSize = 64
	c := mustConnect(t, fb.addr(), cfg)

	if err := c.Publish("", []byte("x")); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("tópico vazio: erro = %v", err)
	}
	if err := c.Publish("t", make([]byte, 64)); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("payload grande: erro = %v", err)
	}
	if err := c.Publish("t", make([]byte, 64-headerSize-1)); err != nil {
		t.Fatalf("payload no limite: erro = %v", err)
	}
}

func TestDisconnectNotifiesBrokerAndClosesClient(t *testing.T) {
	fb := newFakeBroker(t, nil)
	c := mustConnect(t, fb.addr(), fastConfig())

	if err := c.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if got := fb.count(protocol.CmdDisconnect); got != 1 {
		t.Fatalf("broker recebeu %d DISCONNECT, quer 1", got)
	}
	if err := c.Publish("t", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish após Disconnect: %v", err)
	}
	if err := c.Subscribe("t", func(Message) {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe após Disconnect: %v", err)
	}
	if err := c.Disconnect(); !errors.Is(err, ErrClosed) {
		t.Fatalf("segundo Disconnect: %v", err)
	}
}

func TestDisconnectClosesSocketEvenIfBrokerIsGone(t *testing.T) {
	fb := newFakeBroker(t, nil)
	c := mustConnect(t, fb.addr(), fastConfig())
	fb.silent.Store(true)

	if err := c.Disconnect(); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Disconnect sem broker: erro = %v, quer ErrTimeout", err)
	}
	if err := c.Publish("t", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("cliente deveria estar fechado, Publish: %v", err)
	}
}

func TestDisconnectFromInsideHandler(t *testing.T) {
	fb := newFakeBroker(t, nil)
	c := mustConnect(t, fb.addr(), fastConfig())

	done := make(chan error, 1)
	if err := c.Subscribe("fim", func(Message) { done <- c.Disconnect() }); err != nil {
		t.Fatal(err)
	}
	clientAddr, _ := net.ResolveUDPAddr("udp", c.LocalAddr())
	fb.send(clientAddr, protocol.CmdPublish, "fim", "")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Disconnect dentro do handler: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Disconnect dentro do handler travou")
	}
}
