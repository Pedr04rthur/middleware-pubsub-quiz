package client

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

// statefulBroker é um broker falso com tabela de tópicos própria, como uma
// instância real: recusa inscrição em tópico inexistente e criação duplicada.
type statefulBroker struct {
	*fakeBroker
	mu     sync.Mutex
	topics map[string]bool
}

func newStatefulBroker(t *testing.T, topics ...string) *statefulBroker {
	t.Helper()
	sb := &statefulBroker{topics: make(map[string]bool)}
	for _, tp := range topics {
		sb.topics[tp] = true
	}
	sb.fakeBroker = newFakeBroker(t, func(fb *fakeBroker, m *protocol.Message, from *net.UDPAddr) {
		sb.mu.Lock()
		defer sb.mu.Unlock()
		switch m.Command {
		case protocol.CmdCreateTopic:
			if sb.topics[m.Topic] {
				fb.send(from, protocol.CmdNack, m.Topic, "topico ja existe")
				return
			}
			sb.topics[m.Topic] = true
		case protocol.CmdSubscribe:
			if !sb.topics[m.Topic] {
				fb.send(from, protocol.CmdNack, m.Topic, reasonTopicMissing)
				return
			}
		}
		fb.send(from, protocol.CmdAck, m.Topic, "")
	})
	return sb
}

func (sb *statefulBroker) hasTopic(tp string) bool {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.topics[tp]
}

func TestHeartbeatsAreSentPeriodically(t *testing.T) {
	fb := newFakeBroker(t, nil)
	mustConnect(t, fb.addr(), fastConfig()) // intervalo de 20ms

	time.Sleep(250 * time.Millisecond)
	if got := fb.count(protocol.CmdHeartbeat); got < 5 {
		t.Fatalf("broker recebeu %d heartbeats em 250ms, quer pelo menos 5", got)
	}
}

func TestHeartbeatCanBeDisabled(t *testing.T) {
	fb := newFakeBroker(t, nil)
	cfg := fastConfig()
	cfg.HeartbeatInterval = -1
	c := mustConnect(t, fb.addr(), cfg)

	fb.silent.Store(true)
	time.Sleep(250 * time.Millisecond)
	if got := fb.count(protocol.CmdHeartbeat); got != 0 {
		t.Fatalf("broker recebeu %d heartbeats com heartbeat desativado", got)
	}
	if c.Stats().Failovers != 0 {
		t.Fatal("failover sem heartbeat")
	}
}

func TestBrokerAliveDoesNotTriggerFailover(t *testing.T) {
	primary := newFakeBroker(t, nil)
	fallback := newFakeBroker(t, nil)
	cfg := fastConfig()
	cfg.Fallbacks = []string{fallback.addr()}
	c := mustConnect(t, primary.addr(), cfg)

	time.Sleep(400 * time.Millisecond) // 4x o HeartbeatTimeout
	if c.Stats().Failovers != 0 || c.BrokerAddr() != primary.addr() {
		t.Fatalf("failover com broker vivo: %+v, broker %s", c.Stats(), c.BrokerAddr())
	}
}

func TestConnectFallsBackWhenPrimaryIsDown(t *testing.T) {
	primary := newFakeBroker(t, nil)
	primary.silent.Store(true)
	fallback := newFakeBroker(t, nil)
	cfg := fastConfig()
	cfg.Fallbacks = []string{fallback.addr()}

	c := mustConnect(t, primary.addr(), cfg)
	if c.BrokerAddr() != fallback.addr() {
		t.Fatalf("conectou em %s, quer a reserva %s", c.BrokerAddr(), fallback.addr())
	}
}

func TestFailoverRestoresTopicsAndSubscriptions(t *testing.T) {
	const criado, inscrito = "quiz/s1/perguntas", "quiz/s1/placar"
	primary := newStatefulBroker(t, inscrito) // "placar" foi criado por outro cliente
	fallback := newStatefulBroker(t)          // instância reserva começa vazia

	events := make(chan FailoverEvent, 1)
	cfg := fastConfig()
	cfg.Fallbacks = []string{fallback.addr()}
	cfg.OnFailover = func(ev FailoverEvent) { events <- ev }
	c := mustConnect(t, primary.addr(), cfg)

	if err := c.CreateTopic(criado); err != nil {
		t.Fatal(err)
	}
	got := make(chan Message, 1)
	if err := c.Subscribe(inscrito, func(m Message) { got <- m }); err != nil {
		t.Fatal(err)
	}

	primary.silent.Store(true) // broker primário cai

	var ev FailoverEvent
	select {
	case ev = <-events:
	case <-time.After(3 * time.Second):
		t.Fatal("failover não aconteceu")
	}
	if ev.From != primary.addr() || ev.To != fallback.addr() {
		t.Fatalf("failover de %s para %s", ev.From, ev.To)
	}
	if ev.Restored != 1 || ev.Failed != 0 {
		t.Fatalf("Restored=%d Failed=%d, quer 1 e 0", ev.Restored, ev.Failed)
	}
	if ev.Recovery <= 0 || ev.Downtime < ev.Recovery {
		t.Fatalf("tempos inconsistentes: %+v", ev)
	}
	if c.BrokerAddr() != fallback.addr() {
		t.Fatalf("BrokerAddr = %s após failover", c.BrokerAddr())
	}

	// Tópico criado pelo cliente foi recriado; o tópico só inscrito também,
	// porque a reserva recusou a inscrição com "topico nao existe".
	if !fallback.hasTopic(criado) || !fallback.hasTopic(inscrito) {
		t.Fatalf("tópicos na reserva: criado=%v inscrito=%v", fallback.hasTopic(criado), fallback.hasTopic(inscrito))
	}

	// Mensagens da nova instância chegam ao handler antigo, sem a aplicação
	// ter feito nada.
	clientAddr, _ := net.ResolveUDPAddr("udp", c.LocalAddr())
	fallback.send(clientAddr, protocol.CmdPublish, inscrito, "ranking")
	select {
	case m := <-got:
		if string(m.Payload) != "ranking" {
			t.Fatalf("payload = %q", m.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler não recebeu mensagem da instância reserva")
	}
	if c.Stats().Failovers != 1 {
		t.Fatalf("Failovers = %d", c.Stats().Failovers)
	}
}

func TestControlOpInterruptedByFailoverIsRetriedOnNewBroker(t *testing.T) {
	primary := newFakeBroker(t, nil)
	fallback := newStatefulBroker(t, "quiz/s1/reacoes")
	cfg := fastConfig()
	cfg.Fallbacks = []string{fallback.addr()}
	c := mustConnect(t, primary.addr(), cfg)

	// O broker cai e a aplicação chama Subscribe antes da queda ser
	// detectada: a operação deve esperar o failover e ser feita na reserva.
	primary.silent.Store(true)
	if err := c.Subscribe("quiz/s1/reacoes", func(Message) {}); err != nil {
		t.Fatalf("Subscribe durante failover: %v", err)
	}
	if c.BrokerAddr() != fallback.addr() {
		t.Fatalf("Subscribe concluído em %s, quer a reserva", c.BrokerAddr())
	}
	// O handler já estava registrado, então o próprio failover refaz a
	// inscrição e depois a operação da aplicação é repetida (idempotente).
	if got := fallback.count(protocol.CmdSubscribe); got < 1 {
		t.Fatalf("reserva não recebeu SUBSCRIBE")
	}
	if got := primary.count(protocol.CmdSubscribe); got < 1 {
		t.Fatalf("a operação deveria ter começado na primária")
	}
}

func TestKeepsRetryingUntilABrokerReturns(t *testing.T) {
	primary := newFakeBroker(t, nil)
	fallback := newFakeBroker(t, nil)
	fallback.silent.Store(true)
	events := make(chan FailoverEvent, 1)
	cfg := fastConfig()
	cfg.Fallbacks = []string{fallback.addr()}
	cfg.OnFailover = func(ev FailoverEvent) { events <- ev }
	c := mustConnect(t, primary.addr(), cfg)

	primary.silent.Store(true) // as duas instâncias fora do ar
	waitFor(t, "início do failover", func() bool { return c.failingOver.Load() })

	// Publicar sem broker não é erro para a aplicação (at-most-once).
	if err := c.Publish("quiz/s1/reacoes", []byte("👏")); err != nil {
		t.Fatalf("Publish durante failover: %v", err)
	}
	if c.Stats().PublishDropped != 1 {
		t.Fatalf("PublishDropped = %d, quer 1", c.Stats().PublishDropped)
	}
	// Operação de controle sem nenhum broker desiste com ErrTimeout.
	if err := c.CreateTopic("quiz/s1/placar"); !errors.Is(err, ErrTimeout) {
		t.Fatalf("CreateTopic sem broker: %v, quer ErrTimeout", err)
	}

	time.Sleep(300 * time.Millisecond) // algumas voltas pela lista
	primary.silent.Store(false)        // primária reinicia

	select {
	case ev := <-events:
		if ev.To != primary.addr() {
			t.Fatalf("reconectou em %s, quer %s", ev.To, primary.addr())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("não reconectou quando o broker voltou")
	}
	if err := c.Publish("quiz/s1/reacoes", []byte("👏")); err != nil {
		t.Fatal(err)
	}
	if c.Stats().Published != 1 {
		t.Fatalf("Published = %d após reconectar", c.Stats().Published)
	}
}

func TestDisconnectDuringFailoverStopsRetrying(t *testing.T) {
	primary := newFakeBroker(t, nil)
	c, err := Connect(primary.addr(), fastConfig())
	if err != nil {
		t.Fatal(err)
	}
	primary.silent.Store(true)
	waitFor(t, "início do failover", func() bool { return c.failingOver.Load() })

	c.Disconnect() // broker fora: erro esperado, mas o cliente fecha
	before := primary.count(protocol.CmdConnect)
	time.Sleep(200 * time.Millisecond)
	if after := primary.count(protocol.CmdConnect); after != before {
		t.Fatalf("cliente continuou tentando reconectar após Disconnect (%d -> %d)", before, after)
	}
}
