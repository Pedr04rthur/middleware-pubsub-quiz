package outbox

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/scheduler"
)

func deliver(h *Hub, id string, payload []byte) Result {
	return deliverPrio(h, id, payload, scheduler.Normal)
}

func deliverPrio(h *Hub, id string, payload []byte, prio scheduler.Priority) Result {
	return h.Deliver(id, scheduler.Message{Payload: payload, Priority: prio})
}

type collector struct {
	mu   sync.Mutex
	msgs []string
}

func (c *collector) send(p []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, string(p))
	return nil
}

func (c *collector) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.msgs...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("tempo esgotado esperando: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// blockingSender trava no primeiro envio até release ser fechado, simulando
// um consumidor que parou de ler.
func blockingSender() (send Sender, entered, release chan struct{}, got *collector) {
	entered = make(chan struct{})
	release = make(chan struct{})
	got = &collector{}
	var once sync.Once
	send = func(p []byte) error {
		once.Do(func() { close(entered) })
		<-release
		return got.send(p)
	}
	return send, entered, release, got
}

func TestSlowConsumerDoesNotAffectOthers(t *testing.T) {
	type event struct {
		id     string
		reason error
	}
	events := make(chan event, 10)
	h := NewHub(Config{QueueSize: 64, MaxDrops: 50, DropWindow: time.Minute}, func(id string, reason error) {
		events <- event{id, reason}
	})

	fast := &collector{}
	slowSend, _, release, _ := blockingSender()
	defer close(release)
	h.Add("rapido", fast.send)
	h.Add("lento", slowSend)

	const total = 500
	var slowest time.Duration
	for i := 0; i < total; i++ {
		msg := []byte(fmt.Sprintf("msg-%d", i))
		start := time.Now()
		deliver(h, "rapido", msg)
		deliver(h, "lento", msg)
		if d := time.Since(start); d > slowest {
			slowest = d
		}
		time.Sleep(100 * time.Microsecond)
	}

	waitFor(t, "o inscrito rápido receber tudo", func() bool { return len(fast.snapshot()) == total })
	for i, m := range fast.snapshot() {
		if want := fmt.Sprintf("msg-%d", i); m != want {
			t.Fatalf("inscrito rápido recebeu %q na posição %d, esperado %q", m, i, want)
		}
	}
	if st, _ := h.Stats("rapido"); st.Dropped != 0 {
		t.Fatalf("inscrito rápido não deveria perder nada, perdeu %d", st.Dropped)
	}

	select {
	case ev := <-events:
		if ev.id != "lento" || !errors.Is(ev.reason, ErrSlowConsumer) {
			t.Fatalf("desconexão inesperada: %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("o inscrito lento deveria ter sido desconectado")
	}
	select {
	case ev := <-events:
		t.Fatalf("só deveria haver uma desconexão, houve também %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}

	if got := deliver(h, "lento", []byte("x")); got != Unknown {
		t.Fatalf("inscrito desconectado deveria ter sido removido, obtido %v", got)
	}
	if h.Len() != 1 {
		t.Fatalf("deveria restar 1 inscrito, restam %d", h.Len())
	}
	if slowest > 10*time.Millisecond {
		t.Fatalf("Deliver chegou a levar %v; nunca deveria esperar pelo consumidor", slowest)
	}
	t.Logf("Deliver mais demorado: %v", slowest)
}

func TestFullQueueDropsOldest(t *testing.T) {
	h := NewHub(Config{QueueSize: 3}, nil)
	send, entered, release, got := blockingSender()
	h.Add("a", send)

	deliver(h, "a", []byte("1"))
	<-entered

	results := map[Result]int{}
	for i := 2; i <= 10; i++ {
		results[deliver(h, "a", []byte(fmt.Sprint(i)))]++
	}
	if results[Queued] != 3 || results[Dropped] != 6 {
		t.Fatalf("esperado 3 enfileiradas e 6 com descarte, obtido %v", results)
	}

	close(release)
	waitFor(t, "a fila esvaziar", func() bool { return len(got.snapshot()) == 4 })

	want := []string{"1", "8", "9", "10"}
	for i, m := range got.snapshot() {
		if m != want[i] {
			t.Fatalf("recebido %v, esperado %v (as mais antigas é que devem sair)", got.snapshot(), want)
		}
	}
}

func TestDropOnlyPolicyNeverDisconnects(t *testing.T) {
	disconnected := false
	h := NewHub(Config{QueueSize: 2, MaxDrops: 0}, func(string, error) { disconnected = true })
	send, entered, release, _ := blockingSender()
	defer close(release)
	h.Add("a", send)

	deliver(h, "a", []byte("0"))
	<-entered
	for i := 0; i < 1000; i++ {
		deliver(h, "a", []byte("x"))
	}

	if disconnected || h.Len() != 1 {
		t.Fatal("com MaxDrops zero o inscrito nunca deveria ser desconectado")
	}
	if st, _ := h.Stats("a"); st.Dropped != 998 {
		t.Fatalf("esperado 998 descartes, obtido %d", st.Dropped)
	}
}

func TestSendErrorDisconnects(t *testing.T) {
	reasons := make(chan error, 1)
	h := NewHub(DefaultConfig(), func(_ string, reason error) { reasons <- reason })
	broken := errors.New("conexão perdida")
	h.Add("a", func([]byte) error { return broken })

	deliver(h, "a", []byte("x"))

	select {
	case r := <-reasons:
		if !errors.Is(r, broken) {
			t.Fatalf("motivo inesperado: %v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("erro de envio deveria desconectar o inscrito")
	}
	waitFor(t, "o inscrito ser removido", func() bool { return h.Len() == 0 })
}

func TestRemoveDoesNotNotify(t *testing.T) {
	notified := false
	h := NewHub(DefaultConfig(), func(string, error) { notified = true })
	h.Add("a", (&collector{}).send)

	h.Remove("a")

	if notified {
		t.Fatal("Remove não deveria chamar onDisconnect")
	}
	if got := deliver(h, "a", []byte("x")); got != Unknown {
		t.Fatalf("inscrito removido deveria ser desconhecido, obtido %v", got)
	}
}

func TestReactionsNeverEvictOrDelayCritical(t *testing.T) {
	h := NewHub(Config{QueueSize: 3}, nil)
	send, entered, release, got := blockingSender()
	h.Add("a", send)

	deliverPrio(h, "a", []byte("em-envio"), scheduler.Normal)
	<-entered

	deliverPrio(h, "a", []byte("pergunta-1"), scheduler.Critical)
	for i := 0; i < 10; i++ {
		deliverPrio(h, "a", []byte(fmt.Sprintf("reacao-%d", i)), scheduler.Low)
	}
	deliverPrio(h, "a", []byte("placar-1"), scheduler.Critical)

	close(release)
	waitFor(t, "a fila esvaziar", func() bool { return len(got.snapshot()) == 6 })

	want := []string{"em-envio", "pergunta-1", "placar-1", "reacao-7", "reacao-8", "reacao-9"}
	for i, m := range got.snapshot() {
		if m != want[i] {
			t.Fatalf("recebido %v, esperado %v", got.snapshot(), want)
		}
	}
}

func TestExpiredMessagesAreNotSent(t *testing.T) {
	h := NewHub(DefaultConfig(), nil)
	send, entered, release, got := blockingSender()
	h.Add("a", send)

	deliver(h, "a", []byte("em-envio"))
	<-entered

	h.Deliver("a", scheduler.Message{Payload: []byte("resposta-velha"), ExpiresAt: time.Now().Add(20 * time.Millisecond)})
	h.Deliver("a", scheduler.Message{Payload: []byte("resposta-nova"), ExpiresAt: time.Now().Add(time.Hour)})
	h.Deliver("a", scheduler.Message{Payload: []byte("sem-validade")})
	time.Sleep(50 * time.Millisecond)

	close(release)
	waitFor(t, "a fila esvaziar", func() bool { return len(got.snapshot()) == 3 })
	time.Sleep(20 * time.Millisecond)

	want := []string{"em-envio", "resposta-nova", "sem-validade"}
	if fmt.Sprint(got.snapshot()) != fmt.Sprint(want) {
		t.Fatalf("recebido %v, esperado %v", got.snapshot(), want)
	}
	if st, _ := h.Stats("a"); st.Expired != 1 {
		t.Fatalf("1 mensagem deveria ter caducado, stats %+v", st)
	}
}

func TestHooksReportSentAndDropped(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(e string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	cfg := Config{
		QueueSize: 1,
		OnSent:    func(id string, m scheduler.Message) { record(id + ":enviada:" + string(m.Payload)) },
		OnDrop: func(id string, m scheduler.Message, reason string) {
			record(id + ":" + reason + ":" + string(m.Payload))
		},
	}
	h := NewHub(cfg, nil)
	send, entered, release, _ := blockingSender()
	h.Add("a", send)

	deliver(h, "a", []byte("1"))
	<-entered
	deliver(h, "a", []byte("2"))
	deliver(h, "a", []byte("3"))
	h.Deliver("a", scheduler.Message{Payload: []byte("velha"), Priority: scheduler.Critical, ExpiresAt: time.Now().Add(10 * time.Millisecond)})
	deliver(h, "ninguem", []byte("4"))
	time.Sleep(30 * time.Millisecond)

	close(release)
	waitFor(t, "os envios terminarem", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) == 5
	})

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"a:fila_saida_cheia:2",
		"ninguem:sem_fila:4",
		"a:enviada:1",
		"a:expirada:velha",
		"a:enviada:3",
	}
	got := fmt.Sprint(events)
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Fatalf("faltou o evento %q em %v", w, events)
		}
	}
}

func TestEnsureKeepsExistingQueue(t *testing.T) {
	h := NewHub(DefaultConfig(), nil)
	send, entered, release, got := blockingSender()

	if !h.Ensure("a", send) {
		t.Fatal("o primeiro Ensure deveria criar a fila")
	}
	deliver(h, "a", []byte("1"))
	<-entered
	deliver(h, "a", []byte("2"))

	other := &collector{}
	if h.Ensure("a", other.send) {
		t.Fatal("o segundo Ensure não deveria recriar a fila")
	}

	close(release)
	waitFor(t, "as duas mensagens chegarem", func() bool { return len(got.snapshot()) == 2 })
	if len(other.snapshot()) != 0 {
		t.Fatalf("a fila original deveria ter sido mantida, mas o novo sender recebeu %v", other.snapshot())
	}
}
