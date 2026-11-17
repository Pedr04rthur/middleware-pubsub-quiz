package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	cases := map[string]Priority{
		"quiz/sala1/perguntas": Critical,
		"quiz/sala1/placar":    Critical,
		"quiz/sala1/reacoes":   Low,
		"quiz/sala1/chat":      Normal,
		"quiz//perguntas":      Normal,
		"quiz/perguntas":       Normal,
		"outro/sala1/placar":   Normal,
	}
	for topic, want := range cases {
		if got := Classify(topic); got != want {
			t.Errorf("Classify(%q) = %v, esperado %v", topic, got, want)
		}
	}
}

type recorder struct {
	mu     sync.Mutex
	topics []string
	done   chan struct{}
	want   int
}

func newRecorder(want int) *recorder {
	return &recorder{done: make(chan struct{}), want: want}
}

func (r *recorder) handle(m Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics = append(r.topics, m.Topic)
	if len(r.topics) == r.want {
		close(r.done)
	}
}

func (r *recorder) wait(t *testing.T) []string {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("tempo esgotado esperando as entregas")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.topics...)
}

func TestCriticalIsDeliveredBeforeQueuedReactions(t *testing.T) {
	rec := newRecorder(103)
	s := New(DefaultConfig(), rec.handle)

	for i := 0; i < 100; i++ {
		s.Publish(Message{Topic: "quiz/sala1/reacoes"})
	}
	s.Publish(Message{Topic: "quiz/sala1/chat"})
	s.Publish(Message{Topic: "quiz/sala1/placar"})
	s.Publish(Message{Topic: "quiz/sala1/perguntas"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	got := rec.wait(t)
	want := []string{"quiz/sala1/placar", "quiz/sala1/perguntas", "quiz/sala1/chat"}
	for i, topic := range want {
		if got[i] != topic {
			t.Fatalf("entrega %d = %q, esperado %q (ordem: %v)", i, got[i], topic, got[:3])
		}
	}
}

func TestReactionBurstDropsOldest(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LowQueueSize = 5
	s := New(cfg, func(Message) {})

	for i := 0; i < 10; i++ {
		if !s.Publish(Message{Topic: "quiz/sala1/reacoes", Payload: []byte(fmt.Sprint(i))}) {
			t.Fatalf("a reação %d deveria entrar no lugar da mais antiga", i)
		}
	}
	if !s.Publish(Message{Topic: "quiz/sala1/perguntas"}) {
		t.Fatal("pergunta deveria entrar mesmo com a fila de reações cheia")
	}

	st := s.Stats()
	if st.Dropped[Low] != 5 || st.Dropped[Critical] != 0 {
		t.Fatalf("descartes inesperados: %+v", st.Dropped)
	}
	var kept []string
	for len(s.queues[Low]) > 0 {
		kept = append(kept, string((<-s.queues[Low]).Payload))
	}
	if want := []string{"5", "6", "7", "8", "9"}; fmt.Sprint(kept) != fmt.Sprint(want) {
		t.Fatalf("deveriam sobrar as 5 reações mais novas %v, sobraram %v", want, kept)
	}
}

func TestFullCriticalQueueRefusesNewMessage(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CriticalQueueSize = 1
	s := New(cfg, func(Message) {})

	s.Publish(Message{Topic: "quiz/sala1/perguntas", Payload: []byte("1")})
	if s.Publish(Message{Topic: "quiz/sala1/perguntas", Payload: []byte("2")}) {
		t.Fatal("fila crítica cheia deveria recusar a nova pergunta em vez de descartar a anterior")
	}
	if got := string((<-s.queues[Critical]).Payload); got != "1" {
		t.Fatalf("a primeira pergunta deveria ter sido mantida, sobrou %q", got)
	}
}

func TestAnswersExpireAfterTTL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AnswerTTL = 5 * time.Second
	rec := newRecorder(1)
	s := New(cfg, rec.handle)

	var mu sync.Mutex
	now := time.Unix(0, 0)
	s.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	s.Publish(Message{Topic: "quiz/sala1/respostas", Payload: []byte("atrasada")})
	mu.Lock()
	now = now.Add(6 * time.Second)
	mu.Unlock()
	s.Publish(Message{Topic: "quiz/sala1/respostas", Payload: []byte("no prazo")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	rec.wait(t)
	time.Sleep(50 * time.Millisecond)
	st := s.Stats()
	if st.Expired[Normal] != 1 || st.Delivered[Normal] != 1 {
		t.Fatalf("a resposta velha deveria caducar e a nova ser entregue, stats %+v", st)
	}
}

func TestAnswerTTLIsClampedToWindow(t *testing.T) {
	cases := map[time.Duration]time.Duration{
		time.Second:      MinAnswerTTL,
		10 * time.Second: 10 * time.Second,
		time.Minute:      MaxAnswerTTL,
	}
	for in, want := range cases {
		if got := ClampAnswerTTL(in); got != want {
			t.Errorf("ClampAnswerTTL(%v) = %v, esperado %v", in, got, want)
		}
	}
}

func TestParseTopic(t *testing.T) {
	room, kind, ok := ParseTopic("quiz/sala1/respostas")
	if !ok || room != "sala1" || kind != KindAnswers {
		t.Fatalf("ParseTopic = %q, %q, %v", room, kind, ok)
	}
	for _, invalid := range []string{"quiz/sala1", "quiz//respostas", "quiz/sala1/", "outro/sala1/respostas"} {
		if _, _, ok := ParseTopic(invalid); ok {
			t.Errorf("ParseTopic(%q) deveria ser inválido", invalid)
		}
	}
}

func TestStaleReactionsExpire(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LowMaxAge = time.Second
	rec := newRecorder(1)
	s := New(cfg, rec.handle)

	var mu sync.Mutex
	now := time.Unix(0, 0)
	s.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	s.Publish(Message{Topic: "quiz/sala1/reacoes"})
	mu.Lock()
	now = now.Add(5 * time.Second)
	mu.Unlock()
	s.Publish(Message{Topic: "quiz/sala1/placar"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	got := rec.wait(t)
	if len(got) != 1 || got[0] != "quiz/sala1/placar" {
		t.Fatalf("só o placar deveria ser entregue, obtido %v", got)
	}
	time.Sleep(50 * time.Millisecond)
	if st := s.Stats(); st.Expired[Low] != 1 {
		t.Fatalf("a reação velha deveria expirar, obtido %+v", st.Expired)
	}
}

// Simula um broker ocupado: cada entrega custa 100µs e chegam reações sem
// parar. Mede quanto tempo perguntas publicadas no meio da enxurrada esperam.
func TestCriticalLatencyUnderReactionFlood(t *testing.T) {
	const questions = 20
	var mu sync.Mutex
	var latencies []time.Duration
	done := make(chan struct{})

	s := New(DefaultConfig(), func(m Message) {
		time.Sleep(100 * time.Microsecond)
		if m.Priority == Critical {
			mu.Lock()
			latencies = append(latencies, time.Since(m.EnqueuedAt))
			if len(latencies) == questions {
				close(done)
			}
			mu.Unlock()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	stopFlood := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopFlood:
				return
			default:
				s.Publish(Message{Topic: "quiz/sala1/reacoes"})
			}
		}
	}()

	for i := 0; i < questions; i++ {
		time.Sleep(10 * time.Millisecond)
		s.Publish(Message{Topic: "quiz/sala1/perguntas"})
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("as perguntas não foram entregues a tempo")
	}
	close(stopFlood)

	var worst time.Duration
	for _, l := range latencies {
		if l > worst {
			worst = l
		}
	}
	st := s.Stats()
	t.Logf("pior espera de uma pergunta: %v | reações pendentes: %d | reações descartadas por fila cheia: %d",
		worst, s.Pending()[Low], st.Dropped[Low])

	if worst > 50*time.Millisecond {
		t.Fatalf("pergunta esperou %v; com prioridade deveria ser quase imediata", worst)
	}
}
