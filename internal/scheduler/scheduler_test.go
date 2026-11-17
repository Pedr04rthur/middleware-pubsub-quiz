package scheduler

import (
	"context"
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

func TestFullLowQueueDropsOnlyReactions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LowQueueSize = 5
	s := New(cfg, func(Message) {})

	accepted := 0
	for i := 0; i < 10; i++ {
		if s.Publish(Message{Topic: "quiz/sala1/reacoes"}) {
			accepted++
		}
	}
	if accepted != 5 {
		t.Fatalf("só 5 reações deveriam caber na fila, aceitas %d", accepted)
	}
	if !s.Publish(Message{Topic: "quiz/sala1/perguntas"}) {
		t.Fatal("pergunta deveria entrar mesmo com a fila de reações cheia")
	}

	st := s.Stats()
	if st.Dropped[Low] != 5 || st.Dropped[Critical] != 0 {
		t.Fatalf("descartes inesperados: %+v", st.Dropped)
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
