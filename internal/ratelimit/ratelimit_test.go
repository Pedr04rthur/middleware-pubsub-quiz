package ratelimit

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time          { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

func TestTokenBucketBurstAndRefill(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newTokenBucketWithClock(2, 3, clock.now)

	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("mensagem %d da rajada deveria passar", i)
		}
	}
	if b.Allow() {
		t.Fatal("balde vazio deveria rejeitar")
	}

	clock.advance(500 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("após 0,5s a 2 msgs/s deveria haver 1 ficha")
	}
	if b.Allow() {
		t.Fatal("só 1 ficha deveria ter sido reabastecida")
	}

	clock.advance(10 * time.Second)
	if got := b.Tokens(); got != 3 {
		t.Fatalf("fichas deveriam parar na capacidade 3, obtido %v", got)
	}
}

func TestTokenBucketSetLimitsClampsTokens(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newTokenBucketWithClock(10, 10, clock.now)

	b.SetLimits(1, 2)
	if got := b.Tokens(); got != 2 {
		t.Fatalf("fichas deveriam ser limitadas à nova capacidade 2, obtido %v", got)
	}
	if got := b.Rate(); got != 1 {
		t.Fatalf("taxa deveria ser 1, obtido %v", got)
	}
}

func testConfig() Config {
	return Config{
		BaseRate:          10,
		Burst:             10,
		MinRate:           1,
		HighLoadThreshold: 100,
		AdjustInterval:    time.Second,
		NoisyRejections:   3,
		PenaltyFactor:     0.5,
		RecoveryFactor:    2,
		MaxNoisyIntervals: 3,
	}
}

func TestNoisyClientIsPenalizedAndRecovers(t *testing.T) {
	m := NewManager(testConfig())
	noisy := m.Register("barulhento")
	quiet := m.Register("tranquilo")

	for i := 0; i < 20; i++ {
		noisy.Allow()
	}
	quiet.Allow()
	m.adjust(time.Second)

	if got := noisy.Rate(); got != 5 {
		t.Fatalf("cliente barulhento deveria cair para 5 msgs/s, obtido %v", got)
	}
	if got := quiet.Rate(); got != 10 {
		t.Fatalf("cliente tranquilo deveria manter 10 msgs/s, obtido %v", got)
	}

	m.adjust(time.Second)
	if got := noisy.Rate(); got != 10 {
		t.Fatalf("cliente deveria se recuperar para 10 msgs/s, obtido %v", got)
	}
}

func TestPersistentNoisyClientIsDisconnected(t *testing.T) {
	m := NewManager(testConfig())
	c := m.Register("abusivo")

	for interval := 0; interval < 3; interval++ {
		for i := 0; i < 50; i++ {
			c.Allow()
		}
		m.adjust(time.Second)
	}

	if got := c.Allow(); got != Disconnect {
		t.Fatalf("cliente abusivo deveria ser desconectado, obtido %v", got)
	}
}

func TestHighLoadReducesEveryClient(t *testing.T) {
	m := NewManager(testConfig())
	a := m.Register("a")
	b := m.Register("b")

	m.received.Add(400)
	m.adjust(time.Second)

	if got := m.GlobalFactor(); got != 0.25 {
		t.Fatalf("carga 4x acima do limiar deveria dar fator 0,25, obtido %v", got)
	}
	if a.Rate() != 2.5 || b.Rate() != 2.5 {
		t.Fatalf("todos deveriam cair para 2,5 msgs/s, obtido %v e %v", a.Rate(), b.Rate())
	}

	m.adjust(time.Second)
	if got := m.GlobalFactor(); got != 0.5 {
		t.Fatalf("com carga normal o fator deveria subir gradualmente para 0,5, obtido %v", got)
	}
}
