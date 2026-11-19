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

func TestPersistentNoisyClientIsBlocked(t *testing.T) {
	m := NewManager(testConfig())
	c := m.Register("abusivo")

	for interval := 0; interval < 3; interval++ {
		for i := 0; i < 50; i++ {
			c.Allow()
		}
		m.adjust(time.Second)
	}

	if got := c.Allow(); got != Blocked {
		t.Fatalf("cliente abusivo deveria ser bloqueado, obtido %v", got)
	}
}

func TestAllowByAddressReusesClient(t *testing.T) {
	m := NewManager(testConfig())

	for i := 0; i < 10; i++ {
		if got := m.Allow("10.0.0.1:5000"); got != Allowed {
			t.Fatalf("pacote %d da rajada deveria passar, obtido %v", i, got)
		}
	}
	if got := m.Allow("10.0.0.1:5000"); got != Throttled {
		t.Fatalf("o mesmo endereço deveria compartilhar o balde, obtido %v", got)
	}
	if got := m.Allow("10.0.0.2:5000"); got != Allowed {
		t.Fatalf("outro endereço deveria ter balde próprio, obtido %v", got)
	}
	if got := m.ClientCount(); got != 2 {
		t.Fatalf("deveria haver 2 clientes, obtido %v", got)
	}
}

func TestIdleClientsAreForgotten(t *testing.T) {
	cfg := testConfig()
	cfg.IdleTimeout = 30 * time.Second
	m := NewManager(cfg)
	clock := &fakeClock{t: time.Unix(0, 0)}
	m.now = clock.now

	m.Allow("inativo")
	m.Allow("ativo")

	clock.advance(20 * time.Second)
	m.Allow("ativo")
	m.adjust(time.Second)
	if got := m.ClientCount(); got != 2 {
		t.Fatalf("ninguém passou do tempo limite ainda, obtido %v clientes", got)
	}

	clock.advance(15 * time.Second)
	m.adjust(time.Second)
	if got := m.ClientCount(); got != 1 {
		t.Fatalf("o cliente inativo deveria ter sido esquecido, obtido %v clientes", got)
	}
}

func TestHighLoadReducesOnlyReactions(t *testing.T) {
	m := NewManager(testConfig())
	a := m.Register("a")
	b := m.Register("b")

	m.received.Add(400)
	m.adjust(time.Second)

	if got := m.GlobalFactor(); got != 0.25 {
		t.Fatalf("carga 4x acima do limiar deveria dar fator 0,25, obtido %v", got)
	}
	if a.BulkRate() != 2.5 || b.BulkRate() != 2.5 {
		t.Fatalf("as reacoes de todos deveriam cair para 2,5 msgs/s, obtido %v e %v", a.BulkRate(), b.BulkRate())
	}
	if a.Rate() != 10 || b.Rate() != 10 {
		t.Fatalf("o trafego do jogo nao deveria sofrer a reducao global, obtido %v e %v", a.Rate(), b.Rate())
	}

	m.adjust(time.Second)
	if got := m.GlobalFactor(); got != 0.5 {
		t.Fatalf("com carga normal o fator deveria subir gradualmente para 0,5, obtido %v", got)
	}
}

// Cenário encontrado pelo gerador de carga (TASK-SC04): com o broker
// sobrecarregado por reações, um jogador normal reage e logo depois responde.
// A reação não pode gastar a ficha da resposta.
func TestReactionDoesNotConsumeAnswerToken(t *testing.T) {
	m := NewManager(testConfig())
	jogador := m.Register("jogador")

	m.received.Add(100_000) // spam dos barulhentos: fator global mínimo
	m.adjust(time.Second)

	for i := 0; i < 50; i++ {
		jogador.AllowTraffic(Bulk) // esgota o balde de reações
	}
	if got := jogador.AllowTraffic(Bulk); got != Throttled {
		t.Fatalf("reacoes deveriam estar limitadas, obtido %v", got)
	}
	if got := jogador.AllowTraffic(Game); got != Allowed {
		t.Fatalf("a resposta deveria passar mesmo com as reacoes esgotadas, obtido %v", got)
	}
}

// Quem abusa continua penalizado nos dois baldes: o spam de reações também
// reduz o limite das mensagens de jogo do barulhento, e o bloqueio vale para
// tudo.
func TestNoisyReactionsPenalizeBothBuckets(t *testing.T) {
	m := NewManager(testConfig())
	c := m.Register("barulhento")

	for i := 0; i < 50; i++ {
		c.AllowTraffic(Bulk)
	}
	m.adjust(time.Second)
	if c.Rate() != 5 || c.BulkRate() != 5 {
		t.Fatalf("penalidade deveria valer nos dois baldes, obtido jogo=%v reacoes=%v", c.Rate(), c.BulkRate())
	}

	for interval := 0; interval < 3; interval++ {
		for i := 0; i < 50; i++ {
			c.AllowTraffic(Bulk)
		}
		m.adjust(time.Second)
	}
	if got := c.AllowTraffic(Game); got != Blocked {
		t.Fatalf("barulhento persistente deveria ser bloqueado tambem no jogo, obtido %v", got)
	}
}
