// Package ratelimit implementa o controle de taxa de mensagens do broker.
package ratelimit

import (
	"sync"
	"time"
)

// TokenBucket é um balde de fichas: cada mensagem consome uma ficha e o balde
// é reabastecido continuamente a uma taxa fixa (fichas por segundo), até a
// capacidade máxima (rajada permitida).
type TokenBucket struct {
	mu         sync.Mutex
	rate       float64
	capacity   float64
	tokens     float64
	lastRefill time.Time
	now        func() time.Time
}

// NewTokenBucket cria um balde cheio com a taxa (msgs/s) e a capacidade dadas.
func NewTokenBucket(rate, capacity float64) *TokenBucket {
	return newTokenBucketWithClock(rate, capacity, time.Now)
}

func newTokenBucketWithClock(rate, capacity float64, now func() time.Time) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: now(),
		now:        now,
	}
}

// Allow consome uma ficha se houver alguma disponível.
func (b *TokenBucket) Allow() bool {
	return b.AllowN(1)
}

// AllowN consome n fichas se houver n disponíveis; caso contrário não consome nada.
func (b *TokenBucket) AllowN(n float64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill()
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

// SetLimits altera a taxa e a capacidade em tempo de execução, preservando as
// fichas já acumuladas (limitadas à nova capacidade).
func (b *TokenBucket) SetLimits(rate, capacity float64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill()
	b.rate = rate
	b.capacity = capacity
	if b.tokens > capacity {
		b.tokens = capacity
	}
}

// Rate retorna a taxa atual de reabastecimento em fichas por segundo.
func (b *TokenBucket) Rate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rate
}

// Tokens retorna quantas fichas estão disponíveis agora.
func (b *TokenBucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return b.tokens
}

// refill deve ser chamado com b.mu travado.
func (b *TokenBucket) refill() {
	now := b.now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.lastRefill = now
}
