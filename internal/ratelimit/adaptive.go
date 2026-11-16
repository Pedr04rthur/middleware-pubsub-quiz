package ratelimit

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Config define os parâmetros do limitador adaptativo.
type Config struct {
	// BaseRate é o limite normal de mensagens por segundo de cada cliente.
	BaseRate float64
	// Burst é a capacidade normal do balde (rajada máxima) de cada cliente.
	Burst float64
	// MinRate é o piso: nenhum cliente fica abaixo dessa taxa.
	MinRate float64

	// HighLoadThreshold é o total de mensagens/s recebidas pelo broker a partir
	// do qual ele é considerado sob alta carga e passa a reduzir os limites.
	HighLoadThreshold float64
	// AdjustInterval é de quanto em quanto tempo os limites são recalculados.
	AdjustInterval time.Duration

	// NoisyRejections é quantas mensagens rejeitadas em um intervalo fazem o
	// cliente ser considerado "barulhento".
	NoisyRejections int
	// PenaltyFactor multiplica a taxa de um cliente barulhento (ex.: 0.5 = metade).
	PenaltyFactor float64
	// RecoveryFactor multiplica a taxa a cada intervalo bem-comportado, até voltar ao normal.
	RecoveryFactor float64
	// MaxNoisyIntervals é quantos intervalos barulhentos seguidos levam à
	// desconexão do cliente. Zero desativa a desconexão.
	MaxNoisyIntervals int
}

// DefaultConfig retorna valores razoáveis para um quiz ao vivo.
func DefaultConfig() Config {
	return Config{
		BaseRate:          10,
		Burst:             20,
		MinRate:           1,
		HighLoadThreshold: 1000,
		AdjustInterval:    time.Second,
		NoisyRejections:   5,
		PenaltyFactor:     0.5,
		RecoveryFactor:    1.25,
		MaxNoisyIntervals: 10,
	}
}

// Decision é o resultado da verificação de uma mensagem.
type Decision int

const (
	// Allowed indica que a mensagem pode ser processada.
	Allowed Decision = iota
	// Throttled indica que a mensagem deve ser descartada.
	Throttled
	// Disconnect indica que o cliente abusou repetidamente e deve ser desconectado.
	Disconnect
)

// Client guarda o Token Bucket individual de uma conexão e seu histórico de abuso.
type Client struct {
	ID      string
	manager *Manager
	bucket  *TokenBucket

	rejected atomic.Int64
	kicked   atomic.Bool

	// Campos abaixo só são acessados pelo Manager com manager.mu travado.
	penalty        float64
	noisyIntervals int
}

// Allow verifica se a próxima mensagem do cliente pode passar.
func (c *Client) Allow() Decision {
	c.manager.received.Add(1)

	if c.kicked.Load() {
		return Disconnect
	}
	if c.bucket.Allow() {
		return Allowed
	}
	c.rejected.Add(1)
	return Throttled
}

// Rate retorna o limite atual de mensagens/s do cliente.
func (c *Client) Rate() float64 {
	return c.bucket.Rate()
}

// Manager mantém um Token Bucket por cliente e ajusta dinamicamente os limites
// conforme a carga global do broker e o comportamento de cada cliente.
type Manager struct {
	cfg Config

	mu           sync.Mutex
	clients      map[string]*Client
	globalFactor float64

	received atomic.Int64
}

// NewManager cria um gerenciador com a configuração dada.
func NewManager(cfg Config) *Manager {
	return &Manager{
		cfg:          cfg,
		clients:      make(map[string]*Client),
		globalFactor: 1,
	}
}

// Register cria o Token Bucket individual de um novo cliente.
func (m *Manager) Register(id string) *Client {
	m.mu.Lock()
	defer m.mu.Unlock()

	c := &Client{ID: id, manager: m, penalty: 1}
	rate, burst := m.limitsFor(c)
	c.bucket = NewTokenBucket(rate, burst)
	m.clients[id] = c
	return c
}

// Unregister remove o cliente quando a conexão é encerrada.
func (m *Manager) Unregister(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, id)
}

// GlobalFactor retorna o fator de redução global atual (1 = sem redução).
func (m *Manager) GlobalFactor() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.globalFactor
}

// Run recalcula os limites a cada AdjustInterval até o contexto ser cancelado.
func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.AdjustInterval)
	defer ticker.Stop()

	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.adjust(now.Sub(last))
			last = now
		}
	}
}

// adjust aplica os dois níveis de ajuste: o global (carga total do broker) e
// o individual (penalidade para clientes barulhentos).
func (m *Manager) adjust(elapsed time.Duration) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		return
	}
	load := float64(m.received.Swap(0)) / seconds

	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateGlobalFactor(load)

	for _, c := range m.clients {
		rejected := c.rejected.Swap(0)
		if rejected >= int64(m.cfg.NoisyRejections) {
			c.noisyIntervals++
			c.penalty = math.Max(c.penalty*m.cfg.PenaltyFactor, m.cfg.MinRate/m.cfg.BaseRate)
		} else {
			c.noisyIntervals = 0
			c.penalty = math.Min(1, c.penalty*m.cfg.RecoveryFactor)
		}

		if m.cfg.MaxNoisyIntervals > 0 && c.noisyIntervals >= m.cfg.MaxNoisyIntervals {
			c.kicked.Store(true)
		}

		rate, burst := m.limitsFor(c)
		c.bucket.SetLimits(rate, burst)
	}
}

// updateGlobalFactor reduz os limites imediatamente quando a carga passa do
// limiar e os restaura gradualmente quando a carga cai, evitando oscilação.
func (m *Manager) updateGlobalFactor(load float64) {
	target := 1.0
	if load > m.cfg.HighLoadThreshold {
		target = m.cfg.HighLoadThreshold / load
	}

	if target < m.globalFactor {
		m.globalFactor = target
	} else {
		m.globalFactor = math.Min(target, m.globalFactor*m.cfg.RecoveryFactor)
	}
}

// limitsFor deve ser chamado com m.mu travado.
func (m *Manager) limitsFor(c *Client) (rate, burst float64) {
	factor := m.globalFactor * c.penalty
	rate = math.Max(m.cfg.MinRate, m.cfg.BaseRate*factor)
	burst = math.Max(1, m.cfg.Burst*factor)
	return rate, burst
}
