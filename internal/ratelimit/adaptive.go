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
	// do qual ele é considerado sob alta carga e passa a reduzir os limites de
	// reações (tráfego Bulk) de todos os clientes. O tráfego do jogo não sofre
	// essa redução.
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
	// MaxNoisyIntervals é quantos intervalos barulhentos seguidos levam ao
	// bloqueio do cliente. Zero desativa o bloqueio.
	MaxNoisyIntervals int

	// IdleTimeout é quanto tempo sem mensagens até o cliente ser esquecido.
	// Em UDP não há aviso de desconexão, então sem isso a tabela de clientes
	// cresceria para sempre. Um cliente bloqueado só é esquecido (e desbloqueado)
	// depois de ficar em silêncio por esse tempo. Zero desativa a remoção.
	IdleTimeout time.Duration
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
		IdleTimeout:       30 * time.Second,
	}
}

// Decision é o resultado da verificação de uma mensagem.
type Decision int

const (
	// Allowed indica que a mensagem pode ser processada.
	Allowed Decision = iota
	// Throttled indica que a mensagem deve ser descartada.
	Throttled
	// Blocked indica que o cliente abusou repetidamente: suas mensagens devem
	// ser ignoradas (e, se houver conexão, ela deve ser encerrada).
	Blocked
)

// Traffic é a classe do tráfego de uma mensagem. Cada cliente tem um balde
// por classe, para que as reações não consumam as fichas das respostas.
type Traffic int

const (
	// Game é o tráfego do jogo: perguntas, respostas, placar e comandos de
	// controle. Não sofre a redução global por carga, só a penalidade
	// individual de quem abusa.
	Game Traffic = iota
	// Bulk é o tráfego descartável (reações). Sofre a redução global quando o
	// broker está sobrecarregado e a penalidade individual.
	Bulk
)

// Client guarda os Token Buckets individuais de um cliente e seu histórico de abuso.
type Client struct {
	ID      string
	manager *Manager
	bucket  *TokenBucket // tráfego do jogo
	bulk    *TokenBucket // reações

	rejected atomic.Int64
	blocked  atomic.Bool
	lastSeen atomic.Int64

	// Campos abaixo só são acessados pelo Manager com manager.mu travado.
	penalty        float64
	noisyIntervals int
}

// Allow verifica se a próxima mensagem de jogo do cliente pode passar.
func (c *Client) Allow() Decision {
	return c.AllowTraffic(Game)
}

// AllowTraffic verifica se a próxima mensagem da classe t pode passar.
// Rejeições em qualquer classe contam para a penalidade do cliente.
func (c *Client) AllowTraffic(t Traffic) Decision {
	c.manager.received.Add(1)
	c.lastSeen.Store(c.manager.now().UnixNano())

	if c.blocked.Load() {
		return Blocked
	}
	b := c.bucket
	if t == Bulk {
		b = c.bulk
	}
	if b.Allow() {
		return Allowed
	}
	c.rejected.Add(1)
	return Throttled
}

// Rate retorna o limite atual de mensagens de jogo/s do cliente.
func (c *Client) Rate() float64 {
	return c.bucket.Rate()
}

// BulkRate retorna o limite atual de reações/s do cliente.
func (c *Client) BulkRate() float64 {
	return c.bulk.Rate()
}

// Manager mantém um Token Bucket por cliente e ajusta dinamicamente os limites
// conforme a carga global do broker e o comportamento de cada cliente.
type Manager struct {
	cfg Config

	mu           sync.Mutex
	clients      map[string]*Client
	globalFactor float64

	received atomic.Int64
	now      func() time.Time
}

// NewManager cria um gerenciador com a configuração dada.
func NewManager(cfg Config) *Manager {
	return &Manager{
		cfg:          cfg,
		clients:      make(map[string]*Client),
		globalFactor: 1,
		now:          time.Now,
	}
}

// Register retorna o Token Bucket individual do cliente, criando-o se ainda
// não existir.
func (m *Manager) Register(id string) *Client {
	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.clients[id]; ok {
		return c
	}
	c := &Client{ID: id, manager: m, penalty: 1}
	c.lastSeen.Store(m.now().UnixNano())
	rate, burst := m.limitsFor(c, Game)
	c.bucket = NewTokenBucket(rate, burst)
	rate, burst = m.limitsFor(c, Bulk)
	c.bulk = NewTokenBucket(rate, burst)
	m.clients[id] = c
	return c
}

// Allow verifica a mensagem de jogo do cliente identificado por id (em UDP,
// o endereço de origem do pacote), registrando-o no primeiro pacote.
func (m *Manager) Allow(id string) Decision {
	return m.Register(id).Allow()
}

// AllowTraffic é Allow para uma classe de tráfego específica.
func (m *Manager) AllowTraffic(id string, t Traffic) Decision {
	return m.Register(id).AllowTraffic(t)
}

// ClientCount retorna quantos clientes estão sendo acompanhados.
func (m *Manager) ClientCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.clients)
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
// o individual (penalidade para clientes barulhentos), e esquece clientes inativos.
func (m *Manager) adjust(elapsed time.Duration) {
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		return
	}
	load := float64(m.received.Swap(0)) / seconds

	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateGlobalFactor(load)

	now := m.now()
	for id, c := range m.clients {
		if m.cfg.IdleTimeout > 0 && now.Sub(time.Unix(0, c.lastSeen.Load())) > m.cfg.IdleTimeout {
			delete(m.clients, id)
			continue
		}

		rejected := c.rejected.Swap(0)
		if rejected >= int64(m.cfg.NoisyRejections) {
			c.noisyIntervals++
			c.penalty = math.Max(c.penalty*m.cfg.PenaltyFactor, m.cfg.MinRate/m.cfg.BaseRate)
		} else {
			c.noisyIntervals = 0
			c.penalty = math.Min(1, c.penalty*m.cfg.RecoveryFactor)
		}

		if m.cfg.MaxNoisyIntervals > 0 && c.noisyIntervals >= m.cfg.MaxNoisyIntervals {
			c.blocked.Store(true)
		}

		rate, burst := m.limitsFor(c, Game)
		c.bucket.SetLimits(rate, burst)
		rate, burst = m.limitsFor(c, Bulk)
		c.bulk.SetLimits(rate, burst)
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

// limitsFor deve ser chamado com m.mu travado. A redução global por carga só
// vale para as reações: se ela valesse para todos, um grupo barulhento que
// sobrecarrega o broker com reações encolheria também o balde dos jogadores
// normais, e uma reação deles roubaria a ficha da resposta (TASK-SC04).
func (m *Manager) limitsFor(c *Client, t Traffic) (rate, burst float64) {
	factor := c.penalty
	if t == Bulk {
		factor *= m.globalFactor
	}
	rate = math.Max(m.cfg.MinRate, m.cfg.BaseRate*factor)
	burst = math.Max(1, m.cfg.Burst*factor)
	return rate, burst
}
