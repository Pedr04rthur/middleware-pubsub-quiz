// Package scheduler implementa o escalonador de mensagens do broker, que
// entrega primeiro o tráfego crítico do quiz e só depois o secundário.
package scheduler

import (
	"context"
	"strings"
	"sync/atomic"
	"time"
)

// Priority é a classe de prioridade de uma mensagem. Valores menores são
// atendidos primeiro.
type Priority int

const (
	// Critical é o tráfego que não pode atrasar: perguntas e placar.
	Critical Priority = iota
	// Normal é qualquer tópico que não seja crítico nem secundário.
	Normal
	// Low é o tráfego secundário e descartável: reações.
	Low

	// NumPriorities é a quantidade de classes de prioridade.
	NumPriorities
)

func (p Priority) String() string {
	switch p {
	case Critical:
		return "critica"
	case Normal:
		return "normal"
	case Low:
		return "baixa"
	}
	return "desconhecida"
}

// Classify define a prioridade de um tópico no formato quiz/{sala}/{tipo}.
func Classify(topic string) Priority {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "quiz" || parts[1] == "" {
		return Normal
	}
	switch parts[2] {
	case "perguntas", "placar":
		return Critical
	case "reacoes":
		return Low
	}
	return Normal
}

// Message é uma publicação recebida pelo broker.
type Message struct {
	Topic   string
	Payload []byte
	From    string

	// Preenchidos pelo escalonador em Publish.
	Priority   Priority
	EnqueuedAt time.Time
}

// Config define o tamanho das filas e a validade das mensagens secundárias.
type Config struct {
	CriticalQueueSize int
	NormalQueueSize   int
	LowQueueSize      int

	// LowMaxAge descarta reações que esperaram mais do que isso na fila, pois
	// uma reação atrasada já não tem valor para quem está jogando. Zero desativa.
	LowMaxAge time.Duration
}

// DefaultConfig retorna valores razoáveis para um quiz ao vivo.
func DefaultConfig() Config {
	return Config{
		CriticalQueueSize: 4096,
		NormalQueueSize:   1024,
		LowQueueSize:      1024,
		LowMaxAge:         2 * time.Second,
	}
}

// Stats conta o que aconteceu com as mensagens de cada prioridade.
type Stats struct {
	Enqueued  [NumPriorities]int64
	Delivered [NumPriorities]int64
	// Dropped são mensagens recusadas porque a fila estava cheia.
	Dropped [NumPriorities]int64
	// Expired são reações descartadas por terem passado de LowMaxAge.
	Expired [NumPriorities]int64
}

// Scheduler mantém uma fila por prioridade e entrega as mensagens ao handler
// em prioridade estrita: uma mensagem só é entregue se não houver nenhuma de
// prioridade maior esperando. Dentro da mesma prioridade a ordem de chegada é
// preservada.
type Scheduler struct {
	cfg     Config
	handler func(Message)
	queues  [NumPriorities]chan Message
	now     func() time.Time

	enqueued  [NumPriorities]atomic.Int64
	delivered [NumPriorities]atomic.Int64
	dropped   [NumPriorities]atomic.Int64
	expired   [NumPriorities]atomic.Int64
}

// New cria um escalonador que entrega as mensagens a handler, que é chamado
// sempre pela mesma goroutine (a de Run).
func New(cfg Config, handler func(Message)) *Scheduler {
	s := &Scheduler{cfg: cfg, handler: handler, now: time.Now}
	s.queues[Critical] = make(chan Message, cfg.CriticalQueueSize)
	s.queues[Normal] = make(chan Message, cfg.NormalQueueSize)
	s.queues[Low] = make(chan Message, cfg.LowQueueSize)
	return s
}

// Publish classifica a mensagem pelo tópico e a coloca na fila correspondente.
// Nunca bloqueia: se a fila estiver cheia, a mensagem é descartada e Publish
// retorna false, para que um pico de reações não trave a leitura do broker.
func (s *Scheduler) Publish(m Message) bool {
	m.Priority = Classify(m.Topic)
	m.EnqueuedAt = s.now()

	select {
	case s.queues[m.Priority] <- m:
		s.enqueued[m.Priority].Add(1)
		return true
	default:
		s.dropped[m.Priority].Add(1)
		return false
	}
}

// Run entrega as mensagens ao handler até o contexto ser cancelado.
func (s *Scheduler) Run(ctx context.Context) {
	for {
		m, ok := s.next(ctx)
		if !ok {
			return
		}
		if m.Priority == Low && s.cfg.LowMaxAge > 0 && s.now().Sub(m.EnqueuedAt) > s.cfg.LowMaxAge {
			s.expired[m.Priority].Add(1)
			continue
		}
		s.handler(m)
		s.delivered[m.Priority].Add(1)
	}
}

// next retorna a mensagem de maior prioridade disponível, esperando se todas
// as filas estiverem vazias.
func (s *Scheduler) next(ctx context.Context) (Message, bool) {
	for _, q := range s.queues {
		select {
		case m := <-q:
			return m, true
		default:
		}
	}

	select {
	case m := <-s.queues[Critical]:
		return m, true
	case m := <-s.queues[Normal]:
		return m, true
	case m := <-s.queues[Low]:
		return m, true
	case <-ctx.Done():
		return Message{}, false
	}
}

// Stats retorna os contadores atuais.
func (s *Scheduler) Stats() Stats {
	var st Stats
	for p := Priority(0); p < NumPriorities; p++ {
		st.Enqueued[p] = s.enqueued[p].Load()
		st.Delivered[p] = s.delivered[p].Load()
		st.Dropped[p] = s.dropped[p].Load()
		st.Expired[p] = s.expired[p].Load()
	}
	return st
}

// Pending retorna quantas mensagens de cada prioridade estão esperando.
func (s *Scheduler) Pending() [NumPriorities]int {
	var n [NumPriorities]int
	for p, q := range s.queues {
		n[p] = len(q)
	}
	return n
}
