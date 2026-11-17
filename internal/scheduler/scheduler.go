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

// Tipos de tópico do quiz (o último segmento de quiz/{sala}/{tipo}).
const (
	KindQuestions = "perguntas"
	KindScore     = "placar"
	KindReactions = "reacoes"
	KindAnswers   = "respostas"
)

// Limites da janela de validade das respostas.
const (
	MinAnswerTTL = 5 * time.Second
	MaxAnswerTTL = 20 * time.Second
)

// ClampAnswerTTL ajusta d para dentro da janela [MinAnswerTTL, MaxAnswerTTL].
func ClampAnswerTTL(d time.Duration) time.Duration {
	return min(max(d, MinAnswerTTL), MaxAnswerTTL)
}

// ParseTopic separa um tópico quiz/{sala}/{tipo}; ok é false para qualquer
// outro formato.
func ParseTopic(topic string) (room, kind string, ok bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "quiz" || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// Classify define a prioridade de um tópico no formato quiz/{sala}/{tipo}.
func Classify(topic string) Priority {
	_, kind, ok := ParseTopic(topic)
	if !ok {
		return Normal
	}
	switch kind {
	case KindQuestions, KindScore:
		return Critical
	case KindReactions:
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
	// ExpiresAt é quando a mensagem caduca; zero significa que não caduca.
	ExpiresAt time.Time
}

// Expired informa se a mensagem já caducou no instante now.
func (m Message) Expired(now time.Time) bool {
	return !m.ExpiresAt.IsZero() && now.After(m.ExpiresAt)
}

// Config define o tamanho das filas e a validade das mensagens.
type Config struct {
	CriticalQueueSize int
	NormalQueueSize   int
	// LowQueueSize limita a fila de reações. Numa rajada, quando ela enche,
	// a reação mais antiga é descartada para dar lugar à nova.
	LowQueueSize int

	// LowMaxAge é a validade das reações, pois uma reação atrasada já não tem
	// valor para quem está jogando. Zero desativa.
	LowMaxAge time.Duration
	// AnswerTTL é a validade das respostas (tópico quiz/{sala}/respostas),
	// limitada à janela [MinAnswerTTL, MaxAnswerTTL]. Zero desativa.
	AnswerTTL time.Duration
}

// DefaultConfig retorna valores razoáveis para um quiz ao vivo.
func DefaultConfig() Config {
	return Config{
		CriticalQueueSize: 4096,
		NormalQueueSize:   1024,
		LowQueueSize:      1024,
		LowMaxAge:         2 * time.Second,
		AnswerTTL:         MaxAnswerTTL,
	}
}

// Stats conta o que aconteceu com as mensagens de cada prioridade.
type Stats struct {
	Enqueued  [NumPriorities]int64
	Delivered [NumPriorities]int64
	// Dropped são mensagens descartadas porque a fila estava cheia.
	Dropped [NumPriorities]int64
	// Expired são mensagens descartadas por terem caducado na fila.
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
	if cfg.AnswerTTL > 0 {
		cfg.AnswerTTL = ClampAnswerTTL(cfg.AnswerTTL)
	}
	s := &Scheduler{cfg: cfg, handler: handler, now: time.Now}
	s.queues[Critical] = make(chan Message, cfg.CriticalQueueSize)
	s.queues[Normal] = make(chan Message, cfg.NormalQueueSize)
	s.queues[Low] = make(chan Message, cfg.LowQueueSize)
	return s
}

// Publish classifica a mensagem pelo tópico, define sua validade e a coloca na
// fila correspondente. Nunca bloqueia, para que um pico não trave a leitura do
// broker: numa fila de reações cheia a reação mais antiga é descartada; nas
// demais a nova é recusada e Publish retorna false.
func (s *Scheduler) Publish(m Message) bool {
	m.Priority = Classify(m.Topic)
	m.EnqueuedAt = s.now()
	if ttl := s.ttlFor(m); ttl > 0 {
		m.ExpiresAt = m.EnqueuedAt.Add(ttl)
	}
	q := s.queues[m.Priority]

	select {
	case q <- m:
		s.enqueued[m.Priority].Add(1)
		return true
	default:
	}

	s.dropped[m.Priority].Add(1)
	if m.Priority != Low {
		return false
	}
	select {
	case <-q:
	default:
	}
	select {
	case q <- m:
		s.enqueued[m.Priority].Add(1)
		return true
	default:
		return false
	}
}

func (s *Scheduler) ttlFor(m Message) time.Duration {
	if m.Priority == Low {
		return s.cfg.LowMaxAge
	}
	if _, kind, ok := ParseTopic(m.Topic); ok && kind == KindAnswers {
		return s.cfg.AnswerTTL
	}
	return 0
}

// Run entrega as mensagens ao handler até o contexto ser cancelado.
func (s *Scheduler) Run(ctx context.Context) {
	for {
		m, ok := s.next(ctx)
		if !ok {
			return
		}
		if m.Expired(s.now()) {
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
