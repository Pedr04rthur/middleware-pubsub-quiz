// Package outbox implementa as filas de saída privadas de cada inscrito, que
// isolam consumidores lentos para que não atrasem os demais.
package outbox

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/scheduler"
)

// ErrSlowConsumer é o motivo de desconexão de um inscrito que não consegue
// acompanhar o ritmo das mensagens.
var ErrSlowConsumer = errors.New("consumidor lento: fila de saída cheia")

// Sender entrega uma mensagem ao inscrito (por exemplo, escrevendo no socket).
// Um erro retornado faz o inscrito ser desconectado.
type Sender func(payload []byte) error

// Config define o tamanho das filas e a política de isolamento.
type Config struct {
	// QueueSize é quantas mensagens de cada prioridade cabem na fila de saída
	// de cada inscrito. Cada prioridade tem a sua faixa, então reações nunca
	// ocupam o espaço de perguntas e placar.
	QueueSize int

	// Quando a faixa está cheia, a mensagem mais antiga dela é descartada para
	// dar lugar à nova, pois no quiz a informação mais recente é a que importa.
	// Se um inscrito tiver mais de MaxDrops descartes dentro de DropWindow,
	// ele é desconectado. MaxDrops zero desativa a desconexão (só descarta).
	MaxDrops   int
	DropWindow time.Duration
}

// DefaultConfig retorna valores razoáveis para um quiz ao vivo.
func DefaultConfig() Config {
	return Config{
		QueueSize:  256,
		MaxDrops:   200,
		DropWindow: 5 * time.Second,
	}
}

// Result indica o que aconteceu com uma mensagem entregue à fila.
type Result int

const (
	// Queued indica que a mensagem entrou na fila sem descartar nada.
	Queued Result = iota
	// Dropped indica que a fila estava cheia e uma mensagem foi descartada.
	Dropped
	// Disconnected indica que o inscrito foi (ou já estava) desconectado.
	Disconnected
	// Unknown indica que não há inscrito com esse identificador.
	Unknown
)

// Stats conta o que aconteceu com as mensagens de um inscrito.
type Stats struct {
	Sent    int64
	Dropped int64
	Pending int
}

type subscriber struct {
	id     string
	send   Sender
	queues [scheduler.NumPriorities]chan []byte
	done   chan struct{}
	once   sync.Once

	sent    atomic.Int64
	dropped atomic.Int64

	mu          sync.Mutex
	windowStart time.Time
	windowDrops int
}

// Hub guarda as filas de saída de todos os inscritos.
type Hub struct {
	cfg          Config
	onDisconnect func(id string, reason error)
	now          func() time.Time

	mu   sync.RWMutex
	subs map[string]*subscriber
}

// NewHub cria o hub. onDisconnect é chamado uma única vez por inscrito
// desconectado (por lentidão ou erro de envio), para o broker remover suas
// inscrições e, se houver conexão, fechá-la. Ele roda em uma goroutine própria
// para que um tratamento demorado não atrase as entregas. Pode ser nil.
func NewHub(cfg Config, onDisconnect func(id string, reason error)) *Hub {
	return &Hub{
		cfg:          cfg,
		onDisconnect: onDisconnect,
		now:          time.Now,
		subs:         make(map[string]*subscriber),
	}
}

// Add cria a fila de saída do inscrito e a goroutine que envia para ele.
// Se já existir um inscrito com o mesmo id, ele é substituído e as mensagens
// pendentes na fila antiga são perdidas; use Ensure para manter a existente.
func (h *Hub) Add(id string, send Sender) {
	s := h.newSubscriber(id, send)

	h.mu.Lock()
	old := h.subs[id]
	h.subs[id] = s
	h.mu.Unlock()

	if old != nil {
		old.stop()
	}
	go h.run(s)
}

// Ensure cria a fila de saída do inscrito apenas se ela ainda não existir, e
// informa se criou. Útil quando o mesmo cliente se inscreve em vários tópicos.
func (h *Hub) Ensure(id string, send Sender) bool {
	h.mu.Lock()
	if _, ok := h.subs[id]; ok {
		h.mu.Unlock()
		return false
	}
	s := h.newSubscriber(id, send)
	h.subs[id] = s
	h.mu.Unlock()

	go h.run(s)
	return true
}

func (h *Hub) newSubscriber(id string, send Sender) *subscriber {
	s := &subscriber{id: id, send: send, done: make(chan struct{})}
	for p := range s.queues {
		s.queues[p] = make(chan []byte, h.cfg.QueueSize)
	}
	return s
}

// Remove encerra a fila do inscrito sem chamar onDisconnect.
func (h *Hub) Remove(id string) {
	h.mu.Lock()
	s := h.subs[id]
	delete(h.subs, id)
	h.mu.Unlock()

	if s != nil {
		s.stop()
	}
}

// Deliver coloca a mensagem na faixa da prioridade dada na fila do inscrito.
// Nunca bloqueia, então um inscrito lento não atrasa a entrega para os outros.
func (h *Hub) Deliver(id string, payload []byte, prio scheduler.Priority) Result {
	h.mu.RLock()
	s := h.subs[id]
	h.mu.RUnlock()

	if s == nil {
		return Unknown
	}
	if s.stopped() {
		return Disconnected
	}
	if prio < 0 || prio >= scheduler.NumPriorities {
		prio = scheduler.Normal
	}
	q := s.queues[prio]

	select {
	case q <- payload:
		return Queued
	default:
	}

	select {
	case <-q:
	default:
	}
	select {
	case q <- payload:
	default:
	}
	s.dropped.Add(1)

	if s.tooManyDrops(h.cfg, h.now()) {
		h.disconnect(s, ErrSlowConsumer)
		return Disconnected
	}
	return Dropped
}

// Stats retorna os contadores do inscrito.
func (h *Hub) Stats(id string) (Stats, bool) {
	h.mu.RLock()
	s := h.subs[id]
	h.mu.RUnlock()

	if s == nil {
		return Stats{}, false
	}
	pending := 0
	for _, q := range s.queues {
		pending += len(q)
	}
	return Stats{Sent: s.sent.Load(), Dropped: s.dropped.Load(), Pending: pending}, true
}

// Len retorna quantos inscritos estão ativos.
func (h *Hub) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

func (h *Hub) run(s *subscriber) {
	for {
		payload, ok := s.next()
		if !ok {
			return
		}
		if err := s.send(payload); err != nil {
			h.disconnect(s, err)
			return
		}
		s.sent.Add(1)
	}
}

// next retorna a próxima mensagem a enviar, sempre da faixa de maior
// prioridade que tiver alguma, esperando se todas estiverem vazias.
func (s *subscriber) next() ([]byte, bool) {
	if s.stopped() {
		return nil, false
	}
	for _, q := range s.queues {
		select {
		case p := <-q:
			return p, true
		default:
		}
	}

	select {
	case <-s.done:
		return nil, false
	case p := <-s.queues[scheduler.Critical]:
		return p, true
	case p := <-s.queues[scheduler.Normal]:
		return p, true
	case p := <-s.queues[scheduler.Low]:
		return p, true
	}
}

func (h *Hub) disconnect(s *subscriber, reason error) {
	h.mu.Lock()
	if h.subs[s.id] == s {
		delete(h.subs, s.id)
	}
	h.mu.Unlock()

	if s.stop() && h.onDisconnect != nil {
		go h.onDisconnect(s.id, reason)
	}
}

// stop encerra a goroutine de envio e informa se foi esta chamada que parou.
// As filas não são fechadas, para que um Deliver concorrente nunca entre em pânico.
func (s *subscriber) stop() bool {
	stopped := false
	s.once.Do(func() {
		close(s.done)
		stopped = true
	})
	return stopped
}

func (s *subscriber) stopped() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *subscriber) tooManyDrops(cfg Config, now time.Time) bool {
	if cfg.MaxDrops <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if now.Sub(s.windowStart) > cfg.DropWindow {
		s.windowStart = now
		s.windowDrops = 0
	}
	s.windowDrops++
	return s.windowDrops > cfg.MaxDrops
}
