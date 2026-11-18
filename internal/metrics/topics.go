package metrics

import "sync"

// OtherTopics é onde caem os tópicos além do limite de MaxTopics.
const OtherTopics = "(outros)"

// DefaultMaxTopics limita quantos tópicos são acompanhados individualmente.
const DefaultMaxTopics = 1000

// Topics conta, por tópico, as publicações e entregas e os descartes de cada
// uma, por motivo. Publicação e entrega são separadas porque uma publicação
// vira uma entrega para cada inscrito: misturar as duas daria taxas acima de
// 100%.
type Topics struct {
	maxTopics int

	mu     sync.Mutex
	topics map[string]*topicCounters
}

type topicCounters struct {
	published     int64
	publishDrops  map[string]int64
	delivered     int64
	deliveryDrops map[string]int64
}

// NewTopics cria o contador. Acima de maxTopics tópicos diferentes, os novos
// são somados em OtherTopics, para que nomes aleatórios não esgotem a memória.
func NewTopics(maxTopics int) *Topics {
	return &Topics{maxTopics: max(1, maxTopics), topics: make(map[string]*topicCounters)}
}

func (t *Topics) get(topic string) *topicCounters {
	c := t.topics[topic]
	if c != nil {
		return c
	}
	if len(t.topics) >= t.maxTopics {
		topic = OtherTopics
		if c = t.topics[topic]; c != nil {
			return c
		}
	}
	c = &topicCounters{publishDrops: make(map[string]int64), deliveryDrops: make(map[string]int64)}
	t.topics[topic] = c
	return c
}

// Published registra uma publicação recebida no tópico.
func (t *Topics) Published(topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.get(topic).published++
}

// PublishDropped registra uma publicação descartada antes de chegar a
// qualquer inscrito.
func (t *Topics) PublishDropped(topic, reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.get(topic).publishDrops[reason]++
}

// Delivered registra uma mensagem enviada a um inscrito.
func (t *Topics) Delivered(topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.get(topic).delivered++
}

// DeliveryDropped registra uma mensagem que não chegou a um inscrito.
func (t *Topics) DeliveryDropped(topic, reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.get(topic).deliveryDrops[reason]++
}

// TopicSnapshot resume um tópico. As taxas vão de 0 a 1.
type TopicSnapshot struct {
	Published        int64            `json:"publicacoes"`
	PublishDropped   int64            `json:"publicacoes_descartadas"`
	PublishDropRate  float64          `json:"taxa_descarte_publicacao"`
	PublishDrops     map[string]int64 `json:"descartes_publicacao,omitempty"`
	Delivered        int64            `json:"entregas"`
	DeliveryDropped  int64            `json:"entregas_descartadas"`
	DeliveryDropRate float64          `json:"taxa_descarte_entrega"`
	DeliveryDrops    map[string]int64 `json:"descartes_entrega,omitempty"`
}

// Snapshot retorna os contadores de todos os tópicos.
func (t *Topics) Snapshot() map[string]TopicSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make(map[string]TopicSnapshot, len(t.topics))
	for name, c := range t.topics {
		s := TopicSnapshot{
			Published:     c.published,
			PublishDrops:  copyCounts(c.publishDrops),
			Delivered:     c.delivered,
			DeliveryDrops: copyCounts(c.deliveryDrops),
		}
		s.PublishDropped = sum(c.publishDrops)
		s.DeliveryDropped = sum(c.deliveryDrops)
		s.PublishDropRate = rate(s.PublishDropped, s.Published)
		s.DeliveryDropRate = rate(s.DeliveryDropped, s.Delivered+s.DeliveryDropped)
		out[name] = s
	}
	return out
}

func copyCounts(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func sum(m map[string]int64) int64 {
	var n int64
	for _, v := range m {
		n += v
	}
	return n
}

func rate(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(part) / float64(total)
}
