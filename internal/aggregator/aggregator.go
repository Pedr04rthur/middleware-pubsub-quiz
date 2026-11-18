// Package aggregator resume as reações de cada sala em uma única mensagem por
// janela de tempo, para que uma rajada de emojis não se multiplique pelo
// número de inscritos e economize banda no broker.
package aggregator

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Other é a chave em que caem reações inválidas ou além de MaxKinds.
const Other = "outros"

// Config define a janela de agregação e os limites de memória.
type Config struct {
	// Interval é de quanto em quanto tempo um resumo é emitido por tópico.
	Interval time.Duration
	// MaxKinds limita quantos emojis diferentes são contados por janela; os
	// demais vão para Other. Impede que um cliente encha a memória do broker
	// mandando payloads sempre diferentes.
	MaxKinds int
	// MaxReactionBytes é o tamanho máximo de uma reação; maiores vão para Other.
	MaxReactionBytes int
}

// DefaultConfig retorna valores razoáveis para um quiz ao vivo.
func DefaultConfig() Config {
	return Config{
		Interval:         time.Second,
		MaxKinds:         32,
		MaxReactionBytes: 32,
	}
}

// Summary é o resumo emitido no próprio tópico de reações.
type Summary struct {
	WindowMs  int64          `json:"janela_ms"`
	Total     int            `json:"total"`
	PerSecond float64        `json:"por_segundo"`
	Reactions map[string]int `json:"reacoes"`
}

// Emitter recebe o resumo serializado em JSON de um tópico.
type Emitter func(topic string, payload []byte)

// Aggregator conta as reações de cada tópico e emite os resumos.
type Aggregator struct {
	cfg  Config
	emit Emitter

	mu     sync.Mutex
	counts map[string]map[string]int
}

// New cria o agregador; emit é chamado pela goroutine de Run.
func New(cfg Config, emit Emitter) *Aggregator {
	return &Aggregator{cfg: cfg, emit: emit, counts: make(map[string]map[string]int)}
}

// Add conta uma reação no tópico. O payload não é guardado, então o chamador
// pode reutilizar o buffer.
func (a *Aggregator) Add(topic string, payload []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()

	c := a.counts[topic]
	if c == nil {
		c = make(map[string]int)
		a.counts[topic] = c
	}
	key := a.key(payload)
	if _, seen := c[key]; !seen && len(c) >= a.cfg.MaxKinds {
		key = Other
	}
	c[key]++
}

func (a *Aggregator) key(payload []byte) string {
	if len(payload) == 0 || len(payload) > a.cfg.MaxReactionBytes || !utf8.Valid(payload) {
		return Other
	}
	if k := strings.TrimSpace(string(payload)); k != "" {
		return k
	}
	return Other
}

// Run emite os resumos a cada Interval até o contexto ser cancelado.
func (a *Aggregator) Run(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.Interval)
	defer ticker.Stop()

	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			a.Flush(now.Sub(last))
			last = now
		}
	}
}

// Flush emite um resumo para cada tópico que recebeu reações na janela e
// zera as contagens. Tópicos sem reações não geram mensagem.
func (a *Aggregator) Flush(window time.Duration) {
	a.mu.Lock()
	counts := a.counts
	a.counts = make(map[string]map[string]int)
	a.mu.Unlock()

	for topic, c := range counts {
		s := Summary{WindowMs: window.Milliseconds(), Reactions: c}
		for _, n := range c {
			s.Total += n
		}
		if secs := window.Seconds(); secs > 0 {
			s.PerSecond = float64(s.Total) / secs
		}
		payload, err := json.Marshal(s)
		if err != nil {
			continue
		}
		a.emit(topic, payload)
	}
}
