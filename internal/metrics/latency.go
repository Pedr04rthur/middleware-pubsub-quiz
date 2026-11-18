// Package metrics reúne os coletores usados pelo broker e pelos testes de
// estresse: latência (p50, p95, p99), vazão, descarte por tópico e consumo de
// recursos.
package metrics

import (
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// DefaultLatencySamples é quantas amostras um coletor de latência guarda.
const DefaultLatencySamples = 100_000

// Latency coleta durações e calcula percentis. Guarda no máximo maxSamples
// amostras: acima disso usa amostragem por reservatório, que mantém uma
// amostra uniforme de tudo o que foi observado com memória constante.
type Latency struct {
	mu         sync.Mutex
	samples    []time.Duration
	maxSamples int
	count      int64
	sum        time.Duration
	min, max   time.Duration
}

// NewLatency cria um coletor que guarda até maxSamples amostras.
func NewLatency(maxSamples int) *Latency {
	return &Latency{maxSamples: max(1, maxSamples)}
}

// Observe registra uma duração.
func (l *Latency) Observe(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.count++
	l.sum += d
	if l.count == 1 || d < l.min {
		l.min = d
	}
	if d > l.max {
		l.max = d
	}

	if len(l.samples) < l.maxSamples {
		l.samples = append(l.samples, d)
		return
	}
	if i := rand.Int64N(l.count); i < int64(l.maxSamples) {
		l.samples[i] = d
	}
}

// LatencySnapshot resume as latências em milissegundos.
type LatencySnapshot struct {
	Count  int64   `json:"amostras"`
	MinMs  float64 `json:"min_ms"`
	MeanMs float64 `json:"media_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
}

// Snapshot calcula os percentis do que foi observado até agora.
func (l *Latency) Snapshot() LatencySnapshot {
	l.mu.Lock()
	sorted := slices.Clone(l.samples)
	s := LatencySnapshot{Count: l.count, MinMs: ms(l.min), MaxMs: ms(l.max)}
	if l.count > 0 {
		s.MeanMs = ms(l.sum / time.Duration(l.count))
	}
	l.mu.Unlock()

	slices.Sort(sorted)
	s.P50Ms = ms(percentile(sorted, 0.50))
	s.P95Ms = ms(percentile(sorted, 0.95))
	s.P99Ms = ms(percentile(sorted, 0.99))
	return s
}

// percentile usa o método nearest-rank sobre amostras ordenadas.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p * float64(len(sorted))))
	return sorted[min(max(rank, 1), len(sorted))-1]
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
