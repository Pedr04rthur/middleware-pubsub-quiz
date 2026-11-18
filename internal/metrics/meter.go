package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Meter mede vazão: quantos eventos aconteceram e a que taxa por segundo.
type Meter struct {
	count atomic.Int64
	start time.Time

	mu        sync.Mutex
	lastCount int64
	lastRate  float64
	peakRate  float64
}

// NewMeter cria um medidor começando agora.
func NewMeter() *Meter {
	return &Meter{start: time.Now()}
}

// Mark registra n eventos.
func (m *Meter) Mark(n int64) {
	m.count.Add(n)
}

// tick fecha um intervalo de medição e atualiza a taxa atual e a de pico.
func (m *Meter) tick(elapsed time.Duration) {
	secs := elapsed.Seconds()
	if secs <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	count := m.count.Load()
	m.lastRate = float64(count-m.lastCount) / secs
	m.lastCount = count
	m.peakRate = max(m.peakRate, m.lastRate)
}

// MeterSnapshot resume a vazão medida.
type MeterSnapshot struct {
	Count    int64   `json:"total"`
	MeanRate float64 `json:"media_por_s"`
	LastRate float64 `json:"ultimo_s"`
	PeakRate float64 `json:"pico_por_s"`
}

// Snapshot retorna o total, a taxa média desde o início, a do último
// intervalo e a maior taxa já vista em um intervalo.
func (m *Meter) Snapshot() MeterSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := MeterSnapshot{Count: m.count.Load(), LastRate: m.lastRate, PeakRate: m.peakRate}
	if secs := time.Since(m.start).Seconds(); secs > 0 {
		s.MeanRate = float64(s.Count) / secs
	}
	return s
}
