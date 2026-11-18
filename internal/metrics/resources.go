package metrics

import (
	"runtime"
	"sync"
	"time"
)

// Resources amostra o consumo de memória, goroutines e CPU do processo.
type Resources struct {
	mu       sync.Mutex
	lastCPU  time.Duration
	lastWall time.Time
	snap     ResourceSnapshot
}

// NewResources cria o coletor e faz a primeira amostra.
func NewResources() *Resources {
	r := &Resources{}
	r.lastCPU, _ = processCPUTime()
	r.lastWall = time.Now()
	r.Sample()
	return r
}

// ResourceSnapshot é a última amostra e os picos desde o início. CPU é em
// porcentagem de um núcleo (200% = dois núcleos ocupados).
type ResourceSnapshot struct {
	HeapMB         float64 `json:"heap_mb"`
	HeapPeakMB     float64 `json:"heap_pico_mb"`
	SysMB          float64 `json:"memoria_do_so_mb"`
	Goroutines     int     `json:"goroutines"`
	GoroutinesPeak int     `json:"goroutines_pico"`
	NumGC          uint32  `json:"ciclos_gc"`
	GCPauseTotalMs float64 `json:"pausa_gc_total_ms"`
	CPUSeconds     float64 `json:"cpu_s"`
	CPUPercent     float64 `json:"cpu_pct"`
	CPUPercentPeak float64 `json:"cpu_pct_pico"`
	CPUAvailable   bool    `json:"cpu_disponivel"`
}

// Sample lê o consumo atual e atualiza os picos.
func (r *Resources) Sample() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	goroutines := runtime.NumGoroutine()
	cpu, cpuOK := processCPUTime()
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	s := &r.snap
	s.HeapMB = mb(m.HeapAlloc)
	s.HeapPeakMB = max(s.HeapPeakMB, s.HeapMB)
	s.SysMB = mb(m.Sys)
	s.Goroutines = goroutines
	s.GoroutinesPeak = max(s.GoroutinesPeak, goroutines)
	s.NumGC = m.NumGC
	s.GCPauseTotalMs = float64(m.PauseTotalNs) / 1e6
	s.CPUAvailable = cpuOK
	if cpuOK {
		s.CPUSeconds = cpu.Seconds()
		if wall := now.Sub(r.lastWall); wall > 0 {
			s.CPUPercent = 100 * float64(cpu-r.lastCPU) / float64(wall)
			s.CPUPercentPeak = max(s.CPUPercentPeak, s.CPUPercent)
		}
		r.lastCPU, r.lastWall = cpu, now
	}
}

// Snapshot retorna a última amostra.
func (r *Resources) Snapshot() ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snap
}

func mb(b uint64) float64 {
	return float64(b) / (1 << 20)
}
