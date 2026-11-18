package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

// Registry agrupa os coletores com nome, o contador por tópico e o de recursos.
type Registry struct {
	start     time.Time
	Topics    *Topics
	Resources *Resources

	mu        sync.Mutex
	latencies map[string]*Latency
	meters    map[string]*Meter
}

// NewRegistry cria um registro vazio.
func NewRegistry() *Registry {
	return &Registry{
		start:     time.Now(),
		Topics:    NewTopics(DefaultMaxTopics),
		Resources: NewResources(),
		latencies: make(map[string]*Latency),
		meters:    make(map[string]*Meter),
	}
}

// Latency retorna o coletor de latência com o nome dado, criando-o se preciso.
func (r *Registry) Latency(name string) *Latency {
	r.mu.Lock()
	defer r.mu.Unlock()
	l := r.latencies[name]
	if l == nil {
		l = NewLatency(DefaultLatencySamples)
		r.latencies[name] = l
	}
	return l
}

// Meter retorna o medidor de vazão com o nome dado, criando-o se preciso.
func (r *Registry) Meter(name string) *Meter {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.meters[name]
	if m == nil {
		m = NewMeter()
		r.meters[name] = m
	}
	return m
}

// Run fecha os intervalos de vazão e amostra os recursos a cada interval até
// o contexto ser cancelado.
func (r *Registry) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.Tick(now.Sub(last))
			last = now
		}
	}
}

// Tick fecha um intervalo de medição de duração elapsed.
func (r *Registry) Tick(elapsed time.Duration) {
	r.mu.Lock()
	meters := make([]*Meter, 0, len(r.meters))
	for _, m := range r.meters {
		meters = append(meters, m)
	}
	r.mu.Unlock()

	for _, m := range meters {
		m.tick(elapsed)
	}
	r.Resources.Sample()
}

// Snapshot é o estado de todas as métricas, pronto para virar JSON.
type Snapshot struct {
	UptimeSeconds float64                    `json:"tempo_s"`
	Latencies     map[string]LatencySnapshot `json:"latencias"`
	Meters        map[string]MeterSnapshot   `json:"vazao"`
	Topics        map[string]TopicSnapshot   `json:"topicos"`
	Resources     ResourceSnapshot           `json:"recursos"`
}

// Snapshot coleta o estado atual de todas as métricas.
func (r *Registry) Snapshot() Snapshot {
	r.mu.Lock()
	latencies := make(map[string]*Latency, len(r.latencies))
	for k, v := range r.latencies {
		latencies[k] = v
	}
	meters := make(map[string]*Meter, len(r.meters))
	for k, v := range r.meters {
		meters[k] = v
	}
	r.mu.Unlock()

	s := Snapshot{
		UptimeSeconds: time.Since(r.start).Seconds(),
		Latencies:     make(map[string]LatencySnapshot, len(latencies)),
		Meters:        make(map[string]MeterSnapshot, len(meters)),
		Topics:        r.Topics.Snapshot(),
		Resources:     r.Resources.Snapshot(),
	}
	for k, l := range latencies {
		s.Latencies[k] = l.Snapshot()
	}
	for k, m := range meters {
		s.Meters[k] = m.Snapshot()
	}
	return s
}

// Handler serve o Snapshot em JSON, para ser lido pela ferramenta de estresse.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(r.Snapshot())
	})
}

// WriteText escreve o Snapshot como um relatório legível.
func (s Snapshot) WriteText(w io.Writer) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	defer tw.Flush()

	if len(s.Latencies) > 0 {
		fmt.Fprintln(tw, "LATENCIA (ms)\tamostras\tp50\tp95\tp99\tmax\t")
		for _, k := range sortedKeys(s.Latencies) {
			l := s.Latencies[k]
			fmt.Fprintf(tw, "%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t\n", k, l.Count, l.P50Ms, l.P95Ms, l.P99Ms, l.MaxMs)
		}
		fmt.Fprintln(tw, "\t")
	}

	if len(s.Meters) > 0 {
		fmt.Fprintln(tw, "VAZAO\ttotal\tmedia/s\tpico/s\t")
		for _, k := range sortedKeys(s.Meters) {
			m := s.Meters[k]
			fmt.Fprintf(tw, "%s\t%d\t%.1f\t%.1f\t\n", k, m.Count, m.MeanRate, m.PeakRate)
		}
		fmt.Fprintln(tw, "\t")
	}

	if len(s.Topics) > 0 {
		fmt.Fprintln(tw, "DESCARTE POR TOPICO\tpublic.\tdescart.\ttaxa\tentregas\tdescart.\ttaxa\tmotivos\t")
		for _, k := range sortedKeys(s.Topics) {
			t := s.Topics[k]
			fmt.Fprintf(tw, "%s\t%d\t%d\t%.2f%%\t%d\t%d\t%.2f%%\t%s\t\n", k,
				t.Published, t.PublishDropped, 100*t.PublishDropRate,
				t.Delivered, t.DeliveryDropped, 100*t.DeliveryDropRate,
				reasons(t.PublishDrops, t.DeliveryDrops))
		}
		fmt.Fprintln(tw, "\t")
	}

	r := s.Resources
	fmt.Fprintln(tw, "RECURSOS\tatual\tpico\t")
	fmt.Fprintf(tw, "heap (MB)\t%.1f\t%.1f\t\n", r.HeapMB, r.HeapPeakMB)
	fmt.Fprintf(tw, "memoria do SO (MB)\t%.1f\t\t\n", r.SysMB)
	fmt.Fprintf(tw, "goroutines\t%d\t%d\t\n", r.Goroutines, r.GoroutinesPeak)
	if r.CPUAvailable {
		fmt.Fprintf(tw, "CPU (%% de 1 nucleo)\t%.1f\t%.1f\t\n", r.CPUPercent, r.CPUPercentPeak)
		fmt.Fprintf(tw, "CPU total (s)\t%.2f\t\t\n", r.CPUSeconds)
	}
	fmt.Fprintf(tw, "ciclos de GC / pausa total (ms)\t%d\t%.2f\t\n", r.NumGC, r.GCPauseTotalMs)
}

func reasons(maps ...map[string]int64) string {
	var parts []string
	for _, m := range maps {
		for _, k := range sortedKeys(m) {
			parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
		}
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
