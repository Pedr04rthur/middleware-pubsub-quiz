package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLatencyPercentiles(t *testing.T) {
	l := NewLatency(1000)
	for i := 100; i >= 1; i-- {
		l.Observe(time.Duration(i) * time.Millisecond)
	}

	s := l.Snapshot()
	if s.Count != 100 || s.P50Ms != 50 || s.P95Ms != 95 || s.P99Ms != 99 || s.MinMs != 1 || s.MaxMs != 100 || s.MeanMs != 50.5 {
		t.Fatalf("percentis errados para 1..100ms: %+v", s)
	}
}

func TestLatencyEmpty(t *testing.T) {
	if s := NewLatency(10).Snapshot(); s != (LatencySnapshot{}) {
		t.Fatalf("coletor vazio deveria zerar tudo, obtido %+v", s)
	}
}

func TestLatencyReservoirKeepsMemoryBounded(t *testing.T) {
	l := NewLatency(1000)
	for i := 0; i < 100_000; i++ {
		l.Observe(time.Duration(i%100+1) * time.Millisecond)
	}

	if len(l.samples) != 1000 {
		t.Fatalf("deveria guardar só 1000 amostras, guarda %d", len(l.samples))
	}
	s := l.Snapshot()
	if s.Count != 100_000 || s.MaxMs != 100 {
		t.Fatalf("contagem e máximo devem considerar tudo: %+v", s)
	}
	if s.P50Ms < 40 || s.P50Ms > 60 || s.P99Ms < 95 {
		t.Fatalf("a amostra deveria representar a distribuição uniforme 1..100ms: %+v", s)
	}
}

func TestLatencyConcurrentObserve(t *testing.T) {
	l := NewLatency(100)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				l.Observe(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if s := l.Snapshot(); s.Count != 8000 {
		t.Fatalf("esperado 8000 amostras, obtido %d", s.Count)
	}
}

func TestMeterRates(t *testing.T) {
	m := NewMeter()
	m.Mark(100)
	m.tick(time.Second)
	m.Mark(300)
	m.tick(time.Second)
	m.Mark(50)
	m.tick(500 * time.Millisecond)

	s := m.Snapshot()
	if s.Count != 450 || s.LastRate != 100 || s.PeakRate != 300 {
		t.Fatalf("vazão errada: %+v", s)
	}
}

func TestTopicDropRates(t *testing.T) {
	tp := NewTopics(10)
	for i := 0; i < 10; i++ {
		tp.Published("quiz/s1/respostas")
	}
	tp.PublishDropped("quiz/s1/respostas", "fora_da_janela")
	tp.PublishDropped("quiz/s1/respostas", "rate_limit")
	for i := 0; i < 30; i++ {
		tp.Delivered("quiz/s1/respostas")
	}
	for i := 0; i < 10; i++ {
		tp.DeliveryDropped("quiz/s1/respostas", "expirada")
	}

	s := tp.Snapshot()["quiz/s1/respostas"]
	if s.PublishDropped != 2 || s.PublishDropRate != 0.2 {
		t.Fatalf("taxa de descarte de publicação errada: %+v", s)
	}
	if s.DeliveryDropped != 10 || s.DeliveryDropRate != 0.25 {
		t.Fatalf("taxa de descarte de entrega errada: %+v", s)
	}
	if s.PublishDrops["fora_da_janela"] != 1 || s.DeliveryDrops["expirada"] != 10 {
		t.Fatalf("motivos errados: %+v", s)
	}
}

func TestTopicsAreCapped(t *testing.T) {
	tp := NewTopics(3)
	for i := 0; i < 100; i++ {
		tp.Published(fmt.Sprintf("aleatorio/%d", i))
	}

	s := tp.Snapshot()
	if len(s) != 4 || s[OtherTopics].Published != 97 {
		t.Fatalf("esperado 3 tópicos + %q com 97, obtido %d tópicos: %+v", OtherTopics, len(s), s[OtherTopics])
	}
}

func TestResourcesMeasureCPUAndMemory(t *testing.T) {
	r := NewResources()
	deadline := time.Now().Add(200 * time.Millisecond)
	x := 0
	for time.Now().Before(deadline) {
		x++
	}
	_ = make([]byte, 8<<20)
	r.Sample()

	s := r.Snapshot()
	if s.Goroutines < 1 || s.HeapPeakMB <= 0 || s.SysMB <= 0 {
		t.Fatalf("recursos não medidos: %+v", s)
	}
	if s.CPUAvailable && (s.CPUPercent < 20 || s.CPUSeconds <= 0) {
		t.Fatalf("um laço ocupado de 200ms deveria aparecer na CPU: %+v", s)
	}
	t.Logf("recursos: %+v", s)
}

func TestRegistryHandlerAndReport(t *testing.T) {
	r := NewRegistry()
	r.Latency("entrega").Observe(3 * time.Millisecond)
	r.Meter("respostas").Mark(42)
	r.Topics.Published("quiz/s1/perguntas")
	r.Tick(time.Second)

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	var s Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("o endpoint deveria devolver JSON: %v\n%s", err, rec.Body)
	}
	if s.Meters["respostas"].Count != 42 || s.Latencies["entrega"].Count != 1 || s.Topics["quiz/s1/perguntas"].Published != 1 {
		t.Fatalf("snapshot incompleto: %+v", s)
	}

	var buf bytes.Buffer
	s.WriteText(&buf)
	for _, want := range []string{"LATENCIA", "VAZAO", "DESCARTE POR TOPICO", "RECURSOS", "quiz/s1/perguntas"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("relatório sem %q:\n%s", want, buf.String())
		}
	}
}
