package aggregator

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type emitted struct {
	topic   string
	summary Summary
	bytes   int
}

func collect(t *testing.T) (Emitter, *[]emitted) {
	t.Helper()
	var out []emitted
	return func(topic string, payload []byte) {
		var s Summary
		if err := json.Unmarshal(payload, &s); err != nil {
			t.Fatalf("resumo não é JSON válido: %v (%s)", err, payload)
		}
		out = append(out, emitted{topic, s, len(payload)})
	}, &out
}

func TestBurstBecomesOneSummaryPerRoom(t *testing.T) {
	emit, out := collect(t)
	a := New(DefaultConfig(), emit)

	for i := 0; i < 300; i++ {
		a.Add("quiz/sala1/reacoes", []byte("👏"))
	}
	for i := 0; i < 100; i++ {
		a.Add("quiz/sala1/reacoes", []byte("😂"))
	}
	for i := 0; i < 50; i++ {
		a.Add("quiz/sala2/reacoes", []byte("🔥"))
	}
	a.Flush(time.Second)

	if len(*out) != 2 {
		t.Fatalf("deveria haver 1 resumo por sala, houve %d", len(*out))
	}
	for _, e := range *out {
		switch e.topic {
		case "quiz/sala1/reacoes":
			if e.summary.Total != 400 || e.summary.Reactions["👏"] != 300 || e.summary.Reactions["😂"] != 100 || e.summary.PerSecond != 400 {
				t.Fatalf("resumo da sala1 errado: %+v", e.summary)
			}
		case "quiz/sala2/reacoes":
			if e.summary.Total != 50 || e.summary.Reactions["🔥"] != 50 {
				t.Fatalf("resumo da sala2 errado: %+v", e.summary)
			}
		default:
			t.Fatalf("tópico inesperado %q", e.topic)
		}
	}
}

func TestFlushResetsAndSkipsQuietRooms(t *testing.T) {
	emit, out := collect(t)
	a := New(DefaultConfig(), emit)

	a.Add("quiz/sala1/reacoes", []byte("👏"))
	a.Flush(time.Second)
	a.Flush(time.Second)

	if len(*out) != 1 {
		t.Fatalf("a segunda janela, sem reações, não deveria emitir nada; emitidos %d", len(*out))
	}
}

func TestInvalidOrExcessiveReactionsGoToOther(t *testing.T) {
	emit, out := collect(t)
	cfg := DefaultConfig()
	cfg.MaxKinds = 2
	a := New(cfg, emit)

	a.Add("t", []byte("👏"))
	a.Add("t", []byte("😂"))
	a.Add("t", []byte("🔥"))
	a.Add("t", []byte(strings.Repeat("x", 100)))
	a.Add("t", []byte{0xff, 0xfe})
	a.Add("t", nil)
	a.Add("t", []byte("👏"))
	a.Flush(time.Second)

	r := (*out)[0].summary.Reactions
	if r["👏"] != 2 || r["😂"] != 1 || r[Other] != 4 || len(r) != 3 {
		t.Fatalf("contagem inesperada: %v", r)
	}
}

func TestSummarySavesBandwidth(t *testing.T) {
	emit, out := collect(t)
	a := New(DefaultConfig(), emit)

	const reactions = 1000
	rawBytes := 0
	emojis := []string{"👏", "😂", "🔥", "😮", "❤️"}
	for i := 0; i < reactions; i++ {
		e := emojis[i%len(emojis)]
		a.Add("quiz/sala1/reacoes", []byte(e))
		rawBytes += len(e)
	}
	a.Flush(time.Second)

	summary := (*out)[0].bytes
	t.Logf("%d reações (%d bytes de emoji) viraram 1 resumo de %d bytes", reactions, rawBytes, summary)
	if summary*10 > rawBytes {
		t.Fatalf("o resumo (%d bytes) deveria ser bem menor que as reações (%d bytes)", summary, rawBytes)
	}
}
