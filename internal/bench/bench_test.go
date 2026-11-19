package bench

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestMediana(t *testing.T) {
	casos := []struct {
		xs   []float64
		quer float64
	}{
		{nil, 0},
		{[]float64{3}, 3},
		{[]float64{5, 1, 3}, 3},
		{[]float64{4, 1, 3, 2}, 2.5},
	}
	for _, c := range casos {
		if got := mediana(c.xs); got != c.quer {
			t.Errorf("mediana(%v) = %v, quer %v", c.xs, got, c.quer)
		}
	}
}

func TestCarimbo(t *testing.T) {
	seq, ns, ok := lerCarimbo(carimbo(42))
	if !ok || seq != 42 || ns <= 0 {
		t.Fatalf("lerCarimbo(carimbo(42)) = %d %d %v", seq, ns, ok)
	}
	if _, _, ok := lerCarimbo([]byte("sem-barra")); ok {
		t.Fatal("payload sem carimbo aceito")
	}
}

// Versão pequena do cenário de failover: todos os clientes têm que migrar
// para a reserva e voltar a receber perguntas, e a interrupção tem que ficar
// perto do HeartbeatTimeout.
func TestFailoverPequeno(t *testing.T) {
	if testing.Short() {
		t.Skip("cenario de failover")
	}
	cfg := DefaultFailover()[1] // heartbeat rápido: 100ms / 400ms
	cfg.Jogadores = 30
	cfg.Antes, cfg.Depois = time.Second, 2*time.Second

	r, err := Failover(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Migraram != cfg.Jogadores+1 || r.SemRecuperar != 0 {
		t.Fatalf("migraram %d de %d, %d sem recuperar", r.Migraram, cfg.Jogadores+1, r.SemRecuperar)
	}
	if r.Interrupcao.P50Ms < 300 || r.Interrupcao.MaxMs > 2000 {
		t.Fatalf("interrupcao inesperada: %+v", r.Interrupcao)
	}
	if !strings.Contains(TabelaFailover([]*ResultadoFailover{r}), "100ms / 400ms") {
		t.Fatal("tabela sem a configuracao do heartbeat")
	}
}

func TestConexoesPequeno(t *testing.T) {
	if testing.Short() {
		t.Skip("cenario de conexoes")
	}
	cfg := DefaultConexoes()
	cfg.Degraus = []int{50, 100}
	cfg.Perguntas, cfg.IntervaloPergunta, cfg.Estabilizar = 2, 200*time.Millisecond, 300*time.Millisecond

	ds, err := Conexoes(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 {
		t.Fatalf("%d degraus, quer 2", len(ds))
	}
	for _, d := range ds {
		if d.Degradou || d.Conectados != d.Clientes || d.Entrega < 0.99 {
			t.Fatalf("degrau %d: %+v", d.Clientes, d)
		}
	}
}
