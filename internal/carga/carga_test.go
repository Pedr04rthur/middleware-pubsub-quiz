package carga

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/broker"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
)

func subirBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b, err := broker.New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.SetQuiet(true)
	done := make(chan struct{})
	go func() { b.Run(); close(done) }()
	t.Cleanup(func() { b.Close(); <-done })
	return b
}

// Cenário de carga em escala reduzida contra o broker real: rajadas de
// respostas e um grupo barulhento acima do limite de carga do broker
// (1000 msg/s). Verifica as garantias do middleware para o Cenário C:
//   - o spam dos barulhentos é contido pelo rate limit;
//   - perguntas continuam chegando a todos, com ou sem barulho;
//   - as rajadas de respostas dos jogadores normais não se perdem.
func TestBarulhentosNaoPrejudicamOsDemais(t *testing.T) {
	if testing.Short() {
		t.Skip("teste de carga")
	}
	b := subirBroker(t)

	cfg := DefaultConfig()
	cfg.Broker = b.Addr()
	cfg.Jogadores = 500
	cfg.Barulhentos = 50
	cfg.ReacoesPorBarulhento = 400 // 20.000 reações/s: 20x o limiar de carga do broker
	// Jogadores normais também reagem: antes da correção, com o broker
	// sobrecarregado pelo spam, o balde de todos caía para 1 ficha e uma
	// reação gastava a ficha da resposta.
	cfg.ReacoesPorJogador = 2
	cfg.PerguntasPorFase = 3
	cfg.IntervaloPergunta = 500 * time.Millisecond
	cfg.JanelaRajada = 200 * time.Millisecond
	cfg.Aquecimento = 2 * time.Second
	cfg.MetricasBroker = func() (*metrics.Snapshot, error) {
		s := b.Metrics().Snapshot()
		return &s, nil
	}

	rel, err := Executar(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var texto bytes.Buffer
	rel.Escrever(&texto)
	t.Log(texto.String())

	if rel.Conexoes != 550 || rel.FalhasConexao != 0 {
		t.Fatalf("conexoes: %d abertas, %d falhas", rel.Conexoes, rel.FalhasConexao)
	}
	if len(rel.Fases) != 2 {
		t.Fatalf("%d fases, quer 2", len(rel.Fases))
	}
	base, barulho := rel.Fases[0], rel.Fases[1]

	for _, f := range rel.Fases {
		if f.PerguntasEnviadas != 3 || f.EntregaPerguntas < 0.99 {
			t.Errorf("%s: perguntas entregues %d/%d", f.Nome, f.PerguntasRecebidas, f.PerguntasEsperadas)
		}
		if f.RespostasEnviadas < 1400 || f.EntregaRespostas < 0.99 {
			t.Errorf("%s: respostas %d/%d", f.Nome, f.RespostasRecebidas, f.RespostasEnviadas)
		}
	}
	if base.ReacoesBarulhentos != 0 {
		t.Errorf("barulhentos reagiram na linha de base: %d", base.ReacoesBarulhentos)
	}
	if barulho.ReacoesBarulhentos < 20000 {
		t.Fatalf("barulhentos mandaram so %d reacoes", barulho.ReacoesBarulhentos)
	}
	limitadas := barulho.DescartesBroker["quiz/carga/reacoes"]["rate_limit"]
	if limitadas < barulho.ReacoesBarulhentos/2 {
		t.Errorf("rate limit conteve so %d de %d reacoes de spam", limitadas, barulho.ReacoesBarulhentos)
	}
	if barulho.ReacoesContadas > barulho.ReacoesBarulhentos/2 {
		t.Errorf("%d de %d reacoes de spam chegaram ao telao", barulho.ReacoesContadas, barulho.ReacoesBarulhentos)
	}
	if n := barulho.DescartesBroker["quiz/carga/respostas"]["rate_limit"]; n > 0 {
		t.Errorf("%d respostas de jogadores normais cortadas pelo rate limit", n)
	}
	if imp, _ := rel.Impacto(); imp.P99Ms > 100 {
		t.Errorf("barulho aumentou o p99 das perguntas em %.1f ms", imp.P99Ms)
	}
}

func TestLerCarimbo(t *testing.T) {
	if f, q, ns, ok := lerCarimbo([]byte("1|3|12345")); !ok || f != 1 || q != 3 || ns != 12345 {
		t.Fatalf("lerCarimbo = %d %d %d %v", f, q, ns, ok)
	}
	for _, ruim := range []string{"", "1|2", "a|1|2", "1|2|x", "-1|2|3", "1|2|3|4"} {
		if _, _, _, ok := lerCarimbo([]byte(ruim)); ok {
			t.Errorf("lerCarimbo(%q) aceitou payload invalido", ruim)
		}
	}
}

func TestRelatorioMostraImpacto(t *testing.T) {
	rel := &Relatorio{Fases: []Fase{
		{Nome: FaseSemBarulho, EntregaRespostas: 1},
		{Nome: FaseComBarulho, EntregaRespostas: 0.8},
	}}
	rel.Fases[0].LatenciaPergunta.P99Ms = 10
	rel.Fases[1].LatenciaPergunta.P99Ms = 25
	imp, ok := rel.Impacto()
	if !ok || imp.P99Ms != 15 || imp.EntregaRespostas > -19.99 || imp.EntregaRespostas < -20.01 {
		t.Fatalf("impacto = %+v", imp)
	}
	var b bytes.Buffer
	rel.Escrever(&b)
	if !strings.Contains(b.String(), "p99 +15.00 ms") || !strings.Contains(b.String(), "entrega de respostas: -20.00 pp") {
		t.Fatalf("relatorio sem o impacto:\n%s", b.String())
	}
}
