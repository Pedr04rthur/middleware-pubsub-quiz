package bench

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/carga"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
)

// ConfigRuido define o cenário de latência com e sem barulho.
type ConfigRuido struct {
	Jogadores            []int // um teste para cada quantidade
	Barulhentos          int
	ReacoesPorBarulhento float64
	ReacoesPorJogador    float64
	Repeticoes           int
	PerguntasPorFase     int
	IntervaloPergunta    time.Duration
	JanelaRajada         time.Duration
}

// DefaultRuido: 1000 e 3000 jogadores, 200 barulhentos a 100 reações/s.
func DefaultRuido() ConfigRuido {
	return ConfigRuido{
		Jogadores:            []int{1000, 3000},
		Barulhentos:          200,
		ReacoesPorBarulhento: 100,
		ReacoesPorJogador:    0.5,
		Repeticoes:           3,
		PerguntasPorFase:     5,
		IntervaloPergunta:    2 * time.Second,
		JanelaRajada:         300 * time.Millisecond,
	}
}

// LinhaRuido é a mediana das repetições para uma quantidade de jogadores.
type LinhaRuido struct {
	Jogadores   int `json:"jogadores"`
	Barulhentos int `json:"barulhentos"`
	Repeticoes  int `json:"repeticoes"`

	// Medianas entre as repetições, por fase: [0] sem barulho, [1] com barulho.
	P50Ms            [2]float64 `json:"p50_ms"`
	P95Ms            [2]float64 `json:"p95_ms"`
	P99Ms            [2]float64 `json:"p99_ms"`
	EntregaPerguntas [2]float64 `json:"entrega_perguntas"`
	EntregaRespostas [2]float64 `json:"entrega_respostas"`
	PicoRespostas    [2]float64 `json:"pico_respostas_por_s"`
	// SpamContido é a fração das reações dos barulhentos cortada pelo rate limit.
	SpamContido float64 `json:"spam_contido"`

	Execucoes []*carga.Relatorio `json:"execucoes"`
}

// Ruido roda o gerador de carga contra um broker novo para cada repetição.
func Ruido(ctx context.Context, cfg ConfigRuido, log io.Writer) ([]LinhaRuido, error) {
	var linhas []LinhaRuido
	for _, n := range cfg.Jogadores {
		l := LinhaRuido{Jogadores: n, Barulhentos: cfg.Barulhentos, Repeticoes: cfg.Repeticoes}
		var col [6][2][]float64
		var contido []float64
		for r := 1; r <= cfg.Repeticoes; r++ {
			logf(log, "[ruido] %d jogadores, repeticao %d/%d...\n", n, r, cfg.Repeticoes)
			b, parar, err := subirBroker()
			if err != nil {
				return nil, err
			}
			cc := carga.DefaultConfig()
			cc.Broker = b.Addr()
			cc.Sala = fmt.Sprintf("ruido%d", r)
			cc.Jogadores = n
			cc.Barulhentos = cfg.Barulhentos
			cc.ReacoesPorBarulhento = cfg.ReacoesPorBarulhento
			cc.ReacoesPorJogador = cfg.ReacoesPorJogador
			cc.PerguntasPorFase = cfg.PerguntasPorFase
			cc.IntervaloPergunta = cfg.IntervaloPergunta
			cc.JanelaRajada = cfg.JanelaRajada
			cc.MetricasBroker = func() (*metrics.Snapshot, error) {
				s := b.Metrics().Snapshot()
				return &s, nil
			}
			rel, err := carga.Executar(ctx, cc)
			parar()
			if err != nil {
				return nil, err
			}
			l.Execucoes = append(l.Execucoes, rel)
			for i, f := range rel.Fases {
				col[0][i] = append(col[0][i], f.LatenciaPergunta.P50Ms)
				col[1][i] = append(col[1][i], f.LatenciaPergunta.P95Ms)
				col[2][i] = append(col[2][i], f.LatenciaPergunta.P99Ms)
				col[3][i] = append(col[3][i], f.EntregaPerguntas)
				col[4][i] = append(col[4][i], f.EntregaRespostas)
				col[5][i] = append(col[5][i], f.PicoRespostasPorSeg)
			}
			if len(rel.Fases) == 2 && rel.Fases[1].ReacoesBarulhentos > 0 {
				f := rel.Fases[1]
				cortadas := f.DescartesBroker["quiz/"+cc.Sala+"/reacoes"]["rate_limit"]
				contido = append(contido, float64(cortadas)/float64(f.ReacoesBarulhentos))
			}
			esperar(ctx, time.Second) // deixa o SO liberar as portas
		}
		for i := 0; i < 2; i++ {
			l.P50Ms[i] = mediana(col[0][i])
			l.P95Ms[i] = mediana(col[1][i])
			l.P99Ms[i] = mediana(col[2][i])
			l.EntregaPerguntas[i] = mediana(col[3][i])
			l.EntregaRespostas[i] = mediana(col[4][i])
			l.PicoRespostas[i] = mediana(col[5][i])
		}
		l.SpamContido = mediana(contido)
		linhas = append(linhas, l)
	}
	return linhas, nil
}

// TabelaRuido formata o resultado em Markdown.
func TabelaRuido(linhas []LinhaRuido) string {
	var b strings.Builder
	b.WriteString("| Jogadores | Fase | p50 | p95 | p99 | Perguntas entregues | Respostas entregues | Pico de respostas |\n")
	b.WriteString("|---:|---|---:|---:|---:|---:|---:|---:|\n")
	for _, l := range linhas {
		for i, fase := range []string{"sem barulho", fmt.Sprintf("com %d barulhentos", l.Barulhentos)} {
			fmt.Fprintf(&b, "| %d | %s | %.1f ms | %.1f ms | %.1f ms | %.2f%% | %.2f%% | %.0f/s |\n",
				l.Jogadores, fase, l.P50Ms[i], l.P95Ms[i], l.P99Ms[i],
				100*l.EntregaPerguntas[i], 100*l.EntregaRespostas[i], l.PicoRespostas[i])
		}
	}
	return b.String()
}
