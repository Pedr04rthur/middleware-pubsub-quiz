package carga

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// Escrever imprime o relatório em texto, com as duas fases lado a lado.
func (r *Relatorio) Escrever(w io.Writer) {
	fmt.Fprintln(w, "\n================ RELATORIO DO GERADOR DE CARGA ================")
	chaves := make([]string, 0, len(r.Config))
	for k := range r.Config {
		chaves = append(chaves, k)
	}
	sort.Strings(chaves)
	for _, k := range chaves {
		fmt.Fprintf(w, "%-24s %s\n", k, r.Config[k])
	}
	fmt.Fprintf(w, "\nconexoes: %d abertas em %s (%.0f/s), %d falhas\n",
		r.Conexoes, r.TempoConexao, r.ConexoesPorSeg, r.FalhasConexao)

	if len(r.Fases) == 0 {
		return
	}
	nomes := make([]string, len(r.Fases))
	for i, f := range r.Fases {
		nomes[i] = f.Nome
	}
	linha := func(rotulo string, valor func(Fase) string) {
		fmt.Fprintf(w, "%-34s", rotulo)
		for _, f := range r.Fases {
			fmt.Fprintf(w, " %16s", valor(f))
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "\n%-34s", "")
	for _, n := range nomes {
		fmt.Fprintf(w, " %16s", n)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, strings.Repeat("-", 34+17*len(r.Fases)))

	inteiro := func(n int64) string { return fmt.Sprintf("%d", n) }
	pct := func(x float64) string { return fmt.Sprintf("%.2f%%", 100*x) }
	msf := func(x float64) string { return fmt.Sprintf("%.2f ms", x) }

	fmt.Fprintln(w, "PERGUNTAS (apresentador -> jogadores)")
	linha("  entregues / esperadas", func(f Fase) string {
		return fmt.Sprintf("%d/%d", f.PerguntasRecebidas, f.PerguntasEsperadas)
	})
	linha("  taxa de entrega", func(f Fase) string { return pct(f.EntregaPerguntas) })
	linha("  latencia p50", func(f Fase) string { return msf(f.LatenciaPergunta.P50Ms) })
	linha("  latencia p95", func(f Fase) string { return msf(f.LatenciaPergunta.P95Ms) })
	linha("  latencia p99", func(f Fase) string { return msf(f.LatenciaPergunta.P99Ms) })
	linha("  latencia max", func(f Fase) string { return msf(f.LatenciaPergunta.MaxMs) })

	fmt.Fprintln(w, "RESPOSTAS (rajada jogadores -> apurador)")
	linha("  enviadas", func(f Fase) string { return inteiro(f.RespostasEnviadas) })
	linha("  recebidas", func(f Fase) string { return inteiro(f.RespostasRecebidas) })
	linha("  taxa de entrega", func(f Fase) string { return pct(f.EntregaRespostas) })
	linha("  pico de vazao", func(f Fase) string { return fmt.Sprintf("%.0f/s", f.PicoRespostasPorSeg) })
	linha("  latencia p99", func(f Fase) string { return msf(f.LatenciaResposta.P99Ms) })

	fmt.Fprintln(w, "REACOES")
	linha("  enviadas pelos barulhentos", func(f Fase) string { return inteiro(f.ReacoesBarulhentos) })
	linha("  enviadas pelos jogadores", func(f Fase) string { return inteiro(f.ReacoesJogadores) })
	linha("  contadas nos resumos do broker", func(f Fase) string { return inteiro(f.ReacoesContadas) })
	linha("  resumos recebidos pelo telao", func(f Fase) string { return inteiro(f.Resumos) })

	temBroker := false
	for _, f := range r.Fases {
		temBroker = temBroker || len(f.DescartesBroker) > 0
	}
	if temBroker {
		fmt.Fprintln(w, "DESCARTES NO BROKER (topico: motivo)")
		chaves := map[string]bool{}
		for _, f := range r.Fases {
			for t, ms := range f.DescartesBroker {
				for m := range ms {
					chaves[t+": "+m] = true
				}
			}
		}
		lista := make([]string, 0, len(chaves))
		for k := range chaves {
			lista = append(lista, k)
		}
		sort.Strings(lista)
		for _, k := range lista {
			t, m, _ := strings.Cut(k, ": ")
			linha("  "+strings.TrimPrefix(t, "quiz/")+": "+m, func(f Fase) string { return inteiro(f.DescartesBroker[t][m]) })
		}
	} else if r.ErroBroker != "" {
		fmt.Fprintln(w, "metricas do broker indisponiveis:", r.ErroBroker)
	} else {
		fmt.Fprintln(w, "(use -metricas para incluir os descartes do lado do broker)")
	}

	if imp, ok := r.Impacto(); ok {
		fmt.Fprintln(w, "\nIMPACTO DOS BARULHENTOS sobre os demais (com_barulho - sem_barulho)")
		fmt.Fprintf(w, "  latencia das perguntas: p50 %+.2f ms | p95 %+.2f ms | p99 %+.2f ms\n", imp.P50Ms, imp.P95Ms, imp.P99Ms)
		fmt.Fprintf(w, "  entrega de perguntas: %+.2f pp | entrega de respostas: %+.2f pp\n", imp.EntregaPerguntas, imp.EntregaRespostas)
	}

	s := r.Cliente
	fmt.Fprintf(w, "\nbiblioteca cliente: publicadas=%d recebidas=%d fila_local_cheia=%d reenvios=%d failovers=%d\n",
		s.Published, s.Received, s.InboxDropped, s.Retransmitted, s.Failovers)
	g := r.Gerador
	fmt.Fprintf(w, "gerador: goroutines pico=%d heap pico=%.1f MB cpu pico=%.0f%%\n",
		g.GoroutinesPeak, g.HeapPeakMB, g.CPUPercentPeak)
}
