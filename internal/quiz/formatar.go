package quiz

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
)

// FormatarPergunta mostra a pergunta com as opções numeradas a partir de 1.
func FormatarPergunta(p Pergunta) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n=== Pergunta %d/%d (%v) ===\n%s\n", p.Numero, p.Total, p.Prazo(), p.Texto)
	for i, o := range p.Opcoes {
		fmt.Fprintf(&b, "  %d) %s\n", i+1, o)
	}
	return b.String()
}

// FormatarPlacar mostra a resposta certa e o ranking.
func FormatarPlacar(pl Placar, max int) string {
	var b strings.Builder
	titulo := "Placar"
	if pl.Final {
		titulo = "PLACAR FINAL"
	}
	fmt.Fprintf(&b, "\n--- %s (pergunta %d/%d) ---\n", titulo, pl.Pergunta, pl.Total)
	if pl.Pergunta > 0 {
		fmt.Fprintf(&b, "Resposta certa: %d) %s | %d de %d jogadores responderam, %d acertaram\n",
			pl.Correta+1, pl.TextoCorreta, pl.Respostas, pl.Jogadores, pl.Acertos)
	}
	for i, pos := range pl.Ranking {
		if max > 0 && i >= max {
			break
		}
		rodada := ""
		if pos.Rodada > 0 {
			rodada = fmt.Sprintf(" (+%d)", pos.Rodada)
		}
		fmt.Fprintf(&b, "  %2d. %-*s %5d%s\n", i+1, MaxNome, pos.Jogador, pos.Pontos, rodada)
	}
	return b.String()
}

// FormatarReacoes mostra o resumo de reações do broker, mais frequentes
// primeiro.
func FormatarReacoes(s aggregator.Summary) string {
	type par struct {
		r string
		n int
	}
	pares := make([]par, 0, len(s.Reactions))
	for r, n := range s.Reactions {
		pares = append(pares, par{r, n})
	}
	sort.Slice(pares, func(i, j int) bool {
		if pares[i].n != pares[j].n {
			return pares[i].n > pares[j].n
		}
		return pares[i].r < pares[j].r
	})
	partes := make([]string, len(pares))
	for i, p := range pares {
		partes[i] = fmt.Sprintf("%s %d", p.r, p.n)
	}
	return fmt.Sprintf("[reacoes] %s (%.0f/s)", strings.Join(partes, "  "), s.PerSecond)
}
