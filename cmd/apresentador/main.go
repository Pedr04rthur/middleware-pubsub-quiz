// Comando apresentador abre uma sala do quiz ao vivo, espera os jogadores
// entrarem, dispara as perguntas e publica o placar ao fim de cada uma.
//
//	go run ./cmd/broker
//	go run ./cmd/apresentador -sala sala1
//	go run ./cmd/jogador -sala sala1 -nome Ana      (em outro terminal)
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/quiz"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:8080", "endereco UDP do broker primario")
	fallbacks := flag.String("fallback", "", "instancias reserva separadas por virgula (ex.: localhost:8081)")
	sala := flag.String("sala", "sala1", "nome da sala")
	arquivo := flag.String("perguntas", "", "arquivo JSON com as perguntas (vazio usa as perguntas padrao)")
	prazo := flag.Duration("prazo", 10*time.Second, "tempo para responder cada pergunta (5s a 20s)")
	intervalo := flag.Duration("intervalo", 4*time.Second, "pausa entre o placar e a proxima pergunta")
	espera := flag.Duration("espera", 0, "tempo de lobby antes da primeira pergunta (0 = esperar ENTER)")
	flag.Parse()

	questoes := quiz.PerguntasPadrao
	if *arquivo != "" {
		var err error
		if questoes, err = quiz.CarregarPerguntas(*arquivo); err != nil {
			log.Fatal(err)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cfg := client.DefaultConfig()
	if *fallbacks != "" {
		cfg.Fallbacks = strings.Split(*fallbacks, ",")
	}
	cfg.OnFailover = func(ev client.FailoverEvent) {
		fmt.Printf("[FAILOVER] broker %s caiu; sala restaurada em %s em %v\n", ev.From, ev.To, ev.Downtime.Round(time.Millisecond))
	}
	c, err := client.Connect(*brokerAddr, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Disconnect()

	p := quiz.LimitarPrazo(*prazo)
	ap, err := quiz.AbrirSala(c, quiz.ConfigApresentador{
		Sala:      *sala,
		Questoes:  questoes,
		Prazo:     p,
		Intervalo: *intervalo,
		Eventos: quiz.Eventos{
			Entrada: func(jogador string, total int) {
				fmt.Printf("+ %s entrou na sala (%d jogadores)\n", jogador, total)
			},
			Pergunta: func(q quiz.Pergunta) { fmt.Print(quiz.FormatarPergunta(q)) },
			Resposta: func(jogador string, pontos int, err error) {
				switch {
				case err != nil:
					fmt.Printf("  x %s: %v\n", jogador, err)
				case pontos > 0:
					fmt.Printf("  %s acertou (+%d)\n", jogador, pontos)
				default:
					fmt.Printf("  %s errou\n", jogador)
				}
			},
			Reacoes: func(s aggregator.Summary) { fmt.Println(quiz.FormatarReacoes(s)) },
			Placar:  func(pl quiz.Placar) { fmt.Print(quiz.FormatarPlacar(pl, 0)) },
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Sala %q aberta com %d perguntas de %v.\n", *sala, len(questoes), p)
	fmt.Printf("Jogadores entram com: go run ./cmd/jogador -sala %s -nome SEU_NOME\n", *sala)
	if err := lobby(ctx, *espera); err == nil {
		final, err := ap.Jogar(ctx)
		if err == nil {
			fmt.Printf("\nFim de jogo! Vencedor: %s\n", vencedor(final))
			time.Sleep(time.Second) // dá tempo do placar final chegar antes de apagar a sala
		} else if !errors.Is(err, context.Canceled) {
			fmt.Println("Erro na partida:", err)
		}
	}

	if err := ap.Encerrar(); err != nil {
		fmt.Println("Erro ao encerrar a sala:", err)
	} else {
		fmt.Printf("Sala %q encerrada.\n", *sala)
	}
}

// lobby espera o tempo dado ou, se zero, o ENTER do apresentador.
func lobby(ctx context.Context, espera time.Duration) error {
	if espera > 0 {
		fmt.Printf("A primeira pergunta sai em %v...\n", espera)
		select {
		case <-time.After(espera):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fmt.Println("Pressione ENTER para comecar.")
	enter := make(chan struct{})
	go func() {
		bufio.NewReader(os.Stdin).ReadString('\n')
		close(enter)
	}()
	select {
	case <-enter:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func vencedor(pl quiz.Placar) string {
	if len(pl.Ranking) == 0 || pl.Ranking[0].Pontos == 0 {
		return "ninguem pontuou"
	}
	return fmt.Sprintf("%s com %d pontos", pl.Ranking[0].Jogador, pl.Ranking[0].Pontos)
}
