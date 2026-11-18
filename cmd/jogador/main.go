// Comando jogador entra em uma sala do quiz ao vivo. No modo interativo, o
// jogador digita o número da opção para responder; qualquer outro texto
// (ex.: um emoji) é enviado como reação. Com -bots N, simula N jogadores que
// respondem sozinhos.
//
//	go run ./cmd/jogador -sala sala1 -nome Ana
//	go run ./cmd/jogador -sala sala1 -bots 20 -reacoes 2
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/quiz"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

type opcoes struct {
	broker    string
	fallbacks []string
	sala      string
	nome      string
	bots      int
	reacoes   float64
	atraso    float64
}

func main() {
	var o opcoes
	var fallbacks string
	flag.StringVar(&o.broker, "broker", "localhost:8080", "endereco UDP do broker primario")
	flag.StringVar(&fallbacks, "fallback", "", "instancias reserva separadas por virgula (ex.: localhost:8081)")
	flag.StringVar(&o.sala, "sala", "sala1", "nome da sala")
	flag.StringVar(&o.nome, "nome", "", "seu nome no placar (vazio sorteia um)")
	flag.IntVar(&o.bots, "bots", 0, "simula N jogadores automaticos em vez do modo interativo")
	flag.Float64Var(&o.reacoes, "reacoes", 0, "reacoes por segundo de cada bot (0 desliga)")
	flag.Float64Var(&o.atraso, "atraso", 0.6, "fracao do prazo ate a qual cada bot demora para responder")
	flag.Parse()
	if fallbacks != "" {
		o.fallbacks = strings.Split(fallbacks, ",")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if o.bots > 0 {
		rodarBots(ctx, o)
		return
	}
	rodarInterativo(ctx, o)
}

func conectar(o opcoes, avisarFailover bool) (*client.Client, error) {
	cfg := client.DefaultConfig()
	cfg.Fallbacks = o.fallbacks
	if avisarFailover {
		cfg.OnFailover = func(ev client.FailoverEvent) {
			fmt.Printf("[FAILOVER] broker %s caiu; continuando em %s\n", ev.From, ev.To)
		}
	}
	return client.Connect(o.broker, cfg)
}

func rodarInterativo(ctx context.Context, o opcoes) {
	if o.nome == "" {
		o.nome = fmt.Sprintf("jogador-%04d", rand.N(10000))
	}
	c, err := conectar(o, true)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Disconnect()

	j, err := quiz.EntrarNaSala(c, o.sala, o.nome, quiz.EventosJogador{
		Pergunta: func(p quiz.Pergunta) {
			fmt.Print(quiz.FormatarPergunta(p))
			fmt.Printf("Digite o numero da opcao (1-%d). Outro texto vira reacao.\n> ", len(p.Opcoes))
		},
		Placar: func(pl quiz.Placar, eu quiz.MeuResultado) {
			fmt.Print(quiz.FormatarPlacar(pl, 5))
			fmt.Println(resultado(eu))
		},
		Reacoes: func(s aggregator.Summary) { fmt.Println(quiz.FormatarReacoes(s)) },
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Voce entrou na sala %q como %q. Aguardando o apresentador...\n", o.sala, j.Nome())

	linhas := make(chan string)
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			linhas <- strings.TrimSpace(s.Text())
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case pl := <-j.Fim():
			if eu := posicao(pl, j.Nome()); eu > 0 {
				fmt.Printf("\nFim de jogo! Voce terminou em %do lugar.\n", eu)
			} else {
				fmt.Println("\nFim de jogo!")
			}
			return
		case linha := <-linhas:
			if linha == "" {
				continue
			}
			if n, err := strconv.Atoi(linha); err == nil {
				if err := j.Responder(n - 1); err != nil {
					fmt.Println("Resposta nao enviada:", err)
				} else {
					fmt.Println("Resposta enviada! Aguarde o placar.")
				}
				continue
			}
			if err := j.Reagir(linha); err != nil {
				fmt.Println("Reacao nao enviada:", err)
			}
		}
	}
}

func resultado(eu quiz.MeuResultado) string {
	var s string
	switch {
	case !eu.Respondeu:
		s = "Voce nao respondeu."
	case eu.Acertou:
		s = "Voce ACERTOU!"
	default:
		s = "Voce errou."
	}
	if eu.Posicao > 0 {
		s += fmt.Sprintf(" Voce esta em %do lugar com %d pontos.", eu.Posicao, eu.Pontos)
	}
	return s
}

func posicao(pl quiz.Placar, nome string) int {
	for i, p := range pl.Ranking {
		if p.Jogador == nome {
			return i + 1
		}
	}
	return 0
}

var emojis = []string{"👏", "😂", "🔥", "😮", "❤️"}

// rodarBots simula vários jogadores, cada um com seu próprio socket (como
// jogadores reais em máquinas diferentes).
func rodarBots(ctx context.Context, o opcoes) {
	prefixo := o.nome
	if prefixo == "" {
		prefixo = "bot"
	}

	var wg sync.WaitGroup
	var clientes []*client.Client
	var placarFinal quiz.Placar
	var once sync.Once
	fim := make(chan struct{})

	for i := 1; i <= o.bots; i++ {
		c, err := conectar(o, i == 1)
		if err != nil {
			log.Fatal(err)
		}
		clientes = append(clientes, c)
		nome := fmt.Sprintf("%s-%02d", prefixo, i)

		cfgBot := quiz.ConfigBot{AtrasoMax: o.atraso}
		if i == 1 { // um bot só narra a partida, para não repetir N vezes
			cfgBot.Eventos = quiz.EventosJogador{
				Pergunta: func(p quiz.Pergunta) { fmt.Printf("Pergunta %d/%d: %s\n", p.Numero, p.Total, p.Texto) },
				Placar: func(pl quiz.Placar, _ quiz.MeuResultado) {
					fmt.Printf("  placar: %d de %d jogadores responderam, %d acertaram\n", pl.Respostas, pl.Jogadores, pl.Acertos)
				},
			}
		}
		j, err := quiz.EntrarComoBot(c, o.sala, nome, cfgBot)
		if err != nil {
			if errors.Is(err, quiz.ErrSalaInexistente) {
				log.Fatalf("sala %q: %v", o.sala, err)
			}
			log.Fatal(err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			reagir(j, o.reacoes, fim)
		}()
		go func() {
			select {
			case pl := <-j.Fim():
				once.Do(func() { placarFinal = pl; close(fim) })
			case <-fim:
			}
		}()
	}
	fmt.Printf("%d bots entraram na sala %q. Aguardando o apresentador...\n", o.bots, o.sala)

	select {
	case <-fim:
		fmt.Print(quiz.FormatarPlacar(placarFinal, 0))
	case <-ctx.Done():
		once.Do(func() { close(fim) })
	}
	wg.Wait()
	for _, c := range clientes {
		c.Disconnect()
	}
}

func reagir(j *quiz.Jogador, porSegundo float64, fim <-chan struct{}) {
	if porSegundo <= 0 {
		return
	}
	intervalo := time.Duration(float64(time.Second) / porSegundo)
	time.Sleep(rand.N(intervalo)) // defasagem para os bots não reagirem juntos
	t := time.NewTicker(intervalo)
	defer t.Stop()
	for {
		select {
		case <-fim:
			return
		case <-t.C:
			j.Reagir(emojis[rand.N(len(emojis))])
		}
	}
}
