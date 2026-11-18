package quiz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/broker"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

func subirBroker(t *testing.T) string {
	t.Helper()
	b, err := broker.New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.SetQuiet(true)
	done := make(chan struct{})
	go func() { b.Run(); close(done) }()
	t.Cleanup(func() { b.Close(); <-done })
	return b.Addr()
}

func conectar(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Connect(addr, client.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Disconnect() })
	return c
}

// Partida completa contra o broker real: dois jogadores entram, um acerta
// tudo e outro erra tudo, e todos recebem o mesmo placar final.
func TestPartidaCompleta(t *testing.T) {
	addr := subirBroker(t)

	var mu sync.Mutex
	entradas := map[string]bool{}
	var perguntasVistas []int
	ap, err := AbrirSala(conectar(t, addr), ConfigApresentador{
		Sala:      "teste",
		Questoes:  questoesTeste,
		Prazo:     400 * time.Millisecond,
		Intervalo: 100 * time.Millisecond,
		Eventos: Eventos{
			Entrada: func(j string, _ int) { mu.Lock(); entradas[j] = true; mu.Unlock() },
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	certa := func(p Pergunta) int { return questoesTeste[p.Numero-1].Correta }
	errada := func(p Pergunta) int { return (certa(p) + 1) % len(p.Opcoes) }
	ana, err := EntrarComoBot(conectar(t, addr), "teste", "ana", ConfigBot{
		Escolher: certa,
		Eventos: EventosJogador{Pergunta: func(p Pergunta) {
			mu.Lock()
			perguntasVistas = append(perguntasVistas, p.Numero)
			mu.Unlock()
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var resultadosBia []MeuResultado
	bia, err := EntrarComoBot(conectar(t, addr), "teste", "bia", ConfigBot{
		Escolher: errada,
		Eventos: EventosJogador{Placar: func(_ Placar, r MeuResultado) {
			mu.Lock()
			resultadosBia = append(resultadosBia, r)
			mu.Unlock()
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	esperarAte(t, "entrada dos dois jogadores no lobby", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return entradas["ana"] && entradas["bia"]
	})

	final, err := ap.Jogar(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !final.Final || final.Pergunta != 2 {
		t.Fatalf("placar final = %+v", final)
	}
	if len(final.Ranking) != 2 || final.Ranking[0].Jogador != "ana" || final.Ranking[0].Pontos < 2*PontosBase || final.Ranking[1].Pontos != 0 {
		t.Fatalf("ranking final = %+v", final.Ranking)
	}

	for _, j := range []*Jogador{ana, bia} {
		select {
		case pl := <-j.Fim():
			if pl.Ranking[0].Jogador != "ana" {
				t.Fatalf("%s recebeu ranking %+v", j.Nome(), pl.Ranking)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s nao recebeu o placar final", j.Nome())
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(perguntasVistas) != 2 {
		t.Fatalf("ana viu as perguntas %v, quer [1 2]", perguntasVistas)
	}
	if len(resultadosBia) != 2 || !resultadosBia[0].Respondeu || resultadosBia[0].Acertou || resultadosBia[0].Posicao != 2 {
		t.Fatalf("resultados da bia = %+v", resultadosBia)
	}

	if err := ap.Encerrar(); err != nil {
		t.Fatalf("Encerrar: %v", err)
	}
	if _, err := EntrarNaSala(conectar(t, addr), "teste", "atrasado", EventosJogador{}); !errors.Is(err, ErrSalaInexistente) {
		t.Fatalf("entrar em sala encerrada: %v, quer ErrSalaInexistente", err)
	}
}

func TestSalaInexistenteEDuplicada(t *testing.T) {
	addr := subirBroker(t)
	if _, err := EntrarNaSala(conectar(t, addr), "fantasma", "ana", EventosJogador{}); !errors.Is(err, ErrSalaInexistente) {
		t.Fatalf("erro = %v, quer ErrSalaInexistente", err)
	}

	cfg := ConfigApresentador{Sala: "s", Questoes: questoesTeste, Prazo: time.Second}
	if _, err := AbrirSala(conectar(t, addr), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := AbrirSala(conectar(t, addr), cfg); !errors.Is(err, ErrSalaExiste) {
		t.Fatalf("segunda sala com o mesmo nome: %v, quer ErrSalaExiste", err)
	}
}

// Reações dos jogadores chegam ao apresentador e aos jogadores como resumo
// agregado pelo broker.
func TestReacoesChegamAgregadas(t *testing.T) {
	addr := subirBroker(t)
	resumos := make(chan aggregator.Summary, 4)
	_, err := AbrirSala(conectar(t, addr), ConfigApresentador{
		Sala: "r", Questoes: questoesTeste, Prazo: time.Second,
		Eventos: Eventos{Reacoes: func(s aggregator.Summary) { resumos <- s }},
	})
	if err != nil {
		t.Fatal(err)
	}
	j, err := EntrarNaSala(conectar(t, addr), "r", "ana", EventosJogador{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		j.Reagir("👏")
	}

	total := 0
	prazo := time.After(3 * time.Second)
	for total < 5 {
		select {
		case s := <-resumos:
			total += s.Reactions["👏"]
		case <-prazo:
			t.Fatalf("apresentador recebeu %d de 5 reacoes", total)
		}
	}
}

func TestJogadorRespeitaPrazoLocal(t *testing.T) {
	addr := subirBroker(t)
	ap, err := AbrirSala(conectar(t, addr), ConfigApresentador{
		Sala: "p", Questoes: questoesTeste, Prazo: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	chegou := make(chan struct{}, 2)
	j, err := EntrarNaSala(conectar(t, addr), "p", "ana", EventosJogador{Pergunta: func(Pergunta) { chegou <- struct{}{} }})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Responder(0); !errors.Is(err, ErrSemPergunta) {
		t.Fatalf("responder sem pergunta: %v", err)
	}

	go ap.Jogar(context.Background())
	<-chegou
	time.Sleep(250 * time.Millisecond)
	if err := j.Responder(0); !errors.Is(err, ErrForaDoPrazo) {
		t.Fatalf("responder depois do prazo: %v, quer ErrForaDoPrazo", err)
	}
}

func esperarAte(t *testing.T, oque string, cond func() bool) {
	t.Helper()
	limite := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(limite) {
			t.Fatalf("tempo esgotado esperando: %s", oque)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
