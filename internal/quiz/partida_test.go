package quiz

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

var questoesTeste = []Questao{
	{Texto: "1+1?", Opcoes: []string{"1", "2", "3"}, Correta: 1},
	{Texto: "Capital da PB?", Opcoes: []string{"Patos", "Joao Pessoa"}, Correta: 1},
}

func novaPartida(t *testing.T) *Partida {
	t.Helper()
	p, err := NovaPartida(questoesTeste, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func resposta(jogador string, pergunta, opcao int) MensagemJogador {
	return MensagemJogador{Tipo: TipoResposta, Jogador: jogador, Pergunta: pergunta, Opcao: opcao}
}

func TestPontuar(t *testing.T) {
	prazo := 10 * time.Second
	casos := []struct {
		acertou   bool
		decorrido time.Duration
		quer      int
	}{
		{false, 0, 0},
		{true, 0, 1000},
		{true, 5 * time.Second, 750},
		{true, 10 * time.Second, 500},
		{true, 11 * time.Second, 500}, // dentro da tolerância: nunca menos que a base
	}
	for _, c := range casos {
		if got := Pontuar(c.acertou, c.decorrido, prazo); got != c.quer {
			t.Errorf("Pontuar(%v, %v) = %d, quer %d", c.acertou, c.decorrido, got, c.quer)
		}
	}
}

func TestNovaPartidaValidaPerguntas(t *testing.T) {
	invalidas := [][]Questao{
		nil,
		{{Texto: "", Opcoes: []string{"a", "b"}}},
		{{Texto: "x", Opcoes: []string{"a"}}},
		{{Texto: "x", Opcoes: []string{"a", "b"}, Correta: 2}},
	}
	for i, qs := range invalidas {
		if _, err := NovaPartida(qs, time.Second); err == nil {
			t.Errorf("caso %d: perguntas invalidas aceitas", i)
		}
	}
}

func TestPerguntaNaoRevelaGabarito(t *testing.T) {
	p := novaPartida(t)
	q, err := p.AbrirProxima(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(q)
	if strings.Contains(string(b), "correta") {
		t.Fatalf("a pergunta enviada aos jogadores contem o gabarito: %s", b)
	}
	if q.Numero != 1 || q.Total != 2 || q.PrazoMs != 10000 {
		t.Fatalf("pergunta = %+v", q)
	}
}

func TestRespostasDaRodada(t *testing.T) {
	p := novaPartida(t)
	inicio := time.Now()
	if _, err := p.Responder(resposta("ana", 1, 1), inicio); !errors.Is(err, ErrSemPergunta) {
		t.Fatalf("resposta antes da pergunta: %v", err)
	}

	p.AbrirProxima(inicio)
	casos := []struct {
		nome   string
		msg    MensagemJogador
		depois time.Duration
		pontos int
		err    error
	}{
		{"acerto rapido", resposta("ana", 1, 1), 0, 1000, nil},
		{"segunda resposta nao conta", resposta("ana", 1, 0), time.Second, 0, ErrJaRespondeu},
		{"erro", resposta("bia", 1, 0), time.Second, 0, nil},
		{"acerto na metade", resposta("caio", 1, 1), 5 * time.Second, 750, nil},
		{"outra pergunta", resposta("davi", 2, 1), time.Second, 0, ErrOutraPergunta},
		{"opcao invalida", resposta("davi", 1, 9), time.Second, 0, ErrOpcaoInvalida},
		{"dentro da tolerancia", resposta("davi", 1, 1), 10*time.Second + Tolerancia/2, 500, nil},
		{"fora do prazo", resposta("eva", 1, 1), 11 * time.Second, 0, ErrForaDoPrazo},
		{"nome vazio", resposta("  ", 1, 1), time.Second, 0, ErrNomeInvalido},
	}
	for _, c := range casos {
		pts, err := p.Responder(c.msg, inicio.Add(c.depois))
		if !errors.Is(err, c.err) || pts != c.pontos {
			t.Errorf("%s: pontos=%d err=%v, quer %d %v", c.nome, pts, err, c.pontos, c.err)
		}
	}

	pl := p.Fechar()
	if pl.Pergunta != 1 || pl.Correta != 1 || pl.TextoCorreta != "2" || pl.Final {
		t.Fatalf("placar = %+v", pl)
	}
	if pl.Respostas != 4 || pl.Acertos != 3 || pl.Jogadores != 4 {
		t.Fatalf("contagem errada: respostas=%d acertos=%d jogadores=%d", pl.Respostas, pl.Acertos, pl.Jogadores)
	}
	ordem := []string{"ana", "caio", "davi", "bia"}
	for i, nome := range ordem {
		if pl.Ranking[i].Jogador != nome {
			t.Fatalf("ranking = %+v, quer ordem %v", pl.Ranking, ordem)
		}
	}

	// Depois do placar, a pergunta está fechada.
	if _, err := p.Responder(resposta("eva", 1, 1), inicio.Add(time.Second)); !errors.Is(err, ErrSemPergunta) {
		t.Fatalf("resposta depois do placar: %v", err)
	}
}

func TestPontosAcumulamEPlacarFinal(t *testing.T) {
	p := novaPartida(t)
	agora := time.Now()

	p.Entrar("lurker") // entrou e nunca respondeu: aparece com 0
	p.AbrirProxima(agora)
	p.Responder(resposta("ana", 1, 1), agora)
	p.Fechar()

	p.AbrirProxima(agora)
	p.Responder(resposta("bia", 2, 1), agora)
	p.Responder(resposta("ana", 2, 1), agora.Add(5*time.Second))
	pl := p.Fechar()

	if !pl.Final {
		t.Fatal("ultimo placar deveria ser final")
	}
	quer := []Posicao{{"ana", 1750, 750}, {"bia", 1000, 1000}, {"lurker", 0, 0}}
	if fmt.Sprint(pl.Ranking) != fmt.Sprint(quer) {
		t.Fatalf("ranking = %+v, quer %+v", pl.Ranking, quer)
	}
	if _, err := p.AbrirProxima(agora); !errors.Is(err, ErrPartidaTerminou) {
		t.Fatalf("pergunta depois do fim: %v", err)
	}
}

func TestEntrar(t *testing.T) {
	p := novaPartida(t)
	if novo, total, _ := p.Entrar("ana"); !novo || total != 1 {
		t.Fatalf("primeira entrada: novo=%v total=%d", novo, total)
	}
	if novo, total, _ := p.Entrar(" ana "); novo || total != 1 {
		t.Fatalf("entrada repetida: novo=%v total=%d", novo, total)
	}
	if _, _, err := p.Entrar(""); !errors.Is(err, ErrNomeInvalido) {
		t.Fatalf("nome vazio: %v", err)
	}
	if got := NormalizarNome(strings.Repeat("x", 50)); len(got) != MaxNome {
		t.Fatalf("nome nao foi cortado: %d", len(got))
	}
}

func TestPlacarCabeNoDatagrama(t *testing.T) {
	p := novaPartida(t)
	agora := time.Now()
	p.AbrirProxima(agora)
	for i := 0; i < 500; i++ {
		p.Responder(resposta(fmt.Sprintf("jogador-com-nome-%03d", i), 1, 1), agora)
	}
	pl := p.Fechar()
	if len(pl.Ranking) != TopRanking || pl.Jogadores != 500 {
		t.Fatalf("ranking com %d posicoes e %d jogadores", len(pl.Ranking), pl.Jogadores)
	}
	b, err := codificarPlacar(pl)
	if err != nil || len(b) > MaxPayload {
		t.Fatalf("placar com %d bytes, err %v", len(b), err)
	}

	pl.Ranking = make([]Posicao, 200)
	for i := range pl.Ranking {
		pl.Ranking[i] = Posicao{Jogador: strings.Repeat("n", MaxNome), Pontos: 1000}
	}
	if b, err := codificarPlacar(pl); err != nil || len(b) > MaxPayload {
		t.Fatalf("placar grande nao foi cortado: %d bytes, err %v", len(b), err)
	}
}

func TestLimitarPrazo(t *testing.T) {
	if LimitarPrazo(time.Second) != MinPrazo || LimitarPrazo(time.Minute) != MaxPrazo || LimitarPrazo(8*time.Second) != 8*time.Second {
		t.Fatal("LimitarPrazo fora da janela de 5 a 20 segundos")
	}
}
