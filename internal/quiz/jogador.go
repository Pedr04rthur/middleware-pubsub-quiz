package quiz

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

// ErrSalaInexistente indica que a sala não foi aberta por um apresentador.
var ErrSalaInexistente = errors.New("sala nao existe (o apresentador ainda nao abriu?)")

// EventosJogador são os avisos do jogador para a interface. Todos são
// opcionais e chamados na goroutine da biblioteca cliente.
type EventosJogador struct {
	Pergunta func(Pergunta)
	Placar   func(Placar, MeuResultado)
	Reacoes  func(aggregator.Summary)
}

// MeuResultado é como o jogador se saiu na pergunta do placar.
type MeuResultado struct {
	Respondeu bool
	Acertou   bool
	// Posicao no ranking (a partir de 1); 0 se ficou fora do top do placar.
	Posicao int
	// Pontos totais; -1 se ficou fora do top do placar.
	Pontos int
}

// Jogador participa de uma sala.
type Jogador struct {
	c       *client.Client
	nome    string
	topicos Topicos
	ev      EventosJogador

	mu         sync.Mutex
	atual      Pergunta
	recebidaEm time.Time
	aberta     bool
	escolhas   map[int]int // número da pergunta -> opção escolhida

	fim     chan Placar
	fimOnce sync.Once
}

// EntrarNaSala inscreve o jogador nos tópicos da sala e avisa o apresentador.
func EntrarNaSala(c *client.Client, sala, nome string, ev EventosJogador) (*Jogador, error) {
	nome = NormalizarNome(nome)
	if nome == "" {
		return nil, ErrNomeInvalido
	}
	j := &Jogador{
		c:        c,
		nome:     nome,
		topicos:  TopicosDaSala(sala),
		ev:       ev,
		escolhas: make(map[int]int),
		fim:      make(chan Placar, 1),
	}
	// Perguntas por último: quando a primeira chegar, placar e reações já
	// estão inscritos.
	inscricoes := []struct {
		topico  string
		handler client.Handler
	}{
		{j.topicos.Placar, j.onPlacar},
		{j.topicos.Reacoes, j.onReacoes},
		{j.topicos.Perguntas, j.onPergunta},
	}
	for _, s := range inscricoes {
		if err := c.Subscribe(s.topico, s.handler); err != nil {
			var nack *client.NackError
			if errors.As(err, &nack) && nack.Reason == "topico nao existe" {
				return nil, ErrSalaInexistente
			}
			return nil, err
		}
	}
	// Aviso de entrada para o lobby. Se se perder, o jogador entra na
	// partida na primeira resposta.
	if payload, err := codificar(MensagemJogador{Tipo: TipoEntrada, Jogador: nome}); err == nil {
		c.Publish(j.topicos.Respostas, payload)
	}
	return j, nil
}

// Nome é o nome do jogador na partida.
func (j *Jogador) Nome() string { return j.nome }

// Fim recebe o placar final quando a partida termina.
func (j *Jogador) Fim() <-chan Placar { return j.fim }

// PerguntaAtual retorna a pergunta aberta e o tempo restante.
func (j *Jogador) PerguntaAtual() (Pergunta, time.Duration, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.aberta {
		return Pergunta{}, 0, false
	}
	return j.atual, j.atual.Prazo() - time.Since(j.recebidaEm), true
}

// Responder envia a resposta para a pergunta aberta. O prazo é contado a
// partir de quando a pergunta chegou.
func (j *Jogador) Responder(opcao int) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.aberta {
		return ErrSemPergunta
	}
	if _, ja := j.escolhas[j.atual.Numero]; ja {
		return ErrJaRespondeu
	}
	if time.Since(j.recebidaEm) > j.atual.Prazo() {
		j.aberta = false
		return ErrForaDoPrazo
	}
	if opcao < 0 || opcao >= len(j.atual.Opcoes) {
		return ErrOpcaoInvalida
	}
	payload, err := codificar(MensagemJogador{Tipo: TipoResposta, Jogador: j.nome, Pergunta: j.atual.Numero, Opcao: opcao})
	if err != nil {
		return err
	}
	if err := j.c.Publish(j.topicos.Respostas, payload); err != nil {
		return err
	}
	j.escolhas[j.atual.Numero] = opcao
	return nil
}

// Reagir envia uma reação (ex.: um emoji) para a sala.
func (j *Jogador) Reagir(reacao string) error {
	return j.c.Publish(j.topicos.Reacoes, []byte(reacao))
}

func (j *Jogador) onPergunta(m client.Message) {
	var p Pergunta
	if json.Unmarshal(m.Payload, &p) != nil || p.Numero <= 0 {
		return
	}
	j.mu.Lock()
	j.atual = p
	j.recebidaEm = m.ReceivedAt
	j.aberta = true
	j.mu.Unlock()
	if f := j.ev.Pergunta; f != nil {
		f(p)
	}
}

func (j *Jogador) onPlacar(m client.Message) {
	var pl Placar
	if json.Unmarshal(m.Payload, &pl) != nil {
		return
	}
	j.mu.Lock()
	if j.atual.Numero == pl.Pergunta {
		j.aberta = false
	}
	escolha, respondeu := j.escolhas[pl.Pergunta]
	j.mu.Unlock()

	res := MeuResultado{Respondeu: respondeu, Acertou: respondeu && escolha == pl.Correta, Pontos: -1}
	for i, pos := range pl.Ranking {
		if pos.Jogador == j.nome {
			res.Posicao, res.Pontos = i+1, pos.Pontos
			break
		}
	}
	if f := j.ev.Placar; f != nil {
		f(pl, res)
	}
	if pl.Final {
		j.fimOnce.Do(func() { j.fim <- pl })
	}
}

func (j *Jogador) onReacoes(m client.Message) {
	var s aggregator.Summary
	if json.Unmarshal(m.Payload, &s) != nil {
		return
	}
	if f := j.ev.Reacoes; f != nil {
		f(s)
	}
}

// ConfigBot define o comportamento de um jogador simulado.
type ConfigBot struct {
	// AtrasoMax é a fração do prazo (0 a 1) até a qual o bot demora para
	// responder, sorteada a cada pergunta.
	AtrasoMax float64
	// Escolher decide a opção; nil sorteia.
	Escolher func(Pergunta) int
	// Eventos extras (ex.: para a CLI mostrar o placar).
	Eventos EventosJogador
}

// EntrarComoBot cria um jogador que responde sozinho a cada pergunta.
func EntrarComoBot(c *client.Client, sala, nome string, cfg ConfigBot) (*Jogador, error) {
	var j *Jogador
	pronto := make(chan struct{})
	ev := cfg.Eventos
	extra := ev.Pergunta
	ev.Pergunta = func(p Pergunta) {
		if extra != nil {
			extra(p)
		}
		atraso := time.Duration(rand.Float64() * cfg.AtrasoMax * float64(p.Prazo()))
		time.AfterFunc(atraso, func() {
			<-pronto
			if j == nil {
				return // EntrarNaSala falhou
			}
			opcao := rand.N(len(p.Opcoes))
			if cfg.Escolher != nil {
				opcao = cfg.Escolher(p)
			}
			j.Responder(opcao)
		})
	}
	j, err := EntrarNaSala(c, sala, nome, ev)
	close(pronto)
	return j, err
}
