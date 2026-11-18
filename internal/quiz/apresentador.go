package quiz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

// ErrSalaExiste indica que já existe uma sala com esse nome no broker.
var ErrSalaExiste = errors.New("ja existe uma sala com esse nome")

// Eventos são os avisos do apresentador para a interface (CLI ou web).
// Todos são opcionais e chamados na goroutine da biblioteca cliente ou na de
// Jogar, então devem ser rápidos.
type Eventos struct {
	Entrada  func(jogador string, total int)
	Pergunta func(Pergunta)
	Resposta func(jogador string, pontos int, err error)
	Reacoes  func(aggregator.Summary)
	Placar   func(Placar)
}

// ConfigApresentador define a partida.
type ConfigApresentador struct {
	Sala     string
	Questoes []Questao
	Prazo    time.Duration
	// Intervalo é a pausa entre o placar de uma pergunta e a próxima.
	Intervalo time.Duration
	Eventos   Eventos
}

// Apresentador conduz a partida de uma sala.
type Apresentador struct {
	c       *client.Client
	cfg     ConfigApresentador
	topicos Topicos
	partida *Partida
}

// AbrirSala cria os tópicos da sala e passa a ouvir respostas e reações.
func AbrirSala(c *client.Client, cfg ConfigApresentador) (*Apresentador, error) {
	if cfg.Sala == "" {
		return nil, errors.New("sala sem nome")
	}
	partida, err := NovaPartida(cfg.Questoes, cfg.Prazo)
	if err != nil {
		return nil, err
	}
	a := &Apresentador{c: c, cfg: cfg, topicos: TopicosDaSala(cfg.Sala), partida: partida}

	var criados []string
	desfazer := func() {
		for _, t := range criados {
			c.RemoveTopic(t)
		}
	}
	for _, t := range a.topicos.Todos() {
		if err := c.CreateTopic(t); err != nil {
			desfazer()
			var nack *client.NackError
			if errors.As(err, &nack) {
				return nil, fmt.Errorf("%w: %s", ErrSalaExiste, cfg.Sala)
			}
			return nil, err
		}
		criados = append(criados, t)
	}
	if err := c.Subscribe(a.topicos.Respostas, a.onMensagemJogador); err != nil {
		desfazer()
		return nil, err
	}
	if err := c.Subscribe(a.topicos.Reacoes, a.onReacoes); err != nil {
		desfazer()
		return nil, err
	}
	return a, nil
}

// Topicos são os tópicos da sala.
func (a *Apresentador) Topicos() Topicos { return a.topicos }

// Partida dá acesso ao estado do jogo.
func (a *Apresentador) Partida() *Partida { return a.partida }

// Jogar dispara as perguntas em sequência: publica a pergunta, espera o
// prazo, publica o placar e faz uma pausa antes da próxima. Retorna o placar
// final.
func (a *Apresentador) Jogar(ctx context.Context) (Placar, error) {
	for {
		p, err := a.partida.AbrirProxima(time.Now())
		if err != nil {
			return Placar{}, err
		}
		payload, err := codificar(p)
		if err != nil {
			return Placar{}, fmt.Errorf("pergunta %d: %w", p.Numero, err)
		}
		if err := a.c.Publish(a.topicos.Perguntas, payload); err != nil {
			return Placar{}, err
		}
		if f := a.cfg.Eventos.Pergunta; f != nil {
			f(p)
		}

		if err := esperar(ctx, a.cfg.Prazo+Tolerancia); err != nil {
			return Placar{}, err
		}

		pl := a.partida.Fechar()
		payload, err = codificarPlacar(pl)
		if err != nil {
			return Placar{}, err
		}
		if err := a.c.Publish(a.topicos.Placar, payload); err != nil {
			return Placar{}, err
		}
		if f := a.cfg.Eventos.Placar; f != nil {
			f(pl)
		}
		if pl.Final {
			return pl, nil
		}
		if err := esperar(ctx, a.cfg.Intervalo); err != nil {
			return Placar{}, err
		}
	}
}

// Encerrar apaga os tópicos da sala no broker.
func (a *Apresentador) Encerrar() error {
	var errs []error
	for _, t := range a.topicos.Todos() {
		if err := a.c.RemoveTopic(t); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (a *Apresentador) onMensagemJogador(m client.Message) {
	var msg MensagemJogador
	if json.Unmarshal(m.Payload, &msg) != nil {
		return
	}
	switch msg.Tipo {
	case TipoEntrada:
		novo, total, err := a.partida.Entrar(msg.Jogador)
		if err == nil && novo {
			if f := a.cfg.Eventos.Entrada; f != nil {
				f(NormalizarNome(msg.Jogador), total)
			}
		}
	case TipoResposta:
		// ReceivedAt é quando o datagrama saiu do socket, não quando o
		// handler rodou: a fila local não pesa contra o jogador.
		pts, err := a.partida.Responder(msg, m.ReceivedAt)
		if f := a.cfg.Eventos.Resposta; f != nil {
			f(NormalizarNome(msg.Jogador), pts, err)
		}
	}
}

func (a *Apresentador) onReacoes(m client.Message) {
	var s aggregator.Summary
	if json.Unmarshal(m.Payload, &s) != nil {
		return
	}
	if f := a.cfg.Eventos.Reacoes; f != nil {
		f(s)
	}
}

func esperar(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
