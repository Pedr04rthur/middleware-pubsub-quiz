// Package quiz é a aplicação de exemplo do middleware: um quiz ao vivo em que
// o apresentador abre uma sala, dispara perguntas e publica o placar, e os
// jogadores entram na sala, respondem dentro do prazo e mandam reações.
//
// Tópicos de cada sala (criados pelo apresentador e apagados no fim):
//
//	quiz/{sala}/perguntas  apresentador -> jogadores  (Pergunta)
//	quiz/{sala}/respostas  jogadores -> apresentador  (MensagemJogador)
//	quiz/{sala}/reacoes    jogadores -> todos         (emoji; o broker entrega resumos)
//	quiz/{sala}/placar     apresentador -> jogadores  (Placar)
//
// As mensagens são JSON. Só perguntas podem ir para o tópico de perguntas:
// cada publicação nele reabre, no broker, a janela de respostas da sala.
package quiz

import (
	"encoding/json"
	"fmt"
	"time"
)

// Limites do prazo de resposta (PDF, seção 2: entre 5 e 20 segundos).
const (
	MinPrazo = 5 * time.Second
	MaxPrazo = 20 * time.Second
)

// MaxPayload é o maior payload que cabe em um datagrama aceito pelo broker
// (4096 bytes menos cabeçalho e tópico, com folga).
const MaxPayload = 3800

// MaxNome é o tamanho máximo do nome de um jogador.
const MaxNome = 20

// Topicos são os tópicos de uma sala.
type Topicos struct {
	Perguntas, Respostas, Reacoes, Placar string
}

// TopicosDaSala monta os tópicos quiz/{sala}/{tipo}.
func TopicosDaSala(sala string) Topicos {
	p := "quiz/" + sala + "/"
	return Topicos{
		Perguntas: p + "perguntas",
		Respostas: p + "respostas",
		Reacoes:   p + "reacoes",
		Placar:    p + "placar",
	}
}

// Todos lista os tópicos da sala.
func (t Topicos) Todos() []string {
	return []string{t.Perguntas, t.Respostas, t.Reacoes, t.Placar}
}

// Questao é uma pergunta com o gabarito; fica só com o apresentador.
type Questao struct {
	Texto   string   `json:"texto"`
	Opcoes  []string `json:"opcoes"`
	Correta int      `json:"correta"` // índice em Opcoes, a partir de 0
}

func (q Questao) validar() error {
	if q.Texto == "" {
		return fmt.Errorf("pergunta sem texto")
	}
	if len(q.Opcoes) < 2 || len(q.Opcoes) > 6 {
		return fmt.Errorf("%q: precisa de 2 a 6 opcoes", q.Texto)
	}
	if q.Correta < 0 || q.Correta >= len(q.Opcoes) {
		return fmt.Errorf("%q: opcao correta %d fora das opcoes", q.Texto, q.Correta)
	}
	return nil
}

// Pergunta é o que os jogadores recebem: sem o gabarito.
type Pergunta struct {
	Numero  int      `json:"numero"` // a partir de 1
	Total   int      `json:"total"`
	Texto   string   `json:"texto"`
	Opcoes  []string `json:"opcoes"`
	PrazoMs int64    `json:"prazo_ms"`
}

// Prazo é a janela de resposta da pergunta.
func (p Pergunta) Prazo() time.Duration { return time.Duration(p.PrazoMs) * time.Millisecond }

// Tipos de MensagemJogador.
const (
	TipoEntrada  = "entrada"
	TipoResposta = "resposta"
)

// MensagemJogador é publicada pelos jogadores no tópico de respostas.
type MensagemJogador struct {
	Tipo     string `json:"tipo"`
	Jogador  string `json:"jogador"`
	Pergunta int    `json:"pergunta,omitempty"` // Numero da pergunta respondida
	Opcao    int    `json:"opcao"`              // índice escolhido, a partir de 0
}

// Posicao é uma linha do ranking.
type Posicao struct {
	Jogador string `json:"jogador"`
	Pontos  int    `json:"pontos"`
	Rodada  int    `json:"rodada"` // pontos ganhos na última pergunta
}

// Placar é publicado ao fim de cada pergunta.
type Placar struct {
	Pergunta     int       `json:"pergunta"`
	Total        int       `json:"total"`
	Correta      int       `json:"correta"`
	TextoCorreta string    `json:"texto_correta"`
	Respostas    int       `json:"respostas"`
	Acertos      int       `json:"acertos"`
	Jogadores    int       `json:"jogadores"`
	Ranking      []Posicao `json:"ranking"` // só os primeiros, para caber no datagrama
	Final        bool      `json:"final"`
}

// codificar serializa v e garante que cabe em um datagrama.
func codificar(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxPayload {
		return nil, fmt.Errorf("mensagem com %d bytes nao cabe no datagrama (max %d)", len(b), MaxPayload)
	}
	return b, nil
}

// codificarPlacar serializa o placar, cortando o ranking até caber no
// datagrama.
func codificarPlacar(p Placar) ([]byte, error) {
	for {
		b, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		if len(b) <= MaxPayload || len(p.Ranking) == 0 {
			return b, nil
		}
		p.Ranking = p.Ranking[:len(p.Ranking)-1]
	}
}
