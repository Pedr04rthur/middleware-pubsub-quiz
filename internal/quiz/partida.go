package quiz

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Tolerancia é quanto uma resposta pode chegar depois do prazo e ainda valer,
// para absorver o atraso da rede (o jogador conta o prazo a partir de quando
// recebeu a pergunta).
const Tolerancia = 300 * time.Millisecond

// TopRanking é quantas posições vão no placar.
const TopRanking = 10

// Pontuação no estilo Kahoot: acertar vale de PontosBase a 2*PontosBase,
// mais quanto mais rápido.
const PontosBase = 500

// Motivos pelos quais uma resposta não conta.
var (
	ErrSemPergunta     = errors.New("nenhuma pergunta aberta")
	ErrOutraPergunta   = errors.New("resposta de outra pergunta")
	ErrForaDoPrazo     = errors.New("resposta fora do prazo")
	ErrJaRespondeu     = errors.New("jogador ja respondeu esta pergunta")
	ErrOpcaoInvalida   = errors.New("opcao invalida")
	ErrNomeInvalido    = errors.New("nome de jogador invalido")
	ErrPartidaTerminou = errors.New("nao ha mais perguntas")
)

// Pontuar calcula os pontos de uma resposta.
func Pontuar(acertou bool, decorrido, prazo time.Duration) int {
	if !acertou {
		return 0
	}
	frac := 1 - float64(decorrido)/float64(prazo)
	frac = min(max(frac, 0), 1)
	return PontosBase + int(PontosBase*frac)
}

// NormalizarNome limpa o nome do jogador; vazio significa inválido.
func NormalizarNome(nome string) string {
	nome = strings.TrimSpace(nome)
	if r := []rune(nome); len(r) > MaxNome {
		nome = string(r[:MaxNome])
	}
	return nome
}

// Partida é o estado do jogo do lado do apresentador: perguntas, respostas e
// pontos. É segura para uso concorrente (as respostas chegam pela goroutine
// da biblioteca cliente enquanto a partida avança).
type Partida struct {
	questoes []Questao
	prazo    time.Duration

	mu        sync.Mutex
	atual     int // índice da pergunta aberta, -1 se nenhuma
	proxima   int
	abertaEm  time.Time
	jogadores map[string]bool
	pontos    map[string]int
	rodada    map[string]int // pontos na pergunta atual
	respostas int
	acertos   int
}

// NovaPartida valida as perguntas. O prazo não é limitado aqui (os testes
// usam prazos curtos); a aplicação usa LimitarPrazo.
func NovaPartida(questoes []Questao, prazo time.Duration) (*Partida, error) {
	if len(questoes) == 0 {
		return nil, errors.New("partida sem perguntas")
	}
	for i, q := range questoes {
		if err := q.validar(); err != nil {
			return nil, fmt.Errorf("pergunta %d: %w", i+1, err)
		}
	}
	if prazo <= 0 {
		return nil, errors.New("prazo deve ser positivo")
	}
	return &Partida{
		questoes:  questoes,
		prazo:     prazo,
		atual:     -1,
		jogadores: make(map[string]bool),
		pontos:    make(map[string]int),
		rodada:    make(map[string]int),
	}, nil
}

// LimitarPrazo ajusta o prazo para a janela [MinPrazo, MaxPrazo].
func LimitarPrazo(d time.Duration) time.Duration {
	return min(max(d, MinPrazo), MaxPrazo)
}

// TotalPerguntas é quantas perguntas a partida tem.
func (p *Partida) TotalPerguntas() int { return len(p.questoes) }

// Entrar registra um jogador; retorna se ele é novo e o total de jogadores.
func (p *Partida) Entrar(nome string) (novo bool, total int, err error) {
	nome = NormalizarNome(nome)
	if nome == "" {
		return false, 0, ErrNomeInvalido
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	novo = !p.jogadores[nome]
	if novo {
		p.jogadores[nome] = true
		p.pontos[nome] = 0 // aparece no ranking mesmo sem pontuar
	}
	return novo, len(p.jogadores), nil
}

// AbrirProxima abre a próxima pergunta no instante agora.
func (p *Partida) AbrirProxima(agora time.Time) (Pergunta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.proxima >= len(p.questoes) {
		return Pergunta{}, ErrPartidaTerminou
	}
	p.atual = p.proxima
	p.proxima++
	p.abertaEm = agora
	p.rodada = make(map[string]int)
	p.respostas, p.acertos = 0, 0

	q := p.questoes[p.atual]
	return Pergunta{
		Numero:  p.atual + 1,
		Total:   len(p.questoes),
		Texto:   q.Texto,
		Opcoes:  q.Opcoes,
		PrazoMs: p.prazo.Milliseconds(),
	}, nil
}

// Responder registra a resposta recebida no instante agora e retorna os
// pontos ganhos. Só a primeira resposta de cada jogador conta. Um jogador
// que responde sem ter mandado "entrada" (ex.: a mensagem se perdeu) entra
// na partida aqui.
func (p *Partida) Responder(m MensagemJogador, agora time.Time) (int, error) {
	nome := NormalizarNome(m.Jogador)
	if nome == "" {
		return 0, ErrNomeInvalido
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.atual < 0 {
		return 0, ErrSemPergunta
	}
	if m.Pergunta != p.atual+1 {
		return 0, ErrOutraPergunta
	}
	decorrido := agora.Sub(p.abertaEm)
	if decorrido > p.prazo+Tolerancia {
		return 0, ErrForaDoPrazo
	}
	q := p.questoes[p.atual]
	if m.Opcao < 0 || m.Opcao >= len(q.Opcoes) {
		return 0, ErrOpcaoInvalida
	}
	if _, ja := p.rodada[nome]; ja {
		return 0, ErrJaRespondeu
	}

	p.jogadores[nome] = true
	pts := Pontuar(m.Opcao == q.Correta, decorrido, p.prazo)
	p.rodada[nome] = pts
	p.pontos[nome] += pts
	p.respostas++
	if m.Opcao == q.Correta {
		p.acertos++
	}
	return pts, nil
}

// Fechar encerra a pergunta aberta e monta o placar. Respostas que chegarem
// depois disso são recusadas com ErrSemPergunta.
func (p *Partida) Fechar() Placar {
	p.mu.Lock()
	defer p.mu.Unlock()

	idx := p.atual
	if idx < 0 {
		idx = p.proxima - 1
	}
	p.atual = -1

	ranking := make([]Posicao, 0, len(p.pontos))
	for nome, pts := range p.pontos {
		ranking = append(ranking, Posicao{Jogador: nome, Pontos: pts, Rodada: p.rodada[nome]})
	}
	sort.Slice(ranking, func(i, j int) bool {
		if ranking[i].Pontos != ranking[j].Pontos {
			return ranking[i].Pontos > ranking[j].Pontos
		}
		return ranking[i].Jogador < ranking[j].Jogador
	})
	if len(ranking) > TopRanking {
		ranking = ranking[:TopRanking]
	}

	pl := Placar{
		Total:     len(p.questoes),
		Respostas: p.respostas,
		Acertos:   p.acertos,
		Jogadores: len(p.jogadores),
		Ranking:   ranking,
		Final:     p.proxima >= len(p.questoes),
	}
	if idx >= 0 {
		q := p.questoes[idx]
		pl.Pergunta = idx + 1
		pl.Correta = q.Correta
		pl.TextoCorreta = q.Opcoes[q.Correta]
	}
	return pl
}
