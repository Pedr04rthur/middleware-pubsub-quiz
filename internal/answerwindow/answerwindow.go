// Package answerwindow controla a janela de respostas de cada sala: uma
// pergunta abre a janela e respostas que chegam depois dela caducam.
package answerwindow

import (
	"sync"
	"time"
)

// Window guarda quando foi a última pergunta de cada sala.
type Window struct {
	ttl time.Duration
	now func() time.Time

	mu    sync.Mutex
	asked map[string]time.Time
}

// New cria a janela com a duração dada (já limitada pelo chamador).
func New(ttl time.Duration) *Window {
	return &Window{ttl: ttl, now: time.Now, asked: make(map[string]time.Time)}
}

// QuestionAsked abre (ou reabre) a janela de respostas da sala.
func (w *Window) QuestionAsked(room string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.asked[room] = w.now()
}

// Accept informa se uma resposta da sala chegou dentro da janela. Se nenhuma
// pergunta passou por este broker (por exemplo, logo após um failover para a
// instância reserva), não há como julgar e a resposta é aceita.
func (w *Window) Accept(room string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	asked, ok := w.asked[room]
	return !ok || w.now().Sub(asked) <= w.ttl
}

// Forget apaga a janela da sala (por exemplo, quando os tópicos dela somem).
func (w *Window) Forget(room string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.asked, room)
}
