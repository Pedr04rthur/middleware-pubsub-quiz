package answerwindow

import (
	"testing"
	"time"
)

func TestAnswersOutsideWindowAreRejected(t *testing.T) {
	now := time.Unix(0, 0)
	w := New(10 * time.Second)
	w.now = func() time.Time { return now }

	if !w.Accept("sala1") {
		t.Fatal("sem pergunta registrada a resposta deveria ser aceita")
	}

	w.QuestionAsked("sala1")
	now = now.Add(10 * time.Second)
	if !w.Accept("sala1") {
		t.Fatal("resposta no limite da janela deveria ser aceita")
	}
	now = now.Add(time.Millisecond)
	if w.Accept("sala1") {
		t.Fatal("resposta depois da janela deveria caducar")
	}
	if !w.Accept("sala2") {
		t.Fatal("a janela de uma sala não deveria afetar outra")
	}

	w.QuestionAsked("sala1")
	if !w.Accept("sala1") {
		t.Fatal("uma nova pergunta deveria reabrir a janela")
	}

	w.Forget("sala1")
	now = now.Add(time.Hour)
	if !w.Accept("sala1") {
		t.Fatal("depois de Forget a sala volta a não ter janela")
	}
}
