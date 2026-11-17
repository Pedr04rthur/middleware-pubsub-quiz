package router

import (
	"testing"
)

func TestCicloDeVidaDoTopico(t *testing.T) {
	r := New()

	// Criar
	if err := r.CreateTopic("quiz/sala1/perguntas"); err != nil {
		t.Fatalf("Erro ao criar tópico: %v", err)
	}
	if !r.TopicExists("quiz/sala1/perguntas") {
		t.Error("Tópico deveria existir")
	}

	// Criar duplicado deve falhar
	if err := r.CreateTopic("quiz/sala1/perguntas"); err == nil {
		t.Error("Deveria falhar ao criar tópico duplicado")
	}

	// Inscrever
	if err := r.Subscribe("quiz/sala1/perguntas", "127.0.0.1:5001"); err != nil {
		t.Fatalf("Erro ao inscrever: %v", err)
	}
	if err := r.Subscribe("quiz/sala1/perguntas", "127.0.0.1:5002"); err != nil {
		t.Fatalf("Erro ao inscrever: %v", err)
	}

	inscritos := r.GetSubscribers("quiz/sala1/perguntas")
	if len(inscritos) != 2 {
		t.Errorf("Esperava 2 inscritos, obteve %d", len(inscritos))
	}

	// Desinscrever
	if err := r.Unsubscribe("quiz/sala1/perguntas", "127.0.0.1:5001"); err != nil {
		t.Fatalf("Erro ao desinscrever: %v", err)
	}
	inscritos = r.GetSubscribers("quiz/sala1/perguntas")
	if len(inscritos) != 1 {
		t.Errorf("Esperava 1 inscrito, obteve %d", len(inscritos))
	}

	// Remover
	if err := r.RemoveTopic("quiz/sala1/perguntas"); err != nil {
		t.Fatalf("Erro ao remover: %v", err)
	}
	if r.TopicExists("quiz/sala1/perguntas") {
		t.Error("Tópico não deveria mais existir")
	}
}

func TestRemoveTopicPrefix(t *testing.T) {
	r := New()
	r.CreateTopic("quiz/sala1/perguntas")
	r.CreateTopic("quiz/sala1/respostas")
	r.CreateTopic("quiz/sala2/perguntas")

	removidos := r.RemoveTopicPrefix("quiz/sala1/")
	if removidos != 2 {
		t.Errorf("Esperava remover 2 tópicos, removeu %d", removidos)
	}

	if r.TopicExists("quiz/sala1/perguntas") || r.TopicExists("quiz/sala1/respostas") {
		t.Error("Tópicos da sala1 deveriam ter sido removidos")
	}
	if !r.TopicExists("quiz/sala2/perguntas") {
		t.Error("Tópico da sala2 deveria continuar existindo")
	}
}
