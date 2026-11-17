package protocol

import (
	"bytes"
	"testing"
)

func TestSerializaEDesserializa(t *testing.T) {
	original := &Message{
		Command: CmdPublish,
		Topic:   "quiz/sala1/perguntas",
		Payload: []byte("Qual a capital da França?"),
	}

	bytesEnviados := original.Serialize()
	recebida, err := Deserialize(bytesEnviados)

	if err != nil {
		t.Fatalf("Erro ao desserializar: %v", err)
	}

	if recebida.Command != original.Command {
		t.Errorf("Comando esperado: %v, recebido: %v", original.Command, recebida.Command)
	}
	if recebida.Topic != original.Topic {
		t.Errorf("Tópico esperado: %s, recebido: %s", original.Topic, recebida.Topic)
	}
	if !bytes.Equal(recebida.Payload, original.Payload) {
		t.Errorf("Payload esperado: %s, recebido: %s", original.Payload, recebida.Payload)
	}
}

func TestPacoteIncompleto(t *testing.T) {
	_, err := Deserialize([]byte{0x01})
	if err == nil {
		t.Error("Esperava erro para pacote incompleto, mas não houve")
	}
}
