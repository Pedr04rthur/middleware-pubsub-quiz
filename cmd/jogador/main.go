package main

import (
	"fmt"
	"net"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

func main() {
	// Conecta ao Broker UDP no localhost:8080
	serverAddr, err := net.ResolveUDPAddr("udp", "localhost:8080")
	if err != nil {
		panic(err)
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Jogador conectado ao broker UDP")

	// Simula um jogador enviando uma resposta
	mensagem := &protocol.Message{
		Command: protocol.CmdPublish,
		Topic:   "quiz/sala1/respostas",
		Payload: []byte("Paris"),
	}

	// Envia
	_, err = conn.Write(mensagem.Serialize())
	if err != nil {
		panic(err)
	}
	fmt.Println("Mensagem enviada:", string(mensagem.Payload))

	// Espera um ACK
	buffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Timeout esperando ACK:", err)
		return
	}

	ack, _ := protocol.Deserialize(buffer[:n])
	fmt.Printf("ACK recebido: Comando=%d, Payload=%s\n", ack.Command, string(ack.Payload))
}
