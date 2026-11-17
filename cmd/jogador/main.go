package main

import (
	"fmt"
	"net"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

func main() {
	serverAddr, _ := net.ResolveUDPAddr("udp", "localhost:8080")
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	buffer := make([]byte, 1024)

	// Função auxiliar para enviar e esperar resposta
	enviar := func(cmd byte, topic string, payload string) {
		msg := &protocol.Message{Command: cmd, Topic: topic, Payload: []byte(payload)}
		conn.Write(msg.Serialize())

		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Timeout em %d: %v\n", cmd, err)
			return
		}
		resp, _ := protocol.Deserialize(buffer[:n])
		fmt.Printf("Resposta: Comando=%d, Topico=%s, Payload=%s\n", resp.Command, resp.Topic, string(resp.Payload))
	}

	// 1. Cria tópico
	enviar(protocol.CmdCreateTopic, "quiz/sala1/perguntas", "")

	// 2. Inscreve-se
	enviar(protocol.CmdSubscribe, "quiz/sala1/perguntas", "")

	// 3. Publica uma pergunta
	enviar(protocol.CmdPublish, "quiz/sala1/perguntas", "Qual a capital da Franca?")

	// 4. Remove o tópico (encerra a sala)
	enviar(protocol.CmdRemoveTopic, "quiz/sala1/perguntas", "")
}
