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

	enviar := func(cmd byte, topic string, payload string) {
		msg := &protocol.Message{Command: cmd, Topic: topic, Payload: []byte(payload)}
		conn.Write(msg.Serialize())

		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Timeout: %v\n", err)
			return
		}
		resp, _ := protocol.Deserialize(buffer[:n])
		status := "ACK"
		if resp.Command == protocol.CmdNack {
			status = "NACK"
		}
		fmt.Printf("[%s] Topico=%s Payload=%s\n", status, resp.Topic, string(resp.Payload))
	}

	fmt.Println("=== TESTE 1: Publicar em tópico sem inscritos (deve dar NACK) ===")
	enviar(protocol.CmdCreateTopic, "quiz/sala1/perguntas", "")
	enviar(protocol.CmdPublish, "quiz/sala1/perguntas", "Qual a capital?")
	// Esperado: NACK com "topico sem inscritos"

	fmt.Println("\n=== TESTE 2: Inscrever e publicar (deve dar ACK) ===")
	enviar(protocol.CmdSubscribe, "quiz/sala1/perguntas", "")
	enviar(protocol.CmdPublish, "quiz/sala1/perguntas", "Qual a capital?")

	fmt.Println("\n=== TESTE 3: Publicar em tópico inexistente (deve dar NACK) ===")
	enviar(protocol.CmdPublish, "quiz/sala999/perguntas", "Teste")
}
