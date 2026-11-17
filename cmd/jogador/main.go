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

	fmt.Println("1. Criar topico")
	enviar(protocol.CmdCreateTopic, "quiz/sala1/perguntas", "")
	fmt.Println("\n2. Se inscrever no topico")
	enviar(protocol.CmdSubscribe, "quiz/sala1/perguntas", "")
	fmt.Println("\n3. Publicar (deve entregar para mim mesmo)")
	enviar(protocol.CmdPublish, "quiz/sala1/perguntas", "Qual a capital?")
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, err := conn.Read(buffer)
	if err == nil {
		resp, _ := protocol.Deserialize(buffer[:n])
		fmt.Printf("[RECEBIDO] Topico=%s Payload=%s\n", resp.Topic, string(resp.Payload))
	}
	fmt.Println("\n4. Desconectar (deve limpar tudo)")
	enviar(protocol.CmdDisconnect, "quiz/sala1/perguntas", "")
	fmt.Println("\n5. Tentar publicar novamente (deve dar NACK: sem inscritos)")
	enviar(protocol.CmdPublish, "quiz/sala1/perguntas", "Teste apos disconnect")
}
