package main

import (
	"fmt"
	"net"
	"strings"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

func main() {
	// Abre um socket UDP na porta 8080
	addr, err := net.ResolveUDPAddr("udp", ":8080")
	if err != nil {
		panic(err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Broker UDP rodando na porta 8080...")

	buffer := make([]byte, 4096)

	for {
		// ReadFromUDP lê o pacote e retorna quantos bytes e quem mandou
		n, clienteEndereco, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Erro ao ler UDP:", err)
			continue
		}

		// Desserializa o pacote usando o nosso protocolo
		msg, err := protocol.Deserialize(buffer[:n])
		if err != nil {
			fmt.Println("Pacote inválido de", clienteEndereco, ":", err)
			continue
		}

		fmt.Printf("Recebido de %s | Comando: %d | Topico: %s | Payload: %s\n",
			clienteEndereco,
			msg.Command,
			msg.Topic,
			string(msg.Payload),
		)

		// Lógica básica de echo para testes (vamos evoluir depois)
		if msg.Command == protocol.CmdPublish {
			resposta := &protocol.Message{
				Command: protocol.CmdAck,
				Topic:   msg.Topic,
				Payload: []byte("OK"),
			}
			// Envia o ACK de volta para quem mandou
			conn.WriteToUDP(resposta.Serialize(), clienteEndereco)
		}
	}
}

// (Apenas um utilitário para evitar erro de importação caso não seja usado)
var _ = strings.TrimSpace
