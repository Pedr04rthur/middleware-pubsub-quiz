package main

import (
	"context"
	"fmt"
	"net"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/ratelimit"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

func main() {
	// Abre um socket UDP na porta 8080 (Regra do projeto: usar UDP)
	addr, err := net.ResolveUDPAddr("udp", ":8080")
	if err != nil {
		panic(err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Inicializa o Rate Limit (do Pedro Lucas) para conter jogadores barulhentos
	limitador := ratelimit.NewManager(ratelimit.DefaultConfig())
	go limitador.Run(context.Background())

	fmt.Println("Broker UDP rodando na porta 8080...")

	buffer := make([]byte, 4096)

	for {
		// ReadFromUDP lê o pacote e retorna quantos bytes e quem mandou
		n, clienteEndereco, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Erro ao ler UDP:", err)
			continue
		}

		// 1. Verifica o Rate Limit antes de processar
		if limitador.Allow(clienteEndereco.String()) != ratelimit.Allowed {
			fmt.Printf("Mensagem de %s descartada pelo Rate Limit\n", clienteEndereco)
			continue // Ignora o pacote e volta ao loop
		}

		// 2. Desserializa o pacote usando o nosso protocolo binário
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

		// 3. Lógica básica de echo para testes
		if msg.Command == protocol.CmdPublish {
			resposta := &protocol.Message{
				Command: protocol.CmdAck,
				Topic:   msg.Topic,
				Payload: []byte("OK"),
			}
			conn.WriteToUDP(resposta.Serialize(), clienteEndereco)
		}
	}
}