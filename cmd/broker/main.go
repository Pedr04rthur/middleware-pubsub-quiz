package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/ratelimit"
)

func main() {

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic("Erro ao abrir a porta: " + err.Error())
	}
	defer listener.Close()

	limitador := ratelimit.NewManager(ratelimit.DefaultConfig())
	go limitador.Run(context.Background())

	fmt.Println("Broker rodando na porta 8080, esperando clientes...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão:", err)
			continue
		}

		go lidarComCliente(conn, limitador)
	}
}

func lidarComCliente(conn net.Conn, limitador *ratelimit.Manager) {
	defer conn.Close()

	enderecoCliente := conn.RemoteAddr().String()
	fmt.Println("Novo cliente conectado:", enderecoCliente)

	cliente := limitador.Register(enderecoCliente)
	defer limitador.Unregister(enderecoCliente)

	leitor := bufio.NewReader(conn)

	for {
		linha, err := leitor.ReadString('\n')
		if err != nil {
			fmt.Println("Cliente desconectado:", enderecoCliente)
			return
		}

		switch cliente.Allow() {
		case ratelimit.Throttled:
			fmt.Printf("Mensagem de %s descartada (limite atual: %.1f msgs/s)\n", enderecoCliente, cliente.Rate())
			continue
		case ratelimit.Disconnect:
			fmt.Println("Cliente desconectado por excesso de mensagens:", enderecoCliente)
			fmt.Fprintln(conn, "ERRO: desconectado por excesso de mensagens")
			return
		}

		mensagem := strings.TrimSpace(linha)
		fmt.Printf("Recebido de %s: %s\n", enderecoCliente, mensagem)
	}
}
