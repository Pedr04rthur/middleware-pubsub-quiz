package main

import (
	"fmt"
	"net"
)

func main() {
	
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic("Erro ao abrir a porta: " + err.Error())
	}
	defer listener.Close()

	fmt.Println("Broker rodando na porta 8080, esperando clientes...")

	
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão:", err)
			continue
		}

		
		go lidarComCliente(conn)
	}
}


func lidarComCliente(conn net.Conn) {
	defer conn.Close()

	enderecoCliente := conn.RemoteAddr().String()
	fmt.Println("Novo cliente conectado:", enderecoCliente)

	buffer := make([]byte, 1024)

	for {
		
		bytesLidos, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Cliente desconectado:", enderecoCliente)
			return
		}

		
		mensagem := string(buffer[:bytesLidos])
		fmt.Printf("Recebido de %s: %s\n", enderecoCliente, mensagem)
	}
}