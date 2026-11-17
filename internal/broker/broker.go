package broker

import (
	"context"
	"fmt"
	"net"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/ratelimit"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/router"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

// Broker é o coração do sistema.
type Broker struct {
	conn      *net.UDPConn
	router    *router.Router
	limitador *ratelimit.Manager
}

// New cria um novo Broker.
func New(porta string) (*Broker, error) {
	addr, err := net.ResolveUDPAddr("udp", porta)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	b := &Broker{
		conn:      conn,
		router:    router.New(),
		limitador: ratelimit.NewManager(ratelimit.DefaultConfig()),
	}

	go b.limitador.Run(context.Background())
	return b, nil
}

// Close fecha o socket.
func (b *Broker) Close() error {
	return b.conn.Close()
}

// Run inicia o loop principal de recebimento.
func (b *Broker) Run() {
	fmt.Println("Broker UDP rodando na porta 8080...")
	buffer := make([]byte, 4096)

	for {
		n, clienteAddr, err := b.conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Erro ao ler UDP:", err)
			continue
		}

		// 1. Rate Limit
		if b.limitador.Allow(clienteAddr.String()) != ratelimit.Allowed {
			fmt.Printf("Mensagem de %s descartada pelo Rate Limit\n", clienteAddr)
			continue
		}

		// 2. Desserializa
		msg, err := protocol.Deserialize(buffer[:n])
		if err != nil {
			fmt.Println("Pacote inválido de", clienteAddr, ":", err)
			continue
		}

		// 3. Processa o comando
		b.handleMessage(msg, clienteAddr)
	}
}

// handleMessage roteia a mensagem conforme o comando.
func (b *Broker) handleMessage(msg *protocol.Message, clienteAddr *net.UDPAddr) {
	clienteStr := clienteAddr.String()

	switch msg.Command {
	case protocol.CmdConnect:
		fmt.Printf("[CONNECT] %s conectado\n", clienteStr)
		b.enviarAck(clienteAddr, msg.Topic)

	case protocol.CmdCreateTopic:
		if err := b.router.CreateTopic(msg.Topic); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		fmt.Printf("[CREATE_TOPIC] Tópico criado: %s (por %s)\n", msg.Topic, clienteStr)
		b.enviarAck(clienteAddr, msg.Topic)

	case protocol.CmdRemoveTopic:
		if err := b.router.RemoveTopic(msg.Topic); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		fmt.Printf("[REMOVE_TOPIC] Tópico removido: %s\n", msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)

	case protocol.CmdSubscribe:
		if err := b.router.Subscribe(msg.Topic, clienteStr); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		fmt.Printf("[SUBSCRIBE] %s inscrito em %s\n", clienteStr, msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)

	case protocol.CmdUnsubscribe:
		if err := b.router.Unsubscribe(msg.Topic, clienteStr); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		fmt.Printf("[UNSUBSCRIBE] %s desinscrito de %s\n", clienteStr, msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)

	case protocol.CmdPublish:
		b.handlePublish(msg, clienteAddr)

	case protocol.CmdDisconnect:
		fmt.Printf("[DISCONNECT] %s desconectado\n", clienteStr)
		b.enviarAck(clienteAddr, msg.Topic)

	default:
		fmt.Printf("[UNKNOWN] Comando desconhecido de %s: %d\n", clienteStr, msg.Command)
		b.enviarNack(clienteAddr, msg.Topic, "comando desconhecido")
	}
}

// handlePublish entrega a mensagem a todos os inscritos no tópico.
func (b *Broker) handlePublish(msg *protocol.Message, clienteAddr *net.UDPAddr) {
	inscritos := b.router.GetSubscribers(msg.Topic)
	if len(inscritos) == 0 {
		fmt.Printf("[PUBLISH] Tópico %s sem inscritos. Avisando publicador.\n", msg.Topic)
		b.enviarNack(clienteAddr, msg.Topic, "sem inscritos")
		return
	}

	fmt.Printf("[PUBLISH] %s -> %s (%d inscritos)\n", msg.Topic, clienteAddr, len(inscritos))

	for _, endereco := range inscritos {
		destAddr, err := net.ResolveUDPAddr("udp", endereco)
		if err != nil {
			continue
		}
		b.conn.WriteToUDP(msg.Serialize(), destAddr)
	}

	b.enviarAck(clienteAddr, msg.Topic)
}

// enviarAck envia um ACK de confirmação.
func (b *Broker) enviarAck(destino *net.UDPAddr, topico string) {
	ack := &protocol.Message{Command: protocol.CmdAck, Topic: topico}
	b.conn.WriteToUDP(ack.Serialize(), destino)
}

// enviarNack envia um NACK (erro).
func (b *Broker) enviarNack(destino *net.UDPAddr, topico string, motivo string) {
	nack := &protocol.Message{Command: protocol.CmdNack, Topic: topico, Payload: []byte(motivo)}
	b.conn.WriteToUDP(nack.Serialize(), destino)
}
