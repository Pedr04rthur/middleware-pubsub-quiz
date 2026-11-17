package broker

import (
	"context"
	"fmt"
	"net"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/outbox"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/ratelimit"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/router"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

type Broker struct {
	conn      *net.UDPConn
	router    *router.Router
	limitador *ratelimit.Manager
	outbox    *outbox.Hub
}

func New(porta string) (*Broker, error) {
	addr, err := net.ResolveUDPAddr("udp", porta)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	// Callback de desconexão do Outbox: remove o cliente do Router
	onDisconnect := func(id string, motivo error) {
		fmt.Printf("[OUTBOX] Cliente %s desconectado: %v\n", id, motivo)
		// (Por simplicidade, não removemos do Router aqui ainda;
		//  isso será feito na Task de Disconnect)
	}

	b := &Broker{
		conn:      conn,
		router:    router.New(),
		limitador: ratelimit.NewManager(ratelimit.DefaultConfig()),
		outbox:    outbox.NewHub(outbox.DefaultConfig(), onDisconnect),
	}

	go b.limitador.Run(context.Background())
	return b, nil
}

func (b *Broker) Close() error {
	return b.conn.Close()
}

func (b *Broker) Run() {
	fmt.Println("Broker UDP rodando na porta 8080...")
	buffer := make([]byte, 4096)

	for {
		n, clienteAddr, err := b.conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Erro ao ler UDP:", err)
			continue
		}

		if b.limitador.Allow(clienteAddr.String()) != ratelimit.Allowed {
			fmt.Printf("Mensagem de %s descartada pelo Rate Limit\n", clienteAddr)
			continue
		}

		msg, err := protocol.Deserialize(buffer[:n])
		if err != nil {
			fmt.Println("Pacote inválido de", clienteAddr, ":", err)
			continue
		}

		b.handleMessage(msg, clienteAddr)
	}
}

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
		fmt.Printf("[CREATE_TOPIC] Tópico criado: %s\n", msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)

	case protocol.CmdRemoveTopic:
		if err := b.router.RemoveTopic(msg.Topic); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		fmt.Printf("[REMOVE_TOPIC] Tópico removido: %s\n", msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)

	case protocol.CmdSubscribe:
		b.handleSubscribe(msg, clienteAddr, clienteStr)

	case protocol.CmdUnsubscribe:
		if err := b.router.Unsubscribe(msg.Topic, clienteStr); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		// Remove do outbox (se existir)
		// b.outbox.Remove(clienteStr) // (método pode não existir, verificar)
		fmt.Printf("[UNSUBSCRIBE] %s desinscrito de %s\n", clienteStr, msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)

	case protocol.CmdPublish:
		b.handlePublish(msg, clienteAddr)

	case protocol.CmdDisconnect:
		fmt.Printf("[DISCONNECT] %s desconectado\n", clienteStr)
		b.enviarAck(clienteAddr, msg.Topic)

	default:
		fmt.Printf("[UNKNOWN] Comando desconhecido: %d\n", msg.Command)
		b.enviarNack(clienteAddr, msg.Topic, "comando desconhecido")
	}
}

func (b *Broker) handleSubscribe(msg *protocol.Message, clienteAddr *net.UDPAddr, clienteStr string) {
	if err := b.router.Subscribe(msg.Topic, clienteStr); err != nil {
		b.enviarNack(clienteAddr, msg.Topic, err.Error())
		return
	}

	// Registra o cliente no Outbox (se ainda não estiver registrado).
	// A função de envio usa o UDP para mandar para o endereço do cliente.
	b.outbox.Add(clienteStr, func(p []byte) error {
		_, err := b.conn.WriteToUDP(p, clienteAddr)
		return err
	})

	fmt.Printf("[SUBSCRIBE] %s inscrito em %s\n", clienteStr, msg.Topic)
	b.enviarAck(clienteAddr, msg.Topic)
}

func (b *Broker) handlePublish(msg *protocol.Message, clienteAddr *net.UDPAddr) {
	inscritos := b.router.GetSubscribers(msg.Topic)

	// ============================================================
	// TASK-PA04: Checagem de tópico sem inscritos
	// ============================================================
	if len(inscritos) == 0 {
		fmt.Printf("[PUBLISH-DESCARTADO] Tópico %s sem inscritos. Avisando %s.\n", msg.Topic, clienteAddr)
		b.enviarNack(clienteAddr, msg.Topic, "topico sem inscritos")
		return
	}

	fmt.Printf("[PUBLISH] %s (publicado por %s) -> %d inscritos\n", msg.Topic, clienteAddr, len(inscritos))

	// Serializa a mensagem uma vez só (otimização)
	payload := msg.Serialize()

	// Entrega para cada inscrito via Outbox (fila privada)
	for _, endereco := range inscritos {
		b.outbox.Deliver(endereco, payload)
	}

	b.enviarAck(clienteAddr, msg.Topic)
}

func (b *Broker) enviarAck(destino *net.UDPAddr, topico string) {
	ack := &protocol.Message{Command: protocol.CmdAck, Topic: topico}
	b.conn.WriteToUDP(ack.Serialize(), destino)
}

func (b *Broker) enviarNack(destino *net.UDPAddr, topico string, motivo string) {
	nack := &protocol.Message{Command: protocol.CmdNack, Topic: topico, Payload: []byte(motivo)}
	b.conn.WriteToUDP(nack.Serialize(), destino)
}
