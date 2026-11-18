package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/answerwindow"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/outbox"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/ratelimit"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/router"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/scheduler"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

type Broker struct {
	conn        *net.UDPConn
	router      *router.Router
	limitador   *ratelimit.Manager
	escalonador *scheduler.Scheduler
	agregador   *aggregator.Aggregator
	janela      *answerwindow.Window
	outbox      *outbox.Hub
	metricas    *metrics.Registry
	silencioso  atomic.Bool
	cancel      context.CancelFunc
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

	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		conn:      conn,
		router:    router.New(),
		limitador: ratelimit.NewManager(ratelimit.DefaultConfig()),
		metricas:  metrics.NewRegistry(),
		cancel:    cancel,
	}

	cfgOutbox := outbox.DefaultConfig()
	cfgOutbox.OnSent = b.registrarEnvio
	cfgOutbox.OnDrop = b.registrarDescarteEntrega
	b.outbox = outbox.NewHub(cfgOutbox, func(id string, motivo error) {
		b.logf("[OUTBOX] Cliente %s desconectado: %v\n", id, motivo)
		removidos := b.router.RemoveClient(id)
		b.logf("[OUTBOX] Removidas %d inscricoes de %s\n", removidos, id)
	})

	cfgEscalonador := scheduler.DefaultConfig()
	cfgEscalonador.OnDrop = b.registrarDescartePublicacao
	b.escalonador = scheduler.New(cfgEscalonador, b.entregar)
	b.janela = answerwindow.New(scheduler.ClampAnswerTTL(cfgEscalonador.AnswerTTL))
	b.agregador = aggregator.New(aggregator.DefaultConfig(), b.publicarResumo)

	go b.limitador.Run(ctx)
	go b.escalonador.Run(ctx)
	go b.agregador.Run(ctx)
	go b.metricas.Run(ctx, time.Second)
	return b, nil
}

func (b *Broker) Close() error {
	b.cancel()
	return b.conn.Close()
}

// Addr é o endereço UDP em que o broker está escutando.
func (b *Broker) Addr() string {
	return b.conn.LocalAddr().String()
}

// Run processa datagramas até o socket ser fechado por Close.
func (b *Broker) Run() {
	fmt.Printf("Broker UDP rodando em %s...\n", b.Addr())
	buffer := make([]byte, 4096)
	for {
		n, clienteAddr, err := b.conn.ReadFromUDP(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			b.logf("Erro ao ler UDP: %v\n", err)
			continue
		}
		b.metricas.Meter(MeterPackets).Mark(1)
		msg, err := protocol.Deserialize(buffer[:n])
		if err != nil {
			b.logf("Pacote invalido de %s: %v\n", clienteAddr, err)
			continue
		}
		if msg.Command == protocol.CmdPublish {
			b.metricas.Topics.Published(msg.Topic)
			b.metricas.Meter(MeterPublishes).Mark(1)
		}
		// O heartbeat não passa pelo rate limit: um jogador barulhento que
		// estoura o limite com reações não pode perder o próprio heartbeat,
		// senão a biblioteca cliente acharia que o broker caiu.
		if msg.Command == protocol.CmdHeartbeat {
			b.conn.WriteToUDP(msg.Serialize(), clienteAddr)
			continue
		}
		if b.limitador.Allow(clienteAddr.String()) != ratelimit.Allowed {
			b.logf("Mensagem de %s descartada pelo Rate Limit\n", clienteAddr)
			if msg.Command == protocol.CmdPublish {
				b.metricas.Topics.PublishDropped(msg.Topic, DropRateLimit)
			}
			continue
		}
		b.handleMessage(msg, clienteAddr)
	}
}

func (b *Broker) handleMessage(msg *protocol.Message, clienteAddr *net.UDPAddr) {
	clienteStr := clienteAddr.String()
	switch msg.Command {
	case protocol.CmdConnect:
		b.logf("[CONNECT] %s conectado\n", clienteStr)
		b.enviarAck(clienteAddr, msg.Topic)
	case protocol.CmdCreateTopic:
		if err := b.router.CreateTopic(msg.Topic); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		b.logf("[CREATE_TOPIC] Topico criado: %s\n", msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)
	case protocol.CmdRemoveTopic:
		if err := b.router.RemoveTopic(msg.Topic); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		if sala, tipo, ok := scheduler.ParseTopic(msg.Topic); ok && tipo == scheduler.KindQuestions {
			b.janela.Forget(sala)
		}
		b.logf("[REMOVE_TOPIC] Topico removido: %s\n", msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)
	case protocol.CmdSubscribe:
		b.handleSubscribe(msg, clienteAddr, clienteStr)
	case protocol.CmdUnsubscribe:
		if err := b.router.Unsubscribe(msg.Topic, clienteStr); err != nil {
			b.enviarNack(clienteAddr, msg.Topic, err.Error())
			return
		}
		b.logf("[UNSUBSCRIBE] %s desinscrito de %s\n", clienteStr, msg.Topic)
		b.enviarAck(clienteAddr, msg.Topic)
	case protocol.CmdPublish:
		b.handlePublish(msg, clienteAddr)
	case protocol.CmdDisconnect:
		b.logf("[DISCONNECT] %s desconectado\n", clienteStr)
		removidos := b.router.RemoveClient(clienteStr)
		b.outbox.Remove(clienteStr)
		b.logf("[DISCONNECT] Removidas %d inscricoes e fila de saida de %s\n", removidos, clienteStr)
		b.enviarAck(clienteAddr, msg.Topic)
	default:
		b.logf("[UNKNOWN] Comando desconhecido: %d\n", msg.Command)
		b.enviarNack(clienteAddr, msg.Topic, "comando desconhecido")
	}
}

func (b *Broker) handleSubscribe(msg *protocol.Message, clienteAddr *net.UDPAddr, clienteStr string) {
	if err := b.router.Subscribe(msg.Topic, clienteStr); err != nil {
		b.enviarNack(clienteAddr, msg.Topic, err.Error())
		return
	}
	b.outbox.Ensure(clienteStr, func(p []byte) error {
		_, err := b.conn.WriteToUDP(p, clienteAddr)
		return err
	})
	b.logf("[SUBSCRIBE] %s inscrito em %s\n", clienteStr, msg.Topic)
	b.enviarAck(clienteAddr, msg.Topic)
}

func (b *Broker) handlePublish(msg *protocol.Message, clienteAddr *net.UDPAddr) {
	recebidoNs := metrics.Nanotime()
	inscritos := b.router.GetSubscribers(msg.Topic)
	if len(inscritos) == 0 {
		b.metricas.Topics.PublishDropped(msg.Topic, DropNoSubscribers)
		b.logf("[PUBLISH-DESCARTADO] Topico %s sem inscritos. Avisando %s.\n", msg.Topic, clienteAddr)
		b.enviarNack(clienteAddr, msg.Topic, "topico sem inscritos")
		return
	}

	// Os descartes abaixo são silenciosos, sem Nack: a biblioteca cliente
	// trataria um Nack com outro motivo como resposta de uma operação de
	// controle no mesmo tópico. Publish é at-most-once, como no rate limit.
	sala, tipo, _ := scheduler.ParseTopic(msg.Topic)
	switch tipo {
	case scheduler.KindReactions:
		b.agregador.Add(msg.Topic, msg.Payload)
		b.metricas.Meter(MeterAggregatedReactions).Mark(1)
		b.enviarAck(clienteAddr, msg.Topic)
		return
	case scheduler.KindQuestions:
		b.janela.QuestionAsked(sala)
	case scheduler.KindAnswers:
		if !b.janela.Accept(sala) {
			b.metricas.Topics.PublishDropped(msg.Topic, DropOutsideWindow)
			b.logf("[PUBLISH-DESCARTADO] Resposta de %s em %s fora da janela da pergunta\n", clienteAddr, msg.Topic)
			return
		}
	}

	if !b.escalonador.Publish(scheduler.Message{Topic: msg.Topic, Payload: msg.Serialize(), From: clienteAddr.String(), ReceivedNs: recebidoNs}) {
		b.logf("[PUBLISH-DESCARTADO] Fila de prioridade %s cheia; %s de %s descartado\n", scheduler.Classify(msg.Topic), msg.Topic, clienteAddr)
		return
	}
	if tipo == scheduler.KindAnswers {
		b.metricas.Meter(MeterAnswers).Mark(1)
	}
	b.logf("[PUBLISH] %s (publicado por %s) -> %d inscritos\n", msg.Topic, clienteAddr, len(inscritos))
	b.enviarAck(clienteAddr, msg.Topic)
}

// publicarResumo é chamado pelo agregador com o resumo das reações de um
// tópico e o publica para os inscritos como uma reação comum.
func (b *Broker) publicarResumo(topico string, resumo []byte) {
	if len(b.router.GetSubscribers(topico)) == 0 {
		return
	}
	pacote := (&protocol.Message{Command: protocol.CmdPublish, Topic: topico, Payload: resumo}).Serialize()
	b.escalonador.Publish(scheduler.Message{Topic: topico, Payload: pacote, From: "broker", ReceivedNs: metrics.Nanotime()})
	b.metricas.Meter(MeterReactionSummaries).Mark(1)
}

// entregar é chamado pelo escalonador, em ordem de prioridade, e repassa a
// mensagem para a fila de saída de cada inscrito do tópico.
func (b *Broker) entregar(m scheduler.Message) {
	for _, endereco := range b.router.GetSubscribers(m.Topic) {
		b.outbox.Deliver(endereco, m)
	}
}

func (b *Broker) enviarAck(destino *net.UDPAddr, topico string) {
	ack := &protocol.Message{Command: protocol.CmdAck, Topic: topico}
	b.conn.WriteToUDP(ack.Serialize(), destino)
}

func (b *Broker) enviarNack(destino *net.UDPAddr, topico string, motivo string) {
	nack := &protocol.Message{Command: protocol.CmdNack, Topic: topico, Payload: []byte(motivo)}
	b.conn.WriteToUDP(nack.Serialize(), destino)
}
