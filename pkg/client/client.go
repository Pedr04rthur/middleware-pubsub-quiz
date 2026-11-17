// Package client é a biblioteca cliente (SDK) do middleware publish/subscribe.
//
// Ela esconde das aplicações o protocolo binário e o socket UDP: a aplicação
// só chama Connect, CreateTopic, Subscribe, Publish e Disconnect.
//
// Semântica de entrega:
//
//   - Operações de controle (Connect, CreateTopic, RemoveTopic, Subscribe,
//     Unsubscribe, Disconnect) esperam o Ack/Nack do broker. Como UDP pode
//     perder datagramas (e o rate limit do broker descarta em silêncio), cada
//     tentativa tem um prazo e a operação é reenviada algumas vezes.
//   - Publish é at-most-once: envia o datagrama e retorna, sem esperar
//     confirmação. Se o broker avisar que o tópico não tem inscritos, o aviso
//     chega de forma assíncrona em Config.OnPublishNack.
//   - As mensagens recebidas são entregues aos handlers por uma goroutine
//     própria, então um handler lento não atrasa a leitura do socket.
package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

// Motivo do Nack que o broker envia quando uma publicação é descartada por
// falta de inscritos (internal/broker.handlePublish).
const reasonNoSubscribers = "topico sem inscritos"

// headerSize é o tamanho do cabeçalho do protocolo: comando (1) +
// tamanho do tópico (2) + tamanho do payload (4). (1 + 2 + 4 = 7)
const headerSize = 7

var (
	// ErrTimeout indica que o broker não respondeu a nenhuma das tentativas.
	ErrTimeout = errors.New("client: broker nao respondeu")
	// ErrClosed indica uso do cliente depois de Disconnect.
	ErrClosed = errors.New("client: conexao encerrada")
	// ErrPacketTooLarge indica que a mensagem não cabe em um datagrama aceito
	// pelo broker.
	ErrPacketTooLarge = errors.New("client: mensagem maior que o tamanho maximo do pacote")
	// ErrEmptyTopic indica que o tópico informado é vazio.
	ErrEmptyTopic = errors.New("client: topico vazio")
)

// NackError é retornado quando o broker recusa uma operação de controle.
type NackError struct {
	Op     string
	Topic  string
	Reason string
}

func (e *NackError) Error() string {
	return fmt.Sprintf("client: %s %q recusado pelo broker: %s", e.Op, e.Topic, e.Reason)
}

// Message é uma publicação recebida de um tópico inscrito.
type Message struct {
	Topic      string
	Payload    []byte
	ReceivedAt time.Time
}

// Handler trata as mensagens de um tópico inscrito.
type Handler func(Message)

// Config define prazos, filas e callbacks do cliente.
type Config struct {
	// RequestTimeout é quanto cada tentativa de uma operação de controle
	// espera pelo Ack/Nack do broker.
	RequestTimeout time.Duration
	// Retries é quantas vezes a operação é reenviada depois da primeira
	// tentativa sem resposta.
	Retries int

	// InboxSize é quantas mensagens recebidas podem esperar pelos handlers.
	// Se a fila encher, a mais antiga é descartada (a informação mais recente
	// é a que importa no quiz).
	InboxSize int

	// MaxPacketSize é o maior datagrama que o cliente envia. Deve ser no
	// máximo o buffer de leitura do broker (4096 bytes), senão o broker
	// recebe o pacote truncado.
	MaxPacketSize int

	// OnPublishNack é chamado (na goroutine dos handlers) quando o broker
	// descarta uma publicação por falta de inscritos. Pode ser nil.
	OnPublishNack func(topic, reason string)
}

// DefaultConfig retorna valores razoáveis para um quiz ao vivo em rede local.
func DefaultConfig() Config {
	return Config{
		RequestTimeout: 500 * time.Millisecond,
		Retries:        3,
		InboxSize:      1024,
		MaxPacketSize:  4096,
	}
}

// Stats são os contadores do cliente, úteis para as métricas do projeto.
type Stats struct {
	Published     uint64 // publicações enviadas
	PublishNacks  uint64 // publicações descartadas pelo broker (sem inscritos)
	Received      uint64 // mensagens recebidas de tópicos inscritos
	InboxDropped  uint64 // mensagens descartadas porque a fila local encheu
	Unroutable    uint64 // mensagens recebidas de tópicos sem handler
	Retransmitted uint64 // reenvios de operações de controle
}

// event é o que a goroutine de leitura repassa para a de despacho.
type event struct {
	msg  *Message
	nack *protocol.Message // Nack de publicação
}

// waiter é a operação de controle esperando resposta.
type waiter struct {
	topic string
	resp  chan *protocol.Message
}

// Client é uma sessão com o broker. É seguro para uso concorrente.
type Client struct {
	cfg  Config
	conn *net.UDPConn

	// reqMu garante uma operação de controle por vez. O protocolo não tem id
	// de requisição, então o Ack/Nack é associado à operação pelo tópico.
	reqMu sync.Mutex

	mu       sync.Mutex
	waiter   *waiter
	handlers map[string]Handler

	inbox      chan event
	closed     chan struct{}
	closeOnce  sync.Once
	readerDone chan struct{}

	published, publishNacks, received atomic.Uint64
	inboxDropped, unroutable, retrans atomic.Uint64
}

// Connect abre a sessão com o broker em addr (ex.: "localhost:8080") e
// espera a confirmação do registro.
func Connect(addr string, cfg Config) (*Client, error) {
	cfg = withDefaults(cfg)

	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: endereco invalido %q: %w", addr, err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, fmt.Errorf("client: abrindo socket UDP: %w", err)
	}

	c := &Client{
		cfg:        cfg,
		conn:       conn,
		handlers:   make(map[string]Handler),
		inbox:      make(chan event, cfg.InboxSize),
		closed:     make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	go c.readLoop()
	go c.dispatchLoop()

	if err := c.request("connect", protocol.CmdConnect, ""); err != nil {
		c.shutdown()
		return nil, err
	}
	return c, nil
}

func withDefaults(cfg Config) Config {
	def := DefaultConfig()
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = def.RequestTimeout
	}
	if cfg.Retries < 0 {
		cfg.Retries = 0
	}
	if cfg.InboxSize <= 0 {
		cfg.InboxSize = def.InboxSize
	}
	if cfg.MaxPacketSize <= 0 {
		cfg.MaxPacketSize = def.MaxPacketSize
	}
	return cfg
}

// LocalAddr é o endereço (IP:porta) com que o broker identifica este cliente.
func (c *Client) LocalAddr() string {
	return c.conn.LocalAddr().String()
}

// CreateTopic registra um novo tópico no broker.
func (c *Client) CreateTopic(topic string) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	return c.request("create_topic", protocol.CmdCreateTopic, topic)
}

// RemoveTopic apaga um tópico do broker (ex.: quando a sala termina).
func (c *Client) RemoveTopic(topic string) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	return c.request("remove_topic", protocol.CmdRemoveTopic, topic)
}

// Subscribe inscreve o cliente em topic. Cada mensagem publicada nele é
// entregue a handler. Inscrever-se de novo no mesmo tópico troca o handler.
// O tópico precisa existir no broker (ver CreateTopic).
func (c *Client) Subscribe(topic string, handler Handler) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if handler == nil {
		return errors.New("client: handler nil")
	}

	// O handler é registrado antes do envio para não perder mensagens que
	// cheguem logo após o Ack.
	c.mu.Lock()
	prev, had := c.handlers[topic]
	c.handlers[topic] = handler
	c.mu.Unlock()

	if err := c.request("subscribe", protocol.CmdSubscribe, topic); err != nil {
		c.mu.Lock()
		if had {
			c.handlers[topic] = prev
		} else {
			delete(c.handlers, topic)
		}
		c.mu.Unlock()
		return err
	}
	return nil
}

// Unsubscribe cancela a inscrição em topic.
func (c *Client) Unsubscribe(topic string) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if err := c.request("unsubscribe", protocol.CmdUnsubscribe, topic); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.handlers, topic)
	c.mu.Unlock()
	return nil
}

// Publish envia payload para topic sem esperar confirmação (at-most-once).
// Só retorna erro se a mensagem for inválida ou não puder ser enviada pelo
// socket; um descarte no broker é avisado em Config.OnPublishNack.
func (c *Client) Publish(topic string, payload []byte) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if c.isClosed() {
		return ErrClosed
	}
	if headerSize+len(topic)+len(payload) > c.cfg.MaxPacketSize {
		return ErrPacketTooLarge
	}
	msg := &protocol.Message{Command: protocol.CmdPublish, Topic: topic, Payload: payload}
	if _, err := c.conn.Write(msg.Serialize()); err != nil {
		if c.isClosed() {
			return ErrClosed
		}
		return fmt.Errorf("client: publish: %w", err)
	}
	c.published.Add(1)
	return nil
}

// Disconnect encerra a sessão de forma amigável: avisa o broker (que remove
// as inscrições e a fila de saída) e libera o socket. O socket é fechado
// mesmo que o broker não responda; nesse caso o erro é retornado.
func (c *Client) Disconnect() error {
	if c.isClosed() {
		return ErrClosed
	}
	err := c.request("disconnect", protocol.CmdDisconnect, "")
	c.shutdown()
	return err
}

// Stats retorna os contadores do cliente.
func (c *Client) Stats() Stats {
	return Stats{
		Published:     c.published.Load(),
		PublishNacks:  c.publishNacks.Load(),
		Received:      c.received.Load(),
		InboxDropped:  c.inboxDropped.Load(),
		Unroutable:    c.unroutable.Load(),
		Retransmitted: c.retrans.Load(),
	}
}

// request envia uma operação de controle e espera o Ack/Nack, reenviando
// quando o prazo de uma tentativa vence.
func (c *Client) request(op string, cmd byte, topic string) error {
	c.reqMu.Lock()
	defer c.reqMu.Unlock()

	if c.isClosed() {
		return ErrClosed
	}

	w := &waiter{topic: topic, resp: make(chan *protocol.Message, 1)}
	c.mu.Lock()
	c.waiter = w
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.waiter = nil
		c.mu.Unlock()
	}()

	packet := (&protocol.Message{Command: cmd, Topic: topic}).Serialize()
	timer := time.NewTimer(c.cfg.RequestTimeout)
	defer timer.Stop()

	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			c.retrans.Add(1)
			timer.Reset(c.cfg.RequestTimeout)
		}
		if _, err := c.conn.Write(packet); err != nil {
			if c.isClosed() {
				return ErrClosed
			}
			// Ex.: ICMP "connection refused" quando não há broker na porta.
			// Trata como perda e tenta de novo depois do prazo.
		}

		select {
		case resp := <-w.resp:
			if resp.Command == protocol.CmdNack {
				return &NackError{Op: op, Topic: topic, Reason: string(resp.Payload)}
			}
			return nil
		case <-timer.C:
		case <-c.closed:
			return ErrClosed
		}
	}
	return fmt.Errorf("%w (%s %q, %d tentativas)", ErrTimeout, op, topic, c.cfg.Retries+1)
}

func (c *Client) readLoop() {
	defer close(c.readerDone)
	buf := make([]byte, 64*1024)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			if c.isClosed() || errors.Is(err, net.ErrClosed) {
				return
			}
			// Erros como "connection refused" (ICMP) não encerram a sessão:
			// o broker pode voltar, e a detecção de falha é feita por prazo.
			continue
		}
		msg, err := protocol.Deserialize(buf[:n])
		if err != nil {
			continue
		}
		c.handlePacket(msg)
	}
}

func (c *Client) handlePacket(msg *protocol.Message) {
	switch msg.Command {
	case protocol.CmdPublish:
		c.received.Add(1)
		payload := make([]byte, len(msg.Payload)) // buf é reutilizado
		copy(payload, msg.Payload)
		c.enqueue(event{msg: &Message{Topic: msg.Topic, Payload: payload, ReceivedAt: time.Now()}})

	case protocol.CmdAck, protocol.CmdNack:
		// O único Nack de publicação é "sem inscritos"; ele nunca é resposta
		// de uma operação de controle.
		if msg.Command == protocol.CmdNack && string(msg.Payload) == reasonNoSubscribers {
			c.publishNacks.Add(1)
			nack := &protocol.Message{Command: msg.Command, Topic: msg.Topic, Payload: append([]byte(nil), msg.Payload...)}
			c.enqueue(event{nack: nack})
			return
		}
		// Os demais Acks/Nacks vão para a operação de controle pendente no
		// mesmo tópico. Acks de publicação sem operação pendente são
		// ignorados. Se uma publicação e uma operação de controle no mesmo
		// tópico se cruzarem, o Ack da publicação pode encerrar a espera da
		// operação; isso é inofensivo porque um Ack de publicação prova que o
		// tópico existe, e então Subscribe/Unsubscribe/RemoveTopic também
		// seriam aceitos (e CreateTopic só falharia por "já existe").
		c.mu.Lock()
		w := c.waiter
		c.mu.Unlock()
		if w == nil || w.topic != msg.Topic {
			return
		}
		resp := &protocol.Message{Command: msg.Command, Topic: msg.Topic, Payload: append([]byte(nil), msg.Payload...)}
		select {
		case w.resp <- resp:
		default: // já respondida (Ack duplicado de um reenvio)
		}
	}
}

// enqueue coloca o evento na fila dos handlers sem bloquear a leitura. Se a
// fila estiver cheia, descarta o mais antigo.
func (c *Client) enqueue(ev event) {
	select {
	case c.inbox <- ev:
		return
	default:
	}
	select {
	case <-c.inbox:
		c.inboxDropped.Add(1)
	default:
	}
	select {
	case c.inbox <- ev:
	default:
		c.inboxDropped.Add(1)
	}
}

func (c *Client) dispatchLoop() {
	for {
		select {
		case <-c.closed:
			return
		case ev := <-c.inbox:
			c.dispatch(ev)
		}
	}
}

func (c *Client) dispatch(ev event) {
	if ev.nack != nil {
		if cb := c.cfg.OnPublishNack; cb != nil {
			cb(ev.nack.Topic, string(ev.nack.Payload))
		}
		return
	}
	c.mu.Lock()
	h := c.handlers[ev.msg.Topic]
	c.mu.Unlock()
	if h == nil {
		c.unroutable.Add(1)
		return
	}
	h(*ev.msg)
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// shutdown fecha o socket e para as goroutines. Não espera o despacho, para
// que Disconnect possa ser chamado de dentro de um handler.
func (c *Client) shutdown() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.conn.Close()
		<-c.readerDone
	})
}
