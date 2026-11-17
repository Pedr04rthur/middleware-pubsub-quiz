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
//
// Resiliência (ver failover.go): o cliente envia heartbeats periódicos ao
// broker ativo. Se o broker ficar em silêncio por Config.HeartbeatTimeout, a
// biblioteca passa para a próxima instância de Config.Fallbacks, recria os
// tópicos que este cliente criou e refaz as inscrições, sem a aplicação
// perceber.
package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

// Motivos de Nack enviados pelo broker (internal/broker e internal/router).
const (
	reasonNoSubscribers = "topico sem inscritos"
	reasonTopicMissing  = "topico nao existe"
)

// headerSize é o tamanho do cabeçalho do protocolo: comando (1) +
// tamanho do tópico (2) + tamanho do payload (4).
const headerSize = 1 + 2 + 4

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

	// errLinkLost indica que a conexão usada pela operação foi declarada
	// morta no meio dela; a operação é refeita na nova instância.
	errLinkLost = errors.New("client: broker ativo caiu durante a operacao")
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

	// Fallbacks são os endereços das instâncias reserva do broker, tentadas
	// em ordem quando a instância ativa cai (ou não responde no Connect).
	Fallbacks []string

	// HeartbeatInterval é de quanto em quanto tempo o cliente envia um
	// heartbeat ao broker ativo. Negativo desativa heartbeat e failover.
	HeartbeatInterval time.Duration
	// HeartbeatTimeout é quanto tempo sem receber nada do broker ativo até
	// ele ser considerado fora do ar. Deve ser algumas vezes o intervalo,
	// para que a perda de um heartbeat isolado não cause failover.
	HeartbeatTimeout time.Duration
	// ReconnectBackoff é a pausa entre duas voltas completas pela lista de
	// brokers quando nenhum responde.
	ReconnectBackoff time.Duration

	// OnPublishNack é chamado (na goroutine dos handlers) quando o broker
	// descarta uma publicação por falta de inscritos. Pode ser nil.
	OnPublishNack func(topic, reason string)
	// OnFailover é chamado (em uma goroutine própria) depois que o cliente
	// passou para outra instância e refez tópicos e inscrições. Pode ser nil.
	OnFailover func(FailoverEvent)
}

// DefaultConfig retorna valores razoáveis para um quiz ao vivo em rede local.
func DefaultConfig() Config {
	return Config{
		RequestTimeout:    500 * time.Millisecond,
		Retries:           3,
		InboxSize:         1024,
		MaxPacketSize:     4096,
		HeartbeatInterval: 250 * time.Millisecond,
		HeartbeatTimeout:  time.Second,
		ReconnectBackoff:  500 * time.Millisecond,
	}
}

// FailoverEvent descreve uma troca de instância do broker.
type FailoverEvent struct {
	From, To string
	// Downtime é o tempo entre o último datagrama recebido da instância que
	// caiu e o fim da restauração na nova (o que a aplicação "sentiu").
	Downtime time.Duration
	// Recovery é o tempo entre a detecção da queda e o fim da restauração.
	Recovery time.Duration
	// Restored é quantas inscrições foram refeitas; Failed, quantas o novo
	// broker recusou.
	Restored, Failed int
}

// Stats são os contadores do cliente, úteis para as métricas do projeto.
type Stats struct {
	Published      uint64 // publicações enviadas
	PublishNacks   uint64 // publicações descartadas pelo broker (sem inscritos)
	PublishDropped uint64 // publicações perdidas porque o broker ativo estava fora
	Received       uint64 // mensagens recebidas de tópicos inscritos
	InboxDropped   uint64 // mensagens descartadas porque a fila local encheu
	Unroutable     uint64 // mensagens recebidas de tópicos sem handler
	Retransmitted  uint64 // reenvios de operações de controle
	Failovers      uint64 // trocas de instância do broker
	RestoreFailed  uint64 // inscrições que não puderam ser refeitas no failover
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

// link é o socket com uma instância do broker.
type link struct {
	addr     string
	udp      *net.UDPConn
	lost     chan struct{} // fechado quando a instância é declarada morta
	lastSeen atomic.Int64  // UnixNano do último datagrama recebido
}

func (l *link) touch() { l.lastSeen.Store(time.Now().UnixNano()) }

func (l *link) silence() time.Duration { return time.Since(time.Unix(0, l.lastSeen.Load())) }

// Client é uma sessão com o broker. É seguro para uso concorrente.
type Client struct {
	cfg   Config
	addrs []string // primária seguida das reservas

	link        atomic.Pointer[link] // instância ativa
	failingOver atomic.Bool

	// reqMu garante uma operação de controle por vez (o protocolo não tem id
	// de requisição, então o Ack/Nack é associado à operação pelo tópico) e
	// impede operações da aplicação durante o failover.
	reqMu sync.Mutex

	mu       sync.Mutex
	waiter   *waiter
	handlers map[string]Handler  // inscrições ativas
	topics   map[string]struct{} // tópicos criados por este cliente
	ready    chan struct{}       // fechado quando não há failover em curso

	inbox     chan event
	closed    chan struct{}
	closeOnce sync.Once

	published, publishNacks, publishDropped, received atomic.Uint64
	inboxDropped, unroutable, retrans                 atomic.Uint64
	failovers, restoreFailed                          atomic.Uint64
}

// Connect abre a sessão com o broker em addr (ex.: "localhost:8080") e
// espera a confirmação do registro. Se addr não responder, tenta as
// instâncias de cfg.Fallbacks em ordem.
func Connect(addr string, cfg Config) (*Client, error) {
	cfg = withDefaults(cfg)

	ready := make(chan struct{})
	close(ready)
	c := &Client{
		cfg:      cfg,
		addrs:    append([]string{addr}, cfg.Fallbacks...),
		handlers: make(map[string]Handler),
		topics:   make(map[string]struct{}),
		ready:    ready,
		inbox:    make(chan event, cfg.InboxSize),
		closed:   make(chan struct{}),
	}
	go c.dispatchLoop()

	var lastErr error
	for _, a := range c.addrs {
		l, err := c.dial(a)
		if err != nil {
			lastErr = err
			continue
		}
		c.link.Store(l)
		if err := c.doRequest(l, "connect", protocol.CmdConnect, ""); err != nil {
			l.udp.Close()
			lastErr = err
			continue
		}
		if cfg.HeartbeatInterval > 0 {
			go c.heartbeatLoop()
		}
		return c, nil
	}
	c.shutdown()
	return nil, lastErr
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
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = def.HeartbeatInterval
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 4 * cfg.HeartbeatInterval
	}
	if cfg.ReconnectBackoff <= 0 {
		cfg.ReconnectBackoff = def.ReconnectBackoff
	}
	return cfg
}

// dial abre o socket com uma instância e inicia sua goroutine de leitura.
func (c *Client) dial(addr string) (*link, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: endereco invalido %q: %w", addr, err)
	}
	udp, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, fmt.Errorf("client: abrindo socket UDP para %s: %w", addr, err)
	}
	l := &link{addr: addr, udp: udp, lost: make(chan struct{})}
	l.touch()
	go c.readLoop(l)
	return l, nil
}

// LocalAddr é o endereço (IP:porta) com que o broker ativo identifica este
// cliente. Muda depois de um failover.
func (c *Client) LocalAddr() string {
	return c.link.Load().udp.LocalAddr().String()
}

// BrokerAddr é o endereço da instância do broker em uso.
func (c *Client) BrokerAddr() string {
	return c.link.Load().addr
}

// CreateTopic registra um novo tópico no broker. O tópico é recriado
// automaticamente se o cliente passar para outra instância.
func (c *Client) CreateTopic(topic string) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if err := c.request("create_topic", protocol.CmdCreateTopic, topic); err != nil {
		return err
	}
	c.mu.Lock()
	c.topics[topic] = struct{}{}
	c.mu.Unlock()
	return nil
}

// RemoveTopic apaga um tópico do broker (ex.: quando a sala termina).
func (c *Client) RemoveTopic(topic string) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if err := c.request("remove_topic", protocol.CmdRemoveTopic, topic); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.topics, topic)
	c.mu.Unlock()
	return nil
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
// Só retorna erro se a mensagem for inválida ou o cliente estiver encerrado.
// Um descarte no broker é avisado em Config.OnPublishNack; publicações feitas
// enquanto o broker ativo está fora são perdidas (Stats.PublishDropped).
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
	if c.failingOver.Load() {
		c.publishDropped.Add(1)
		return nil
	}
	msg := &protocol.Message{Command: protocol.CmdPublish, Topic: topic, Payload: payload}
	if _, err := c.link.Load().udp.Write(msg.Serialize()); err != nil {
		if c.isClosed() {
			return ErrClosed
		}
		// Socket trocado por um failover, ou ICMP "connection refused" de um
		// broker que acabou de cair: no at-most-once isso é só uma perda.
		if errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNREFUSED) {
			c.publishDropped.Add(1)
			return nil
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
		Published:      c.published.Load(),
		PublishNacks:   c.publishNacks.Load(),
		PublishDropped: c.publishDropped.Load(),
		Received:       c.received.Load(),
		InboxDropped:   c.inboxDropped.Load(),
		Unroutable:     c.unroutable.Load(),
		Retransmitted:  c.retrans.Load(),
		Failovers:      c.failovers.Load(),
		RestoreFailed:  c.restoreFailed.Load(),
	}
}

// request executa uma operação de controle da aplicação. Se um failover
// estiver em curso, espera ele terminar; se o broker cair no meio da
// operação, ela é refeita na nova instância.
func (c *Client) request(op string, cmd byte, topic string) error {
	// Espera pelo failover no máximo o mesmo que uma operação sem resposta.
	budget := time.Duration(c.cfg.Retries+1) * c.cfg.RequestTimeout
	for {
		if err := c.waitReady(time.Now().Add(budget)); err != nil {
			if errors.Is(err, ErrTimeout) {
				return fmt.Errorf("%w (%s %q: failover ainda em curso)", err, op, topic)
			}
			return err
		}
		c.reqMu.Lock()
		err := c.doRequest(c.link.Load(), op, cmd, topic)
		c.reqMu.Unlock()
		if !errors.Is(err, errLinkLost) {
			return err
		}
	}
}

// waitReady espera não haver failover em curso.
func (c *Client) waitReady(deadline time.Time) error {
	c.mu.Lock()
	ready := c.ready
	c.mu.Unlock()

	select {
	case <-ready:
		return nil
	default:
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ready:
		return nil
	case <-c.closed:
		return ErrClosed
	case <-timer.C:
		return ErrTimeout
	}
}

// doRequest envia uma operação de controle pela conexão l e espera o
// Ack/Nack, reenviando quando o prazo de uma tentativa vence. Quem chama deve
// segurar reqMu.
func (c *Client) doRequest(l *link, op string, cmd byte, topic string) error {
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
		if _, err := l.udp.Write(packet); err != nil {
			if c.isClosed() {
				return ErrClosed
			}
			if errors.Is(err, net.ErrClosed) {
				return errLinkLost
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
		case <-l.lost:
			return errLinkLost
		case <-c.closed:
			return ErrClosed
		}
	}
	return fmt.Errorf("%w (%s %q em %s, %d tentativas)", ErrTimeout, op, topic, l.addr, c.cfg.Retries+1)
}

func (c *Client) readLoop(l *link) {
	buf := make([]byte, 64*1024)
	for {
		n, err := l.udp.Read(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// Erros como "connection refused" (ICMP) não encerram a leitura:
			// a queda do broker é detectada pela falta de heartbeat.
			continue
		}
		l.touch() // qualquer datagrama do broker prova que ele está vivo
		if c.link.Load() != l {
			continue // resposta atrasada de uma instância abandonada
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

	case protocol.CmdHeartbeat:
		// Já contou como sinal de vida em readLoop.
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

// shutdown fecha o socket ativo e para as goroutines. Não espera o despacho,
// para que Disconnect possa ser chamado de dentro de um handler.
func (c *Client) shutdown() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		close(c.closed)
		if l := c.link.Load(); l != nil {
			l.udp.Close()
		}
		c.mu.Unlock()
	})
}
