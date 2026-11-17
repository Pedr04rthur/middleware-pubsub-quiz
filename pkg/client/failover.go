package client

import (
	"errors"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

// heartbeatLoop envia o "estou vivo" ao broker ativo e dispara o failover
// quando ele fica em silêncio por mais de HeartbeatTimeout. Qualquer
// datagrama recebido (resposta do heartbeat, Ack, mensagem entregue) conta
// como sinal de vida.
func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()
	hb := (&protocol.Message{Command: protocol.CmdHeartbeat}).Serialize()

	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
		}
		l := c.link.Load()
		if l.silence() > c.cfg.HeartbeatTimeout {
			c.failover(l) // bloqueia este loop até reconectar
			continue
		}
		l.udp.Write(hb) // erro (ex.: ICMP) é tratado como heartbeat perdido
	}
}

// failover troca a instância morta pela próxima da lista que responder,
// refazendo registro, tópicos e inscrições. Enquanto roda, as operações de
// controle da aplicação esperam e as publicações são descartadas. Se nenhuma
// instância responder, continua tentando (com ReconnectBackoff entre as
// voltas) até alguma voltar ou Disconnect ser chamado.
func (c *Client) failover(dead *link) {
	detected := time.Now()
	lastSeen := detected.Add(-dead.silence())

	c.mu.Lock()
	c.ready = make(chan struct{})
	c.mu.Unlock()
	c.failingOver.Store(true)
	close(dead.lost) // aborta a operação de controle em curso, se houver

	c.reqMu.Lock()
	defer c.reqMu.Unlock()
	dead.udp.Close()

	start := 0
	for i, a := range c.addrs {
		if a == dead.addr {
			start = i
			break
		}
	}

	for {
		// Começa pela instância seguinte; a que caiu é a última tentativa da
		// volta (ela pode ter reiniciado).
		for i := 1; i <= len(c.addrs); i++ {
			if c.isClosed() {
				return
			}
			l, err := c.dial(c.addrs[(start+i)%len(c.addrs)])
			if err != nil {
				continue
			}
			c.mu.Lock()
			if c.isClosed() {
				c.mu.Unlock()
				l.udp.Close()
				return
			}
			c.link.Store(l)
			c.mu.Unlock()

			restored, failed, err := c.establish(l)
			if err != nil {
				l.udp.Close()
				continue
			}

			now := time.Now()
			l.touch()
			c.failovers.Add(1)
			c.failingOver.Store(false)
			c.mu.Lock()
			close(c.ready)
			c.mu.Unlock()

			if cb := c.cfg.OnFailover; cb != nil {
				go cb(FailoverEvent{
					From:     dead.addr,
					To:       l.addr,
					Downtime: now.Sub(lastSeen),
					Recovery: now.Sub(detected),
					Restored: restored,
					Failed:   failed,
				})
			}
			return
		}

		select {
		case <-c.closed:
			return
		case <-time.After(c.cfg.ReconnectBackoff):
		}
	}
}

// establish registra o cliente na instância l e refaz o estado da sessão: os
// tópicos criados por este cliente e as inscrições. Como cada instância tem
// sua própria tabela de roteamento em memória, a nova instância não conhece
// nada disso. Retorna erro só se a instância não responder; inscrições
// recusadas são contadas em failed.
func (c *Client) establish(l *link) (restored, failed int, err error) {
	if err := c.doRequest(l, "connect", protocol.CmdConnect, ""); err != nil {
		return 0, 0, err
	}

	c.mu.Lock()
	topics := make([]string, 0, len(c.topics))
	for t := range c.topics {
		topics = append(topics, t)
	}
	subs := make([]string, 0, len(c.handlers))
	for t := range c.handlers {
		subs = append(subs, t)
	}
	c.mu.Unlock()

	for _, t := range topics {
		if err := c.ensureTopic(l, t); err != nil {
			return 0, 0, err
		}
	}

	for _, t := range subs {
		err := c.doRequest(l, "subscribe", protocol.CmdSubscribe, t)
		var nack *NackError
		// Um inscrito pode chegar à nova instância antes de quem criou o
		// tópico; nesse caso ele mesmo recria o tópico para não perder a
		// inscrição.
		if errors.As(err, &nack) && nack.Reason == reasonTopicMissing {
			if err = c.ensureTopic(l, t); err == nil {
				err = c.doRequest(l, "subscribe", protocol.CmdSubscribe, t)
			}
		}
		switch {
		case err == nil:
			restored++
		case errors.As(err, &nack):
			failed++
			c.restoreFailed.Add(1)
		default:
			return restored, failed, err
		}
	}
	return restored, failed, nil
}

// ensureTopic cria o tópico na instância l; um Nack (ex.: "já existe", porque
// outro cliente recriou primeiro) não é erro.
func (c *Client) ensureTopic(l *link, topic string) error {
	err := c.doRequest(l, "create_topic", protocol.CmdCreateTopic, topic)
	var nack *NackError
	if errors.As(err, &nack) {
		return nil
	}
	return err
}
