package broker

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/aggregator"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/answerwindow"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/scheduler"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/protocol"
)

func startBroker(t *testing.T) (*Broker, chan struct{}) {
	t.Helper()
	b, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		b.Run()
		close(done)
	}()
	return b, done
}

func TestRunReturnsAfterClose(t *testing.T) {
	b, done := startBroker(t)
	b.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run não terminou depois de Close")
	}
}

// O heartbeat é respondido mesmo quando o cliente já estourou o rate limit.
func TestHeartbeatBypassesRateLimit(t *testing.T) {
	b, done := startBroker(t)
	defer func() { b.Close(); <-done }()

	raddr, _ := net.ResolveUDPAddr("udp", b.Addr())
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Esgota o balde do cliente (Burst padrão = 20).
	flood := (&protocol.Message{Command: protocol.CmdPublish, Topic: "quiz/s/reacoes"}).Serialize()
	for i := 0; i < 100; i++ {
		conn.Write(flood)
	}
	time.Sleep(50 * time.Millisecond)
	conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	buf := make([]byte, 4096)
	for {
		if _, err := conn.Read(buf); err != nil {
			break // descarta as respostas do flood
		}
		conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	}

	conn.Write((&protocol.Message{Command: protocol.CmdHeartbeat}).Serialize())
	conn.SetReadDeadline(time.Now().Add(time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("heartbeat sem resposta com o rate limit estourado: %v", err)
	}
	resp, err := protocol.Deserialize(buf[:n])
	if err != nil || resp.Command != protocol.CmdHeartbeat {
		t.Fatalf("resposta = %+v, %v; quer CmdHeartbeat", resp, err)
	}
}

func dial(t *testing.T, b *Broker) *net.UDPConn {
	t.Helper()
	raddr, _ := net.ResolveUDPAddr("udp", b.Addr())
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func receive(t *testing.T, conn *net.UDPConn) *protocol.Message {
	t.Helper()
	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("sem resposta do broker: %v", err)
	}
	msg, err := protocol.Deserialize(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func request(t *testing.T, conn *net.UDPConn, cmd byte, topic string) {
	t.Helper()
	conn.Write((&protocol.Message{Command: cmd, Topic: topic}).Serialize())
	if resp := receive(t, conn); resp.Command != protocol.CmdAck {
		t.Fatalf("comando %d em %q: resposta %d %q, quer Ack", cmd, topic, resp.Command, resp.Payload)
	}
}

func TestPublishIsDeliveredThroughScheduler(t *testing.T) {
	b, done := startBroker(t)
	defer func() { b.Close(); <-done }()

	jogador := dial(t, b)
	host := dial(t, b)
	request(t, host, protocol.CmdCreateTopic, "quiz/s1/perguntas")
	request(t, jogador, protocol.CmdSubscribe, "quiz/s1/perguntas")

	host.Write((&protocol.Message{Command: protocol.CmdPublish, Topic: "quiz/s1/perguntas", Payload: []byte("Qual a capital?")}).Serialize())

	got := receive(t, jogador)
	if got.Command != protocol.CmdPublish || got.Topic != "quiz/s1/perguntas" || string(got.Payload) != "Qual a capital?" {
		t.Fatalf("jogador recebeu %+v", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for b.escalonador.Stats().Delivered[scheduler.Critical] != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("a pergunta deveria passar pelo escalonador como crítica, stats %+v", b.escalonador.Stats())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReactionBurstIsAggregated(t *testing.T) {
	b, done := startBroker(t)
	defer func() { b.Close(); <-done }()

	const topico = "quiz/s1/reacoes"
	telao := dial(t, b)
	request(t, telao, protocol.CmdCreateTopic, topico)
	request(t, telao, protocol.CmdSubscribe, topico)

	const jogadores, porJogador = 5, 10
	for i := 0; i < jogadores; i++ {
		j := dial(t, b)
		for k := 0; k < porJogador; k++ {
			j.Write((&protocol.Message{Command: protocol.CmdPublish, Topic: topico, Payload: []byte("👏")}).Serialize())
		}
	}

	total, mensagens := 0, 0
	for total < jogadores*porJogador {
		m := receive(t, telao)
		if m.Command != protocol.CmdPublish {
			continue
		}
		var resumo aggregator.Summary
		if err := json.Unmarshal(m.Payload, &resumo); err != nil {
			t.Fatalf("o telão deveria receber um resumo JSON, recebeu %q", m.Payload)
		}
		total += resumo.Reactions["👏"]
		mensagens++
	}
	if mensagens > 2 {
		t.Fatalf("%d reações deveriam virar no máximo 2 resumos, viraram %d", jogadores*porJogador, mensagens)
	}
}

func TestLateAnswersAreDropped(t *testing.T) {
	b, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.janela = answerwindow.New(100 * time.Millisecond)
	done := make(chan struct{})
	go func() { b.Run(); close(done) }()
	defer func() { b.Close(); <-done }()

	host, jogador, apurador := dial(t, b), dial(t, b), dial(t, b)
	request(t, host, protocol.CmdCreateTopic, "quiz/s1/perguntas")
	request(t, host, protocol.CmdCreateTopic, "quiz/s1/respostas")
	request(t, jogador, protocol.CmdSubscribe, "quiz/s1/perguntas")
	request(t, apurador, protocol.CmdSubscribe, "quiz/s1/respostas")

	publicar := func(conn *net.UDPConn, topico, payload string) {
		conn.Write((&protocol.Message{Command: protocol.CmdPublish, Topic: topico, Payload: []byte(payload)}).Serialize())
	}
	esperarPublicacao := func(conn *net.UDPConn) *protocol.Message {
		for {
			if m := receive(t, conn); m.Command == protocol.CmdPublish {
				return m
			}
		}
	}

	publicar(host, "quiz/s1/perguntas", "pergunta 1")
	esperarPublicacao(jogador)
	publicar(jogador, "quiz/s1/respostas", "no prazo")
	if got := esperarPublicacao(apurador); string(got.Payload) != "no prazo" {
		t.Fatalf("apurador recebeu %q", got.Payload)
	}

	time.Sleep(200 * time.Millisecond)
	publicar(jogador, "quiz/s1/respostas", "atrasada")

	publicar(host, "quiz/s1/perguntas", "pergunta 2")
	esperarPublicacao(jogador)
	publicar(jogador, "quiz/s1/respostas", "pergunta 2 no prazo")

	if got := esperarPublicacao(apurador); string(got.Payload) != "pergunta 2 no prazo" {
		t.Fatalf("a resposta atrasada deveria ter caducado, mas o apurador recebeu %q", got.Payload)
	}
}

func TestMetricsCountPublishesDeliveriesAndDrops(t *testing.T) {
	b, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.SetQuiet(true)
	b.janela = answerwindow.New(50 * time.Millisecond)
	done := make(chan struct{})
	go func() { b.Run(); close(done) }()
	defer func() { b.Close(); <-done }()

	host, jogador, apurador := dial(t, b), dial(t, b), dial(t, b)
	request(t, host, protocol.CmdCreateTopic, "quiz/s1/perguntas")
	request(t, host, protocol.CmdCreateTopic, "quiz/s1/respostas")
	request(t, jogador, protocol.CmdSubscribe, "quiz/s1/perguntas")
	request(t, apurador, protocol.CmdSubscribe, "quiz/s1/respostas")

	publicar := func(conn *net.UDPConn, topico, payload string) {
		conn.Write((&protocol.Message{Command: protocol.CmdPublish, Topic: topico, Payload: []byte(payload)}).Serialize())
	}
	publicar(host, "quiz/s1/perguntas", "p1")
	receive(t, jogador)
	publicar(jogador, "quiz/s1/respostas", "no prazo")
	receive(t, apurador)
	time.Sleep(100 * time.Millisecond)
	publicar(jogador, "quiz/s1/respostas", "atrasada")
	publicar(host, "quiz/s1/placar", "sem inscritos")

	deadline := time.Now().Add(2 * time.Second)
	var s metrics.Snapshot
	for {
		s = b.Metrics().Snapshot()
		if s.Topics["quiz/s1/respostas"].PublishDropped == 1 && s.Topics["quiz/s1/placar"].PublishDropped == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descartes não registrados: %+v", s.Topics)
		}
		time.Sleep(5 * time.Millisecond)
	}

	respostas := s.Topics["quiz/s1/respostas"]
	if respostas.Published != 2 || respostas.Delivered != 1 || respostas.PublishDrops[DropOutsideWindow] != 1 || respostas.PublishDropRate != 0.5 {
		t.Fatalf("métricas de respostas erradas: %+v", respostas)
	}
	if s.Topics["quiz/s1/placar"].PublishDrops[DropNoSubscribers] != 1 {
		t.Fatalf("publicação sem inscritos não contada: %+v", s.Topics["quiz/s1/placar"])
	}
	if s.Topics["quiz/s1/perguntas"].Delivered != 1 {
		t.Fatalf("entrega da pergunta não contada: %+v", s.Topics["quiz/s1/perguntas"])
	}
	if s.Meters[MeterAnswers].Count != 1 {
		t.Fatalf("vazão de respostas deveria contar só a aceita: %+v", s.Meters[MeterAnswers])
	}
	if l := s.Latencies[LatencyPrefix+"critica"]; l.Count != 1 || l.MaxMs <= 0 {
		t.Fatalf("latência da pergunta não medida: %+v", l)
	}
}

func TestMetricsCountRateLimitDrops(t *testing.T) {
	b, done := startBroker(t)
	b.SetQuiet(true)
	defer func() { b.Close(); <-done }()

	conn := dial(t, b)
	for i := 0; i < 100; i++ {
		conn.Write((&protocol.Message{Command: protocol.CmdPublish, Topic: "quiz/s1/reacoes", Payload: []byte("👏")}).Serialize())
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		r := b.Metrics().Snapshot().Topics["quiz/s1/reacoes"]
		if r.Published == 100 {
			if r.PublishDrops[DropRateLimit] < 50 {
				t.Fatalf("100 publicações seguidas deveriam estourar o rate limit (rajada de 20): %+v", r)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("publicações não contadas: %+v", r)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSubscribingToMoreTopicsKeepsOutbox(t *testing.T) {
	b, done := startBroker(t)
	defer func() { b.Close(); <-done }()

	jogador := dial(t, b)
	host := dial(t, b)
	for _, topico := range []string{"quiz/s1/perguntas", "quiz/s1/placar"} {
		request(t, host, protocol.CmdCreateTopic, topico)
	}
	request(t, jogador, protocol.CmdSubscribe, "quiz/s1/perguntas")

	host.Write((&protocol.Message{Command: protocol.CmdPublish, Topic: "quiz/s1/perguntas", Payload: []byte("p1")}).Serialize())
	receive(t, jogador)

	id := jogador.LocalAddr().String()
	deadline := time.Now().Add(2 * time.Second)
	for st, _ := b.outbox.Stats(id); st.Sent != 1; st, _ = b.outbox.Stats(id) {
		if time.Now().After(deadline) {
			t.Fatalf("fila de saída deveria registrar 1 envio, stats %+v", st)
		}
		time.Sleep(time.Millisecond)
	}

	request(t, jogador, protocol.CmdSubscribe, "quiz/s1/placar")

	if st, ok := b.outbox.Stats(id); !ok || st.Sent != 1 {
		t.Fatalf("a nova inscrição recriou a fila de saída (stats %+v, existe=%v)", st, ok)
	}
}
