package broker

import (
	"net"
	"testing"
	"time"

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
