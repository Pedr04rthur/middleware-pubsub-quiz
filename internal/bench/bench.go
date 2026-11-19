// Package bench reúne os cenários de validação do middleware (TASK-SC05):
//
//   - Ruido: latência das perguntas com e sem clientes barulhentos;
//   - Failover: interrupção e perdas quando o broker primário cai;
//   - Conexoes: até quantos clientes simultâneos o broker aguenta sem degradar.
//
// Cada cenário sobe brokers novos no próprio processo (internal/broker), para
// começar de um estado limpo e poder derrubar uma instância no meio do teste.
// Por isso broker e clientes dividem a mesma máquina e o mesmo runtime Go: os
// números são um limite inferior do que o broker faria sozinho.
package bench

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/broker"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

// Ambiente descreve a máquina em que os testes rodaram.
type Ambiente struct {
	SO          string `json:"so"`
	Arquitetura string `json:"arquitetura"`
	CPUs        int    `json:"cpus"`
	Go          string `json:"go"`
	Data        string `json:"data"`
}

// LerAmbiente coleta os dados do ambiente atual.
func LerAmbiente() Ambiente {
	return Ambiente{
		SO:          runtime.GOOS,
		Arquitetura: runtime.GOARCH,
		CPUs:        runtime.NumCPU(),
		Go:          runtime.Version(),
		Data:        time.Now().Format("2006-01-02 15:04"),
	}
}

// subirBroker inicia um broker silencioso numa porta livre.
func subirBroker() (*broker.Broker, func(), error) {
	b, err := broker.New("127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	b.SetQuiet(true)
	done := make(chan struct{})
	go func() { b.Run(); close(done) }()
	var once sync.Once
	parar := func() { once.Do(func() { b.Close(); <-done }) }
	return b, parar, nil
}

// conectarVarios abre n clientes com no máximo paralelas conexões ao mesmo
// tempo; preparar roda logo após a conexão (ex.: Subscribe).
func conectarVarios(ctx context.Context, n, paralelas int, addr string, cfg client.Config,
	preparar func(i int, c *client.Client) error) ([]*client.Client, int, time.Duration) {

	inicio := time.Now()
	clientes := make([]*client.Client, n)
	sem := make(chan struct{}, max(paralelas, 1))
	var wg sync.WaitGroup
	for i := 0; i < n && ctx.Err() == nil; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			c, err := client.Connect(addr, cfg)
			if err != nil {
				return
			}
			if preparar != nil {
				if err := preparar(i, c); err != nil {
					c.Disconnect()
					return
				}
			}
			clientes[i] = c
		}(i)
	}
	wg.Wait()
	ok := clientes[:0]
	for _, c := range clientes {
		if c != nil {
			ok = append(ok, c)
		}
	}
	return ok, n - len(ok), time.Since(inicio)
}

// fecharVarios encerra os clientes sem esperar o Ack do broker (que pode já
// ter caído), em paralelo.
func fecharVarios(clientes []*client.Client) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 256)
	for _, c := range clientes {
		sem <- struct{}{}
		wg.Add(1)
		go func(c *client.Client) {
			defer func() { <-sem; wg.Done() }()
			c.Disconnect()
		}(c)
	}
	wg.Wait()
}

func carimbo(seq int) []byte {
	return []byte(strconv.Itoa(seq) + "|" + strconv.FormatInt(metrics.Nanotime(), 10))
}

func lerCarimbo(p []byte) (seq int, enviado int64, ok bool) {
	a, b, achou := strings.Cut(string(p), "|")
	if !achou {
		return 0, 0, false
	}
	s, err1 := strconv.Atoi(a)
	ns, err2 := strconv.ParseInt(b, 10, 64)
	return s, ns, err1 == nil && err2 == nil
}

func somarStats(clientes []*client.Client) client.Stats {
	var a client.Stats
	for _, c := range clientes {
		b := c.Stats()
		a.Published += b.Published
		a.PublishNacks += b.PublishNacks
		a.PublishDropped += b.PublishDropped
		a.Received += b.Received
		a.InboxDropped += b.InboxDropped
		a.Unroutable += b.Unroutable
		a.Retransmitted += b.Retransmitted
		a.Failovers += b.Failovers
		a.RestoreFailed += b.RestoreFailed
	}
	return a
}

func esperar(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func mediana(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func logf(w io.Writer, format string, args ...any) {
	if w != nil {
		fmt.Fprintf(w, format, args...)
	}
}

func heapMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapInuse) / (1 << 20)
}
