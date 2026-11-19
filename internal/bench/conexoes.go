package bench

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/pkg/client"
)

// ConfigConexoes define a escada de conexões simultâneas.
type ConfigConexoes struct {
	Degraus           []int
	Perguntas         int
	IntervaloPergunta time.Duration
	// Estabilizar é quanto esperar depois de conectar todos, para que os
	// heartbeats entrem em regime antes das medições.
	Estabilizar time.Duration
	Paralelas   int
	// Heartbeat é o intervalo de heartbeat dos clientes (0 = padrão da SDK).
	Heartbeat time.Duration

	// Critérios de degradação: o teste para no primeiro degrau que violar
	// algum deles.
	MinEntrega   float64
	MaxP99       time.Duration
	MaxFalhasPct float64
}

// DefaultConexoes sobe de 1000 até 32000 clientes.
func DefaultConexoes() ConfigConexoes {
	return ConfigConexoes{
		Degraus:           []int{1000, 2000, 4000, 8000, 12000, 16000, 24000, 32000},
		Perguntas:         5,
		IntervaloPergunta: time.Second,
		Estabilizar:       3 * time.Second,
		Paralelas:         200,
		MinEntrega:        0.99,
		MaxP99:            250 * time.Millisecond,
		MaxFalhasPct:      0.01,
	}
}

// DegrauConexoes é o resultado de um degrau da escada.
type DegrauConexoes struct {
	Clientes        int                     `json:"clientes"`
	Conectados      int                     `json:"conectados"`
	Falhas          int                     `json:"falhas"`
	TempoConexao    string                  `json:"tempo_conexao"`
	ConexoesPorSeg  float64                 `json:"conexoes_por_s"`
	Entrega         float64                 `json:"entrega_perguntas"`
	Latencia        metrics.LatencySnapshot `json:"latencia_pergunta"`
	FailoversFalsos uint64                  `json:"failovers_falsos"`
	Reenvios        uint64                  `json:"reenvios"`
	HeapMB          float64                 `json:"heap_mb"`
	Goroutines      int                     `json:"goroutines"`
	Degradou        bool                    `json:"degradou"`
	Motivo          string                  `json:"motivo,omitempty"`
}

// Conexoes sobe a quantidade de clientes simultâneos em degraus, cada um
// contra um broker novo, até o primeiro degrau que degradar.
func Conexoes(ctx context.Context, cfg ConfigConexoes, log io.Writer) ([]DegrauConexoes, error) {
	var out []DegrauConexoes
	for _, n := range cfg.Degraus {
		d, err := degrau(ctx, cfg, n, log)
		if err != nil {
			return out, err
		}
		out = append(out, d)
		logf(log, "[conexoes] %d clientes: entrega %.2f%%, p99 %.1f ms, falhas %d, failovers falsos %d\n",
			n, 100*d.Entrega, d.Latencia.P99Ms, d.Falhas, d.FailoversFalsos)
		if d.Degradou {
			logf(log, "[conexoes] degradou em %d clientes: %s\n", n, d.Motivo)
			break
		}
		runtime.GC()
		esperar(ctx, 2*time.Second) // deixa o SO liberar as portas
	}
	return out, nil
}

func degrau(ctx context.Context, cfg ConfigConexoes, n int, log io.Writer) (DegrauConexoes, error) {
	d := DegrauConexoes{Clientes: n}
	b, parar, err := subirBroker()
	if err != nil {
		return d, err
	}
	defer parar()

	ccfg := client.DefaultConfig()
	ccfg.InboxSize = 16
	if cfg.Heartbeat > 0 {
		ccfg.HeartbeatInterval = cfg.Heartbeat
		ccfg.HeartbeatTimeout = 0
	}
	var falsos atomic.Uint64
	ccfg.OnFailover = func(client.FailoverEvent) { falsos.Add(1) }

	const topico = "quiz/conexoes/perguntas"
	host, err := client.Connect(b.Addr(), ccfg)
	if err != nil {
		return d, err
	}
	defer host.Disconnect()
	host.CreateTopic(topico)

	lat := metrics.NewLatency(metrics.DefaultLatencySamples)
	var recebidas atomic.Int64
	logf(log, "[conexoes] abrindo %d clientes...\n", n)
	clientes, falhas, tempo := conectarVarios(ctx, n, cfg.Paralelas, b.Addr(), ccfg,
		func(_ int, c *client.Client) error {
			return c.Subscribe(topico, func(m client.Message) {
				if _, enviado, ok := lerCarimbo(m.Payload); ok {
					lat.Observe(time.Duration(metrics.Nanotime() - enviado))
					recebidas.Add(1)
				}
			})
		})
	defer fecharVarios(clientes)
	d.Conectados, d.Falhas = len(clientes), falhas
	d.TempoConexao = tempo.Round(time.Millisecond).String()
	if s := tempo.Seconds(); s > 0 {
		d.ConexoesPorSeg = float64(len(clientes)) / s
	}

	if err := esperar(ctx, cfg.Estabilizar); err != nil {
		return d, err
	}
	for q := 1; q <= cfg.Perguntas; q++ {
		host.Publish(topico, carimbo(q))
		if err := esperar(ctx, cfg.IntervaloPergunta); err != nil {
			return d, err
		}
	}
	esperar(ctx, 500*time.Millisecond)

	esperadas := int64(cfg.Perguntas) * int64(len(clientes))
	if esperadas > 0 {
		d.Entrega = float64(recebidas.Load()) / float64(esperadas)
	}
	d.Latencia = lat.Snapshot()
	st := somarStats(clientes)
	d.FailoversFalsos = falsos.Load()
	d.Reenvios = st.Retransmitted
	d.HeapMB = heapMB()
	d.Goroutines = runtime.NumGoroutine()

	var motivos []string
	if float64(falhas) > cfg.MaxFalhasPct*float64(n) {
		motivos = append(motivos, fmt.Sprintf("%d conexoes falharam", falhas))
	}
	if d.Entrega < cfg.MinEntrega {
		motivos = append(motivos, fmt.Sprintf("entrega de %.2f%%", 100*d.Entrega))
	}
	if p99 := time.Duration(d.Latencia.P99Ms * float64(time.Millisecond)); p99 > cfg.MaxP99 {
		motivos = append(motivos, fmt.Sprintf("p99 de %.0f ms", d.Latencia.P99Ms))
	}
	if d.FailoversFalsos > 0 {
		motivos = append(motivos, fmt.Sprintf("%d failovers falsos (heartbeats perdidos)", d.FailoversFalsos))
	}
	d.Degradou = len(motivos) > 0
	d.Motivo = strings.Join(motivos, "; ")
	return d, nil
}

// TabelaConexoes formata os degraus em Markdown.
func TabelaConexoes(ds []DegrauConexoes) string {
	var b strings.Builder
	b.WriteString("| Clientes | Conectados | Tempo p/ conectar | Perguntas entregues | p50 | p99 | Failovers falsos | Reenvios | Heap (broker + clientes) | Situação |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
	for _, d := range ds {
		sit := "ok"
		if d.Degradou {
			sit = "**degradou:** " + d.Motivo
		}
		fmt.Fprintf(&b, "| %d | %d | %s | %.2f%% | %.1f ms | %.1f ms | %d | %d | %.0f MB | %s |\n",
			d.Clientes, d.Conectados, d.TempoConexao, 100*d.Entrega, d.Latencia.P50Ms, d.Latencia.P99Ms,
			d.FailoversFalsos, d.Reenvios, d.HeapMB, sit)
	}
	return b.String()
}
