package broker

import (
	"fmt"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/metrics"
	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/scheduler"
)

// Nomes das métricas de vazão do broker.
const (
	MeterPackets             = "pacotes_recebidos"
	MeterPublishes           = "publicacoes"
	MeterAnswers             = "respostas"
	MeterAggregatedReactions = "reacoes_agregadas"
	MeterReactionSummaries   = "resumos_de_reacoes"
	MeterDeliveries          = "entregas"
)

// LatencyPrefix é o prefixo das latências internas do broker, medidas da
// chegada da publicação até o envio ao inscrito, uma por prioridade
// (ex.: "broker_critica").
const LatencyPrefix = "broker_"

// Motivos de descarte de publicação registrados pelo próprio broker. Os da
// fila do escalonador e das filas de saída vêm de scheduler.Drop* e outbox.Drop*.
const (
	DropRateLimit     = "rate_limit"
	DropNoSubscribers = "sem_inscritos"
	DropOutsideWindow = "fora_da_janela"
)

// Metrics retorna o registro de métricas do broker.
func (b *Broker) Metrics() *metrics.Registry {
	return b.metricas
}

// SetQuiet desliga (ou religa) o log de cada mensagem. Nos testes de estresse
// escrever no console a cada pacote custaria mais que o próprio broker.
func (b *Broker) SetQuiet(quiet bool) {
	b.silencioso.Store(quiet)
}

func (b *Broker) logf(format string, args ...any) {
	if !b.silencioso.Load() {
		fmt.Printf(format, args...)
	}
}

func (b *Broker) registrarEnvio(_ string, m scheduler.Message) {
	b.metricas.Topics.Delivered(m.Topic)
	b.metricas.Meter(MeterDeliveries).Mark(1)
	if m.ReceivedNs != 0 {
		b.metricas.Latency(LatencyPrefix + m.Priority.String()).Observe(metrics.Since(m.ReceivedNs))
	}
}

func (b *Broker) registrarDescarteEntrega(_ string, m scheduler.Message, motivo string) {
	b.metricas.Topics.DeliveryDropped(m.Topic, motivo)
}

func (b *Broker) registrarDescartePublicacao(m scheduler.Message, motivo string) {
	b.metricas.Topics.PublishDropped(m.Topic, motivo)
}
