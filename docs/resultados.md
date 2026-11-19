# Resultados dos testes de validação e benchmarks (TASK-SC05)

Medições do middleware nos três cenários pedidos: latência das perguntas com e
sem ruído, comportamento no failover e limite de conexões simultâneas. Os
dados brutos estão em [`benchmarks/resultados.json`](benchmarks/resultados.json)
e [`benchmarks/conexoes-heartbeat-1s.json`](benchmarks/conexoes-heartbeat-1s.json).

## Ambiente e método

- **Máquina:** Apple M4 (10 núcleos), 16 GB de RAM, macOS 26.5, Go 1.27.
- **Ferramenta:** `go run ./cmd/bench` (código em `internal/bench`). Cada
  cenário sobe brokers novos **no mesmo processo** dos clientes, para começar
  de um estado limpo e poder derrubar uma instância no meio do teste.
- **Latência** é medida de ponta a ponta (do `Publish` do apresentador até o
  handler do jogador) com o relógio monotônico, dentro do mesmo processo, sem
  depender de relógios sincronizados.
- **Limitação importante:** broker e milhares de clientes dividem a mesma
  máquina e o mesmo runtime Go (o gerador sozinho chega a 200% de CPU com
  3200 clientes). Os números são um **limite inferior** do que o broker faria
  numa máquina dedicada, com os clientes espalhados pela rede.

Para reproduzir:

```bash
go run ./cmd/bench                     # os três cenários (~5 min)
go run ./cmd/bench -cenarios conexoes -heartbeat 1s -degraus 8000,12000,16000,24000
```

## 1. Latência das perguntas com e sem ruído

Cada linha é a **mediana de 3 repetições**. Em cada repetição as mesmas
conexões passam por duas fases: sem barulho e com 200 clientes barulhentos
mandando 100 reações/s cada (20.000 reações/s, 20x o limiar de carga do
broker). Os jogadores normais também reagem (0,5/s) e respondem todos dentro
de uma janela de 300 ms após cada pergunta.

| Jogadores | Fase | p50 | p95 | p99 | Perguntas entregues | Respostas entregues | Pico de respostas |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1000 | sem barulho | 4.7 ms | 12.0 ms | 13.0 ms | 100.00% | 100.00% | 3373/s |
| 1000 | com 200 barulhentos | 5.9 ms | 11.1 ms | 12.0 ms | 100.00% | 100.00% | 3376/s |
| 3000 | sem barulho | 13.2 ms | 23.9 ms | 25.4 ms | 100.00% | 100.00% | 10091/s |
| 3000 | com 200 barulhentos | 10.6 ms | 16.6 ms | 17.7 ms | 100.00% | 100.00% | 10084/s |

**Leitura:**

- **O barulho não piorou as perguntas.** Entrega de 100% e p99 dentro da
  mesma faixa nas duas fases, em todas as repetições. É o efeito combinado
  do escalonador (perguntas e placar passam na frente), da agregação de
  reações e do rate limit, que cortou **97% a 99% do spam** antes de ele
  entrar nas filas.
- **Nenhuma resposta perdida**, mesmo com 3000 jogadores respondendo em
  300 ms (~10.000 respostas/s no pico). Antes da correção do rate limit
  (PR #13), 19% delas eram cortadas nesse mesmo cenário.
- Com 3000 jogadores o p99 com barulho ficou *menor* que sem barulho nas três
  repetições. Não é o barulho ajudando: a fase com barulho sempre roda depois
  da linha de base, já com memória e goroutines aquecidas, e com a CPU em
  frequência alta. A ordem das fases não foi alternada; a conclusão segura é
  "o barulho não aumentou a latência", não "diminuiu".
- A latência cresce com o número de jogadores (13 ms → 25 ms de p99), porque
  cada pergunta vira uma entrega para cada inscrito.

## 2. Failover (derrubando o broker primário)

1000 jogadores e o apresentador ligados ao broker primário, com uma reserva
configurada. O apresentador publica uma pergunta a cada 125 ms; depois de 3 s
o primário é derrubado (`Close()`) e o teste observa mais 5 s. Comparamos o
heartbeat padrão da SDK com um mais agressivo.

| Heartbeat (intervalo / timeout) | Clientes migrados | Interrupção p50 | p95 | máx | Restauração na reserva (p95) | Perguntas perdidas por jogador | p99 antes | p99 depois |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 250ms / 1s | 1001 de 1001 | 1372 ms | 1500 ms | 1512 ms | 17.0 ms | 10.1 de 64 | 14.9 ms | 15.0 ms |
| 100ms / 400ms | 1001 de 1001 | 745 ms | 755 ms | 758 ms | 24.0 ms | 4.7 de 64 | 15.2 ms | 15.0 ms |

*Interrupção:* para cada jogador, o tempo entre a última pergunta recebida
antes da queda e a primeira depois dela.

**Leitura:**

- **Todos os 1001 clientes migraram sozinhos** e nenhum ficou sem receber
  depois da queda; a aplicação não fez nada.
- **A interrupção é dominada pela detecção**, não pela troca: a SDK leva o
  `HeartbeatTimeout` (mais até um intervalo de heartbeat) para perceber a
  queda, e depois restaura registro, tópicos e inscrições na reserva em
  10–25 ms, mesmo com 1000 clientes chegando juntos.
- **Trade-off do heartbeat:** 100 ms / 400 ms corta a interrupção quase pela
  metade (1,37 s → 0,75 s) e as perdas de 10 para 5 perguntas por jogador,
  ao custo de 2,5x mais pacotes de heartbeat (ver cenário 3).
- As perdas batem com a interrupção (10,1 perdidas contra 11 que cabem em
  1,37 s): o que se perde é só o que foi publicado enquanto não havia broker,
  coerente com a entrega at-most-once.
- A latência depois do failover é igual à de antes (p99 ~15 ms).

## 3. Limite de conexões simultâneas

Escada de clientes, cada degrau contra um broker novo: todos se inscrevem no
tópico de perguntas, esperam 3 s para os heartbeats entrarem em regime e
recebem 5 perguntas. Um degrau "degrada" se mais de 1% das conexões falha, a
entrega fica abaixo de 99%, o p99 passa de 250 ms ou algum cliente faz
failover sem o broker ter caído.

**Heartbeat padrão (250 ms):**

| Clientes | Conectados | Tempo p/ conectar | Perguntas entregues | p50 | p99 | Failovers falsos | Heap (broker + clientes) | Situação |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1000 | 1000 | 33 ms | 100.00% | 5.9 ms | 10.7 ms | 0 | 373 MB | ok |
| 2000 | 2000 | 70 ms | 100.00% | 10.9 ms | 20.2 ms | 0 | 538 MB | ok |
| 4000 | 4000 | 130 ms | 100.00% | 11.5 ms | 21.5 ms | 0 | 873 MB | ok |
| 8000 | 8000 | 380 ms | 100.00% | 33.2 ms | 65.5 ms | 0 | 1550 MB | ok |
| 12000 | 11769 | 14.1 s | 2.73% | 466.6 ms | 522.0 ms | 27363 | 3466 MB | **colapso** |

**Heartbeat de 1 s:**

| Clientes | Conectados | Tempo p/ conectar | Perguntas entregues | p50 | p99 | Failovers falsos | Heap (broker + clientes) | Situação |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 8000 | 8000 | 275 ms | 100.00% | 28.9 ms | 54.5 ms | 0 | 1326 MB | ok |
| 12000 | 12000 | 568 ms | 100.00% | 53.9 ms | 101.4 ms | 0 | 1986 MB | ok |
| 16000 | 16000 | 2.8 s | 100.00% | 112.3 ms | 199.8 ms | 0 | 2651 MB | ok |
| 24000 | 16890 | 2 min 12 s | 0.00% | — | — | 62456 | 4014 MB | **colapso** |

**Leitura:**

- **Nesta máquina, o middleware aguenta 8.000 clientes simultâneos sem
  degradar com o heartbeat padrão, e 16.000 com heartbeat de 1 s.** Todos
  com 100% de entrega.
- **A degradação não é gradual, é um colapso.** Quando o broker começa a
  perder heartbeats (fila do socket UDP cheia), clientes acham que ele caiu
  e fazem failover. Sem reserva configurada, cada failover é uma nova rodada
  de Connect + Subscribe no mesmo broker, o que aumenta ainda mais a carga
  e derruba os demais: 27 mil failovers falsos em 12 mil clientes.
- **O heartbeat é o que mais pesa na escala:** com 250 ms, 12.000 clientes
  geram 48.000 pacotes/s só de heartbeat (mais as respostas). Aumentar o
  intervalo para 1 s dobrou o limite.
- Perto do limite também pesam o fan-out (cada pergunta vira uma entrega por
  inscrito, com uma goroutine de saída por cliente no broker) e o fato de
  broker e clientes dividirem a máquina: em 24.000 clientes o processo
  passava de 4 GB de heap e ~65 mil goroutines.

## Conclusões para o Cenário C

| Requisito | Resultado |
|---|---|
| Latência extremamente baixa | p99 das perguntas de 13 ms (1000 jogadores) a 25 ms (3000), de ponta a ponta |
| Tráfego intenso de alguns não prejudica os demais | 20.000 reações/s de spam não aumentaram a latência nem causaram perda de perguntas ou respostas |
| Rajadas de respostas | ~10.000 respostas/s absorvidas sem perda |
| A transmissão não pode cair | queda do broker vira uma interrupção de ~1,4 s (ou ~0,75 s com heartbeat rápido), com failover transparente de 100% dos clientes |
| Milhares de participantes | 8.000 clientes simultâneos sem degradar (16.000 com heartbeat de 1 s), numa máquina só |

Melhorias sugeridas a partir destes resultados estão em
[`melhorias.md`](melhorias.md) (seção "Benchmarks").
