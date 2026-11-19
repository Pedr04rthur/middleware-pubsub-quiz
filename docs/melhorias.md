# Melhorias e mudanças para discutir com o grupo

Pontos encontrados durante o desenvolvimento do Módulo 3 (Suetone). Nenhum
deles bloqueia o que já funciona; são sugestões para o grupo decidir.

## Protocolo (`pkg/protocol`)

- **Id de requisição no cabeçalho.** Ack/Nack só trazem o tópico, então o
  cliente associa a resposta à operação pelo tópico e faz uma operação de
  controle por vez. Isso já limitou o broker: os descartes de resposta fora da
  janela e de fila cheia ficaram silenciosos para não confundir o cliente. Um
  campo `uint32` de id, ecoado pelo broker, resolveria a ambiguidade e
  permitiria avisar o publicador desses descartes. *(TASK-SC01)*
- **Códigos de erro em vez de texto.** O cliente e o quiz dependem das frases
  exatas `"topico sem inscritos"` e `"topico nao existe"`. Se alguém mudar a
  mensagem no router/broker, eles quebram sem aviso. Um byte de código no
  payload do Nack seria mais robusto. *(TASK-SC01/SC02/SC03)*
- **Exportar o tamanho do cabeçalho** (`1 + 2 + 4`) e o tamanho máximo do
  pacote (4096, o buffer de leitura do broker) como constantes do pacote
  `protocol`, para o cliente e o quiz não repetirem esses números. *(TASK-SC01)*
- `protocol.go` não está formatado com `gofmt`.

## Broker (Módulos 1 e 2)

- **Sessão no Connect.** `CmdConnect` só responde Ack; a fila de saída só é
  criada no primeiro Subscribe. O PDF (seção 3.6) diz que o Connect cria a
  sessão, o Token Bucket e a fila.
- **Expirar clientes sem heartbeat.** O cliente envia heartbeat
  (`CmdHeartbeat`), mas o broker não remove sessões de quem parou de enviar.
  Clientes que fizeram failover ou caíram ficam nas tabelas do router e do
  outbox. Falta também chamar `ratelimit.Unregister` no Disconnect.
  *(relacionado à TASK-SC02)*
- **Ack para toda publicação.** Na janela de respostas isso dobra o tráfego do
  broker (uma resposta por resposta de jogador). Como o cliente não espera o
  Ack de publicação, o broker poderia enviar só o Nack de descarte.
- **Rate limit nas operações de controle.** Só o heartbeat é isento. Um jogador
  barulhento pode ter Subscribe/CreateTopic descartados (o cliente reenvia,
  mas atrasa). Avaliar isentar também os comandos de controle.
- **Políticas por tópico no CreateTopic.** A janela de respostas do broker é
  fixa em 20 s (o máximo), qualquer que seja o prazo da pergunta; o quiz
  aplica o próprio prazo do lado do apresentador. O PDF (seção 3.6) prevê que
  o CreateTopic defina as políticas do tópico (prioridade, TTL), o que
  permitiria ao apresentador mandar o prazo real. *(TASK-SC03)*

## Múltiplas instâncias (TASK-SC02)

- **Instâncias divididas.** Se a primária voltar depois de uma queda, os
  clientes que migraram ficam na reserva, mas clientes novos conectam na
  primária e os dois grupos não se veem. Opções: os clientes nunca voltarem à
  primária (exigir que ela fique fora), uma ordem de preferência combinada, ou
  coordenação entre brokers.
- **Janela de perda na queda.** Tudo o que é publicado entre a queda e a
  detecção (até `HeartbeatTimeout`, padrão 1 s) se perde. Dá para reduzir o
  prazo, ao custo de mais heartbeats e mais risco de failover falso.
- O apresentador e os jogadores recriam o tópico na reserva; se o tópico tiver
  políticas (quando o CreateTopic aceitar), quem recriar primeiro define.

## Aplicação do quiz (TASK-SC03)

- **Placar limitado ao top 10.** O placar inteiro não cabe em um datagrama de
  4096 bytes com muitos jogadores. Quem fica fora do top 10 sabe se acertou,
  mas não vê os próprios pontos. Opções: um tópico privado por jogador
  (`quiz/{sala}/jogador/{nome}`) ou dividir o placar em vários pacotes.
- **Identidade pelo nome.** O jogador é identificado pelo nome que ele mesmo
  escolhe: dois jogadores com o mesmo nome somam pontos, e qualquer um pode
  responder em nome de outro. Para uma demo basta; uma melhoria seria o
  apresentador associar o nome ao endereço do primeiro "entrada" (o broker
  teria que repassar o remetente) ou um token por jogador.
- **Apresentador é ponto único de falha.** O failover protege contra a queda
  do broker, mas se o processo do apresentador cair a partida para.
- **Frontend web.** A aplicação é CLI; o PDF cita um frontend web "se houver
  tempo". A lógica está em `internal/quiz` justamente para ser reaproveitada
  por um servidor web.

## Testes de carga (TASK-SC04)

- **Dois geradores de carga.** `cmd/estresse` (Pedro Lucas) mede uma partida
  com jogadores iguais; `cmd/carga` (Suetone) compara as fases sem e com
  clientes barulhentos e abre milhares de conexões em paralelo. Vale decidir
  se unificamos os dois antes do relatório final.
- **Tudo numa máquina só.** Gerador e broker na mesma máquina disputam CPU
  (o gerador chega a ~140% de CPU com 3200 clientes). Para os números finais,
  rodar o gerador em outra máquina (ou em várias).
- **Heartbeat pesa na escala.** Com 3000 clientes e heartbeat de 250 ms são
  12.000 pacotes/s só de heartbeat. A flag `-heartbeat 1s` reduz isso; avaliar
  um padrão maior na SDK.

## Benchmarks (TASK-SC05, ver `docs/resultados.md`)

- **Tempestade de reconexões no limite de conexões.** Com 12.000 clientes
  (heartbeat de 250 ms) o broker começa a perder heartbeats, os clientes
  fazem failover sem o broker ter caído e cada failover gera mais Connect +
  Subscribe, derrubando os demais (27 mil failovers falsos). Na SDK
  (Módulo 3): esperar um tempo aleatório (jitter) e crescente (backoff)
  antes de reconectar, e não refazer o registro quando o "novo" broker é o
  mesmo e ele voltou a responder.
- **Heartbeat padrão agressivo para a escala.** 250 ms / 1 s dá failover em
  ~1,4 s, mas custa 4 pacotes/s por cliente; com 1 s o limite de conexões
  dobrou (8.000 → 16.000). Decidir o padrão do projeto, ou deixar o broker
  sugerir o intervalo no Ack do Connect conforme o número de clientes.
- **Socket do broker.** Uma única goroutine lê o socket UDP com o buffer
  padrão do SO (~768 KB no macOS). Aumentar com `conn.SetReadBuffer` e/ou
  ter várias goroutines de leitura deve adiar a perda de pacotes no pico.
- **Apresentador limitado como jogador.** O rate limit de 10 msg/s vale
  também para o apresentador; um apresentador publicando perguntas e placar
  mais rápido que isso é cortado (aconteceu no primeiro rascunho do
  benchmark de failover). Avaliar um limite maior para quem cria a sala.
- **Ordem das fases.** No benchmark de ruído a fase com barulho sempre roda
  depois da linha de base (já "aquecida"). Alternar a ordem entre as
  repetições daria uma comparação mais justa.
- **Três ferramentas de carga.** `cmd/estresse`, `cmd/carga` e `cmd/bench`
  se sobrepõem em parte; vale consolidar antes da entrega.

## Resolvido

- **Barulhentos derrubavam respostas dos jogadores normais** (encontrado pelo
  gerador de carga, TASK-SC04). Com 3000 jogadores e 200 barulhentos a 100
  reações/s, 19% das respostas eram cortadas pelo rate limit: a redução global
  por carga encolhia o balde de todos para 1 ficha, e uma reação gastava a
  ficha da resposta. Correção no `internal/ratelimit` (Módulo 2): cada cliente
  tem um balde para o jogo e outro para reações, e a redução global só vale
  para as reações ("preservando os tópicos de prioridade", PDF 3.5). A
  penalidade e o bloqueio de quem abusa continuam valendo nos dois. Depois da
  correção, 100% das respostas chegam no mesmo cenário.

- Escalonador ligado ao broker, agregação de reações, TTL das respostas,
  log por mensagem (flag `-silencioso`) e `Close()` parando rate limit,
  escalonador e agregador (Pedro Lucas, PRs 8 a 11). As goroutines do outbox
  ainda continuam depois do `Close()`.
