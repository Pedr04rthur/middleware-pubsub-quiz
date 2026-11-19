// Comando bench executa os cenários de validação do middleware (TASK-SC05) e
// gera as tabelas em Markdown e os dados brutos em JSON:
//
//	go run ./cmd/bench                          # os três cenários
//	go run ./cmd/bench -cenarios failover       # só um
//	go run ./cmd/bench -rapido                  # versão reduzida (~1 min)
//
// Os brokers sobem dentro do próprio processo (ver internal/bench).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Pedr04rthur/middleware-pubsub-quiz/internal/bench"
)

type resultados struct {
	Ambiente bench.Ambiente             `json:"ambiente"`
	Ruido    []bench.LinhaRuido         `json:"ruido,omitempty"`
	Failover []*bench.ResultadoFailover `json:"failover,omitempty"`
	Conexoes []bench.DegrauConexoes     `json:"conexoes,omitempty"`
}

func main() {
	cenarios := flag.String("cenarios", "ruido,failover,conexoes", "cenarios a executar, separados por virgula")
	saida := flag.String("saida", "resultados", "pasta onde salvar resultados.json e tabelas.md")
	rapido := flag.Bool("rapido", false, "versao reduzida dos cenarios, para conferir que tudo roda")
	heartbeat := flag.Duration("heartbeat", 0, "intervalo de heartbeat no cenario de conexoes (0 = padrao da SDK, 250ms)")
	degraus := flag.String("degraus", "", "degraus do cenario de conexoes, separados por virgula (ex.: 8000,16000)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cfgRuido, cfgFailover, cfgConexoes := bench.DefaultRuido(), bench.DefaultFailover(), bench.DefaultConexoes()
	if *rapido {
		cfgRuido.Jogadores, cfgRuido.Repeticoes, cfgRuido.PerguntasPorFase = []int{300}, 1, 2
		cfgRuido.IntervaloPergunta = time.Second
		for i := range cfgFailover {
			cfgFailover[i].Jogadores, cfgFailover[i].Depois = 200, 3*time.Second
		}
		cfgConexoes.Degraus, cfgConexoes.Perguntas = []int{500, 1000}, 2
	}

	cfgConexoes.Heartbeat = *heartbeat
	if *degraus != "" {
		cfgConexoes.Degraus = nil
		for _, d := range strings.Split(*degraus, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(d))
			if err != nil {
				log.Fatalf("degrau invalido: %q", d)
			}
			cfgConexoes.Degraus = append(cfgConexoes.Degraus, n)
		}
	}

	res := resultados{Ambiente: bench.LerAmbiente()}
	var md strings.Builder
	fmt.Fprintf(&md, "Ambiente: %s/%s, %d CPUs, %s, %s\n\n", res.Ambiente.SO, res.Ambiente.Arquitetura,
		res.Ambiente.CPUs, res.Ambiente.Go, res.Ambiente.Data)

	inicio := time.Now()
	for _, c := range strings.Split(*cenarios, ",") {
		switch strings.TrimSpace(c) {
		case "ruido":
			linhas, err := bench.Ruido(ctx, cfgRuido, os.Stdout)
			if err != nil {
				log.Fatal("ruido: ", err)
			}
			res.Ruido = linhas
			md.WriteString("## Latencia das perguntas com e sem ruido\n\n" + bench.TabelaRuido(linhas) + "\n")
		case "failover":
			for _, cfg := range cfgFailover {
				r, err := bench.Failover(ctx, cfg, os.Stdout)
				if err != nil {
					log.Fatal("failover: ", err)
				}
				res.Failover = append(res.Failover, r)
			}
			md.WriteString("## Failover\n\n" + bench.TabelaFailover(res.Failover) + "\n")
		case "conexoes":
			ds, err := bench.Conexoes(ctx, cfgConexoes, os.Stdout)
			if err != nil {
				log.Fatal("conexoes: ", err)
			}
			res.Conexoes = ds
			hb := "250ms (padrao da SDK)"
			if *heartbeat > 0 {
				hb = heartbeat.String()
			}
			md.WriteString("## Conexoes simultaneas (heartbeat " + hb + ")\n\n" + bench.TabelaConexoes(ds) + "\n")
		default:
			log.Fatalf("cenario desconhecido: %q", c)
		}
	}

	fmt.Println()
	fmt.Print(md.String())
	fmt.Printf("Tempo total: %v\n", time.Since(inicio).Round(time.Second))

	if err := os.MkdirAll(*saida, 0o755); err != nil {
		log.Fatal(err)
	}
	dados, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(filepath.Join(*saida, "resultados.json"), dados, 0o644); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(*saida, "tabelas.md"), []byte(md.String()), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Resultados salvos em", *saida)
}
