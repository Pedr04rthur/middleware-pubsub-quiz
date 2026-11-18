package quiz

import (
	"encoding/json"
	"fmt"
	"os"
)

// PerguntasPadrao é usada quando o apresentador não passa um arquivo.
var PerguntasPadrao = []Questao{
	{Texto: "Qual a capital da Paraiba?", Opcoes: []string{"Campina Grande", "Joao Pessoa", "Patos", "Santa Rita"}, Correta: 1},
	{Texto: "Qual protocolo de transporte o nosso middleware usa?", Opcoes: []string{"TCP", "QUIC", "UDP", "SCTP"}, Correta: 2},
	{Texto: "Qual garantia de entrega o broker oferece?", Opcoes: []string{"exactly-once", "at-least-once", "at-most-once"}, Correta: 2},
	{Texto: "Em Go, o que se usa para comunicar goroutines?", Opcoes: []string{"channels", "pipes", "sockets", "signals"}, Correta: 0},
	{Texto: "Qual algoritmo limita a taxa de envio de cada cliente?", Opcoes: []string{"Round Robin", "Token Bucket", "Leaky Pipe", "Dijkstra"}, Correta: 1},
	{Texto: "O ponto mais oriental das Americas fica em qual cidade?", Opcoes: []string{"Natal", "Recife", "Joao Pessoa", "Fortaleza"}, Correta: 2},
}

// CarregarPerguntas lê um arquivo JSON com uma lista de Questao:
//
//	[{"texto": "...", "opcoes": ["a", "b"], "correta": 0}]
func CarregarPerguntas(caminho string) ([]Questao, error) {
	dados, err := os.ReadFile(caminho)
	if err != nil {
		return nil, err
	}
	var qs []Questao
	if err := json.Unmarshal(dados, &qs); err != nil {
		return nil, fmt.Errorf("%s: %w", caminho, err)
	}
	for i, q := range qs {
		if err := q.validar(); err != nil {
			return nil, fmt.Errorf("%s, pergunta %d: %w", caminho, i+1, err)
		}
	}
	return qs, nil
}
