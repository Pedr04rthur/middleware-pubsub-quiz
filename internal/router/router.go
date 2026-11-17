package router

import (
	"errors"
	"sync"
)

// Topic representa um tópico com seus inscritos.
type Topic struct {
	Name     string
	Subscribers map[string]bool // IP:Porta -> true (usamos map para evitar duplicatas)
}

// Router é a tabela de roteamento em memória.
type Router struct {
	mu     sync.RWMutex // Protege o acesso concorrente
	topics map[string]*Topic
}

// New cria um novo roteador vazio.
func New() *Router {
	return &Router{
		topics: make(map[string]*Topic),
	}
}

// CreateTopic cria um tópico se ele não existir.
func (r *Router) CreateTopic(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, existe := r.topics[name]; existe {
		return errors.New("tópico já existe")
	}

	r.topics[name] = &Topic{
		Name:        name,
		Subscribers: make(map[string]bool),
	}
	return nil
}

// RemoveTopic apaga um tópico e todos os seus inscritos.
func (r *Router) RemoveTopic(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, existe := r.topics[name]; !existe {
		return errors.New("tópico não encontrado")
	}

	delete(r.topics, name)
	return nil
}

// RemoveTopicPrefix remove todos os tópicos que começam com um prefixo.
// Útil para encerrar uma sala inteira: RemoveTopicPrefix("quiz/sala1/")
func (r *Router) RemoveTopicPrefix(prefix string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	removidos := 0
	for nome := range r.topics {
		if len(nome) >= len(prefix) && nome[:len(prefix)] == prefix {
			delete(r.topics, nome)
			removidos++
		}
	}
	return removidos
}

// Subscribe inscreve um cliente (identificado por IP:Porta) em um tópico.
func (r *Router) Subscribe(topicName string, clientAddr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	topic, existe := r.topics[topicName]
	if !existe {
		return errors.New("tópico não existe")
	}

	topic.Subscribers[clientAddr] = true
	return nil
}

// Unsubscribe remove um cliente de um tópico.
func (r *Router) Unsubscribe(topicName string, clientAddr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	topic, existe := r.topics[topicName]
	if !existe {
		return errors.New("tópico não existe")
	}

	delete(topic.Subscribers, clientAddr)
	return nil
}

// GetSubscribers retorna a lista de inscritos em um tópico.
func (r *Router) GetSubscribers(topicName string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	topic, existe := r.topics[topicName]
	if !existe {
		return nil
	}

	inscritos := make([]string, 0, len(topic.Subscribers))
	for addr := range topic.Subscribers {
		inscritos = append(inscritos, addr)
	}
	return inscritos
}

// TopicExists verifica se um tópico existe.
func (r *Router) TopicExists(topicName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, existe := r.topics[topicName]
	return existe
}

// ListTopics retorna todos os tópicos registrados (útil para debug).
func (r *Router) ListTopics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nomes := make([]string, 0, len(r.topics))
	for nome := range r.topics {
		nomes = append(nomes, nome)
	}
	return nomes
}
