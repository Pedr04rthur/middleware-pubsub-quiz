package router

import (
	"errors"
	"sync"
)

type Topic struct {
	Name        string
	Subscribers map[string]bool
}

type Router struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

func New() *Router {
	return &Router{topics: make(map[string]*Topic)}
}

func (r *Router) CreateTopic(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, existe := r.topics[name]; existe {
		return errors.New("topico ja existe")
	}
	r.topics[name] = &Topic{Name: name, Subscribers: make(map[string]bool)}
	return nil
}

func (r *Router) RemoveTopic(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, existe := r.topics[name]; !existe {
		return errors.New("topico nao encontrado")
	}
	delete(r.topics, name)
	return nil
}

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

func (r *Router) Subscribe(topicName string, clientAddr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	topic, existe := r.topics[topicName]
	if !existe {
		return errors.New("topico nao existe")
	}
	topic.Subscribers[clientAddr] = true
	return nil
}

func (r *Router) Unsubscribe(topicName string, clientAddr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	topic, existe := r.topics[topicName]
	if !existe {
		return errors.New("topico nao existe")
	}
	delete(topic.Subscribers, clientAddr)
	return nil
}

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

func (r *Router) TopicExists(topicName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, existe := r.topics[topicName]
	return existe
}

func (r *Router) ListTopics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nomes := make([]string, 0, len(r.topics))
	for nome := range r.topics {
		nomes = append(nomes, nome)
	}
	return nomes
}

func (r *Router) RemoveClient(clientAddr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	removidos := 0
	for _, topic := range r.topics {
		if _, inscrito := topic.Subscribers[clientAddr]; inscrito {
			delete(topic.Subscribers, clientAddr)
			removidos++
		}
	}
	return removidos
}
