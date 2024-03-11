package main

import (
	"errors"
	"io"
	"sync"
)

var ErrNoSuchKey = errors.New("no such key")

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	Run()
	io.Closer
}

type Store interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Delete(key string) error
}

type InMemoryStore struct {
	store map[string]string
	sync.RWMutex
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		store: make(map[string]string),
	}
}

func (s *InMemoryStore) Put(key string, value string) error {
	s.Lock()
	s.store[key] = value
	s.Unlock()
	return nil
}

func (s *InMemoryStore) Get(key string) (string, error) {
	s.RLock()
	value, ok := s.store[key]
	s.RUnlock()
	if !ok {
		return "", ErrNoSuchKey
	}
	return value, nil
}

func (s *InMemoryStore) Delete(key string) error {
	s.Lock()
	delete(s.store, key)
	s.Unlock()
	return nil
}
