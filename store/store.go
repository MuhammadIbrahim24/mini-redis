package store

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Entity struct {
	Value     any
	ExpiresAt time.Time
}

type Store struct {
	mu      sync.RWMutex
	data    map[string]Entity
	storage Storage
	dirty   bool
}

// NewStore creates and returns a Store with initialized internal map.
func NewStore(storage Storage) (*Store, error) {
	s := &Store{
		data:    make(map[string]Entity),
		storage: storage,
		dirty:   false,
	}
	if err := readFromStorage(s); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Set(key string, value any) {
	s.set(key, value, time.Time{})
}

func (s *Store) SetWithExpiry(key string, value any, expiresAt time.Time) {
	s.set(key, value, expiresAt)
}

func (s *Store) set(key string, value any, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Entity{
		Value:     value,
		ExpiresAt: expiresAt,
	}

	s.dirty = true
}

func (s *Store) Get(key string) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.data[key]
	if ok && !isExpired(entry) {
		return entry.Value, nil
	}
	return nil, ErrKeyNotFound
}

func (s *Store) Del(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.data[key]
	if !ok || isExpired(entry) {
		return ErrKeyNotFound
	}
	delete(s.data, key)
	s.dirty = true
	return nil
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.data[key]
	if ok && !isExpired(entry) {
		return true
	}
	return false
}

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for key := range s.data {
		if !isExpired(s.data[key]) {
			count++
		}
	}
	return count
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := []string{}

	for key := range s.data {
		if !isExpired(s.data[key]) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (s *Store) removeExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0

	for key := range s.data {
		if isExpired(s.data[key]) {
			delete(s.data, key)
			count++
		}
	}
	if count > 0 {
		s.dirty = true
	}
	return count
}

func isExpired(entity Entity) bool {
	if entity.ExpiresAt.IsZero() || entity.ExpiresAt.After(time.Now()) {
		return false
	}
	return true
}

func (s *Store) WriteToStorage() error {
	snapshot := s.createSnapshot()
	if snapshot == nil {
		return nil
	}

	if err := s.storage.Save(snapshot); err != nil {
		s.mu.Lock()
		s.dirty = true
		s.mu.Unlock()

		return err
	}
	return nil
}

func (s *Store) createSnapshot() map[string]Entity {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}

	snapshot := make(map[string]Entity)

	for key, entity := range s.data {
		if !isExpired(entity) {
			snapshot[key] = entity
		}
	}

	s.dirty = false

	return snapshot
}

func readFromStorage(s *Store) error {
	data, err := s.storage.Load()
	if err != nil {
		return err
	}
	if data != nil {
		s.data = data
	}
	return nil
}
