package store

import (
	"errors"
	"maps"
	"reflect"
	"testing"
	"time"
)

var (
	ErrStorageNotAvailable = errors.New("storage not available")
)

type mockStorage struct {
	data         map[string]Entity
	loadErr      error
	saveErr      error
	storageEmpty bool
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		data: make(map[string]Entity),
	}
}

func (ms *mockStorage) Save(data map[string]Entity) error {
	if ms.saveErr != nil {
		return ms.saveErr
	}
	ms.data = maps.Clone(data)
	return nil
}

func (ms *mockStorage) Load() (map[string]Entity, error) {
	if ms.loadErr != nil {
		return nil, ms.loadErr
	}
	if ms.storageEmpty {
		return nil, nil
	}
	return ms.data, nil
}

func TestWriteToStorage(t *testing.T) {
	ms := newMockStorage()
	testStore, err := NewStore(ms)
	if err != nil {
		t.Fatalf("Store not created. %v", err)
	}
	testStore.Set("name", "Ibrahim")
	testStore.SetWithExpiry("age", 30, time.Now().Add(-1*time.Second))
	err = testStore.WriteToStorage()
	if err != nil {
		t.Fatalf("Error while writing to storage. %v", err)
	}

	entity, ok := ms.data["name"]
	if !ok {
		t.Fatal("name was not passed to Save")
	}
	if entity.Value != "Ibrahim" {
		t.Errorf("expected Ibrahim, got %v", entity.Value)
	}

	if _, ok := ms.data["age"]; ok {
		t.Error("expired age was passed to Save")
	}

}

func TestReadFromStorage(t *testing.T) {
	ms := newMockStorage()
	ms.data["name"] = Entity{
		Value:     "Ibrahim",
		ExpiresAt: time.Now().Add(time.Hour),
	}
	ms.data["age"] = Entity{
		Value:     30,
		ExpiresAt: time.Now(),
	}

	store, err := NewStore(ms)
	if err != nil {
		t.Fatalf("Store not created: %v", err)
	}

	value, err := store.Get("name")
	if err != nil {
		t.Error("key <name> does not exist")
	}
	if value != "Ibrahim" {
		t.Error("key <name> has invalid value")
	}

	value, err = store.Get("age")
	if value != nil {
		t.Error("expired key <age> returned")
	}
}

func TestWriteToStorageError(t *testing.T) {
	expectedError := ErrStorageNotAvailable

	ms := newMockStorage()
	ms.saveErr = expectedError
	testStore, err := NewStore(ms)
	if err != nil {
		t.Fatalf("Store not created. %v", err)
	}
	testStore.Set("name", "Ibrahim")
	testStore.SetWithExpiry("age", 30, time.Now().Add(-1*time.Second))
	err = testStore.WriteToStorage()
	if !errors.Is(err, expectedError) {
		t.Fatalf("expected %v, got %v", expectedError, err)
	}
}

func TestReadFromStorageError(t *testing.T) {
	expectedError := ErrStorageNotAvailable

	ms := newMockStorage()
	ms.loadErr = expectedError

	_, err := NewStore(ms)
	if !errors.Is(err, expectedError) {
		t.Fatalf("expected %v, got %v", expectedError, err)
	}
}

func TestNewStoreWithEmptyStorage(t *testing.T) {

	ms := newMockStorage()
	ms.storageEmpty = true

	_, err := NewStore(ms)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestGet(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*Store)
		key     string
		want    any
		wantErr error
	}{
		{
			name: "without expiry",
			key:  "name",
			setup: func(store *Store) {
				store.Set("name", "Ibrahim")
			},
			want:    "Ibrahim",
			wantErr: nil,
		},
		{
			name:    "missing key",
			key:     "name1",
			want:    nil,
			wantErr: ErrKeyNotFound,
		},
		{
			name: "with future expiry",
			key:  "name",
			setup: func(store *Store) {
				store.SetWithExpiry("name", "Ibrahim", time.Now().Add(time.Hour))
			},
			want:    "Ibrahim",
			wantErr: nil,
		},
		{
			name: "expired key",
			key:  "age",
			setup: func(store *Store) {
				store.SetWithExpiry("age", 30, time.Now().Add(-time.Second))
			},
			want:    nil,
			wantErr: ErrKeyNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newMockStorage()
			store, err := NewStore(s)
			if err != nil {
				t.Fatalf("store not created. %v", err)
			}
			if tt.setup != nil {
				tt.setup(store)
			}

			value, err := store.Get(tt.key)

			if !reflect.DeepEqual(value, tt.want) || err != tt.wantErr {
				t.Errorf("expected %v, %v; got %v, %v", tt.want, tt.wantErr, value, err)
			}
		})
	}

	//ToD: add test cases for other store methods
}
