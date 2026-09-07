package persistence

import (
	"encoding/json"
	"mini-redis/store"
	"os"
)

const FILENAME = "cached_data.json"

type FileStorage struct {
	filename string
}

func NewFileStorage(filename string) *FileStorage {
	return &FileStorage{
		filename,
	}
}

func (s *FileStorage) Save(data map[string]store.Entity) error {
	bytesArray, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return os.WriteFile(s.filename, bytesArray, 0644)
}

func (s *FileStorage) Load() (map[string]store.Entity, error) {
	bytesArray, err := os.ReadFile(s.filename)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data := make(map[string]store.Entity)
	err = json.Unmarshal(bytesArray, &data)
	return data, err
}
