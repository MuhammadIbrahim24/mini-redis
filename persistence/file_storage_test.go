package persistence

import (
	"mini-redis/store"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFileStorageSaveLoad(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "data.json")

	fs := NewFileStorage(filename)

	data := map[string]store.Entity{
		"name": {
			Value: "Ibrahim Khan",
		},
	}

	err := fs.Save(data)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := fs.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if !reflect.DeepEqual(loaded, data) {
		t.Fatalf("Expected %v, Received %v", data, loaded)
	}
}

func TestFileStorageMissingFile(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "does-no-exist.json")

	fs := NewFileStorage(filename)

	data, err := fs.Load()

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if data != nil {
		t.Fatalf("expected nil data, got %v", data)
	}
}

func TestFileStorageMalformedJSON(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "malformed.json")

	err := os.WriteFile(filename, []byte(`{"name": "Ibrahim"`), 0644)
	if err != nil {
		t.Fatalf("failed to create malformed JSON file: %v", err)
	}

	fs := NewFileStorage(filename)

	_, err = fs.Load()

	if err == nil {
		t.Fatalf("Expected json parsing error, got nil")
	}

}
