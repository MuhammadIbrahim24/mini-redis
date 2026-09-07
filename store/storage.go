package store

type Storage interface {
	Load() (map[string]Entity, error)
	Save(map[string]Entity) error
}
