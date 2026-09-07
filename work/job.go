package work

import "time"

type Job struct {
	ID        int
	Command   string
	Key       string
	Value     string
	ExpiresAt time.Time
}
