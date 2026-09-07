package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func (s *Store) StartExpirationLoop(wg *sync.WaitGroup, ctx context.Context) {
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				removalCount := s.removeExpired()
				if removalCount > 0 {
					fmt.Printf("Removed %d keys\n", removalCount)
				}
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}
