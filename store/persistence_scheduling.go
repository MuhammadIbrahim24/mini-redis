package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func (s *Store) StartPersistenceLoop(wg *sync.WaitGroup, ctx context.Context) {
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				{
					err := s.WriteToStorage()
					if err != nil {
						fmt.Println("Error occured while saving data to storage. ", err)
					} else {
						fmt.Println("Snapshot successfully stored")
					}
				}
			case <-ctx.Done():
				{
					ticker.Stop()
					return
				}
			}
		}
	}()

}
