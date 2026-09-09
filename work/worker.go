package work

import (
	"context"
	"fmt"
	"mini-redis/store"
	"sync"
)

func Worker(ctx context.Context, id int, wg *sync.WaitGroup, s *store.Store, jobs <-chan Job) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Worker %d stopping\n", id)
			return

		case job, ok := <-jobs:
			if !ok {
				fmt.Printf("Worker %d stopping\n", id)
				return
			}

			fmt.Printf(
				"Worker %d processing Job %d: %s %s %s\n",
				id,
				job.ID,
				job.Command,
				job.Key,
				job.Value,
			)

			result := Result{
				ID: job.ID,
			}

			switch job.Command {
			case "GET":
				value, err := s.Get(job.Key)
				if err != nil {
					result.Err = err
				} else {
					result.Value = value
				}

			case "SET":
				if job.ExpiresAt.IsZero() {
					s.Set(job.Key, job.Value)
				} else {
					s.SetWithExpiry(job.Key, job.Value, job.ExpiresAt)
				}

			case "DEL":
				err := s.Del(job.Key)
				if err != nil {
					result.Err = err
				}

			case "EXISTS":
				result.Value = s.Exists(job.Key)
			}

			select {
			case job.ResultCh <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}
