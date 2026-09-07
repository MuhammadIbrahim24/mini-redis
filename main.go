package main

import (
	"bufio"
	"context"
	"fmt"
	"mini-redis/command"
	"mini-redis/persistence"
	"mini-redis/store"
	"mini-redis/work"
	"os"
	"strings"
	"sync"
)

var cfg = work.Config{
	WorkerCount:      3,
	JobBufferSize:    10,
	ResultBufferSize: 10,
}

func interactiveCli() {
	storage := persistence.NewFileStorage(persistence.FILENAME)
	s, err := store.NewStore(storage)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//Spin up worker
	pool, err := work.NewPool(cfg, s)

	if err != nil {
		fmt.Printf("Error occured while creating worker pool: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var asyncWg sync.WaitGroup

	pool.Start(ctx)

	results := pool.Results()

	//consumer go routine
	asyncWg.Add(1)
	go func() {
		defer asyncWg.Done()
		for result := range results {
			if result.Err != nil {
				value := result.Err.Error()
				fmt.Printf("%d. FAILED  %v\n", result.ID, value)

			} else {
				value := result.Value
				fmt.Printf("%d. SUCCESS %v\n", result.ID, value)
			}
		}
	}()

	//start expiration job
	asyncWg.Add(2)
	s.StartExpirationLoop(&asyncWg, ctx)
	s.StartPersistenceLoop(&asyncWg, ctx)

	count := 0

	//Interactive CLI
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Ready to go")
	for scanner.Scan() {
		input := scanner.Text()
		if strings.TrimSpace(input) == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(input), "EXIT") {
			break
		}

		job, err := command.Parse(input)
		if err != nil {
			fmt.Printf("Error occured while parsing the command: %v", err)
			continue
		}

		count++
		job.ID = count

		if job.Command == "COUNT" || job.Command == "KEYS" {
			switch job.Command {
			case "COUNT":
				fmt.Printf("%d. SUCCESS %v\n", job.ID, s.Count())
			case "KEYS":
				fmt.Printf("%d. SUCCESS %v\n", job.ID, s.Keys())
			}
		} else {
			pool.Publish(job)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}

	shutdown(pool, &asyncWg, cancel, s)
}

func shutdown(pool *work.Pool, asyncWg *sync.WaitGroup, cancel context.CancelFunc, s *store.Store) {
	pool.Stop()
	cancel()
	//wait for consumer routine to be terminated
	asyncWg.Wait()

	fmt.Println("Saving data to storage...")
	err := s.WriteToStorage()
	if err != nil {
		fmt.Println("Error occured while saving data to storage. ", err)
	}
}

func main() {
	interactiveCli()
}
