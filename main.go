package main

import (
	"context"
	"fmt"
	"mini-redis/cli"
	"mini-redis/persistence"
	"mini-redis/server"
	"mini-redis/store"
	"mini-redis/work"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var cfg = work.Config{
	WorkerCount:   3,
	JobBufferSize: 10,
}

func shutdown(pool *work.Pool, cancel context.CancelFunc, s *store.Store, bgWg *sync.WaitGroup, tcpWg *sync.WaitGroup) {
	pool.Stop()
	cancel()
	tcpWg.Wait()
	bgWg.Wait()

	fmt.Println("Saving data to storage...")
	err := s.WriteToStorage()
	if err != nil {
		fmt.Println("Error occured while saving data to storage. ", err)
	}
}

func main() {
	// create storage
	storage := persistence.NewFileStorage(persistence.FILENAME)

	//create store
	s, err := store.NewStore(storage)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//create pool
	pool, err := work.NewPool(cfg, s)
	if err != nil {
		fmt.Printf("Error occured while creating worker pool: %v", err)
		os.Exit(1)
	}

	//create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//wait group for background jobs
	var bgWg sync.WaitGroup

	//start pool
	pool.Start(ctx)

	//start expiration and persistence loop
	bgWg.Add(2)
	s.StartExpirationLoop(&bgWg, ctx)
	s.StartPersistenceLoop(&bgWg, ctx)

	//Wait groups for main flows
	var tcpWg sync.WaitGroup
	//Create TCP server
	srvr := server.NewServer()
	tcpWg.Add(1)
	go srvr.StartServer(pool, &tcpWg, ctx)

	//listen and act for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)

	//start interactive cli
	go cli.InteractiveCli(pool)

	<-sigChan
	signal.Stop(sigChan)
	shutdown(pool, cancel, s, &bgWg, &tcpWg)
}
