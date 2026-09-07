package work

import (
	"context"
	"errors"
	"hash/fnv"
	"mini-redis/store"
	"sync"
)

type PoolState int

const (
	StateNew PoolState = iota
	StateRunning
	StateStopped
)

var (
	ErrInvalidWorkerCount = errors.New("invalid worker count")
	ErrInvalidBufferSize  = errors.New("invalid buffer size")
	ErrPoolInvalidState   = errors.New("pool invalid state")
	ErrPoolNotRunning     = errors.New("pool not running")
)

type Pool struct {
	mu            sync.Mutex
	state         PoolState
	jobs          []chan Job
	results       chan Result
	workerCount   int
	wg            sync.WaitGroup
	store         *store.Store
	jobBufferSize int
	publishers    sync.WaitGroup
}

func NewPool(cfg Config, s *store.Store) (*Pool, error) {
	if cfg.WorkerCount < 1 {
		return nil, ErrInvalidWorkerCount
	}
	if cfg.JobBufferSize < 0 || cfg.ResultBufferSize < 0 {
		return nil, ErrInvalidBufferSize
	}
	return &Pool{
		jobs:          make([]chan Job, cfg.WorkerCount),
		results:       make(chan Result, cfg.ResultBufferSize),
		workerCount:   cfg.WorkerCount,
		jobBufferSize: cfg.JobBufferSize,
		store:         s,
		state:         StateNew,
	}, nil
}

func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != StateNew {
		return ErrPoolInvalidState
	}
	p.state = StateRunning

	for i := range p.workerCount {
		p.jobs[i] = make(chan Job, p.jobBufferSize)
		p.wg.Add(1)
		go Worker(ctx, i, &p.wg, p.store, p.jobs[i], p.results)
	}
	return nil
}

func (p *Pool) Results() <-chan Result {
	return p.results
}

func (p *Pool) Publish(j Job) error {
	p.mu.Lock()
	if p.state != StateRunning {
		p.mu.Unlock()
		return ErrPoolNotRunning
	}
	p.publishers.Add(1)
	p.mu.Unlock()

	defer p.publishers.Done()
	workerID := int(hashKey(j.Key)) % p.workerCount
	p.jobs[workerID] <- j
	return nil
}

func (p *Pool) Stop() error {
	p.mu.Lock()
	if p.state != StateRunning {
		p.mu.Unlock()
		return ErrPoolNotRunning
	}
	p.state = StateStopped
	p.mu.Unlock()

	p.publishers.Wait()

	for _, ch := range p.jobs {
		close(ch)
	}
	//wait for workers to be terminated
	p.wg.Wait()
	//close results channel
	close(p.results)
	return nil
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}
