package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"mini-redis/command"
	"mini-redis/work"
	"net"
	"strings"
	"sync"
	"time"
)

type ServerState int

const (
	StateNew ServerState = iota
	StateRunning
	StateStopped
)

type Server struct {
	connList []net.Conn
	connWg   sync.WaitGroup
	mu       sync.RWMutex
	state    ServerState
}

func NewServer() *Server {
	return &Server{
		connList: []net.Conn{},
		state:    StateNew,
	}
}

func (srv *Server) StartServer(pool *work.Pool, wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	srv.mu.RLock()
	if srv.state == StateRunning {
		srv.mu.RUnlock()
		fmt.Println("TCP server already running")
		return
	}
	srv.mu.RUnlock()

	// 1. Create a TCP listener on port 8080
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Failed to bind to port 8080: %v", err)
		return
	}

	// 2. Ensure the listener closes when the application exits
	defer listener.Close()
	fmt.Println("TCP Server is listening on port 8080...")

	srv.mu.Lock()
	srv.state = StateRunning
	srv.mu.Unlock()

	go func() {
		<-ctx.Done()
		listener.Close()
		srv.shutdown()
	}()

	// 3. Infinite loop to continuously accept incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Failed to accept connection: %v", err)
			continue // Continue listening for other connections
		}

		// 4. Handle each connection concurrently using a goroutine
		srv.mu.Lock()
		if srv.state != StateRunning {
			srv.mu.Unlock()
			conn.Close()
			break
		}
		srv.connList = append(srv.connList, conn)
		srv.mu.Unlock()
		srv.connWg.Add(1)
		go srv.handleConnection(pool, conn)
	}
	srv.connWg.Wait()
	fmt.Println("TCP server stopped")
}

// handleConnection manages data transmission for an individual client
func (srv *Server) handleConnection(pool *work.Pool, conn net.Conn) {
	// Ensure the connection is cleaned up when done
	defer conn.Close()
	defer srv.connWg.Done()
	fmt.Printf("Client connected from: %s\n", conn.RemoteAddr().String())

	// Read incoming data from the client
	reader := bufio.NewReader(conn)
	count := 0

	for {
		// Read data up until a newline character
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Client disconnected or error: %v", err)

			srv.removeConnection(conn)
			return
		}

		fmt.Printf("Received: %s", message)
		if strings.TrimSpace(message) == "" {
			continue
		}

		job, err := command.Parse(message)
		if err != nil {
			fmt.Printf("Error occured while parsing the command: %v", err)
			continue
		}

		count++
		job.ID = count
		job.ResultCh = make(chan work.Result, 1)

		err = pool.Publish(job)
		if err != nil {
			fmt.Printf("Publishing job failed with error: %v", err)
			continue
		}

		select {
		case result := <-job.ResultCh:
			response := ""

			if result.Err != nil {
				value := result.Err.Error()
				response = fmt.Sprintf("%d. FAILED  %v\n", result.ID, value)

			} else {
				value := result.Value
				response = fmt.Sprintf("%d. SUCCESS %v\n", result.ID, value)
			}

			// send response to the client
			_, err = conn.Write([]byte(response))
			if err != nil {
				log.Printf("Failed to write to client: %v", err)
				srv.removeConnection(conn)
				return
			}
		case <-time.After(5 * time.Second):
			fmt.Printf("job timed out, closing out the connection: %s", conn.RemoteAddr().String())
			conn.Close()
			srv.removeConnection(conn)
			return
		}

	}
}

func (srv *Server) removeConnection(conn net.Conn) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	for index, activeConn := range srv.connList {
		if activeConn == conn {
			srv.connList = append(
				srv.connList[:index],
				srv.connList[index+1:]...,
			)
			return
		}
	}
}

func (srv *Server) shutdown() {
	srv.mu.Lock()
	if srv.state == StateStopped {
		srv.mu.Unlock()
		return
	}
	srv.state = StateStopped
	conns := append([]net.Conn(nil), srv.connList...)
	srv.connList = nil
	srv.mu.Unlock()

	for _, conn := range conns {
		err := conn.Close()
		if err != nil {
			fmt.Printf("Error while closing TCP connection: %v", err)
		}
	}
}
