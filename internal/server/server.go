package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"ghostmq/internal/queue"

	"github.com/google/uuid"
)

// Server represents the HTTP server for GhostMQ.
type Server struct {
	queueManager *queue.QueueManager
	httpServer   *http.Server
}

// NewServer creates a new Server instance.
func NewServer(queueManager *queue.QueueManager) *Server {
	return &Server{
		queueManager: queueManager,
	}
}

// Start starts the HTTP server in a new goroutine.
func (s *Server) Start(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/queues/", s.messageHandler)

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("HTTP server listening on %s", addr)
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
}

func (s *Server) messageHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "queues" {
		http.NotFound(w, r)
		return
	}
	queueName := parts[1]

	switch r.Method {
	case http.MethodPost:
		s.pushHandler(w, r, queueName)
	case http.MethodGet:
		s.popHandler(w, r, queueName)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) pushHandler(w http.ResponseWriter, r *http.Request, queueName string) {
	q := s.queueManager.GetQueue(queueName)
	if q == nil {
		http.NotFound(w, r)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var payload json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	msg := queue.Message{
		ID:        uuid.New().String(),
		Payload:   payload,
		Timestamp: time.Now(),
	}

	if err := q.Push(msg); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Message accepted")
}

func (s *Server) popHandler(w http.ResponseWriter, r *http.Request, queueName string) {
	q := s.queueManager.GetQueue(queueName)
	if q == nil {
		http.NotFound(w, r)
		return
	}

	msg, ok := q.Pop()
	if !ok {
		// This can happen if the queue is closed.
		http.Error(w, "Queue closed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		http.Error(w, "Failed to encode message", http.StatusInternalServerError)
		// We can't do much if we fail to write the error response here.
		return
	}
}
