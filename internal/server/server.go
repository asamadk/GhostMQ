package server

import (
	"context"
	"encoding/json"
	"errors"
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
	mux.HandleFunc("/queues", s.queuesHandler)
	mux.HandleFunc("/queues/", s.queueHandler)

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

func (s *Server) queuesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listQueues(w, r)
	case http.MethodPost:
		s.createQueue(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) queueHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "queues" {
		http.NotFound(w, r)
		return
	}

	queueName := parts[1]

	if len(parts) == 2 {
		switch r.Method {
		case http.MethodPost:
			s.pushHandler(w, r, queueName)
		case http.MethodGet:
			s.popHandler(w, r, queueName)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if len(parts) == 3 && parts[2] == "ack" && r.Method == http.MethodPost {
		s.ackHandler(w, r, queueName)
		return
	}

	http.NotFound(w, r)
}

type createQueueRequest struct {
	Name                     string `json:"name"`
	MaxSize                  int    `json:"maxSize"`
	BackpressureMode         string `json:"backpressureMode"`
	VisibilityTimeoutSeconds int    `json:"visibilityTimeoutSeconds,omitempty"`
}

type ackRequest struct {
	ID string `json:"id"`
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	queues := s.queueManager.ListQueues()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(queues); err != nil {
		http.Error(w, "Failed to encode queue list", http.StatusInternalServerError)
	}
}

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var req createQueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "Queue name is required", http.StatusBadRequest)
		return
	}
	if req.MaxSize <= 0 {
		http.Error(w, "maxSize must be greater than zero", http.StatusBadRequest)
		return
	}
	if req.BackpressureMode == "" {
		req.BackpressureMode = "block"
	}
	if req.BackpressureMode != "block" && req.BackpressureMode != "drop" && req.BackpressureMode != "error" {
		http.Error(w, "backpressureMode must be block, drop, or error", http.StatusBadRequest)
		return
	}

	visibilityTimeout := 30 * time.Second
	if req.VisibilityTimeoutSeconds > 0 {
		visibilityTimeout = time.Duration(req.VisibilityTimeoutSeconds) * time.Second
	}

	queueInfo, err := s.queueManager.CreateQueue(req.Name, req.MaxSize, req.BackpressureMode, visibilityTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(queueInfo.Info()); err != nil {
		http.Error(w, "Failed to encode queue info", http.StatusInternalServerError)
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

	msg, err := q.Pop(r.Context())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "Request canceled or timed out", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		http.Error(w, "Failed to encode message", http.StatusInternalServerError)
		return
	}
}

func (s *Server) ackHandler(w http.ResponseWriter, r *http.Request, queueName string) {
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

	var req ackRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	if err := q.Ack(req.ID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Message acknowledged")
}
