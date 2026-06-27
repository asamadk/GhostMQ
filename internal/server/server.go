package server

import (
	"context"
	"log"
	"net/http"

	"ghostmq/internal/queue"
)

// Server represents the HTTP server for GhostMQ.
type Server struct {
	queueManager *queue.QueueManager
	httpServer   *http.Server
	controller   *Controller
}

// NewServer creates a new Server instance.
func NewServer(queueManager *queue.QueueManager) *Server {
	return &Server{
		queueManager: queueManager,
		controller:   newController(queueManager),
	}
}

// Start starts the HTTP server in a new goroutine.
func (s *Server) Start(addr string) {
	mux := http.NewServeMux()
	s.controller.registerRoutes(mux)

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
