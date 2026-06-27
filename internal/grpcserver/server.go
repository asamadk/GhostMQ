package grpcserver

import (
	"context"
	"fmt"
	"log"
	"net"

	"ghostmq/internal/queue"
	"ghostmq/internal/server"
	"google.golang.org/grpc"
)

// Server wraps the gRPC listener and service.
type Server struct {
	queueManager *queue.QueueManager
	grpcServer   *grpc.Server
	listener     net.Listener
	service      *Service
}

// NewServer creates a new gRPC server instance.
func NewServer(queueManager *queue.QueueManager) *Server {
	service := NewService(server.NewQueueService(queueManager))
	grpcServer := grpc.NewServer()
	RegisterGhostMQServiceServer(grpcServer, service)
	return &Server{queueManager: queueManager, grpcServer: grpcServer, service: service}
}

// Start starts the gRPC server on the provided address.
func (s *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	s.listener = listener
	go func() {
		log.Printf("gRPC server listening on %s", addr)
		if err := s.grpcServer.Serve(listener); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()
	return nil
}

// Shutdown gracefully stops the gRPC server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	return nil
}
