package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ghostmq/internal/config"
	"ghostmq/internal/queue"
	"ghostmq/internal/server"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("ghostmq.yaml")
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	// Create a new queue manager
	queueManager := queue.NewQueueManager()

	// Create queues from the configuration
	for _, qc := range cfg.Queues {
		_, err := queueManager.CreateQueue(qc.Name, qc.MaxSize, qc.BackpressureMode)
		if err != nil {
			log.Printf("failed to create queue '%s': %v", qc.Name, err)
		} else {
			log.Printf("queue '%s' created", qc.Name)
		}
	}

	// Create and start the HTTP server
	httpServer := server.NewServer(queueManager)
	httpServer.Start(":8080")
	log.Println("HTTP server started on :8080")

	// Wait for a shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")

	// Create a context with a timeout for the graceful shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
}
