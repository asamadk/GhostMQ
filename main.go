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
	cfg, err := config.LoadConfig("ghostmq.yaml")
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	queueManager := queue.NewQueueManager()

	for _, qc := range cfg.Queues {
		if qc.Name == "" {
			log.Printf("skipping queue with missing name")
			continue
		}
		if qc.MaxSize <= 0 {
			log.Printf("skipping queue '%s' with invalid maxSize", qc.Name)
			continue
		}
		if qc.BackpressureMode == "" {
			qc.BackpressureMode = "block"
		}
		visibilityTimeout := 30 * time.Second
		if qc.VisibilityTimeoutSeconds > 0 {
			visibilityTimeout = time.Duration(qc.VisibilityTimeoutSeconds) * time.Second
		}

		_, err := queueManager.CreateQueue(qc.Name, qc.MaxSize, qc.BackpressureMode, visibilityTimeout)
		if err != nil {
			log.Printf("failed to create queue '%s': %v", qc.Name, err)
		} else {
			log.Printf("queue '%s' created", qc.Name)
		}
	}

	httpServer := server.NewServer(queueManager)
	httpServer.Start(":8080")
	log.Println("HTTP server started on :8080")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	queueManager.Close()
}
