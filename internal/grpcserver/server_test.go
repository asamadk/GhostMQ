package grpcserver

import (
	"context"
	"testing"
	"time"

	"ghostmq/internal/queue"
	"ghostmq/internal/server"
)

func TestGRPCService_UsesQueueService(t *testing.T) {
	qm := queue.NewQueueManager()
	defer qm.Close()

	_, err := qm.CreateQueue("test", 8, "block", 30*time.Second)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	svc := server.NewQueueService(qm)
	grpcSvc := NewService(svc)

	id, err := grpcSvc.Push(context.Background(), "test", []byte(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if id == "" {
		t.Fatal("expected message id")
	}

	msg, err := grpcSvc.Pop(context.Background(), "test")
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if msg == nil || string(msg.Payload) == "" {
		t.Fatal("expected payload")
	}
}
