package grpcserver

import (
	"context"
	"errors"
	"time"

	"ghostmq/internal/queue"
	"ghostmq/internal/server"
)

// Service adapts the shared queue service to the gRPC API.
type Service struct {
	UnimplementedGhostMQServiceServer
	service *server.QueueService
}

// NewService creates a gRPC-backed service using the shared queue service layer.
func NewService(service *server.QueueService) *Service {
	return &Service{service: service}
}

// CreateQueue creates a queue through the shared service layer.
func (s *Service) CreateQueue(ctx context.Context, req *CreateQueueRequest) (*QueueInfo, error) {
	if req == nil {
		return nil, errors.New("request is required")
	}
	info, err := s.service.CreateQueue(server.CreateQueueInput{
		Name:                     req.Name,
		MaxSize:                  int(req.MaxSize),
		BackpressureMode:         req.BackpressureMode,
		VisibilityTimeoutSeconds: int(req.VisibilityTimeoutSeconds),
		PartitionCount:           int(req.PartitionCount),
	})
	if err != nil {
		return nil, err
	}
	return &QueueInfo{
		Name:                     info.Name,
		MaxSize:                  int32(info.MaxSize),
		BackpressureMode:         info.BackpressureMode,
		VisibilityTimeoutSeconds: int32(info.VisibilityTimeoutSeconds),
		Pending:                  int32(info.Pending),
		InFlight:                 int32(info.InFlight),
	}, nil
}

// PushMessage publishes a message through the shared service layer.
func (s *Service) PushMessage(ctx context.Context, req *PushMessageRequest) (*PushMessageResponse, error) {
	if req == nil {
		return nil, errors.New("request is required")
	}
	msgID, err := s.Push(ctx, req.QueueName, req.Payload)
	if err != nil {
		return nil, err
	}
	return &PushMessageResponse{MessageId: msgID}, nil
}

// Push publishes a message through the shared service layer.
func (s *Service) Push(ctx context.Context, queueName string, payload []byte) (string, error) {
	return s.service.PushMessage(queueName, payload)
}

// PopMessage retrieves the next message through the shared service layer.
func (s *Service) PopMessage(ctx context.Context, req *PopMessageRequest) (*Message, error) {
	if req == nil {
		return nil, errors.New("request is required")
	}
	msg, err := s.Pop(ctx, req.QueueName)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return &Message{
		Id:                msg.ID,
		Payload:           msg.Payload,
		TimestampUnixNano: msg.Timestamp.UnixNano(),
	}, nil
}

// Pop retrieves the next message through the shared service layer.
func (s *Service) Pop(ctx context.Context, queueName string) (*queue.Message, error) {
	return s.service.PopMessage(queueName, ctx)
}

// AckMessage acknowledges a message through the shared service layer.
func (s *Service) AckMessage(ctx context.Context, req *AckMessageRequest) (*AckMessageResponse, error) {
	if req == nil {
		return nil, errors.New("request is required")
	}
	err := s.service.AckMessage(req.QueueName, req.Id)
	if err != nil {
		return nil, err
	}
	return &AckMessageResponse{Acknowledged: true}, nil
}

// ListQueues lists the current queues.
func (s *Service) ListQueues(ctx context.Context, req *ListQueuesRequest) (*ListQueuesResponse, error) {
	queues := s.service.ListQueues()
	res := make([]*QueueInfo, 0, len(queues))
	for _, info := range queues {
		res = append(res, &QueueInfo{
			Name:                     info.Name,
			MaxSize:                  int32(info.MaxSize),
			BackpressureMode:         info.BackpressureMode,
			VisibilityTimeoutSeconds: int32(info.VisibilityTimeoutSeconds),
			Pending:                  int32(info.Pending),
			InFlight:                 int32(info.InFlight),
		})
	}
	return &ListQueuesResponse{Queues: res}, nil
}

// Health returns service health.
func (s *Service) Health(ctx context.Context, req *HealthRequest) (*HealthResponse, error) {
	health := s.service.Health()
	return &HealthResponse{Status: health.Status, QueueCount: int32(health.QueueCount)}, nil
}

// Metrics returns metrics snapshot.
func (s *Service) Metrics(ctx context.Context, req *MetricsRequest) (*MetricsResponse, error) {
	snapshot := s.service.Metrics()
	queues := make(map[string]*QueueMetrics, len(snapshot.Queues))
	for name, metrics := range snapshot.Queues {
		queues[name] = &QueueMetrics{
			EnqueueCount: metrics.Enqueued,
			DequeueCount: metrics.Dequeued,
			AckCount:     metrics.Acked,
			RejectCount:  metrics.Rejected,
		}
	}
	return &MetricsResponse{Queues: queues}, nil
}

// NewQueueService exposes the shared queue service for tests and other packages.
func NewQueueService(qm *queue.QueueManager) *server.QueueService {
	return server.NewQueueService(qm)
}

var _ = time.Second
