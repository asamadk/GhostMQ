package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"ghostmq/internal/observability"
	"ghostmq/internal/queue"

	"github.com/google/uuid"
)

var (
	ErrQueueNotFound = errors.New("queue not found")
	queueNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
)

// QueueService contains the business logic for queue operations.
type QueueService struct {
	queueManager *queue.QueueManager
}

func newQueueService(queueManager *queue.QueueManager) *QueueService {
	return NewQueueService(queueManager)
}

// NewQueueService creates a queue service using the provided queue manager.
func NewQueueService(queueManager *queue.QueueManager) *QueueService {
	return &QueueService{queueManager: queueManager}
}

// CreateQueueInput describes the data required to create a queue.
type CreateQueueInput struct {
	Name                     string
	MaxSize                  int
	BackpressureMode         string
	VisibilityTimeoutSeconds int
}

// Health returns a lightweight health summary for the service.
type HealthResponse struct {
	Status     string `json:"status"`
	QueueCount int    `json:"queueCount"`
}

// ListQueues returns the current queue status snapshot.
func (s *QueueService) ListQueues() []queue.QueueInfo {
	return s.queueManager.ListQueues()
}

// Health returns a health summary for the service.
func (s *QueueService) Health() HealthResponse {
	return HealthResponse{
		Status:     "ok",
		QueueCount: len(s.queueManager.ListQueues()),
	}
}

// Metrics returns the current metrics snapshot.
func (s *QueueService) Metrics() observability.Snapshot {
	return s.queueManager.MetricsSnapshot()
}

func validateCreateQueueInput(input CreateQueueInput) error {
	if input.Name == "" {
		return errors.New("queue name is required")
	}
	if !queueNamePattern.MatchString(input.Name) {
		return fmt.Errorf("queue name can only contain letters, numbers, '.', '_' and '-'")
	}
	if input.MaxSize <= 0 {
		return errors.New("maxSize must be greater than zero")
	}
	if input.BackpressureMode == "" {
		input.BackpressureMode = "block"
	}
	if input.BackpressureMode != "block" && input.BackpressureMode != "drop" && input.BackpressureMode != "error" {
		return errors.New("backpressureMode must be block, drop, or error")
	}
	if input.VisibilityTimeoutSeconds < 0 {
		return errors.New("visibilityTimeoutSeconds must be greater than or equal to zero")
	}
	return nil
}

// CreateQueue validates and creates a queue.
func (s *QueueService) CreateQueue(input CreateQueueInput) (queue.QueueInfo, error) {
	if err := validateCreateQueueInput(input); err != nil {
		return queue.QueueInfo{}, err
	}

	visibilityTimeout := 30 * time.Second
	if input.VisibilityTimeoutSeconds > 0 {
		visibilityTimeout = time.Duration(input.VisibilityTimeoutSeconds) * time.Second
	}

	q, err := s.queueManager.CreateQueue(input.Name, input.MaxSize, input.BackpressureMode, visibilityTimeout)
	if err != nil {
		return queue.QueueInfo{}, err
	}
	return q.Info(), nil
}

// PushMessage validates and enqueues a JSON payload.
func (s *QueueService) PushMessage(queueName string, body []byte) (string, error) {
	q := s.queueManager.GetQueue(queueName)
	if q == nil {
		return "", ErrQueueNotFound
	}

	var payload json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", err
	}

	msg := queue.Message{
		ID:        uuid.New().String(),
		Payload:   payload,
		Timestamp: time.Now(),
	}

	if err := q.Push(msg); err != nil {
		return "", err
	}

	return msg.ID, nil
}

// PopMessage retrieves the next message for a queue.
func (s *QueueService) PopMessage(queueName string, ctx context.Context) (*queue.Message, error) {
	q := s.queueManager.GetQueue(queueName)
	if q == nil {
		return nil, ErrQueueNotFound
	}
	return q.Pop(ctx)
}

// AckMessage acknowledges an in-flight message.
func (s *QueueService) AckMessage(queueName, id string) error {
	q := s.queueManager.GetQueue(queueName)
	if q == nil {
		return ErrQueueNotFound
	}
	if id == "" {
		return errors.New("id is required")
	}
	return q.Ack(id)
}

// ValidateQueueName ensures a queue name conforms to the public API contract.
func ValidateQueueName(name string) error {
	if name == "" {
		return errors.New("queue name is required")
	}
	if !queueNamePattern.MatchString(name) {
		return fmt.Errorf("queue name can only contain letters, numbers, '.', '_' and '-'")
	}
	return nil
}
