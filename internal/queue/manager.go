package queue

import (
	"fmt"
	"sync"
	"time"
)

// QueueManager manages a collection of queues.
type QueueManager struct {
	queues map[string]*Queue
	mu     sync.RWMutex // Protects the queues map
}

// NewQueueManager creates and returns a new QueueManager instance.
func NewQueueManager() *QueueManager {
	return &QueueManager{
		queues: make(map[string]*Queue),
	}
}

// CreateQueue creates a new queue with the given name, max size, backpressure mode, and visibility timeout.
// It returns an error if a queue with the same name already exists.
func (qm *QueueManager) CreateQueue(name string, maxSize int, backpressureMode string, visibilityTimeout time.Duration) (*Queue, error) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if _, exists := qm.queues[name]; exists {
		return nil, fmt.Errorf("queue '%s' already exists", name)
	}

	q := NewQueue(name, maxSize, backpressureMode, visibilityTimeout)
	qm.queues[name] = q
	return q, nil
}

// GetQueue retrieves a queue by its name.
// It returns nil if the queue does not exist.
func (qm *QueueManager) GetQueue(name string) *Queue {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	return qm.queues[name]
}

// ListQueues returns a snapshot of current queues and their status.
func (qm *QueueManager) ListQueues() []QueueInfo {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	infos := make([]QueueInfo, 0, len(qm.queues))
	for _, q := range qm.queues {
		infos = append(infos, q.Info())
	}
	return infos
}

// Close shuts down all managed queues.
func (qm *QueueManager) Close() {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	for _, q := range qm.queues {
		q.Close()
	}
}
