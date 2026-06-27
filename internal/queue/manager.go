package queue

import (
	"fmt"
	"sync"
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

// CreateQueue creates a new queue with the given name, max size, and backpressure mode.
// It returns an error if a queue with the same name already exists.
func (qm *QueueManager) CreateQueue(name string, maxSize int, backpressureMode string) (*Queue, error) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if _, exists := qm.queues[name]; exists {
		return nil, fmt.Errorf("queue '%s' already exists", name)
	}

	q := &Queue{
		Name:             name,
		MaxSize:          maxSize,
		BackpressureMode: backpressureMode,
		ch:               make(chan Message, maxSize),
	}
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