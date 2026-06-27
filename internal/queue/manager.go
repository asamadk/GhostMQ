package queue

import (
	"fmt"
	"sync"
	"time"

	"ghostmq/internal/observability"
)

// QueueManager manages a collection of queues.
type QueueManager struct {
	queues          map[string]QueueBackend
	mu              sync.RWMutex // Protects the queues map
	metricsRecorder *observability.Recorder
}

// NewQueueManager creates and returns a new QueueManager instance.
func NewQueueManager() *QueueManager {
	return NewQueueManagerWithRecorder(observability.NewRecorder())
}

// NewQueueManagerWithRecorder creates a queue manager with an injected metrics recorder.
func NewQueueManagerWithRecorder(recorder *observability.Recorder) *QueueManager {
	if recorder == nil {
		recorder = observability.NewRecorder()
	}
	return &QueueManager{
		queues:          make(map[string]QueueBackend),
		metricsRecorder: recorder,
	}
}

// CreateQueue creates a new queue with the given name, max size, backpressure mode, and visibility timeout.
// It returns an error if a queue with the same name already exists.
func (qm *QueueManager) CreateQueue(name string, maxSize int, backpressureMode string, visibilityTimeout time.Duration, partitionCount int) (QueueBackend, error) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if _, exists := qm.queues[name]; exists {
		return nil, fmt.Errorf("queue '%s' already exists", name)
	}

	var q QueueBackend
	if partitionCount > 1 {
		q = NewPartitionedQueue(name, maxSize, backpressureMode, visibilityTimeout, partitionCount, qm.metricsRecorder)
	} else {
		q = NewQueue(name, maxSize, backpressureMode, visibilityTimeout)
		q.SetMetricsRecorder(qm.metricsRecorder)
	}
	qm.queues[name] = q
	return q, nil
}

// GetQueue retrieves a queue by its name.
// It returns nil if the queue does not exist.
func (qm *QueueManager) GetQueue(name string) QueueBackend {
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

// MetricsSnapshot returns the current in-memory metrics for all queues.
func (qm *QueueManager) MetricsSnapshot() observability.Snapshot {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	if qm.metricsRecorder == nil {
		return observability.Snapshot{Queues: make(map[string]observability.QueueMetrics)}
	}
	return qm.metricsRecorder.Snapshot()
}

// Close shuts down all managed queues.
func (qm *QueueManager) Close() {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	for _, q := range qm.queues {
		q.Close()
	}
}
