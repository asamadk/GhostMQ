package observability

import "sync"

// QueueMetrics tracks lightweight queue-level counters.
type QueueMetrics struct {
	Enqueued int64 `json:"enqueued"`
	Dequeued int64 `json:"dequeued"`
	Acked    int64 `json:"acked"`
	Rejected int64 `json:"rejected"`
}

// Snapshot contains the current metrics for all queues.
type Snapshot struct {
	Queues map[string]QueueMetrics `json:"queues"`
}

// Recorder stores queue metrics in memory.
type Recorder struct {
	mu      sync.RWMutex
	metrics map[string]QueueMetrics
}

// NewRecorder creates a new in-memory metrics recorder.
func NewRecorder() *Recorder {
	return &Recorder{metrics: make(map[string]QueueMetrics)}
}

// RecordEnqueue increments the enqueue counter for a queue.
func (r *Recorder) RecordEnqueue(queueName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	metrics := r.metrics[queueName]
	metrics.Enqueued++
	r.metrics[queueName] = metrics
}

// RecordDequeue increments the dequeue counter for a queue.
func (r *Recorder) RecordDequeue(queueName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	metrics := r.metrics[queueName]
	metrics.Dequeued++
	r.metrics[queueName] = metrics
}

// RecordAck increments the ack counter for a queue.
func (r *Recorder) RecordAck(queueName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	metrics := r.metrics[queueName]
	metrics.Acked++
	r.metrics[queueName] = metrics
}

// RecordReject increments the reject counter for a queue.
func (r *Recorder) RecordReject(queueName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	metrics := r.metrics[queueName]
	metrics.Rejected++
	r.metrics[queueName] = metrics
}

// Snapshot returns an immutable copy of the current metrics.
func (r *Recorder) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshot := Snapshot{Queues: make(map[string]QueueMetrics, len(r.metrics))}
	for name, metrics := range r.metrics {
		snapshot.Queues[name] = metrics
	}
	return snapshot
}
