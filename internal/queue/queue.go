package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"ghostmq/internal/observability"
)

var payloadPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 1024)
	},
}

type inFlightItem struct {
	msg       Message
	expiresAt time.Time
}

// QueueBackend represents a queue implementation with push/pop/ack semantics.
type QueueBackend interface {
	Push(Message) error
	Pop(context.Context) (*Message, error)
	Ack(string) error
	Info() QueueInfo
	Close()
	SetMetricsRecorder(*observability.Recorder)
}

// AcquirePayload returns a preallocated byte slice of the requested length.
func AcquirePayload(length int) []byte {
	buf := payloadPool.Get().([]byte)
	if cap(buf) < length {
		buf = make([]byte, length)
	}
	return buf[:length]
}

// ReleasePayload returns a payload buffer to the pool for reuse.
func ReleasePayload(payload []byte) {
	if len(payload) == 0 {
		return
	}
	payloadPool.Put(payload[:0])
}

// Message represents a message in the queue.
type Message struct {
	ID        string    `json:"id"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

// QueueInfo provides status metadata for a queue.
type QueueInfo struct {
	Name                     string `json:"name"`
	MaxSize                  int    `json:"maxSize"`
	BackpressureMode         string `json:"backpressureMode"`
	VisibilityTimeoutSeconds int    `json:"visibilityTimeoutSeconds"`
	PartitionCount           int    `json:"partitionCount"`
	Pending                  int    `json:"pending"`
	InFlight                 int    `json:"inFlight"`
}

// Queue represents a message queue with a buffered channel and backpressure configuration.
type Queue struct {
	Name              string
	MaxSize           int
	BackpressureMode  string
	VisibilityTimeout time.Duration
	ch                chan Message
	inFlight          map[string]inFlightItem
	inFlightMu        sync.Mutex
	stopCh            chan struct{}
	closeOnce         sync.Once
	mu                sync.RWMutex
	metricsRecorder   *observability.Recorder
}

// NewQueue creates a new queue and starts visibility timeout monitoring.
func NewQueue(name string, maxSize int, backpressureMode string, visibilityTimeout time.Duration) *Queue {
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}

	q := &Queue{
		Name:              name,
		MaxSize:           maxSize,
		BackpressureMode:  backpressureMode,
		VisibilityTimeout: visibilityTimeout,
		ch:                make(chan Message, maxSize),
		inFlight:          make(map[string]inFlightItem),
		stopCh:            make(chan struct{}),
	}
	q.startVisibilityMonitor()
	return q
}

func (q *Queue) SetMetricsRecorder(recorder *observability.Recorder) {
	q.metricsRecorder = recorder
}

func (q *Queue) recordEnqueue() {
	if q.metricsRecorder != nil {
		q.metricsRecorder.RecordEnqueue(q.Name)
	}
}

func (q *Queue) recordDequeue() {
	if q.metricsRecorder != nil {
		q.metricsRecorder.RecordDequeue(q.Name)
	}
}

func (q *Queue) recordAck() {
	if q.metricsRecorder != nil {
		q.metricsRecorder.RecordAck(q.Name)
	}
}

func (q *Queue) recordReject() {
	if q.metricsRecorder != nil {
		q.metricsRecorder.RecordReject(q.Name)
	}
}

func (q *Queue) startVisibilityMonitor() {
	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				q.requeueExpired()
			case <-q.stopCh:
				return
			}
		}
	}()
}

func (q *Queue) requeueExpired() {
	now := time.Now()

	q.inFlightMu.Lock()
	expired := make([]Message, 0)
	for id, item := range q.inFlight {
		if now.After(item.expiresAt) {
			delete(q.inFlight, id)
			expired = append(expired, item.msg)
		}
	}
	q.inFlightMu.Unlock()

	for _, msg := range expired {
		q.requeue(msg)
	}
}

func (q *Queue) requeue(msg Message) {
	defer func() {
		if r := recover(); r != nil {
			// Ignore send on closed channel
		}
	}()

	select {
	case <-q.stopCh:
		return
	case q.ch <- msg:
		return
	}
}

// Push adds a message to the queue, applying the configured backpressure mode if the queue is full.
func (q *Queue) Push(msg Message) (err error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("queue closed")
		}
	}()

	switch q.BackpressureMode {
	case "block":
		q.ch <- msg
		q.recordEnqueue()
		return nil
	case "drop":
		select {
		case q.ch <- msg:
			q.recordEnqueue()
			return nil
		default:
			q.recordReject()
			return errors.New("queue is full, message dropped")
		}
	case "error":
		select {
		case q.ch <- msg:
			q.recordEnqueue()
			return nil
		default:
			q.recordReject()
			return errors.New("queue is full, message rejected")
		}
	default:
		q.ch <- msg
		q.recordEnqueue()
		return nil
	}
}

// Pop retrieves a message from the queue and marks it in-flight.
func (q *Queue) Pop(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-q.ch:
		if !ok {
			return nil, errors.New("queue closed")
		}
		q.registerInFlight(msg)
		q.recordDequeue()
		return &msg, nil
	}
}

func (q *Queue) registerInFlight(msg Message) {
	q.inFlightMu.Lock()
	q.inFlight[msg.ID] = inFlightItem{
		msg:       msg,
		expiresAt: time.Now().Add(q.VisibilityTimeout),
	}
	q.inFlightMu.Unlock()
}

// Ack acknowledges that a message was processed successfully.
func (q *Queue) Ack(messageID string) error {
	q.inFlightMu.Lock()
	item, exists := q.inFlight[messageID]
	if !exists {
		q.inFlightMu.Unlock()
		return errors.New("message not found or already acknowledged")
	}
	delete(q.inFlight, messageID)
	q.inFlightMu.Unlock()

	ReleasePayload(item.msg.Payload)
	q.recordAck()
	return nil
}

// Info returns status metadata for this queue.
func (q *Queue) Info() QueueInfo {
	q.inFlightMu.Lock()
	inFlightCount := len(q.inFlight)
	q.inFlightMu.Unlock()

	return QueueInfo{
		Name:                     q.Name,
		MaxSize:                  q.MaxSize,
		BackpressureMode:         q.BackpressureMode,
		VisibilityTimeoutSeconds: int(q.VisibilityTimeout.Seconds()),
		PartitionCount:           1,
		Pending:                  len(q.ch),
		InFlight:                 inFlightCount,
	}
}

// Close shuts down the queue and its visibility monitoring.
func (q *Queue) Close() {
	q.closeOnce.Do(func() {
		close(q.stopCh)
		close(q.ch)
	})
}
