package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"ghostmq/internal/observability"
)

type queueShard struct {
	id                int
	parentName        string
	BackpressureMode  string
	VisibilityTimeout time.Duration
	ch                chan Message
	inFlight          map[string]inFlightItem
	inFlightMu        sync.Mutex
	stopCh            chan struct{}
	metricsRecorder   *observability.Recorder
}

func newQueueShard(parentName string, id, maxSize int, backpressureMode string, visibilityTimeout time.Duration, recorder *observability.Recorder) *queueShard {
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}

	return &queueShard{
		id:                id,
		parentName:        parentName,
		BackpressureMode:  backpressureMode,
		VisibilityTimeout: visibilityTimeout,
		ch:                make(chan Message, maxSize),
		inFlight:          make(map[string]inFlightItem),
		stopCh:            make(chan struct{}),
		metricsRecorder:   recorder,
	}
}

func (s *queueShard) recordEnqueue() {
	if s.metricsRecorder != nil {
		s.metricsRecorder.RecordEnqueue(s.parentName)
	}
}

func (s *queueShard) recordDequeue() {
	if s.metricsRecorder != nil {
		s.metricsRecorder.RecordDequeue(s.parentName)
	}
}

func (s *queueShard) recordAck() {
	if s.metricsRecorder != nil {
		s.metricsRecorder.RecordAck(s.parentName)
	}
}

func (s *queueShard) recordReject() {
	if s.metricsRecorder != nil {
		s.metricsRecorder.RecordReject(s.parentName)
	}
}

func (s *queueShard) requeueExpired() {
	now := time.Now()

	s.inFlightMu.Lock()
	expired := make([]Message, 0)
	for id, item := range s.inFlight {
		if now.After(item.expiresAt) {
			delete(s.inFlight, id)
			expired = append(expired, item.msg)
		}
	}
	s.inFlightMu.Unlock()

	for _, msg := range expired {
		s.requeue(msg)
	}
}

func (s *queueShard) requeue(msg Message) {
	defer func() {
		if r := recover(); r != nil {
			// Ignore send on closed channel
		}
	}()

	select {
	case <-s.stopCh:
		return
	case s.ch <- msg:
		return
	}
}

func (s *queueShard) startVisibilityMonitor() {
	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.requeueExpired()
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *queueShard) registerInFlight(msg Message) {
	s.inFlightMu.Lock()
	s.inFlight[msg.ID] = inFlightItem{
		msg:       msg,
		expiresAt: time.Now().Add(s.VisibilityTimeout),
	}
	s.inFlightMu.Unlock()
}

func (s *queueShard) Ack(messageID string) error {
	s.inFlightMu.Lock()
	item, exists := s.inFlight[messageID]
	if !exists {
		s.inFlightMu.Unlock()
		return fmt.Errorf("message not found or already acknowledged")
	}
	delete(s.inFlight, messageID)
	s.inFlightMu.Unlock()
	ReleasePayload(item.msg.Payload)
	s.recordAck()
	return nil
}

func (s *queueShard) Info() QueueInfo {
	s.inFlightMu.Lock()
	inFlightCount := len(s.inFlight)
	s.inFlightMu.Unlock()

	return QueueInfo{
		Name:                     s.parentName,
		MaxSize:                  cap(s.ch),
		BackpressureMode:         s.BackpressureMode,
		VisibilityTimeoutSeconds: int(s.VisibilityTimeout.Seconds()),
		Pending:                  len(s.ch),
		InFlight:                 inFlightCount,
	}
}

func (s *queueShard) Close() {
	close(s.stopCh)
	close(s.ch)
}

// PartitionedQueue exposes a sharded queue with a single logical name.
type PartitionedQueue struct {
	Name              string
	MaxSize           int
	BackpressureMode  string
	VisibilityTimeout time.Duration
	PartitionCount    int
	shards            []*queueShard
	popCh             chan Message
	pushIndex         uint64
	metricsRecorder   *observability.Recorder
	stopCh            chan struct{}
	closeOnce         sync.Once
	wg                sync.WaitGroup
}

func (q *PartitionedQueue) SetMetricsRecorder(recorder *observability.Recorder) {
	q.metricsRecorder = recorder
	for _, shard := range q.shards {
		shard.metricsRecorder = recorder
	}
}

// NewPartitionedQueue creates a queue with internal shards for higher parallelism.
func NewPartitionedQueue(name string, maxSize int, backpressureMode string, visibilityTimeout time.Duration, partitionCount int, recorder *observability.Recorder) *PartitionedQueue {
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}
	if partitionCount <= 1 {
		partitionCount = 1
	}

	if recorder == nil {
		recorder = observability.NewRecorder()
	}

	q := &PartitionedQueue{
		Name:              name,
		MaxSize:           maxSize,
		BackpressureMode:  backpressureMode,
		VisibilityTimeout: visibilityTimeout,
		PartitionCount:    partitionCount,
		popCh:             make(chan Message, maxSize),
		metricsRecorder:   recorder,
		stopCh:            make(chan struct{}),
	}

	shardSize := maxSize / partitionCount
	if shardSize == 0 {
		shardSize = 1
	}
	for i := 0; i < partitionCount; i++ {
		q.shards = append(q.shards, newQueueShard(name, i, shardSize, backpressureMode, visibilityTimeout, recorder))
	}

	for _, shard := range q.shards {
		shard.startVisibilityMonitor()
		q.wg.Add(1)
		go q.forwardShard(shard)
	}

	return q
}

func (q *PartitionedQueue) forwardShard(shard *queueShard) {
	defer q.wg.Done()
	batch := make([]Message, 0, 8)

	for {
		select {
		case <-q.stopCh:
			return
		case msg, ok := <-shard.ch:
			if !ok {
				return
			}
			shard.registerInFlight(msg)
			batch = append(batch, msg)
		drain:
			for len(batch) < 8 {
				select {
				case msg2, ok2 := <-shard.ch:
					if !ok2 {
						break drain
					}
					shard.registerInFlight(msg2)
					batch = append(batch, msg2)
				default:
					break drain
				}
			}
			for _, item := range batch {
				select {
				case <-q.stopCh:
					return
				case q.popCh <- item:
				}
			}
			batch = batch[:0]
		}
	}
}

// Push sends a message into one of the internal shards.
func (q *PartitionedQueue) Push(msg Message) error {
	if q.PartitionCount <= 1 {
		return q.shards[0].push(msg)
	}

	shard := q.shards[atomic.AddUint64(&q.pushIndex, 1)%uint64(len(q.shards))]
	return shard.push(msg)
}

func (s *queueShard) push(msg Message) error {
	si := s
	si.recordEnqueue()

	switch s.BackpressureMode {
	case "block":
		s.ch <- msg
		return nil
	case "drop":
		select {
		case s.ch <- msg:
			return nil
		default:
			s.recordReject()
			return fmt.Errorf("queue is full, message dropped")
		}
	case "error":
		select {
		case s.ch <- msg:
			return nil
		default:
			s.recordReject()
			return fmt.Errorf("queue is full, message rejected")
		}
	default:
		s.ch <- msg
		return nil
	}
}

// Pop retrieves the next available message from any shard.
func (q *PartitionedQueue) Pop(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-q.popCh:
		if !ok {
			return nil, fmt.Errorf("queue closed")
		}
		return &msg, nil
	}
}

// Ack acknowledges a message in any shard.
func (q *PartitionedQueue) Ack(messageID string) error {
	for _, shard := range q.shards {
		if err := shard.Ack(messageID); err == nil {
			return nil
		}
	}
	return fmt.Errorf("message not found or already acknowledged")
}

// Info returns aggregated queue information across all partitions.
func (q *PartitionedQueue) Info() QueueInfo {
	info := QueueInfo{
		Name:                     q.Name,
		MaxSize:                  q.MaxSize,
		BackpressureMode:         q.BackpressureMode,
		VisibilityTimeoutSeconds: int(q.VisibilityTimeout.Seconds()),
		PartitionCount:           q.PartitionCount,
	}

	for _, shard := range q.shards {
		shardInfo := shard.Info()
		info.Pending += shardInfo.Pending
		info.InFlight += shardInfo.InFlight
	}

	return info
}

// Close stops all shard workers and closes the queue.
func (q *PartitionedQueue) Close() {
	q.closeOnce.Do(func() {
		close(q.stopCh)
		for _, shard := range q.shards {
			shard.Close()
		}
		q.wg.Wait()
		close(q.popCh)
	})
}
