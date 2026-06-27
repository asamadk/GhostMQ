package queue

import (
	"errors"
	"sync"
	"time"
)

// Message represents a message in the queue.
type Message struct {
	ID        string    `json:"id"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

// Queue represents a message queue with a buffered channel and backpressure configuration.
type Queue struct {
	Name             string
	MaxSize          int
	BackpressureMode string
	ch               chan Message
	mu               sync.RWMutex // For protecting access to queue properties if needed, though channel operations are safe.
}

// Push adds a message to the queue, applying the configured backpressure mode if the queue is full.
func (q *Queue) Push(msg Message) error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	switch q.BackpressureMode {
	case "block":
		q.ch <- msg
		return nil
	case "drop":
		select {
		case q.ch <- msg:
			return nil
		default:
			return errors.New("queue is full, message dropped")
		}
	case "error":
		select {
		case q.ch <- msg:
			return nil
		default:
			return errors.New("queue is full, message rejected")
		}
	default:
		// Default to block if mode is unknown
		q.ch <- msg
		return nil
	}
}

// Pop retrieves a message from the queue. It blocks until a message is available.
func (q *Queue) Pop() (*Message, bool) {
	msg, ok := <-q.ch
	if !ok {
		return nil, false
	}
	return &msg, true
}

// Close closes the queue's channel.
func (q *Queue) Close() {
	close(q.ch)
}
