package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"ghostmq/internal/queue"
)

// Controller handles HTTP requests and delegates to the service layer.
type Controller struct {
	service *QueueService
}

func newController(queueManager *queue.QueueManager) *Controller {
	return &Controller{service: newQueueService(queueManager)}
}

type createQueueRequest struct {
	Name                     string `json:"name"`
	MaxSize                  int    `json:"maxSize"`
	BackpressureMode         string `json:"backpressureMode"`
	VisibilityTimeoutSeconds int    `json:"visibilityTimeoutSeconds,omitempty"`
	PartitionCount           int    `json:"partitionCount,omitempty"`
}

type ackRequest struct {
	ID string `json:"id"`
}

func (c *Controller) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", c.healthHandler)
	mux.HandleFunc("/metrics", c.metricsHandler)
	mux.HandleFunc("/queues", c.queuesHandler)
	mux.HandleFunc("/queues/", c.queueHandler)
}

func (c *Controller) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(c.service.Health()); err != nil {
		http.Error(w, "Failed to encode health response", http.StatusInternalServerError)
	}
}

func (c *Controller) metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(c.service.Metrics()); err != nil {
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
	}
}

func (c *Controller) queuesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		c.listQueues(w, r)
	case http.MethodPost:
		c.createQueue(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (c *Controller) queueHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "queues" {
		http.NotFound(w, r)
		return
	}

	queueName := parts[1]
	if err := ValidateQueueName(queueName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(parts) == 2 {
		switch r.Method {
		case http.MethodPost:
			c.pushHandler(w, r, queueName)
		case http.MethodGet:
			c.popHandler(w, r, queueName)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if len(parts) == 3 && parts[2] == "ack" && r.Method == http.MethodPost {
		c.ackHandler(w, r, queueName)
		return
	}

	http.NotFound(w, r)
}

func (c *Controller) listQueues(w http.ResponseWriter, r *http.Request) {
	queues := c.service.ListQueues()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(queues); err != nil {
		http.Error(w, "Failed to encode queue list", http.StatusInternalServerError)
	}
}

func (c *Controller) createQueue(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var req createQueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	info, err := c.service.CreateQueue(CreateQueueInput{
		Name:                     req.Name,
		MaxSize:                  req.MaxSize,
		BackpressureMode:         req.BackpressureMode,
		VisibilityTimeoutSeconds: req.VisibilityTimeoutSeconds,
		PartitionCount:           req.PartitionCount,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(info); err != nil {
		http.Error(w, "Failed to encode queue info", http.StatusInternalServerError)
	}
}

func (c *Controller) pushHandler(w http.ResponseWriter, r *http.Request, queueName string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	msgID, err := c.service.PushMessage(queueName, body)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Message accepted: %s", msgID)
}

func (c *Controller) popHandler(w http.ResponseWriter, r *http.Request, queueName string) {
	msg, err := c.service.PopMessage(queueName, r.Context())
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			http.NotFound(w, r)
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "Request canceled or timed out", http.StatusRequestTimeout)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		http.Error(w, "Failed to encode message", http.StatusInternalServerError)
		return
	}
}

func (c *Controller) ackHandler(w http.ResponseWriter, r *http.Request, queueName string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var req ackRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if err := c.service.AckMessage(queueName, req.ID); err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Message acknowledged")
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func (c *Controller) logRequest(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("%s %s", r.Method, r.URL.Path)
		next(w, r)
		log.Printf("completed %s in %s", r.URL.Path, time.Since(start))
	}
}
