package observability

import "testing"

func TestRecorderSnapshotTracksQueueMetrics(t *testing.T) {
	recorder := NewRecorder()

	recorder.RecordEnqueue("alpha")
	recorder.RecordDequeue("alpha")
	recorder.RecordAck("alpha")
	recorder.RecordReject("alpha")

	snapshot := recorder.Snapshot()
	queueMetrics, ok := snapshot.Queues["alpha"]
	if !ok {
		t.Fatalf("expected queue metrics for alpha")
	}

	if queueMetrics.Enqueued != 1 {
		t.Fatalf("expected 1 enqueue, got %d", queueMetrics.Enqueued)
	}
	if queueMetrics.Dequeued != 1 {
		t.Fatalf("expected 1 dequeue, got %d", queueMetrics.Dequeued)
	}
	if queueMetrics.Acked != 1 {
		t.Fatalf("expected 1 ack, got %d", queueMetrics.Acked)
	}
	if queueMetrics.Rejected != 1 {
		t.Fatalf("expected 1 reject, got %d", queueMetrics.Rejected)
	}
}
