package server

import "testing"

func TestValidateCreateQueueInput(t *testing.T) {
	tests := []struct {
		name    string
		input   CreateQueueInput
		wantErr string
	}{
		{
			name: "valid input",
			input: CreateQueueInput{
				Name:                     "alpha",
				MaxSize:                  100,
				BackpressureMode:         "block",
				VisibilityTimeoutSeconds: 30,
			},
		},
		{
			name: "empty queue name",
			input: CreateQueueInput{
				MaxSize: 100,
			},
			wantErr: "queue name is required",
		},
		{
			name: "invalid queue name format",
			input: CreateQueueInput{
				Name:    "bad name",
				MaxSize: 100,
			},
			wantErr: "queue name can only contain",
		},
		{
			name: "invalid backpressure mode",
			input: CreateQueueInput{
				Name:             "alpha",
				MaxSize:          100,
				BackpressureMode: "slow",
			},
			wantErr: "backpressureMode must be",
		},
		{
			name: "invalid visibility timeout",
			input: CreateQueueInput{
				Name:                     "alpha",
				MaxSize:                  100,
				VisibilityTimeoutSeconds: -1,
			},
			wantErr: "visibilityTimeoutSeconds must be",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCreateQueueInput(tc.input)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(substr) == 0 || (len(s) >= len(substr) && (s == substr || contains(s[1:], substr) || (len(s) > 0 && s[:len(substr)] == substr)))
}
