package client

import "testing"

func TestChooseChunkSize(t *testing.T) {
	tests := []struct {
		size    int64
		wantMin int
	}{
		{size: 0, wantMin: 1},
		{size: 512 * 1024, wantMin: 512 * 1024},
		{size: 2 * 1024 * 1024, wantMin: 1024 * 1024},
		{size: 50 * 1024 * 1024, wantMin: 1 * 1024 * 1024},
		{size: 200 * 1024 * 1024, wantMin: 4 * 1024 * 1024},
	}

	for _, tt := range tests {
		got := chooseChunkSize(tt.size)
		if got < tt.wantMin {
			t.Fatalf("chooseChunkSize(%d) = %d; want >= %d", tt.size, got, tt.wantMin)
		}
	}
}

func TestChooseReplicationFactor(t *testing.T) {
	if want := 1; chooseReplicationFactor(1, 1) != want {
		t.Fatalf("expected %d", want)
	}
	if got := chooseReplicationFactor(5*1024*1024, 5); got < 1 || got > 3 {
		t.Fatalf("unexpected replication factor: %d", got)
	}
}
