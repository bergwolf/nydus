package committer

import (
	"sync"
	"testing"
)

func TestCounterWrite(t *testing.T) {
	tests := []struct {
		name     string
		inputs   [][]byte
		wantSize int64
	}{
		{
			name:     "empty write",
			inputs:   [][]byte{{}},
			wantSize: 0,
		},
		{
			name:     "single write",
			inputs:   [][]byte{[]byte("hello")},
			wantSize: 5,
		},
		{
			name:     "multiple writes",
			inputs:   [][]byte{[]byte("hello"), []byte(" "), []byte("world")},
			wantSize: 11,
		},
		{
			name:     "no writes",
			inputs:   nil,
			wantSize: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Counter{}
			for _, input := range tt.inputs {
				n, err := c.Write(input)
				if err != nil {
					t.Fatalf("Write() error = %v", err)
				}
				if n != len(input) {
					t.Fatalf("Write() returned %d, want %d", n, len(input))
				}
			}
			if got := c.Size(); got != tt.wantSize {
				t.Errorf("Size() = %d, want %d", got, tt.wantSize)
			}
		})
	}
}

func TestCounterConcurrentWrites(t *testing.T) {
	c := &Counter{}
	var wg sync.WaitGroup
	numGoroutines := 100
	bytesPerWrite := []byte("abcde") // 5 bytes

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			c.Write(bytesPerWrite)
		}()
	}
	wg.Wait()

	expected := int64(numGoroutines * len(bytesPerWrite))
	if got := c.Size(); got != expected {
		t.Errorf("after %d concurrent writes: Size() = %d, want %d", numGoroutines, got, expected)
	}
}

func TestCounterImplementsWriter(t *testing.T) {
	// Verify Counter satisfies io.Writer
	var _ interface {
		Write(p []byte) (n int, err error)
	} = &Counter{}
}

func TestCounterSizeInitiallyZero(t *testing.T) {
	c := &Counter{}
	if got := c.Size(); got != 0 {
		t.Errorf("new Counter.Size() = %d, want 0", got)
	}
}
