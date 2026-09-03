package message

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMessageAck_Idempotent(t *testing.T) {
	var calls atomic.Int32
	var gotErrs []error
	m := New([]byte("x"), nil)
	m.SetAckFn(func(err error) {
		calls.Add(1)
		gotErrs = append(gotErrs, err)
	})

	m.Ack(nil)
	m.Ack(errors.New("late error"))
	m.Ack(nil)

	if calls.Load() != 1 {
		t.Fatalf("ackFn calls = %d, want 1", calls.Load())
	}
	if gotErrs[0] != nil {
		t.Fatalf("first ack error = %v, want nil", gotErrs[0])
	}
}

func TestMessageAck_FirstErrorWins(t *testing.T) {
	ackErr := errors.New("eval failed")
	var calls atomic.Int32
	var gotErr error
	m := New([]byte("x"), nil)
	m.SetAckFn(func(err error) {
		calls.Add(1)
		gotErr = err
	})

	m.Ack(ackErr)
	m.Ack(nil)

	if calls.Load() != 1 {
		t.Fatalf("ackFn calls = %d, want 1", calls.Load())
	}
	if !errors.Is(gotErr, ackErr) {
		t.Fatalf("ack error = %v, want %v", gotErr, ackErr)
	}
}

func TestMessageAck_ConcurrentCallsOnce(t *testing.T) {
	var calls atomic.Int32
	m := New([]byte("x"), nil)
	m.SetAckFn(func(error) {
		calls.Add(1)
	})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Ack(nil)
		}()
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("ackFn calls = %d, want 1", calls.Load())
	}
}

func TestMessageShallowCopy_AckStateIndependent(t *testing.T) {
	var parentCalls, childCalls atomic.Int32
	m := New([]byte("x"), nil)
	m.SetAckFn(func(error) { parentCalls.Add(1) })
	m.Ack(nil)

	cp := m.ShallowCopy()
	cp.SetAckFn(func(error) { childCalls.Add(1) })
	cp.Ack(nil)

	if childCalls.Load() != 1 {
		t.Fatalf("copy ackFn calls = %d, want 1 (copy must have fresh ack state)", childCalls.Load())
	}
	if parentCalls.Load() != 1 {
		t.Fatalf("parent ackFn calls = %d, want 1", parentCalls.Load())
	}
}
