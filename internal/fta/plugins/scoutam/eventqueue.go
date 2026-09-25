// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import (
	"sync"
	"time"
)

// stageEventQueue is an unbounded FIFO queue for stageEvents. NATS notifications for early
// batches can arrive while Setup is still walking a large directory and submitting later batches,
// so pushes must never block (and never drop a message) waiting for a reader.
type stageEventQueue struct {
	mu     sync.Mutex
	events []stageEvent
	notify chan struct{}
}

func newStageEventQueue() *stageEventQueue {
	return &stageEventQueue{notify: make(chan struct{}, 1)}
}

// push enqueues an event. Never blocks.
func (q *stageEventQueue) push(evt stageEvent) {
	q.mu.Lock()
	q.events = append(q.events, evt)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// pop returns the next queued event, waiting until one is available or timeout fires.
func (q *stageEventQueue) pop(timeout <-chan time.Time) (evt stageEvent, ok bool) {
	for {
		q.mu.Lock()
		if len(q.events) > 0 {
			evt = q.events[0]
			q.events = q.events[1:]
			q.mu.Unlock()
			return evt, true
		}
		q.mu.Unlock()

		select {
		case <-q.notify:
		case <-timeout:
			return stageEvent{}, false
		}
	}
}
