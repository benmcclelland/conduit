// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import (
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// copyItem is one entry to copy, relative to its copyGroup's srcRoot ("" for non-directory sources).
type copyItem struct {
	group int
	rel   string
	size  int64
	isDir bool
}

// stagePipeline tracks files from the moment they're requested for staging until they're ready to
// copy. The directory walker adds items, the NATS callback moves them from pending to ready, and
// the copier drains ready items. Directory entries are held back until every file has been copied
// so their mtimes aren't disturbed by files landing in them afterwards.
type stagePipeline struct {
	mu        sync.Mutex
	notify    chan struct{}
	pending   map[string]copyItem // keyed by path relative to its ScoutFS mount, as ScoutAM reports it
	mounts    []string
	ready     []copyItem
	dirs      []copyItem
	walkDone  bool
	walkErr   error
	stageErrs []string
	requested int
	staged    int
	// lastProgress is when a pending file last staged (or the pending set last became non-empty)
	lastProgress time.Time
}

func newStagePipeline() *stagePipeline {
	return &stagePipeline{
		notify:       make(chan struct{}, 1),
		pending:      make(map[string]copyItem),
		lastProgress: time.Now(),
	}
}

func (sp *stagePipeline) wake() {
	select {
	case sp.notify <- struct{}{}:
	default:
	}
}

// addPending must be called before the stage request for apiPath is submitted so a fast
// notification always finds its item. mount is the ScoutFS mount apiPath lives under.
func (sp *stagePipeline) addPending(mount string, apiPath string, item copyItem) error {
	key, err := filepath.Rel(mount, apiPath)
	if err != nil || !pathIsWithin(mount, apiPath) {
		return fmt.Errorf("path %q is outside ScoutFS mount %q", apiPath, mount)
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()
	if !slices.Contains(sp.mounts, mount) {
		sp.mounts = append(sp.mounts, mount)
	}
	if len(sp.pending) == 0 {
		sp.lastProgress = time.Now()
	}
	sp.pending[key] = item
	sp.requested++
	return nil
}

// eventKey maps a notification filename to its pending key. ScoutAM reports paths relative to
// the filesystem mount; absolute paths are accepted too. Must be called with sp.mu held.
func (sp *stagePipeline) eventKey(filename string) string {
	name := filepath.Clean(filename)
	if !filepath.IsAbs(name) {
		return name
	}
	for _, mount := range sp.mounts {
		if pathIsWithin(mount, name) {
			if rel, err := filepath.Rel(mount, name); err == nil {
				return rel
			}
		}
	}
	return name
}

// addReady queues an item that doesn't need staging (directories, symlinks, non-ScoutFS sources).
func (sp *stagePipeline) addReady(item copyItem) {
	sp.mu.Lock()
	if item.isDir {
		sp.dirs = append(sp.dirs, item)
	} else {
		sp.ready = append(sp.ready, item)
	}
	sp.mu.Unlock()
	sp.wake()
}

// onStageEvent returns false if evt isn't for a file we're waiting on.
func (sp *stagePipeline) onStageEvent(evt stageEvent) bool {
	sp.mu.Lock()
	key := sp.eventKey(evt.Filename)
	item, ok := sp.pending[key]
	if !ok {
		sp.mu.Unlock()
		return false
	}
	delete(sp.pending, key)
	sp.staged++
	sp.lastProgress = time.Now()
	if evt.Error != "" {
		sp.stageErrs = append(sp.stageErrs, fmt.Sprintf("%s: %s", evt.Filename, evt.Error))
	} else {
		sp.ready = append(sp.ready, item)
	}
	sp.mu.Unlock()
	sp.wake()
	return true
}

func (sp *stagePipeline) finishWalk(err error) {
	sp.mu.Lock()
	sp.walkDone = true
	sp.walkErr = err
	sp.mu.Unlock()
	sp.wake()
}

// next blocks until up to max items are ready to copy. done is true once everything has been
// handed out or the walk failed. err is set if files are pending staging but none has staged
// within idleTimeout.
func (sp *stagePipeline) next(max int, idleTimeout time.Duration) (batch []copyItem, done bool, err error) {
	for {
		sp.mu.Lock()
		if sp.walkErr != nil {
			sp.mu.Unlock()
			return nil, true, nil
		}
		if len(sp.ready) > 0 {
			n := min(max, len(sp.ready))
			batch = append([]copyItem(nil), sp.ready[:n]...)
			sp.ready = sp.ready[n:]
			sp.mu.Unlock()
			return batch, false, nil
		}
		if sp.walkDone && len(sp.pending) == 0 {
			if len(sp.dirs) > 0 {
				sp.ready, sp.dirs = sp.dirs, nil
				sp.mu.Unlock()
				continue
			}
			sp.mu.Unlock()
			return nil, true, nil
		}

		pending := len(sp.pending)
		wait := time.Until(sp.lastProgress.Add(idleTimeout))
		sp.mu.Unlock()

		if pending == 0 {
			// still walking with nothing outstanding - nothing to time out on
			<-sp.notify
			continue
		}
		if wait <= 0 {
			return nil, false, fmt.Errorf("no stage notification received in %v (%d file(s) still pending)", idleTimeout, pending)
		}
		timer := time.NewTimer(wait)
		select {
		case <-sp.notify:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (sp *stagePipeline) status() (requested, staged int, stageErrs []string, walkErr error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.requested, sp.staged, append([]string(nil), sp.stageErrs...), sp.walkErr
}
