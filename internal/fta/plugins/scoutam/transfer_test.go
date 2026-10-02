// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	proto "github.com/lanl/conduit/api"
)

func TestSourceDestination(t *testing.T) {
	existingDir := t.TempDir()
	missing := filepath.Join(existingDir, "missing")

	tests := []struct {
		name     string
		destRoot string
		destInfo proto.DestInfo
		multiple bool
		want     string
	}{
		{name: "existing dir", destRoot: "/dst", destInfo: proto.DestInfo_DEST_IS_DIR, want: "/dst/src"},
		{name: "dest not exist", destRoot: "/dst", destInfo: proto.DestInfo_DEST_NOT_EXIST, want: "/dst"},
		{name: "dest not dir", destRoot: "/dst", destInfo: proto.DestInfo_DEST_NOT_DIR, want: "/dst"},
		{name: "unvalidated existing dir", destRoot: existingDir, destInfo: proto.DestInfo_DEST_NONE, want: filepath.Join(existingDir, "src")},
		{name: "unvalidated missing single", destRoot: missing, destInfo: proto.DestInfo_DEST_NONE, want: missing},
		{name: "unvalidated missing multiple", destRoot: missing, destInfo: proto.DestInfo_DEST_NONE, multiple: true, want: filepath.Join(missing, "src")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := sourceDestination(test.destRoot, "/user/path/src", test.destInfo, test.multiple)
			if got != test.want {
				t.Errorf("sourceDestination() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestStagePipeline(t *testing.T) {
	pl := newStagePipeline()
	pl.addReady(copyItem{rel: "dir", isDir: true})
	pl.addReady(copyItem{rel: "dir/link"})
	mustAddPending(t, pl, "/fs/dir/a", "dir/a")
	mustAddPending(t, pl, "/fs/dir/b", "dir/b")
	mustAddPending(t, pl, "/fs/dir/c", "dir/c")
	pl.finishWalk(nil)

	expectBatch(t, pl, "dir/link")

	if pl.onStageEvent(stageEvent{Filename: "other"}) {
		t.Fatal("onStageEvent() accepted an unrequested file")
	}
	// ScoutAM reports names relative to the filesystem mount
	pl.onStageEvent(stageEvent{Filename: "dir/a"})
	expectBatch(t, pl, "dir/a")

	pl.onStageEvent(stageEvent{Filename: "/fs/dir/c"})
	expectBatch(t, pl, "dir/c")

	pl.onStageEvent(stageEvent{Filename: "dir/b", Error: "media error"})
	// a duplicate notification must not be double counted
	pl.onStageEvent(stageEvent{Filename: "dir/b"})

	// directories are only handed out once nothing is pending
	expectBatch(t, pl, "dir")
	if _, done, err := pl.next(10, time.Second); !done || err != nil {
		t.Fatalf("next() done = %v, err = %v, want done", done, err)
	}

	requested, staged, stageErrs, _ := pl.status()
	if requested != 3 || staged != 3 || len(stageErrs) != 1 {
		t.Errorf("status() = %d requested, %d staged, %v errors", requested, staged, stageErrs)
	}
}

func TestStagePipelineIdleTimeout(t *testing.T) {
	pl := newStagePipeline()
	mustAddPending(t, pl, "/fs/a", "a")
	pl.finishWalk(nil)

	if _, _, err := pl.next(10, 50*time.Millisecond); err == nil {
		t.Fatal("next() error = nil, want timeout")
	}
}

func TestStagePipelineWaitsForWalk(t *testing.T) {
	pl := newStagePipeline()
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = pl.addPending("/fs", "/fs/a", copyItem{rel: "a"})
		pl.onStageEvent(stageEvent{Filename: "a"})
		pl.finishWalk(nil)
	}()

	expectBatch(t, pl, "a")
}

func TestStagePipelineRejectsOutsideMount(t *testing.T) {
	if err := newStagePipeline().addPending("/fs", "/other/a", copyItem{}); err == nil {
		t.Fatal("addPending() error = nil, want error")
	}
}

func mustAddPending(t *testing.T, pl *stagePipeline, apiPath string, rel string) {
	t.Helper()
	if err := pl.addPending("/fs", apiPath, copyItem{rel: rel}); err != nil {
		t.Fatal(err)
	}
}

func expectBatch(t *testing.T, pl *stagePipeline, wantRels ...string) {
	t.Helper()
	batch, done, err := pl.next(10, time.Second)
	if err != nil || done {
		t.Fatalf("next() done = %v, err = %v", done, err)
	}
	if len(batch) != len(wantRels) {
		t.Fatalf("next() = %+v, want %v", batch, wantRels)
	}
	for i, item := range batch {
		if item.rel != wantRels[i] {
			t.Errorf("next()[%d].rel = %q, want %q", i, item.rel, wantRels[i])
		}
	}
}

// TestRsyncItems checks the rsync invocations copy only what's listed and then fix up the root.
func TestRsyncItems(t *testing.T) {
	rsyncPath, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync not installed")
	}

	src := filepath.Join(t.TempDir(), "src")
	dst := filepath.Join(t.TempDir(), "dst")
	writeFile(t, filepath.Join(src, "a", "one"))
	writeFile(t, filepath.Join(src, "a", "two"))
	if err := os.MkdirAll(filepath.Join(src, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(src, 0o700); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	g := copyGroup{srcRoot: src, destRoot: dst, isDir: true}

	if err := rsyncItems(ctx, rsyncPath, g, []copyItem{{rel: "a/one"}}); err != nil {
		t.Fatal(err)
	}
	assertExists(t, filepath.Join(dst, "a", "one"), true)
	assertExists(t, filepath.Join(dst, "a", "two"), false)

	if err := rsyncItems(ctx, rsyncPath, g, []copyItem{{rel: "a", isDir: true}, {rel: "empty", isDir: true}}); err != nil {
		t.Fatal(err)
	}
	assertExists(t, filepath.Join(dst, "empty"), true)
	assertExists(t, filepath.Join(dst, "a", "two"), false)

	if err := syncRootDir(ctx, rsyncPath, g); err != nil {
		t.Fatal(err)
	}
	assertExists(t, filepath.Join(dst, "a", "two"), false)
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("dest root mode = %v, want 0700", info.Mode().Perm())
	}

	fileDst := filepath.Join(t.TempDir(), "copy")
	if err := rsyncItems(ctx, rsyncPath, copyGroup{srcRoot: filepath.Join(src, "a", "two"), destRoot: fileDst}, []copyItem{{}}); err != nil {
		t.Fatal(err)
	}
	assertExists(t, fileDst, true)
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(path), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertExists(t *testing.T, path string, want bool) {
	t.Helper()
	_, err := os.Lstat(path)
	if got := err == nil; got != want {
		t.Errorf("%s exists = %v, want %v", path, got, want)
	}
}
