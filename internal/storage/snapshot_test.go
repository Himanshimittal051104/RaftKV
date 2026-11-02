package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotRestoresStateAfterRestart(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "node-0", "wal.log")

	// Create a new file engine.
	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine(): %v", err)
	}

	// Write two records to the WAL.
	if err := engine.Put(1, []byte("name"), []byte("RaftKV")); err != nil {
		t.Fatalf("Put(name): %v", err)
	}

	if err := engine.Put(2, []byte("version"), []byte("1")); err != nil {
		t.Fatalf("Put(version): %v", err)
	}

	// Persist the current state as a snapshot.
	if err := engine.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot(): %v", err)
	}

	// Close the original engine before reopening it.
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	// Simulate a restart by creating a new engine at the same WAL path.
	reopened, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine() after restart: %v", err)
	}
	defer reopened.Close()

	// Verify the first key was restored.
	gotName, found, err := reopened.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get(name): %v", err)
	}
	if !found {
		t.Fatal("key 'name' was not restored")
	}
	if !bytes.Equal(gotName, []byte("RaftKV")) {
		t.Fatalf("name = %q, want %q", gotName, "RaftKV")
	}

	// Verify the second key was restored.
	gotVersion, found, err := reopened.Get([]byte("version"))
	if err != nil {
		t.Fatalf("Get(version): %v", err)
	}
	if !found {
		t.Fatal("key 'version' was not restored")
	}
	if !bytes.Equal(gotVersion, []byte("1")) {
		t.Fatalf("version = %q, want %q", gotVersion, "1")
	}

	// Verify the snapshot's applied index was restored.
	lastApplied, err := reopened.LastApplied()
	if err != nil {
		t.Fatalf("LastApplied(): %v", err)
	}
	if lastApplied != 2 {
		t.Fatalf("LastApplied() = %d, want 2", lastApplied)
	}
}

func TestWALReplaysEntriesAfterSnapshot(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "node-0", "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}

	if err := engine.Put(1, []byte("name"), []byte("RaftKV")); err != nil {
		t.Fatalf("Put index 1: %v", err)
	}

	if err := engine.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	if err := engine.Put(2, []byte("version"), []byte("2")); err != nil {
		t.Fatalf("Put index 2: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer reopened.Close()

	tests := []struct {
		key   string
		value string
	}{
		{key: "name", value: "RaftKV"},
		{key: "version", value: "2"},
	}

	for _, tt := range tests {
		got, found, err := reopened.Get([]byte(tt.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", tt.key, err)
		}
		if !found {
			t.Fatalf("key %q not found after recovery", tt.key)
		}
		if string(got) != tt.value {
			t.Errorf("Get(%q) = %q, want %q", tt.key, got, tt.value)
		}
	}

	lastApplied, err := reopened.LastApplied()
	if err != nil {
		t.Fatalf("LastApplied: %v", err)
	}
	if lastApplied != 2 {
		t.Fatalf("LastApplied() = %d, want 2", lastApplied)
	}
}

func TestCompactWALPreservesEntriesAfterSnapshot(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "node-0", "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}

	if err := engine.Put(1, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put index 1: %v", err)
	}
	if err := engine.Put(2, []byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put index 2: %v", err)
	}
	if err := engine.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := engine.Put(3, []byte("c"), []byte("3")); err != nil {
		t.Fatalf("Put index 3: %v", err)
	}

	if err := engine.CompactWAL(); err != nil {
		t.Fatalf("CompactWAL: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen to verify that the snapshot plus compacted WAL
	// reconstructs the complete state.
	reopened, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer reopened.Close()

	for key, want := range map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	} {
		got, found, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !found {
			t.Fatalf("key %q not found after recovery", key)
		}
		if string(got) != want {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
	}

	lastApplied, err := reopened.LastApplied()
	if err != nil {
		t.Fatalf("LastApplied: %v", err)
	}
	if lastApplied != 3 {
		t.Fatalf("LastApplied() = %d, want 3", lastApplied)
	}
}

func TestCompactWALRemovesSnapshottedRecords(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "node-0", "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}
	defer engine.Close()

	if err := engine.Put(1, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(2, []byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := engine.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(3, []byte("c"), []byte("3")); err != nil {
		t.Fatal(err)
	}

	if err := engine.CompactWAL(); err != nil {
		t.Fatalf("CompactWAL: %v", err)
	}

	// Flush buffered writes before reading the WAL directly.
	if err := engine.writer.Flush(); err != nil {
		t.Fatalf("flush WAL: %v", err)
	}

	records, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("expected WAL to retain the record at index 3")
	}

	var foundIndex3 bool
	scanner := bufio.NewScanner(bytes.NewReader(records))
	for scanner.Scan() {
		var record struct {
			Index int `json:"index"`
		}

		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("invalid WAL record: %v", err)
		}

		if record.Index <= 2 {
			t.Errorf("WAL retained snapshotted record at index %d", record.Index)
		}
		if record.Index == 3 {
			foundIndex3 = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan WAL: %v", err)
	}

	if !foundIndex3 {
		t.Fatal("WAL does not contain the expected index 3 record")
	}
}

func TestRecoverTruncatedFinalWALRecord(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "node-0", "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}

	if err := engine.Put(1, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put index 1: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash during the next WAL write.
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	if _, err := f.WriteString(`{"index":2,"op":"SET","key":"b"`); err != nil {
		_ = f.Close()
		t.Fatalf("write truncated record: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close WAL file: %v", err)
	}

	// Reopening should recover index 1 and discard the incomplete line.
	reopened, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer reopened.Close()

	got, found, err := reopened.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if !found || string(got) != "1" {
		t.Fatalf("Get(a) = %q, found=%v; want %q, found=true", got, found, "1")
	}

	_, found, err = reopened.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	if found {
		t.Fatal("incomplete WAL record was incorrectly applied")
	}

	lastApplied, err := reopened.LastApplied()
	if err != nil {
		t.Fatalf("LastApplied: %v", err)
	}
	if lastApplied != 1 {
		t.Fatalf("LastApplied() = %d, want 1", lastApplied)
	}

	// Verify that recovery removed the incomplete bytes from disk.
	walBytes, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read recovered WAL: %v", err)
	}

	if bytes.Contains(walBytes, []byte(`"index":2`)) {
		t.Fatal("incomplete record remains in WAL after recovery")
	}
}

func TestRecoverWALBackupWhenActiveWALMissing(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	backupPath := walPath + ".bak"

	want := []byte("original WAL contents")
	if err := os.WriteFile(backupPath, want, 0600); err != nil {
		t.Fatalf("write backup: %v", err)
	}

	if err := recoverWALBackup(walPath); err != nil {
		t.Fatalf("recoverWALBackup: %v", err)
	}

	got, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read restored WAL: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("restored WAL = %q, want %q", got, want)
	}

	if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
		t.Fatalf("backup should have been renamed away; stat error = %v", err)
	}
}

func TestRecoverWALBackupWhenBothFilesExist(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	backupPath := walPath + ".bak"

	if err := os.WriteFile(walPath, []byte("active WAL"), 0600); err != nil {
		t.Fatalf("write active WAL: %v", err)
	}
	if err := os.WriteFile(backupPath, []byte("backup WAL"), 0600); err != nil {
		t.Fatalf("write backup WAL: %v", err)
	}

	if err := recoverWALBackup(walPath); err != nil {
		t.Fatalf("recoverWALBackup: %v", err)
	}

	got, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read active WAL: %v", err)
	}
	if string(got) != "active WAL" {
		t.Fatalf("active WAL overwritten: got %q", got)
	}

	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("backup should remain untouched: %v", err)
	}
}

func TestCompactWALPreservesDataAfterRestart(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}

	if err := engine.Put(1, []byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put alpha: %v", err)
	}
	if err := engine.Put(2, []byte("beta"), []byte("two")); err != nil {
		t.Fatalf("Put beta: %v", err)
	}

	if err := engine.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Compact the WAL using the existing implementation.
	if err := engine.CompactWAL(); err != nil {
		t.Fatalf("CompactWAL: %v", err)
	}

	// Verify existing data remains available after compaction.
	for key, want := range map[string]string{
		"alpha": "one",
		"beta":  "two",
	} {
		got, found, err := engine.Get([]byte(key))
		if err != nil || !found || string(got) != want {
			t.Fatalf("%s = %q, found=%v, err=%v; want %q",
				key, got, found, err, want)
		}
	}

	// The engine must accept a subsequent write.
	if err := engine.Put(3, []byte("gamma"), []byte("three")); err != nil {
		t.Fatalf("Put after compaction: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify that snapshot + WAL recovery preserves data.
	reopened, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer reopened.Close()

	for key, want := range map[string]string{
		"alpha": "one",
		"beta":  "two",
		"gamma": "three",
	} {
		got, found, err := reopened.Get([]byte(key))
		if err != nil || !found || string(got) != want {
			t.Fatalf("after restart: %s = %q, found=%v, err=%v; want %q",
				key, got, found, err, want)
		}
	}
}

func TestReplaceWALFailsWhenBackupCannotBeRemoved(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	backupPath := walPath + ".bak"

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}
	defer engine.Close()

	if err := engine.Put(1, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Create a non-empty directory at the backup path.
	if err := os.Mkdir(backupPath, 0700); err != nil {
		t.Fatalf("create backup directory: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(backupPath, "keep.txt"),
		[]byte("keep"),
		0600,
	); err != nil {
		t.Fatalf("create file in backup directory: %v", err)
	}

	tmpPath := filepath.Join(dir, "compacted.tmp")
	if err := os.WriteFile(tmpPath, []byte("compacted"), 0600); err != nil {
		t.Fatalf("create temporary WAL: %v", err)
	}

	err = engine.replaceWALAtomically(tmpPath)
	if err == nil {
		t.Fatal("expected replacement to fail when backup cannot be removed")
	}

	// The active WAL should remain on disk.
	if _, statErr := os.Stat(walPath); statErr != nil {
		t.Fatalf("original WAL should remain on disk: %v", statErr)
	}

	// The original data should still be readable from the in-memory state.
	got, found, getErr := engine.Get([]byte("key"))
	if getErr != nil || !found || string(got) != "value" {
		t.Fatalf("Get(key) = %q, found=%v, err=%v",
			got, found, getErr)
	}
}

func TestShouldSnapshotThreshold(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}
	defer engine.Close()

	// Fewer than 100 entries should not trigger a snapshot.
	for i := 1; i <= 99; i++ {
		if err := engine.Put(i, []byte("key"), []byte("value")); err != nil {
			t.Fatalf("Put at index %d: %v", i, err)
		}
	}

	if engine.ShouldSnapshot() {
		t.Fatal("ShouldSnapshot() = true after 99 entries; want false")
	}

	// The 100th entry should trigger a snapshot.
	if err := engine.Put(100, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put at index 100: %v", err)
	}

	if !engine.ShouldSnapshot() {
		t.Fatal("ShouldSnapshot() = false after 100 entries; want true")
	}
}

func TestMaybeSaveSnapshotSkipsBelowThreshold(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}
	defer engine.Close()

	for i := 1; i <= 99; i++ {
		if err := engine.Put(i, []byte("key"), []byte("value")); err != nil {
			t.Fatalf("Put at index %d: %v", i, err)
		}
	}

	engine.MaybeSaveSnapshot()

	if _, err := os.Stat(engine.snapshotPath()); !os.IsNotExist(err) {
		t.Fatalf("snapshot should not exist below threshold; stat error = %v", err)
	}
}

func TestMaybeSaveSnapshotAtThreshold(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}

	for i := 1; i <= 100; i++ {
		if err := engine.Put(i, []byte("key"), []byte("value")); err != nil {
			t.Fatalf("Put at index %d: %v", i, err)
		}
	}

	engine.MaybeSaveSnapshot()

	snapshot, err := engine.ReadSnapshot()
	if err != nil {
		_ = engine.Close()
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if snapshot == nil {
		_ = engine.Close()
		t.Fatal("expected snapshot at threshold")
	}
	if snapshot.LastApplied != 100 {
		_ = engine.Close()
		t.Fatalf("snapshot LastApplied = %d, want 100", snapshot.LastApplied)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMaybeSaveSnapshotHandlesSaveFailure(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}
	defer engine.Close()

	// Make the snapshot destination a directory so replacing it
	// with the snapshot file should fail.
	snapshotPath := engine.snapshotPath()
	if err := os.Mkdir(snapshotPath, 0700); err != nil {
		t.Fatalf("create snapshot directory: %v", err)
	}

	for i := 1; i <= 100; i++ {
		if err := engine.Put(i, []byte("key"), []byte("value")); err != nil {
			t.Fatalf("Put at index %d: %v", i, err)
		}
	}

	// Snapshot creation should fail gracefully rather than panic.
	engine.MaybeSaveSnapshot()

	// The already-applied data should remain available.
	got, found, err := engine.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get after snapshot failure: %v", err)
	}
	if !found || string(got) != "value" {
		t.Fatalf("Get = %q, found=%v; want value", got, found)
	}

	// The snapshot destination should still be a directory,
	// confirming that a snapshot file was not installed.
	info, err := os.Stat(snapshotPath)
	if err != nil {
		t.Fatalf("stat snapshot destination: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected snapshot destination to remain a directory")
	}
}

func TestCompactWALFailsOnIncompleteRecord(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatalf("NewFileEngine: %v", err)
	}
	defer engine.Close()

	for i := 1; i <= 100; i++ {
		if err := engine.Put(i, []byte("key"), []byte("value")); err != nil {
			t.Fatalf("Put at index %d: %v", i, err)
		}
	}

	// Save a snapshot so compaction has records to remove.
	if err := engine.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Append an incomplete WAL record.
	wal, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	if _, err := wal.Write([]byte(`{"index":101,"op":"SET"`)); err != nil {
		_ = wal.Close()
		t.Fatalf("write incomplete record: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	// Explicitly verify that compaction fails.
	err = engine.CompactWAL()
	if err == nil {
		t.Fatal("CompactWAL() error = nil; want error for incomplete WAL record")
	}
}
