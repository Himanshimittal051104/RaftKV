package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileEnginePersistsAndDeletes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	e, err := NewFileEngine(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.Put([]byte("name"), []byte("Himanshi")); err != nil {
		t.Fatal(err)
	}

	if err := e.Put([]byte("city"), []byte("Delhi")); err != nil {
		t.Fatal(err)
	}

	if err := e.Delete([]byte("city")); err != nil {
		t.Fatal(err)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = NewFileEngine(path)
	if err != nil {
		t.Fatal(err)
	}

	defer e.Close()

	got, ok, err := e.Get([]byte("name"))
	if err != nil || !ok || string(got) != "Himanshi" {
		t.Fatalf("name: %q %v %v", got, ok, err)
	}

	_, ok, err = e.Get([]byte("city"))
	if err != nil || ok {
		t.Fatalf("city should be absent: %v %v", ok, err)
	}

}
func TestFileEngineTruncatesIncompleteFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	e, err := NewFileEngine(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.Put([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.WriteString(`{"op":"PUT","key":"Yg==","value":"dHdv"}`); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = NewFileEngine(path)
	if err != nil {
		t.Fatal(err)
	}

	defer e.Close()

	_, ok, err := e.Get([]byte("b"))
	if err != nil || ok {
		t.Fatalf("incomplete record should be ignored: %v %v", ok, err)
	}

}

func TestFileEngineRejectsMalformedCompleteRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	// A complete line containing invalid JSON.
	if err := os.WriteFile(path, []byte("{invalid-json}\n"), 0600); err != nil {
		t.Fatal(err)
	}

	engine, err := NewFileEngine(path)
	if engine != nil {
		_ = engine.Close()
	}

	if err == nil {
		t.Fatal("expected an error for malformed complete WAL record")
	}

	if !errors.Is(err, ErrInvalidWALRecord) {
		t.Fatalf("expected ErrInvalidWALRecord, got %v", err)
	}
}

func TestFileEngineWritesAfterRecovery(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")

	// First session: write data and close.
	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.Put([]byte("first"), []byte("one")); err != nil {
		t.Fatal(err)
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// Second session: recover existing data and write another record.
	engine, err = NewFileEngine(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	if err := engine.Put([]byte("second"), []byte("two")); err != nil {
		t.Fatal(err)
	}

	// Verify both the recovered value and the new value.
	value, found, err := engine.Get([]byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(value) != "one" {
		t.Fatalf("expected first=one, got %q (found=%v)", value, found)
	}

	value, found, err = engine.Get([]byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(value) != "two" {
		t.Fatalf("expected second=two, got %q (found=%v)", value, found)
	}
}

func TestFileEngineRecoversLatestValue(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.Put([]byte("name"), []byte("first")); err != nil {
		t.Fatal(err)
	}

	if err := engine.Put([]byte("name"), []byte("second")); err != nil {
		t.Fatal(err)
	}

	if err := engine.Put([]byte("name"), []byte("latest")); err != nil {
		t.Fatal(err)
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen the WAL and recover its records.
	engine, err = NewFileEngine(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	value, found, err := engine.Get([]byte("name"))
	if err != nil {
		t.Fatal(err)
	}

	if !found {
		t.Fatal("expected key 'name' to exist after recovery")
	}

	if string(value) != "latest" {
		t.Fatalf("expected latest value 'latest', got %q", value)
	}
}

func TestFileEngineRejectsOperationsAfterClose(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.log")

	engine, err := NewFileEngine(walPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// PUT must fail.
	err = engine.Put([]byte("key"), []byte("value"))
	if !errors.Is(err, ErrFileEngineClosed) {
		t.Fatalf("expected ErrFileEngineClosed from Put, got %v", err)
	}

	// GET must fail.
	_, _, err = engine.Get([]byte("key"))
	if !errors.Is(err, ErrFileEngineClosed) {
		t.Fatalf("expected ErrFileEngineClosed from Get, got %v", err)
	}

	// DELETE must fail.
	err = engine.Delete([]byte("key"))
	if !errors.Is(err, ErrFileEngineClosed) {
		t.Fatalf("expected ErrFileEngineClosed from Delete, got %v", err)
	}
}
