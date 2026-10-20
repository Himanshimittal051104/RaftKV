package storage

import (
 "os"
 "path/filepath"
 "testing"
)
func TestFileEnginePersistsAndDeletes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	e, err := NewFileEngine(path); 
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

	e, err = NewFileEngine(path); if err != nil {
		t.Fatal(err) 
	}; 

	defer e.Close()

	got, ok, err := e.Get([]byte("name")); 
	if err != nil || !ok || string(got) != "Himanshi" {
		t.Fatalf("name: %q %v %v", got, ok, err) 
	}

	_, ok, err = e.Get([]byte("city")); 
	if err != nil || ok {
		t.Fatalf("city should be absent: %v %v", ok, err) 
	}

}
func TestFileEngineTruncatesIncompleteFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	e, err := NewFileEngine(path); 
	if err != nil {
		t.Fatal(err) 
	}

	if err := e.Put([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err) 
	}

	if err := e.Close(); err != nil {
		t.Fatal(err) 
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600); 
	if err != nil {
		t.Fatal(err) 
	}

	if _, err := f.WriteString(`{"op":"PUT","key":"Yg==","value":"dHdv"}`); err != nil {
		t.Fatal(err) 
	}

	if err := f.Close(); err != nil {
		t.Fatal(err) 
	}

	e, err = NewFileEngine(path); 
	if err != nil {
		t.Fatal(err) 
	}; 
	
	defer e.Close()

	_, ok, err := e.Get([]byte("b")); 
	if err != nil || ok {
		t.Fatalf("incomplete record should be ignored: %v %v", ok, err) 
	}

}
