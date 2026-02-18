package storage

import (
	"bytes"
	"os"
	"testing"
)

func TestWALReplayErrorsOnCorruptLine(t *testing.T) {
	tmp, err := os.CreateTemp("", "driftq-wal-corrupt-*.wal")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	path := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(path)

	// Write one good line then a corrupted line.
	w, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}

	if err := w.Append(Entry{Type: RecordTypeMessage, Topic: "t1", Partition: 0, Offset: 0, Value: []byte("ok")}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := w.f.Write([]byte("{not json}\n")); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	_ = w.Close()

	w2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL(reopen): %v", err)
	}

	defer w2.Close()

	_, err = w2.Replay()
	if err == nil {
		t.Fatalf("expected Replay to error on corrupt line")
	}
}

func TestWALReplaySupportsLargeLines(t *testing.T) {
	tmp, err := os.CreateTemp("", "driftq-wal-large-*.wal")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}

	path := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(path)

	w, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}
	defer w.Close()

	big := bytes.Repeat([]byte("a"), 200*1024) // >64KB, ensures scanner buffer needs to be larger than default
	if err := w.Append(Entry{Type: RecordTypeMessage, Topic: "t1", Partition: 0, Offset: 0, Value: big}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if got := len(entries[0].Value); got != len(big) {
		t.Fatalf("expected value len=%d, got %d", len(big), got)
	}
}
