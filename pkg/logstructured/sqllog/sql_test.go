package sqllog

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
)

type orderedInsertDialect struct {
	server.Dialect
	next          atomic.Int64
	entered       chan int64
	releaseFirst  chan struct{}
	releaseSecond chan struct{}
}

func (d *orderedInsertDialect) Insert(context.Context, string, bool, bool, int64, int64, int64, []byte) (int64, error) {
	id := d.next.Add(1)
	d.entered <- id
	if id == 1 {
		<-d.releaseFirst
	} else {
		<-d.releaseSecond
	}
	return id, nil
}

func (d *orderedInsertDialect) CurrentRevision(context.Context) (int64, error) {
	return 0, nil
}

func TestAppendKeepsCurrentRevisionMonotonic(t *testing.T) {
	dialect := &orderedInsertDialect{
		entered:       make(chan int64, 2),
		releaseFirst:  make(chan struct{}),
		releaseSecond: make(chan struct{}),
	}
	log := New(dialect, 0, 0, 0, 0, 100, 100)
	results := make(chan int64, 2)

	appendEvent := func(key string) {
		revision, err := log.Append(context.Background(), &server.Event{
			Create: true,
			KV:     &server.KeyValue{Key: key},
		})
		if err != nil {
			t.Errorf("Append(%s): %v", key, err)
		}
		results <- revision
	}
	go appendEvent("first")
	go appendEvent("second")

	<-dialect.entered
	<-dialect.entered
	close(dialect.releaseFirst)
	if revision := <-results; revision != 1 {
		t.Fatalf("first completed revision = %d, want 1", revision)
	}
	close(dialect.releaseSecond)
	if revision := <-results; revision != 2 {
		t.Fatalf("second completed revision = %d, want 2", revision)
	}

	revision, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if revision != 2 {
		t.Fatalf("current revision = %d after revision 2 committed", revision)
	}
}
