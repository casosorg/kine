package sqllog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

type serialInsertDialect struct {
	server.Dialect
	next         atomic.Int64
	active       atomic.Int32
	firstEntered chan struct{}
	concurrent   chan struct{}
	releaseFirst chan struct{}
}

func (d *serialInsertDialect) CurrentRevision(context.Context) (int64, error) {
	return 0, nil
}

func (d *serialInsertDialect) Insert(context.Context, string, bool, bool, int64, int64, int64, []byte) (int64, error) {
	if !d.active.CompareAndSwap(0, 1) {
		select {
		case d.concurrent <- struct{}{}:
		default:
		}
		return 0, errors.New("concurrent insert")
	}
	defer d.active.Store(0)

	id := d.next.Add(1)
	if id == 1 {
		close(d.firstEntered)
		<-d.releaseFirst
	}
	return id, nil
}

func TestAppendSerializesConcurrentInserts(t *testing.T) {
	dialect := &serialInsertDialect{
		firstEntered: make(chan struct{}),
		concurrent:   make(chan struct{}, 1),
		releaseFirst: make(chan struct{}),
	}
	log := New(dialect, 0, 0, 0, 0, 100, 100)
	start := make(chan struct{})
	ready := make(chan struct{}, 2)
	results := make(chan error, 2)
	appendEvent := func(key string) {
		ready <- struct{}{}
		<-start
		_, err := log.Append(context.Background(), &server.Event{
			Create: true,
			KV:     &server.KeyValue{Key: key},
		})
		results <- err
	}
	go appendEvent("first")
	go appendEvent("second")
	<-ready
	<-ready
	close(start)
	<-dialect.firstEntered

	select {
	case <-dialect.concurrent:
		t.Fatal("second append entered the dialect before the first append completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(dialect.releaseFirst)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if revision := dialect.next.Load(); revision != 2 {
		t.Fatalf("inserted revisions = %d, want 2", revision)
	}
	revision, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if revision != 2 {
		t.Fatalf("current revision = %d after revision 2 committed", revision)
	}
}
