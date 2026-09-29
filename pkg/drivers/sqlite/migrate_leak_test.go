package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	"github.com/k3s-io/kine/pkg/drivers"
)

// TestStartupLeavesNoOpenReader checks that opening the datastore does not
// leave a read transaction open: one would stop every WAL checkpoint from
// writing frames back, so the WAL would grow for as long as kine runs.
func TestStartupLeavesNoOpenReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	dbPath := filepath.Join(t.TempDir(), "state.db")
	_, dialect, err := NewVariant(ctx, &wg, "sqlite", &drivers.Config{DataSourceName: dbPath + "?" + DefaultParams})
	if err != nil {
		t.Fatalf("failed to open datastore: %v", err)
	}
	for i := range 10 {
		if _, err := dialect.DB.Exec(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value) VALUES (?, 1, 0, 0, 0, 0, randomblob(4096), NULL)`, i); err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	connector, err := newConnector("sqlite", dbPath+"?_busy_timeout=0")
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	var busy, logFrames, checkpointed int
	if err := db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}
	if busy != 0 {
		t.Fatalf("WAL checkpoint is blocked by an open reader: checkpointed %d of %d frames", checkpointed, logFrames)
	}
}
