package generic

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/k3s-io/kine/pkg/query"
)

const prepareLifecycleDriverName = "kine-prepare-lifecycle"

func init() {
	sql.Register(prepareLifecycleDriverName, prepareLifecycleDriver{})
}

type prepareLifecycleDriver struct{}

func (prepareLifecycleDriver) Open(string) (driver.Conn, error) {
	return prepareLifecycleConn{}, nil
}

type prepareLifecycleConn struct{}

func (prepareLifecycleConn) Prepare(string) (driver.Stmt, error) {
	return prepareLifecycleStmt{}, nil
}

func (prepareLifecycleConn) Close() error {
	return nil
}

func (prepareLifecycleConn) Begin() (driver.Tx, error) {
	return prepareLifecycleTx{}, nil
}

type prepareLifecycleTx struct{}

func (prepareLifecycleTx) Commit() error {
	return nil
}

func (prepareLifecycleTx) Rollback() error {
	return nil
}

type prepareLifecycleStmt struct{}

func (prepareLifecycleStmt) Close() error {
	return nil
}

func (prepareLifecycleStmt) NumInput() int {
	return 0
}

func (prepareLifecycleStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}

func (prepareLifecycleStmt) Query([]driver.Value) (driver.Rows, error) {
	return prepareLifecycleRows{}, nil
}

type prepareLifecycleRows struct{}

func (prepareLifecycleRows) Columns() []string {
	return []string{}
}

func (prepareLifecycleRows) Close() error {
	return nil
}

func (prepareLifecycleRows) Next([]driver.Value) error {
	return io.EOF
}

func TestPrepareReleasesDatabaseConnection(t *testing.T) {
	db, err := sql.Open(prepareLifecycleDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	dialect := &Generic{DB: db}
	stmt, err := dialect.prepare(context.Background(), query.New("SELECT 1", "?", false, "prepare-lifecycle"))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if inUse := db.Stats().InUse; inUse != 0 {
		t.Fatalf("prepared statement leaked %d database connections", inUse)
	}
}
