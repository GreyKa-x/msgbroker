package broker_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GreyKa-x/msgbroker/internal/broker"
	"github.com/GreyKa-x/msgbroker/internal/testutil"
)

func TestDeliveryWorkerConfiguration(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int
	}{{"", 256}, {"  ", 256}, {"1", 1}, {" 12 ", 12}} {
		got, err := broker.ParseDeliveryWorkers(tc.raw)
		if err != nil || got != tc.want {
			t.Fatalf("%q: workers=%d err=%v", tc.raw, got, err)
		}
	}
	for _, raw := range []string{"0", "-1", "many", "1.5", "999999999999999999999999"} {
		if _, err := broker.ParseDeliveryWorkers(raw); err == nil {
			t.Fatalf("accepted invalid worker count %q", raw)
		}
	}
}

// Empty query results let this test measure idle claim cadence without MySQL.
type idleConnector struct{ polls chan time.Time }
type idleConnection struct{ polls chan time.Time }
type idleRows struct{}

func (c idleConnector) Connect(context.Context) (driver.Conn, error) {
	return idleConnection{c.polls}, nil
}
func (c idleConnector) Driver() driver.Driver { return c }
func (c idleConnector) Open(string) (driver.Conn, error) {
	return c.Connect(context.Background())
}
func (idleConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}
func (idleConnection) Close() error { return nil }
func (idleConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction without messages")
}
func (c idleConnection) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "JOIN topics t ON t.id=m.topic_id WHERE w.available_at") {
		select {
		case c.polls <- time.Now():
		default:
		}
	}
	return idleRows{}, nil
}
func (idleConnection) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (idleRows) Columns() []string         { return nil }
func (idleRows) Close() error              { return nil }
func (idleRows) Next([]driver.Value) error { return io.EOF }

func TestIdlePollingStaysOncePerSecondWithManyWorkers(t *testing.T) {
	polls := make(chan time.Time, 128)
	db := sql.OpenDB(idleConnector{polls})
	defer db.Close()
	engine := broker.NewEngine(&broker.Store{DB: db})
	engine.Workers = 32
	run, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); engine.Run(run) }()
	defer func() { cancel(); <-done }()
	var first time.Time
	for i := 0; i < 2; i++ {
		select {
		case at := <-polls:
			if i == 0 {
				first = at
			} else if at.Sub(first) < 900*time.Millisecond {
				t.Fatalf("idle claims only %s apart with 32 workers", at.Sub(first))
			}
		case <-time.After(4 * time.Second):
			t.Fatal("dispatcher stopped polling")
		}
	}
}

func TestDispatcherOnlyLeasesForIdleWorkers(t *testing.T) {
	s := testutil.DB(t)
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	var calls, active, peak atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
		}
		started <- struct{}{}
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	defer receiver.Close()
	topic(t, s, receiver.URL)
	for i := 0; i < 5; i++ {
		send(t, s, broker.ID("pool_"), time.Now())
	}
	engine := broker.NewEngine(s)
	engine.Workers = 2
	run, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); engine.Run(run) }()
	defer func() { cancel(); <-done }()
	waitStarted := func() {
		t.Helper()
		select {
		case <-started:
		case <-time.After(6 * time.Second):
			t.Fatal("message was not dispatched")
		}
	}
	waitStarted()
	waitStarted()
	// Keep workers busy through another dispatch tick; pending messages must
	// retain attempts=0 and no lease instead of waiting in a leased queue.
	select {
	case <-started:
		t.Fatal("worker limit exceeded")
	case <-time.After(1200 * time.Millisecond):
	}
	var pending, leased int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM messages WHERE status='pending' AND attempts=0 AND lease_until IS NULL").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM messages WHERE lease_until IS NOT NULL").Scan(&leased); err != nil {
		t.Fatal(err)
	}
	if pending != 3 || leased != 2 {
		t.Fatalf("pending=%d leased=%d", pending, leased)
	}
	select {
	case release <- struct{}{}:
	case <-time.After(time.Second):
		t.Fatal("no active delivery")
	}
	waitStarted()
	close(release)
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		var completed int
		if err := s.DB.QueryRow("SELECT COUNT(*) FROM messages WHERE status='completed' AND result='success'").Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed == 5 {
			if calls.Load() != 5 || peak.Load() > 2 {
				t.Fatalf("calls=%d peak=%d", calls.Load(), peak.Load())
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("remaining messages did not complete")
}
