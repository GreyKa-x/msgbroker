package broker_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GreyKa-x/msgbroker/internal/broker"
	"github.com/GreyKa-x/msgbroker/internal/testutil"
)

func forceDue(t *testing.T, s *broker.Store, id string) {
	t.Helper()
	if _, err := s.DB.Exec("UPDATE message_waits SET available_at=UTC_TIMESTAMP(6) WHERE message_id=?", id); err != nil {
		t.Fatal(err)
	}
}
func TestSixteenAttemptsAndManualRedelivery(t *testing.T) {
	s := testutil.DB(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(400) }))
	defer server.Close()
	tp, err := s.SaveTopic(ctx, broker.Topic{Environment: "test", Name: "test", URL: server.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if tp.MaxAttempts != 16 {
		t.Fatal(tp.MaxAttempts)
	}
	m := send(t, s, "exhaust", time.Now())
	e := broker.NewEngine(s)
	var total time.Duration
	for attempt := 1; attempt <= 16; attempt++ {
		d, err := e.Claim(ctx, m.ID)
		if err != nil || d == nil {
			t.Fatalf("claim %d: %v", attempt, err)
		}
		before := time.Now()
		e.Deliver(ctx, d)
		m, err = s.Message(ctx, "test", m.ID)
		if err != nil {
			t.Fatal(err)
		}
		if m.Attempts != attempt {
			t.Fatal(m)
		}
		dup := send(t, s, "exhaust", time.Now().Add(time.Hour))
		if dup.ID != m.ID || !dup.RunAt.Equal(m.RunAt) {
			t.Fatal("duplicate replaced started message")
		}
		if attempt < 16 {
			delay := 5 * time.Second * time.Duration(1<<uint(attempt-1))
			total += delay
			if broker.RetryDelay(int64(attempt)) != delay {
				t.Fatal("backoff")
			}
			if m.Status != "delivering" || m.LeaseUntil != nil || m.AvailableAt == nil || m.AvailableAt.Before(before.Add(delay)) || m.AvailableAt.After(time.Now().Add(delay)) {
				t.Fatalf("attempt %d: %+v", attempt, m)
			}
			if d, err = e.Claim(ctx, m.ID); err != nil || d != nil {
				t.Fatal("retry claimed too early", err)
			}
			forceDue(t, s, m.ID)
		}
	}
	if total != 45*time.Hour+30*time.Minute+35*time.Second {
		t.Fatal(total)
	}
	if m.Status != "completed" || m.Result != "failed" || m.AvailableAt != nil || m.FinishedAt == nil {
		t.Fatal(m)
	}
	failed, err := s.Messages(ctx, "test", broker.MessageFilter{OnlyTimed: true, Status: "completed", Result: "failed"})
	if err != nil || len(failed) != 1 {
		t.Fatal(failed, err)
	}
	next, err := s.Redeliver(ctx, "test", m.ID)
	if err != nil || next.ID == m.ID || next.Status != "pending" || next.Attempts != 0 {
		t.Fatal(next, err)
	}
	again, err := s.Redeliver(ctx, "test", m.ID)
	if err != nil || again.ID != next.ID {
		t.Fatal("redelivery duplicate", err)
	}
	old, err := s.Message(ctx, "test", m.ID)
	if err != nil || old.Attempts != 16 || old.Result != "failed" || old.RedeliveredAs != next.ID {
		t.Fatal(old, err)
	}
	dup := send(t, s, "exhaust", time.Now())
	if dup.ID != m.ID {
		t.Fatal("lost original idempotency key")
	}
}
func TestIndependentMessagesAndScope(t *testing.T) {
	s := testutil.DB(t)
	topic(t, s, "http://127.0.0.1:9999")
	a := send(t, s, "a", time.Now())
	b := send(t, s, "b", time.Now())
	e := broker.NewEngine(s)
	first, err := e.Claim(ctx, a.ID)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	second, err := e.Claim(ctx, b.ID)
	if err != nil || second == nil {
		t.Fatal("blocked behind in-flight message", err)
	}
	for _, tp := range []broker.Topic{{Environment: "prod", Name: "test", URL: "http://127.0.0.1", Enabled: true}, {Environment: "test", Name: "other", URL: "http://127.0.0.1", Enabled: true}} {
		if _, err := s.SaveTopic(ctx, tp); err != nil {
			t.Fatal(err)
		}
		m, err := s.Send(ctx, tp.Environment, broker.SendRequest{Topic: tp.Name, UniqueID: "a", RunAt: time.Now(), Payload: json.RawMessage(`{}`)})
		if err != nil || m.ID == a.ID {
			t.Fatal("scope collision", err)
		}
	}
}
func TestTimerStateMigration(t *testing.T) {
	s := testutil.DB(t)
	topic(t, s, "http://127.0.0.1:9999")
	cases := []struct {
		old           string
		attempts      int
		state, result string
	}{
		{"waiting", 0, "pending", ""}, {"ready", 0, "pending", ""}, {"waiting", 1, "delivering", ""}, {"ready", 1, "delivering", ""},
		{"delivering", 1, "delivering", ""}, {"delivered", 1, "completed", "success"}, {"failed", 3, "completed", "failed"}, {"cancelled", 0, "completed", "cancelled"},
	}
	ids := make([]string, len(cases))
	for i, c := range cases {
		m := send(t, s, broker.ID("migration_"), time.Now().Add(time.Hour))
		ids[i] = m.ID
		if _, err := s.DB.Exec("UPDATE messages SET status=?,attempts=? WHERE id=?", c.old, c.attempts, m.ID); err != nil {
			t.Fatal(err)
		}
		if c.old != "waiting" {
			if _, err := s.DB.Exec("DELETE FROM message_waits WHERE message_id=?", m.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := s.DB.Exec("ALTER TABLE messages DROP COLUMN result, DROP COLUMN updated_at, DROP COLUMN redelivered_as"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i, c := range cases {
		m, err := s.Message(ctx, "test", ids[i])
		if err != nil || m.Status != c.state || m.Result != c.result || m.Attempts != c.attempts {
			t.Fatal(c, m, err)
		}
		if (c.old == "waiting" || c.old == "ready") && m.AvailableAt == nil {
			t.Fatal("lost wait", c)
		}
	}
}

func TestMessageKeyFiltersAreLiteral(t *testing.T) {
	s := testutil.DB(t)
	topic(t, s, "http://127.0.0.1:9999")
	for _, key := range []string{"wake_a%_1", "wake_a%_2", "wake_ax_3", "wake_b%_4"} {
		send(t, s, key, time.Now().Add(time.Hour))
	}
	rows, err := s.Messages(ctx, "test", broker.MessageFilter{Topic: "test", UniqueIDPrefix: "wake_a%_"})
	if err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	rows, err = s.Messages(ctx, "test", broker.MessageFilter{Topic: "test", UniqueID: "wake_a%_1"})
	if err != nil || len(rows) != 1 || rows[0].UniqueID != "wake_a%_1" {
		t.Fatal(rows, err)
	}
}
