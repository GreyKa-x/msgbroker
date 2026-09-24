package broker_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GreyKa-x/msgbroker/internal/broker"
	"github.com/GreyKa-x/msgbroker/internal/testutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var ctx = context.Background()

func topic(t *testing.T, s *broker.Store, url string) {
	t.Helper()
	_, e := s.SaveTopic(ctx, broker.Topic{Environment: "test", Name: "test", URL: url, Method: "POST", Enabled: true, MaxAttempts: 3})
	if e != nil {
		t.Fatal(e)
	}
}
func send(t *testing.T, s *broker.Store, key string, at time.Time) broker.Message {
	t.Helper()
	m, e := s.Send(ctx, "test", broker.SendRequest{Topic: "test", UniqueID: key, RunAt: at, Payload: json.RawMessage(`{"number":9007199254740993,"text":"<script>"}`)})
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func TestConcurrentSendAndImmutableID(t *testing.T) {
	s := testutil.DB(t)
	topic(t, s, "http://127.0.0.1:9999")
	at := time.Now().Add(time.Hour)
	var wg sync.WaitGroup
	ids := make(chan string, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, e := s.Send(ctx, "test", broker.SendRequest{Topic: "test", UniqueID: "same", RunAt: at, Payload: json.RawMessage(`{"b":2,"a":1}`)})
			if e != nil {
				errs <- e
			} else {
				ids <- m.ID
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("duplicate created")
		}
	}
	_, e := s.Send(ctx, "test", broker.SendRequest{Topic: "test", UniqueID: "same", RunAt: at, Payload: json.RawMessage(`{"a":3}`)})
	if !errors.Is(e, broker.ErrConflict) {
		t.Fatal(e)
	}
	if e = s.Cancel(ctx, "test", first); e != nil {
		t.Fatal(e)
	}
	m, e := s.Send(ctx, "test", broker.SendRequest{Topic: "test", UniqueID: "same", RunAt: at, Payload: json.RawMessage(`{"a":1,"b":2}`)})
	if e != nil || m.ID != first || m.Status != "cancelled" {
		t.Fatalf("%+v %v", m, e)
	}
	if _, e = s.Message(ctx, "prod", first); !errors.Is(e, broker.ErrNotFound) {
		t.Fatal(e)
	}
}
func TestDeliveryRetryRawPayloadAndLease(t *testing.T) {
	s := testutil.DB(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Error(e)
		}
		if string(body["number"]) != "9007199254740993" {
			t.Error("payload changed")
		}
		if body["payload"] != nil {
			t.Error("unexpected envelope")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	topic(t, s, server.URL)
	m := send(t, s, "retry", time.Now().Add(-time.Second))
	e := broker.NewEngine(s)
	if err := s.Promote(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	d, err := e.Claim(ctx, m.ID)
	if err != nil || d == nil {
		t.Fatal(err)
	}
	if err = s.Cancel(ctx, "test", m.ID); !errors.Is(err, broker.ErrConflict) {
		t.Fatal(err)
	}
	e.Deliver(ctx, d)
	m, _ = s.Message(ctx, "test", m.ID)
	if m.Status != "waiting" || m.Attempts != 1 {
		t.Fatalf("%+v", m)
	}
	if err = s.Promote(ctx, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	d2, err := e.Claim(ctx, m.ID)
	if err != nil || d2 == nil {
		t.Fatal(err)
	}
	e.Deliver(ctx, d2)
	m, _ = s.Message(ctx, "test", m.ID)
	if m.Status != "delivered" || calls.Load() != 2 {
		t.Fatalf("%+v", m)
	}
	e.Deliver(ctx, d)
	m, _ = s.Message(ctx, "test", m.ID)
	if m.Status != "delivered" {
		t.Fatal("stale worker overwrote state")
	}
}
func TestLeaseRecoveryAndMultipleWorkers(t *testing.T) {
	s := testutil.DB(t)
	topic(t, s, "http://127.0.0.1:9999")
	m := send(t, s, "lease", time.Now())
	s.Promote(ctx, time.Now().Add(time.Second))
	e := broker.NewEngine(s)
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := e.Claim(ctx, m.ID)
			if err != nil {
				t.Error(err)
			}
			if d != nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatal(wins.Load())
	}
	if err := e.Recover(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	d, err := e.Claim(ctx, m.ID)
	if err != nil || d == nil {
		t.Fatal(err)
	}
	m, _ = s.Message(ctx, "test", m.ID)
	if m.Attempts != 2 {
		t.Fatal(m)
	}
}
func job(t *testing.T, s *broker.Store, url string, enabled bool) int64 {
	t.Helper()
	id, e := s.SaveJob(ctx, 0, broker.Row{"name": "old job", "environment": "test", "cron_expression": "*/5 * * * * *", "execute_url": url, "execute_method": "POST", "execute_headers": "{}", "execute_body": `{"id":"{{UUID}}"}`, "enabled": enabled})
	if e != nil {
		t.Fatal(e)
	}
	return id
}
func TestLegacyManualHealthAndRecords(t *testing.T) {
	s := testutil.DB(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.WriteHeader(200)
			return
		}
		calls.Add(1)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	id := job(t, s, server.URL, false)
	e := broker.NewEngine(s)
	rec, err := e.RunJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if rec["status"] != "success" || calls.Load() != 1 {
		t.Fatal(rec)
	}
	for i := 0; i < 2; i++ {
		e.ProbeJob(ctx, id, false)
	}
	j, _ := s.Job(ctx, id)
	if j["worker_status"] != "healthy" {
		t.Fatal(j)
	}
	if _, err = e.ProbeJob(ctx, id, true); err != nil {
		t.Fatal(err)
	}
	records, _ := s.Executions(ctx, id, 10)
	if len(records) != 1 {
		t.Fatal(records)
	}
}
func TestPeriodicRollsBeforeHTTPAndEditCancelsOld(t *testing.T) {
	s := testutil.DB(t)
	id := job(t, s, "http://127.0.0.1:9999", true)
	j, _ := s.Job(ctx, id)
	at := broker.Time(j["broker_next_at"])
	if e := s.Promote(ctx, at); e != nil {
		t.Fatal(e)
	}
	j, _ = s.Job(ctx, id)
	if !broker.Time(j["broker_next_at"]).After(at) {
		t.Fatal("did not roll")
	}
	msgs, e := s.Messages(ctx, "test", broker.MessageFilter{ScheduleID: broker.Str(id)})
	if e != nil || len(msgs) != 2 {
		t.Fatalf("%v %v", msgs, e)
	}
	if _, e = s.Toggle(ctx, id); e != nil {
		t.Fatal(e)
	}
	msgs, _ = s.Messages(ctx, "test", broker.MessageFilter{ScheduleID: broker.Str(id)})
	for _, m := range msgs {
		if m.Status != "cancelled" {
			t.Fatal(m)
		}
	}
}
func TestCronAPSchedulerSemantics(t *testing.T) {
	after := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	cases := []struct{ expr, want string }{{"0 3 * * 0", "2026-09-27T19:00:00Z"}, {"0 3 1 * mon", "2027-01-31T19:00:00Z"}, {"*/5 * * * * *", "2026-09-24T12:00:05Z"}}
	for _, c := range cases {
		n, e := broker.LegacyNext(c.expr, after)
		if e != nil || n.Format(time.RFC3339) != c.want {
			t.Errorf("%s: %s %v", c.expr, n, e)
		}
	}
}

func TestUUIDCompatibility(t *testing.T) {
	v := broker.UUID()
	if len(v) != 36 || v[14] != '4' || !strings.Contains("89ab", v[19:20]) || v[8] != '-' || v[13] != '-' || v[18] != '-' || v[23] != '-' {
		t.Fatal(v)
	}
}
