package broker_test

import (
	"encoding/json"
	"github.com/GreyKa-x/msgbroker/internal/broker"
	"github.com/GreyKa-x/msgbroker/internal/testutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAPSchedulerReference(t *testing.T) {
	b, e := os.ReadFile("testdata/apscheduler.json")
	if e != nil {
		t.Fatal(e)
	}
	var cases []struct{ Expr, After, Want string }
	json.Unmarshal(b, &cases)
	for _, c := range cases {
		after, _ := time.Parse(time.RFC3339, c.After)
		want, _ := time.Parse(time.RFC3339, c.Want)
		got, e := broker.LegacyNext(c.Expr, after)
		if e != nil || !got.Equal(want) {
			t.Errorf("%s after %s: got %s want %s err %v", c.Expr, c.After, got, want, e)
		}
	}
}
func TestUpgradeOriginalSchemaPreservesData(t *testing.T) {
	s := testutil.RawDB(t)
	b, e := os.ReadFile("legacy.sql")
	if e != nil {
		t.Fatal(e)
	}
	schema := string(b)
	schema = strings.Replace(schema, "status ENUM('pending','success','failed')", "status ENUM('success','failed')", 1)
	schema = strings.Replace(schema, ",\n managed_key VARCHAR(160) NULL UNIQUE, managed_version INT DEFAULT 0,\n managed_definition TEXT NULL, managed_next_at DATETIME(6) NULL, managed_paused BOOLEAN DEFAULT FALSE", "", 1)
	schema = strings.Replace(schema, " delivery_id VARCHAR(32) NULL UNIQUE, version INT DEFAULT 0, scheduled_at DATETIME(6) NULL,\n retry_at DATETIME(6) NULL, attempts INT DEFAULT 0,\n", "", 1)
	for _, q := range strings.Split(schema, ";") {
		if strings.TrimSpace(q) != "" {
			if _, e = s.DB.Exec(q); e != nil {
				t.Fatal(e)
			}
		}
	}
	_, e = s.DB.Exec(`INSERT INTO jobs(id,name,environment,cron_expression,execute_method,execute_url,execute_headers,execute_body,health_check_method,health_check_url,health_check_headers,health_check_body,enabled,worker_status,created_at,updated_at) VALUES (42,'legacy','test','0 3 * * 0','POST','http://localhost/run','{"Authorization":"old-secret"}','{"value":1}','HEAD','http://localhost/run/_health','{}','{}',TRUE,'healthy',NOW(),NOW())`)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.DB.Exec("INSERT INTO execution_records(id,job_id,status,status_code,response_body,started_at,finished_at) VALUES(77,42,'success',200,'old response',NOW(),NOW())")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	if e = s.Migrate(ctx); e != nil {
		t.Fatal("migration not idempotent", e)
	}
	j, e := s.Job(ctx, 42)
	if e != nil || j["name"] != "legacy" || j["worker_status"] != "healthy" || j["execute_headers"] != `{"Authorization":"old-secret"}` {
		t.Fatal(j, e)
	}
	r, _ := s.Executions(ctx, 42, 10)
	if len(r) != 1 || broker.Int(r[0]["id"]) != 77 || r[0]["response_body"] != "old response" {
		t.Fatal(r)
	}
	if e = s.BootstrapJobs(ctx); e != nil {
		t.Fatal(e)
	}
	if e = s.BootstrapJobs(ctx); e != nil {
		t.Fatal(e)
	}
	messages, e := s.Messages(ctx, "test", broker.MessageFilter{})
	if e != nil || len(messages) != 1 {
		t.Fatal(messages, e)
	}
}
func TestManagedMigrationAndDelivery(t *testing.T) {
	s := testutil.DB(t)
	t.Setenv("TASK_SCHEDULER_SOURCE", "test")
	t.Setenv("TASK_DELIVERY_URL", "http://127.0.0.1:9999/run")
	now := time.Now().UTC().Add(-time.Second)
	spec := broker.ManagedSpec{Version: 1, TaskID: "t1", Name: "managed", Enabled: true, ScheduleType: "once", RunAt: now, FirstAt: now, Timezone: "Asia/Shanghai"}
	if _, e := s.UpsertManaged(ctx, "test:t1", spec); e != nil {
		t.Fatal(e)
	}
	if _, e := s.UpsertManaged(ctx, "test:t1", spec); e != nil {
		t.Fatal(e)
	}
	if e := s.DispatchManaged(ctx, time.Now()); e != nil {
		t.Fatal(e)
	}
	if e := s.DispatchManaged(ctx, time.Now()); e != nil {
		t.Fatal(e)
	}
	out, e := s.ManagedDeliveries(ctx, "test:t1")
	if e != nil || len(out) != 1 {
		t.Fatal(out, e)
	}
	if e := s.Promote(ctx, time.Now().Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	engine := broker.NewEngine(s)
	d, e := engine.Claim(ctx, "")
	if e != nil || d == nil || d.Topic.MaxAttempts != 5 {
		t.Fatal(d, e)
	}
	engine.Deliver(ctx, d)
	out, _ = s.ManagedDeliveries(ctx, "test:t1")
	if broker.Int(out[0]["attempts"]) != 1 || out[0]["status"] != "pending" {
		t.Fatal(out)
	}
}
