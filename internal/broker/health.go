package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"time"
)

func (e *Engine) ProbeJob(ctx context.Context, id int64, force bool) (string, error) {
	var job Row
	var version int64
	err := e.Store.tx(ctx, func(tx *sql.Tx) error {
		var er error
		job, er = one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", id)
		if er != nil {
			return er
		}
		version = Int(job["broker_health_version"]) + 1
		_, er = tx.ExecContext(ctx, "UPDATE jobs SET broker_health_version=? WHERE id=?", version, id)
		return er
	})
	if err != nil {
		return "", err
	}
	headers := map[string]string{}
	json.Unmarshal(expandUUID(json.RawMessage(Str(job["execute_headers"]))), &headers)
	status, _, reason := e.request(ctx, "HEAD", HealthURL(Str(job["execute_url"])), headers, nil, 10)
	raw := "unhealthy"
	if status >= 200 && status < 300 {
		raw = "healthy"
	}
	changed := false
	previous := ""
	err = e.Store.tx(ctx, func(tx *sql.Tx) error {
		j, er := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", id)
		if er != nil {
			return er
		}
		if Int(j["broker_health_version"]) != version {
			return nil
		}
		streak := int64(1)
		if Str(j["broker_health_raw"]) == raw {
			streak = Int(j["broker_health_streak"]) + 1
		}
		previous = Str(j["worker_status"])
		persisted := previous
		threshold := int64(3)
		if n := Int(os.Getenv("HEALTH_ALERT_AFTER_SECONDS")); n > 0 {
			threshold = (n + 59) / 60
		}
		if force || streak >= threshold {
			persisted = raw
		}
		if force {
			streak = 0
		}
		now := time.Now().UTC()
		if _, er = tx.ExecContext(ctx, "UPDATE jobs SET worker_status=?,last_checked_at=?,broker_health_raw=?,broker_health_streak=? WHERE id=?", persisted, now, raw, streak, id); er != nil {
			return er
		}
		if persisted != previous {
			changed = true
			_, er = tx.ExecContext(ctx, "INSERT INTO worker_status_changes(job_id,from_status,to_status,status_code,error_message,changed_at) VALUES (?,?,?,?,?,?)", id, previous, persisted, status, reason, now)
		}
		return er
	})
	if changed {
		e.Notify(ctx, "Worker "+Str(job["name"])+": "+previous+" -> "+raw)
	}
	return raw, err
}
func (e *Engine) RefreshHealth(ctx context.Context) {
	jobs, err := e.Store.Jobs(ctx)
	if err != nil {
		return
	}
	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}
		e.ProbeJob(ctx, Int(j["id"]), false)
	}
	// Retain legacy seven-day execution/status history policy; keep message IDs for idempotency.
	e.Store.DB.ExecContext(ctx, "DELETE FROM execution_records WHERE finished_at<?", time.Now().UTC().Add(-7*24*time.Hour))
}
func (e *Engine) Notify(ctx context.Context, text string) {
	url := os.Getenv("LARK_WEBHOOK_URL")
	if url == "" {
		return
	}
	b, _ := json.Marshal(map[string]any{"msg_type": "text", "content": map[string]string{"text": "【msgbroker】 " + text}})
	e.request(ctx, "POST", url, nil, b, 5)
}
