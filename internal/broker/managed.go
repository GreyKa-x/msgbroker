package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type ManagedSpec struct {
	Version      int        `json:"version"`
	TaskID       string     `json:"task_id"`
	Name         string     `json:"name"`
	Enabled      bool       `json:"enabled"`
	ScheduleType string     `json:"schedule_type"`
	IntervalType string     `json:"interval_type"`
	RunAt        time.Time  `json:"run_at"`
	FirstAt      time.Time  `json:"first_at"`
	Timezone     string     `json:"timezone"`
	EndAt        *time.Time `json:"end_at"`
}

func managedNext(s ManagedSpec, after time.Time) (*time.Time, error) {
	if s.ScheduleType == "once" {
		return nil, nil
	}
	var n time.Time
	switch s.IntervalType {
	case "12h", "24h":
		step := 12 * time.Hour
		if s.IntervalType == "24h" {
			step = 24 * time.Hour
		}
		n = s.RunAt
		if !n.After(after) {
			n = n.Add((after.Sub(n)/step + 1) * step)
		}
	case "weekly":
		loc, er := time.LoadLocation(s.Timezone)
		if er != nil {
			return nil, er
		}
		local := s.RunAt.In(loc)
		expr := fmt.Sprintf("%d %d * * %d", local.Minute(), local.Hour(), local.Weekday())
		n, er = Next(expr, s.Timezone, after)
		if er != nil {
			return nil, er
		}
	default:
		return nil, fmt.Errorf("invalid interval")
	}
	n = n.UTC()
	if s.EndAt != nil && n.After(*s.EndAt) {
		return nil, nil
	}
	return &n, nil
}
func (s *Store) UpsertManaged(ctx context.Context, key string, v ManagedSpec) (Row, error) {
	source := os.Getenv("TASK_SCHEDULER_SOURCE")
	if len(key) > 160 || key != source+":"+v.TaskID || v.TaskID == "" || v.Version < 1 || len([]rune(v.Name)) > 255 || v.RunAt.IsZero() || v.FirstAt.IsZero() {
		return nil, fmt.Errorf("invalid schedule")
	}
	if v.Timezone == "" {
		v.Timezone = "Asia/Shanghai"
	}
	if _, er := time.LoadLocation(v.Timezone); er != nil {
		return nil, er
	}
	if v.ScheduleType != "once" && v.ScheduleType != "recurring" {
		return nil, fmt.Errorf("invalid schedule type")
	}
	if _, er := managedNext(v, time.Now()); er != nil {
		return nil, er
	}
	definition, _ := json.Marshal(v)
	var out Row
	err := s.tx(ctx, func(tx *sql.Tx) error {
		j, er := one(ctx, tx, "SELECT * FROM jobs WHERE managed_key=? FOR UPDATE", key)
		if er != nil && er != ErrNotFound {
			return er
		}
		if er == nil && Int(j["managed_version"]) > int64(v.Version) {
			return ErrConflict
		}
		if er == nil && Int(j["managed_version"]) == int64(v.Version) {
			var old ManagedSpec
			json.Unmarshal([]byte(Str(j["managed_definition"])), &old)
			old.FirstAt = v.FirstAt
			a, _ := json.Marshal(old)
			if string(a) != string(definition) {
				return ErrConflict
			}
			out = Row{"key": key, "version": v.Version, "enabled": Bool(j["enabled"]), "paused": Bool(j["managed_paused"])}
			return nil
		}
		var next any
		if v.Enabled {
			at := v.FirstAt.UTC()
			if v.ScheduleType == "recurring" && at.Before(time.Now().Add(-time.Hour)) {
				n, er := managedNext(v, time.Now())
				if er != nil {
					return er
				}
				if n != nil {
					at = *n
				} else {
					at = time.Time{}
				}
			}
			if !at.IsZero() && (v.EndAt == nil || !at.After(*v.EndAt)) {
				next = at
			}
		}
		if j == nil {
			env := os.Getenv("TASK_SCHEDULER_ENVIRONMENT")
			if !ValidEnvironment(env) {
				if strings.Contains(source, "test") {
					env = "test"
				} else {
					env = "prod"
				}
			}
			now := time.Now().UTC()
			_, er = tx.ExecContext(ctx, "INSERT INTO jobs(name,environment,cron_expression,execute_method,execute_url,execute_headers,execute_body,health_check_method,health_check_url,health_check_headers,health_check_body,enabled,worker_status,created_at,updated_at,managed_key,managed_version,managed_definition,managed_next_at,managed_paused) VALUES (?,?,'managed','POST',?,'{}','{}','HEAD',?,'{}','{}',?,'unknown',?,?,?,?,?,?,FALSE)", v.Name, env, os.Getenv("TASK_DELIVERY_URL"), HealthURL(os.Getenv("TASK_DELIVERY_URL")), v.Enabled, now, now, key, v.Version, string(definition), next)
		} else {
			_, er = tx.ExecContext(ctx, "UPDATE jobs SET name=?,enabled=?,managed_version=?,managed_definition=?,managed_next_at=?,updated_at=? WHERE id=?", v.Name, v.Enabled, v.Version, string(definition), next, time.Now().UTC(), j["id"])
		}
		out = Row{"key": key, "version": v.Version, "enabled": v.Enabled, "paused": j != nil && Bool(j["managed_paused"])}
		return er
	})
	if duplicate(err) {
		return nil, ErrConflict
	}
	return out, err
}
func (s *Store) Managed(ctx context.Context) ([]Row, error) {
	rows, e := Rows(ctx, s.DB, "SELECT managed_key AS `key`,name,managed_version AS version,enabled,managed_paused AS paused,managed_next_at AS next_at FROM jobs WHERE managed_key IS NOT NULL ORDER BY managed_key LIMIT 1000")
	for _, r := range rows {
		r["enabled"] = Bool(r["enabled"])
		r["paused"] = Bool(r["paused"])
	}
	return rows, e
}
func (s *Store) ManagedDeliveries(ctx context.Context, key string) ([]Row, error) {
	return Rows(ctx, s.DB, "SELECT e.delivery_id id,e.scheduled_at,e.status,e.attempts,e.error_message error FROM execution_records e JOIN jobs j ON j.id=e.job_id WHERE j.managed_key=? ORDER BY e.scheduled_at DESC LIMIT 100", key)
}
func (s *Store) PauseManaged(ctx context.Context, key string, pause bool) error {
	r, e := s.DB.ExecContext(ctx, "UPDATE jobs SET managed_paused=? WHERE managed_key=?", pause, key)
	if e != nil {
		return e
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		var id int
		e = s.DB.QueryRowContext(ctx, "SELECT id FROM jobs WHERE managed_key=?", key).Scan(&id)
		return missing(e)
	}
	return nil
}
func (s *Store) DispatchManaged(ctx context.Context, now time.Time) error {
	candidates, e := Rows(ctx, s.DB, "SELECT id FROM jobs WHERE managed_key IS NOT NULL AND enabled=TRUE AND managed_paused=FALSE AND managed_next_at<=? LIMIT 100", now)
	if e != nil {
		return e
	}
	for _, r := range candidates {
		e = s.tx(ctx, func(tx *sql.Tx) error {
			j, er := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", r["id"])
			if er != nil {
				return er
			}
			at := Time(j["managed_next_at"])
			if at.IsZero() || at.After(now) || !Bool(j["enabled"]) || Bool(j["managed_paused"]) {
				return nil
			}
			var spec ManagedSpec
			if er = json.Unmarshal([]byte(Str(j["managed_definition"])), &spec); er != nil {
				return er
			}
			next, er := managedNext(spec, now)
			if er != nil {
				return er
			}
			_, er = tx.ExecContext(ctx, "INSERT INTO execution_records(job_id,delivery_id,version,status,finished_at,scheduled_at,retry_at,started_at,attempts) VALUES (?,?,?,'pending',NULL,?,?,?,0)", j["id"], ID(""), j["managed_version"], at, now, now)
			if er != nil {
				return er
			}
			_, er = tx.ExecContext(ctx, "UPDATE jobs SET managed_next_at=? WHERE id=?", next, j["id"])
			return er
		})
		if e != nil {
			return e
		}
	}
	// Import both freshly produced and pre-existing pending legacy deliveries into the common worker.
	records, e := Rows(ctx, s.DB, "SELECT e.id,e.job_id FROM execution_records e JOIN jobs j ON j.id=e.job_id LEFT JOIN messages m ON m.record_id=e.id WHERE j.managed_key IS NOT NULL AND e.status='pending' AND m.id IS NULL ORDER BY e.id LIMIT 100")
	if e != nil {
		return e
	}
	for _, r := range records {
		e = s.tx(ctx, func(tx *sql.Tx) error {
			j, er := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", r["job_id"])
			if er != nil {
				return er
			}
			run, er := one(ctx, tx, "SELECT * FROM execution_records WHERE id=? FOR UPDATE", r["id"])
			if er != nil {
				return er
			}
			var count int
			if er = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM messages WHERE record_id=?", r["id"]).Scan(&count); er != nil || count > 0 {
				return er
			}
			var spec ManagedSpec
			if er = json.Unmarshal([]byte(Str(j["managed_definition"])), &spec); er != nil {
				return er
			}
			body, _ := json.Marshal(map[string]any{"task_id": spec.TaskID, "delivery_id": run["delivery_id"], "version": Int(run["version"]), "scheduled_at": Time(run["scheduled_at"]).Format(time.RFC3339)})
			headers, _ := json.Marshal(map[string]string{"X-Task-Delivery-Token": os.Getenv("TASK_DELIVERY_TOKEN")})
			id := ID("msg_")
			_, er = tx.ExecContext(ctx, "INSERT INTO messages(id,unique_id,request_hash,job_id,record_id,payload,run_at,status,last_error,response_body,created_at,callback_url,callback_method,callback_headers,attempts) VALUES (?,?,'',?,?,?,?,'waiting','','',?,?,'POST',?,?)", id, Str(run["delivery_id"]), j["id"], run["id"], string(body), run["scheduled_at"], now, os.Getenv("TASK_DELIVERY_URL"), string(headers), Int(run["attempts"]))
			if er != nil {
				return er
			}
			available := Time(run["retry_at"])
			if available.IsZero() {
				available = now
			}
			_, er = tx.ExecContext(ctx, "INSERT INTO message_waits VALUES (?,?)", id, available)
			return er
		})
		if e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) RetryManaged(ctx context.Context, jobID, recordID int64) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		j, e := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", jobID)
		if e != nil {
			return e
		}
		r, e := one(ctx, tx, "SELECT * FROM execution_records WHERE id=? AND job_id=? FOR UPDATE", recordID, jobID)
		if e != nil {
			return e
		}
		if Str(j["managed_key"]) == "" || r["status"] != "failed" || !Bool(j["enabled"]) || Bool(j["managed_paused"]) || Int(j["managed_version"]) != Int(r["version"]) || time.Since(Time(r["scheduled_at"])) > time.Hour {
			return ErrConflict
		}
		if _, e = tx.ExecContext(ctx, "UPDATE execution_records SET status='pending',attempts=0,finished_at=NULL,retry_at=? WHERE id=?", time.Now().UTC(), recordID); e != nil {
			return e
		}
		ms, e := Rows(ctx, tx, "SELECT id FROM messages WHERE record_id=? FOR UPDATE", recordID)
		if e != nil {
			return e
		}
		for _, m := range ms {
			if _, e = tx.ExecContext(ctx, "UPDATE messages SET status='ready',attempts=0,finished_at=NULL WHERE id=?", m["id"]); e != nil {
				return e
			}
			if _, e = tx.ExecContext(ctx, "DELETE FROM message_waits WHERE message_id=?", m["id"]); e != nil {
				return e
			}
		}
		return nil
	})
}
