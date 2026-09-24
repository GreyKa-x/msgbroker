package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func messageRow(r Row) Message {
	b := json.RawMessage(Str(r["payload"]))
	m := Message{ID: Str(r["id"]), Environment: Str(r["environment"]), Topic: Str(r["topic"]), UniqueID: Str(r["unique_id"]), Payload: b, RunAt: Time(r["run_at"]), Status: Str(r["status"]), Attempts: int(Int(r["attempts"])), LeaseToken: Str(r["lease_token"]), HTTPStatus: int(Int(r["http_status"])), Error: Str(r["last_error"]), Response: Str(r["response_body"]), CreatedAt: Time(r["created_at"]), Result: Str(r["result"]), RedeliveredAs: Str(r["redelivered_as"])}
	if r["updated_at"] != nil {
		t := Time(r["updated_at"])
		m.UpdatedAt = &t
	}
	if r["available_at"] != nil {
		t := Time(r["available_at"])
		m.AvailableAt = &t
	}
	if r["lease_until"] != nil {
		t := Time(r["lease_until"])
		m.LeaseUntil = &t
	}
	if r["finished_at"] != nil {
		t := Time(r["finished_at"])
		m.FinishedAt = &t
	}
	if r["job_id"] != nil {
		m.ScheduleID = Str(r["job_id"])
	}
	return m
}

const messageSelect = "SELECT m.*,COALESCE(t.environment,j.environment) environment,COALESCE(t.name,CONCAT('job:',j.id)) topic,w.available_at FROM messages m LEFT JOIN topics t ON t.id=m.topic_id LEFT JOIN jobs j ON j.id=m.job_id LEFT JOIN message_waits w ON w.message_id=m.id "

func (s *Store) Message(ctx context.Context, env, id string) (Message, error) {
	r, e := one(ctx, s.DB, messageSelect+"WHERE m.id=? AND (t.environment=? OR (t.id IS NULL AND j.environment=?))", id, env, env)
	if e != nil {
		return Message{}, e
	}
	return messageRow(r), nil
}
func (s *Store) Messages(ctx context.Context, env string, f MessageFilter) ([]Message, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 100
	}
	q := messageSelect + "WHERE (t.environment=? OR (t.id IS NULL AND j.environment=?))"
	a := []any{env, env}
	if f.OnlyTimed {
		q += " AND m.job_id IS NULL"
	}
	if f.Topic != "" {
		q += " AND t.name=?"
		a = append(a, f.Topic)
	}
	if f.Result != "" {
		q += " AND m.result=?"
		a = append(a, f.Result)
	}
	if f.Status != "" {
		q += " AND m.status=?"
		a = append(a, f.Status)
	}
	if f.ScheduleID != "" {
		q += " AND m.job_id=?"
		a = append(a, f.ScheduleID)
	}
	if !f.Before.IsZero() {
		if f.BeforeID != "" {
			q += " AND (m.created_at<? OR (m.created_at=? AND m.id<?))"
			a = append(a, f.Before, f.Before, f.BeforeID)
		} else {
			q += " AND m.created_at<?"
			a = append(a, f.Before)
		}
	}
	q += " ORDER BY m.created_at DESC,m.id DESC LIMIT ?"
	a = append(a, f.Limit)
	rows, e := Rows(ctx, s.DB, q, a...)
	out := []Message{}
	for _, r := range rows {
		out = append(out, messageRow(r))
	}
	return out, e
}
func ensureJobMessage(ctx context.Context, tx *sql.Tx, j Row) error {
	if !Bool(j["enabled"]) || Str(j["managed_key"]) != "" || j["broker_next_at"] == nil {
		return nil
	}
	at := Time(j["broker_next_at"])
	key := fmt.Sprintf("__schedule:%d:%d:%s", Int(j["id"]), Int(j["broker_version"]), at.Format(time.RFC3339Nano))
	var n int
	if e := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM messages WHERE job_id=? AND unique_id=?", j["id"], key).Scan(&n); e != nil || n > 0 {
		return e
	}
	body := expandUUID(json.RawMessage(Str(j["execute_body"])))
	head := expandUUID(json.RawMessage(Str(j["execute_headers"])))
	id := ID("msg_")
	_, e := tx.ExecContext(ctx, "INSERT INTO messages(id,unique_id,request_hash,job_id,job_version,payload,run_at,status,last_error,response_body,created_at,callback_url,callback_method,callback_headers,preflight) VALUES (?,?,'',?,?,?,?,'waiting','','',?,?,?,?,TRUE)", id, key, j["id"], j["broker_version"], string(body), at, time.Now().UTC(), j["execute_url"], j["execute_method"], string(head))
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO message_waits VALUES (?,?)", id, at)
	return e
}
func (s *Store) BootstrapJobs(ctx context.Context) error {
	jobs, e := s.Jobs(ctx)
	if e != nil {
		return e
	}
	for _, j := range jobs {
		if !Bool(j["enabled"]) || Str(j["managed_key"]) != "" {
			continue
		}
		e = s.tx(ctx, func(tx *sql.Tx) error {
			r, e := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", j["id"])
			if e != nil {
				return e
			}
			if r["broker_next_at"] == nil {
				n, e := LegacyNext(Str(r["cron_expression"]), time.Now())
				if e != nil {
					return fmt.Errorf("job %v: %w", r["id"], e)
				}
				r["broker_next_at"] = n
				if _, e = tx.ExecContext(ctx, "UPDATE jobs SET broker_next_at=? WHERE id=?", n, r["id"]); e != nil {
					return e
				}
			}
			return ensureJobMessage(ctx, tx, r)
		})
		if e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) Promote(ctx context.Context, now time.Time) error {
	candidates, e := Rows(ctx, s.DB, "SELECT w.message_id FROM message_waits w JOIN messages m ON m.id=w.message_id WHERE m.topic_id IS NULL AND w.available_at<=? ORDER BY w.available_at,w.message_id LIMIT 100", now)
	if e != nil {
		return e
	}
	for _, c := range candidates {
		e = s.tx(ctx, func(tx *sql.Tx) error {
			r, e := one(ctx, tx, "SELECT job_id FROM messages WHERE id=?", c["message_id"])
			if errors.Is(e, ErrNotFound) {
				return nil
			}
			if e != nil {
				return e
			}
			var j Row
			if r["job_id"] != nil {
				j, e = one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", r["job_id"])
				if e != nil {
					return e
				}
			}
			m, e := one(ctx, tx, "SELECT m.*,w.available_at FROM messages m JOIN message_waits w ON w.message_id=m.id WHERE m.id=? FOR UPDATE", c["message_id"])
			if errors.Is(e, ErrNotFound) {
				return nil
			}
			if e != nil {
				return e
			}
			if Time(m["available_at"]).After(now) {
				return nil
			}
			if Str(m["status"]) != "waiting" {
				_, e = tx.ExecContext(ctx, "DELETE FROM message_waits WHERE message_id=?", m["id"])
				return e
			}
			if j != nil && Int(m["job_version"]) > 0 {
				if !Bool(j["enabled"]) || Int(j["broker_version"]) != Int(m["job_version"]) {
					_, e = tx.ExecContext(ctx, "UPDATE messages SET status='cancelled',finished_at=? WHERE id=?", now, m["id"])
					if e != nil {
						return e
					}
				} else {
					// Next message is generated when this occurrence becomes ready, never after its HTTP result.
					if Time(j["broker_next_at"]).Equal(Time(m["run_at"])) {
						n, e := LegacyNext(Str(j["cron_expression"]), now)
						if e != nil {
							return e
						}
						j["broker_next_at"] = n
						if _, e = tx.ExecContext(ctx, "UPDATE jobs SET broker_next_at=? WHERE id=?", n, j["id"]); e != nil {
							return e
						}
						if e = ensureJobMessage(ctx, tx, j); e != nil {
							return e
						}
					}
					// Preserve APScheduler's 30-second misfire grace and skip-overlap behavior for legacy jobs.
					if Int(m["attempts"]) == 0 && (now.Sub(Time(m["run_at"])) > 30*time.Second || Time(j["broker_busy_until"]).After(now)) {
						_, e = tx.ExecContext(ctx, "UPDATE messages SET status='cancelled',finished_at=?,last_error='Skipped: misfire or previous execution still running' WHERE id=?", now, m["id"])
						if e != nil {
							return e
						}
					}
				}
			}
			if _, e = tx.ExecContext(ctx, "UPDATE messages SET status='ready' WHERE id=? AND status='waiting'", m["id"]); e != nil {
				return e
			}
			_, e = tx.ExecContext(ctx, "DELETE FROM message_waits WHERE message_id=?", m["id"])
			return e
		})
		if e != nil {
			return e
		}
	}
	return nil
}
