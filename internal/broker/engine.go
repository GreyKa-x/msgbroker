package broker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Delivery struct {
	Message Row
	Topic   Topic
}
type Engine struct {
	Store   *Store
	Client  *http.Client
	Workers int
}

func NewEngine(s *Store) *Engine {
	return &Engine{Store: s, Workers: 8, Client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (e *Engine) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < e.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				d, err := e.Claim(ctx, "")
				if err != nil {
					slog.Error("claim failed", "error", err)
				}
				if d != nil {
					e.Deliver(ctx, d)
					continue
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(300 * time.Millisecond):
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			e.RefreshHealth(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer wg.Wait()
	for ctx.Err() == nil {
		now := time.Now().UTC()
		for _, fn := range []func() error{func() error { return e.Store.Promote(ctx, now) }, func() error { return e.Recover(ctx, now) }, func() error { return e.Store.DispatchManaged(ctx, now) }} {
			if err := fn(); err != nil && ctx.Err() == nil {
				slog.Error("dispatch failed", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (e *Engine) Claim(ctx context.Context, id string) (*Delivery, error) {
	q := "SELECT m.id,m.job_id FROM messages m LEFT JOIN topics t ON t.id=m.topic_id WHERE m.status='ready' AND (m.topic_id IS NULL OR t.enabled=TRUE)"
	args := []any{}
	if id != "" {
		q += " AND m.id=?"
		args = append(args, id)
	}
	q += " ORDER BY m.created_at,m.id LIMIT 32"
	rows, err := Rows(ctx, e.Store.DB, q, args...)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		var out *Delivery
		err = e.Store.tx(ctx, func(tx *sql.Tx) error {
			now := time.Now().UTC()
			var job Row
			if r["job_id"] != nil {
				var er error
				job, er = one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", r["job_id"])
				if er != nil {
					return er
				}
				if Time(job["broker_busy_until"]).After(now) {
					return nil
				}
			}
			m, er := one(ctx, tx, "SELECT * FROM messages WHERE id=? FOR UPDATE SKIP LOCKED", r["id"])
			if er == ErrNotFound {
				return nil
			}
			if er != nil {
				return er
			}
			if m["status"] != "ready" {
				return nil
			}
			t := Topic{URL: Str(m["callback_url"]), Method: Str(m["callback_method"]), TimeoutSeconds: 10, MaxAttempts: 1, Enabled: true}
			json.Unmarshal([]byte(Str(m["callback_headers"])), &t.Headers)
			if job != nil && Str(job["managed_key"]) != "" {
				t.MaxAttempts = 5
				run, err := one(ctx, tx, "SELECT * FROM execution_records WHERE id=?", m["record_id"])
				if err != nil {
					return err
				}
				if !Bool(job["enabled"]) || Bool(job["managed_paused"]) || Int(run["version"]) != Int(job["managed_version"]) || now.Sub(Time(run["scheduled_at"])) > time.Hour || Int(m["attempts"]) >= 5 {
					if _, err = tx.ExecContext(ctx, "UPDATE messages SET status='failed',last_error='Schedule unavailable, replaced or expired',finished_at=? WHERE id=?", now, m["id"]); err != nil {
						return err
					}
					_, err = tx.ExecContext(ctx, "UPDATE execution_records SET status='failed',error_message='Schedule unavailable, replaced or expired',finished_at=? WHERE id=?", now, m["record_id"])
					return err
				}
			}
			if job != nil {
				if v := os.Getenv("REQUEST_TIMEOUT_SECONDS"); v != "" {
					if n := Int(v); n > 0 && n <= 120 {
						t.TimeoutSeconds = int(n)
					}
				}
			}
			if m["topic_id"] != nil {
				t, er = scanTopic(tx.QueryRowContext(ctx, "SELECT "+topicColumns+" FROM topics WHERE id=?", m["topic_id"]))
				if er != nil {
					return er
				}
				if !t.Enabled {
					return nil
				}
			}
			if job != nil && Int(m["job_version"]) > 0 && (!Bool(job["enabled"]) || Int(job["broker_version"]) != Int(m["job_version"])) {
				_, er = tx.ExecContext(ctx, "UPDATE messages SET status='cancelled',finished_at=? WHERE id=?", now, m["id"])
				return er
			}
			token := ID("lease_")
			until := now.Add(time.Duration(t.TimeoutSeconds*2+15) * time.Second)
			if _, er = tx.ExecContext(ctx, "UPDATE messages SET status='delivering',attempts=attempts+1,lease_token=?,lease_until=? WHERE id=?", token, until, m["id"]); er != nil {
				return er
			}
			m["lease_token"] = token
			m["attempts"] = Int(m["attempts"]) + 1
			m["lease_until"] = until
			if job != nil {
				if _, er = tx.ExecContext(ctx, "UPDATE jobs SET broker_busy_until=? WHERE id=?", until, job["id"]); er != nil {
					return er
				}
				if m["record_id"] == nil {
					res, er := tx.ExecContext(ctx, "INSERT INTO execution_records(job_id,status,started_at,finished_at,scheduled_at,attempts,version) VALUES (?,'pending',?,NULL,?,?,0)", job["id"], now, m["run_at"], m["attempts"])
					if er != nil {
						return er
					}
					rec, _ := res.LastInsertId()
					m["record_id"] = rec
					if _, er = tx.ExecContext(ctx, "UPDATE messages SET record_id=? WHERE id=?", rec, m["id"]); er != nil {
						return er
					}
				}
			}
			out = &Delivery{m, t}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if out != nil {
			return out, nil
		}
	}
	return nil, nil
}
func (e *Engine) Deliver(ctx context.Context, d *Delivery) {
	status, body, reason := 0, "", ""
	retry := false
	if Bool(d.Message["preflight"]) {
		ok, err := e.ProbeJob(ctx, Int(d.Message["job_id"]), false)
		if err != nil || ok != "healthy" {
			reason = "Pre-execution health check failed; request was not sent."
		}
	}
	if reason == "" {
		requestBody := []byte(Str(d.Message["payload"]))
		if Bool(d.Message["preflight"]) && string(requestBody) == "{}" {
			requestBody = nil
		}
		status, body, reason = e.request(ctx, d.Topic.Method, d.Topic.URL, d.Topic.Headers, requestBody, d.Topic.TimeoutSeconds)
		retry = status == 0 || status == 408 || status == 429 || status >= 500
	}
	if err := e.finish(ctx, d, status, body, reason, retry); err != nil {
		slog.Error("delivery confirmation failed", "messageId", d.Message["id"])
	} else if reason != "" && (!retry || Int(d.Message["attempts"]) >= int64(d.Topic.MaxAttempts)) {
		e.Notify(ctx, "Delivery "+Str(d.Message["id"])+" failed: "+reason)
	}
}
func (e *Engine) request(ctx context.Context, method, url string, headers map[string]string, body []byte, seconds int) (int, string, string) {
	c, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(c, method, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", "Invalid request"
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := e.Client.Do(req)
	if err != nil {
		return 0, "", "Request failed or timed out"
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 10000))
	if err != nil {
		return resp.StatusCode, "", "Response could not be read"
	}
	text := strings.ToValidUTF8(string(b), "�")
	for _, v := range headers {
		if len(v) > 3 {
			text = strings.ReplaceAll(text, v, "[redacted]")
		}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, text, ""
	}
	return resp.StatusCode, text, fmt.Sprintf("HTTP %d", resp.StatusCode)
}
func (e *Engine) finish(ctx context.Context, d *Delivery, status int, body, reason string, retry bool) error {
	return e.Store.tx(ctx, func(tx *sql.Tx) error {
		if d.Message["job_id"] != nil {
			if _, er := one(ctx, tx, "SELECT id FROM jobs WHERE id=? FOR UPDATE", d.Message["job_id"]); er != nil {
				return er
			}
		}
		m, er := one(ctx, tx, "SELECT * FROM messages WHERE id=? FOR UPDATE", d.Message["id"])
		if er != nil {
			return er
		}
		if m["status"] != "delivering" || m["lease_token"] != d.Message["lease_token"] {
			return nil
		}
		state := "delivered"
		var finished any = time.Now().UTC()
		if reason != "" {
			state = "failed"
		}
		if reason != "" && retry && Int(m["attempts"]) < int64(d.Topic.MaxAttempts) {
			state = "waiting"
			finished = nil
			delay := time.Duration(5*(1<<min(Int(m["attempts"])-1, 4))) * time.Second
			if _, er = tx.ExecContext(ctx, "INSERT INTO message_waits VALUES (?,?) ON DUPLICATE KEY UPDATE available_at=VALUES(available_at)", m["id"], time.Now().UTC().Add(delay)); er != nil {
				return er
			}
		}
		if _, er = tx.ExecContext(ctx, "UPDATE messages SET status=?,http_status=?,last_error=?,response_body=?,finished_at=?,lease_until=NULL,lease_token='' WHERE id=?", state, status, reason, body, finished, m["id"]); er != nil {
			return er
		}
		if m["job_id"] != nil {
			if _, er = tx.ExecContext(ctx, "UPDATE jobs SET broker_busy_until=NULL WHERE id=?", m["job_id"]); er != nil {
				return er
			}
			runStatus := "success"
			if state == "waiting" {
				runStatus = "pending"
			} else if state == "failed" {
				runStatus = "failed"
			}
			_, er = tx.ExecContext(ctx, "UPDATE execution_records SET status=?,status_code=?,error_message=?,response_body=?,finished_at=?,attempts=? WHERE id=?", runStatus, status, reason, body, finished, m["attempts"], m["record_id"])
		}
		return er
	})
}
func (e *Engine) Recover(ctx context.Context, now time.Time) error {
	rows, err := Rows(ctx, e.Store.DB, "SELECT id,job_id FROM messages WHERE status='delivering' AND lease_until<=? LIMIT 100", now)
	if err != nil {
		return err
	}
	for _, r := range rows {
		err = e.Store.tx(ctx, func(tx *sql.Tx) error {
			if r["job_id"] != nil {
				if _, er := one(ctx, tx, "SELECT id FROM jobs WHERE id=? FOR UPDATE", r["job_id"]); er != nil {
					return er
				}
			}
			m, er := one(ctx, tx, "SELECT * FROM messages WHERE id=? FOR UPDATE", r["id"])
			if er != nil {
				return er
			}
			if m["status"] != "delivering" || Time(m["lease_until"]).After(now) {
				return nil
			}
			maxAttempts := int64(1)
			if m["job_id"] != nil {
				var managed sql.NullString
				if er = tx.QueryRowContext(ctx, "SELECT managed_key FROM jobs WHERE id=?", m["job_id"]).Scan(&managed); er != nil {
					return er
				}
				if managed.Valid {
					maxAttempts = 5
				}
			}
			if m["topic_id"] != nil {
				if er = tx.QueryRowContext(ctx, "SELECT max_attempts FROM topics WHERE id=?", m["topic_id"]).Scan(&maxAttempts); er != nil {
					return er
				}
			}
			state := "ready"
			var finish any
			if Int(m["attempts"]) >= maxAttempts {
				state = "failed"
				finish = now
			}
			if _, er = tx.ExecContext(ctx, "UPDATE messages SET status=?,finished_at=?,lease_token='',lease_until=NULL,last_error='Delivery lease expired; result unconfirmed' WHERE id=?", state, finish, m["id"]); er != nil {
				return er
			}
			if r["job_id"] != nil {
				if _, er = tx.ExecContext(ctx, "UPDATE jobs SET broker_busy_until=NULL WHERE id=?", r["job_id"]); er != nil {
					return er
				}
				if state == "failed" {
					_, er = tx.ExecContext(ctx, "UPDATE execution_records SET status='failed',finished_at=?,error_message='Delivery lease expired; result unconfirmed' WHERE id=?", now, m["record_id"])
				}
			}
			return er
		})
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *Engine) RunJob(ctx context.Context, id int64) (Row, error) {
	msgID := ID("msg_")
	err := e.Store.tx(ctx, func(tx *sql.Tx) error {
		j, er := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", id)
		if er != nil {
			return er
		}
		if Str(j["managed_key"]) != "" {
			return ErrConflict
		}
		if Time(j["broker_busy_until"]).After(time.Now()) {
			return fmt.Errorf("%w: This job is already running", ErrConflict)
		}
		var n int
		if er = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM messages WHERE job_id=? AND job_version=0 AND status IN ('ready','delivering')", id).Scan(&n); er != nil {
			return er
		}
		if n > 0 {
			return ErrConflict
		}
		_, er = tx.ExecContext(ctx, "INSERT INTO messages(id,unique_id,request_hash,job_id,payload,run_at,status,last_error,response_body,created_at,callback_url,callback_method,callback_headers,preflight) VALUES (?,?,'',?,?,?,'ready','','',?,?,?,?,TRUE)", msgID, msgID, id, string(expandUUID(json.RawMessage(Str(j["execute_body"])))), time.Now().UTC(), time.Now().UTC(), j["execute_url"], j["execute_method"], string(expandUUID(json.RawMessage(Str(j["execute_headers"])))))
		return er
	})
	if err != nil {
		return nil, err
	}
	d, err := e.Claim(ctx, msgID)
	if err != nil {
		return nil, err
	}
	if d != nil {
		e.Deliver(ctx, d)
	}
	// A worker may win the claim; preserve the old synchronous manual-run contract.
	for {
		r, er := one(ctx, e.Store.DB, "SELECT status,record_id FROM messages WHERE id=?", msgID)
		if er != nil {
			return nil, er
		}
		if r["status"] == "delivered" || r["status"] == "failed" {
			return one(ctx, e.Store.DB, "SELECT * FROM execution_records WHERE id=?", r["record_id"])
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
