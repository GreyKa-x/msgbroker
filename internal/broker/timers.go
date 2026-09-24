package broker

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

func (s *Store) Send(ctx context.Context, env string, r SendRequest) (Message, error) {
	if !ValidEnvironment(env) || !namePattern.MatchString(r.Topic) || strings.TrimSpace(r.UniqueID) == "" || utf8.RuneCountInString(r.UniqueID) > 160 || strings.HasPrefix(r.UniqueID, "__") || r.RunAt.IsZero() {
		return Message{}, fmt.Errorf("valid environment, topic, uniqueId and runAt required")
	}
	body, err := CanonicalJSON(r.Payload)
	if err != nil {
		return Message{}, err
	}
	r.Payload = body
	r.RunAt = r.RunAt.UTC().Truncate(time.Microsecond)
	var id string
	err = s.tx(ctx, func(tx *sql.Tx) error {
		t, err := scanTopic(tx.QueryRowContext(ctx, "SELECT "+topicColumns+" FROM topics WHERE environment=? AND name=? FOR UPDATE", env, r.Topic))
		if err != nil {
			return err
		}
		row, err := one(ctx, tx, "SELECT id FROM messages WHERE topic_id=? AND unique_id=? FOR UPDATE", t.ID, r.UniqueID)
		if err != nil && err != ErrNotFound {
			return err
		}
		if err == ErrNotFound {
			id = ID("msg_")
			return insertTimer(ctx, tx, id, t.ID, r)
		}
		id = Str(row["id"])
		return nil
	})
	if err != nil {
		return Message{}, err
	}
	return s.Message(ctx, env, id)
}
func insertTimer(ctx context.Context, tx *sql.Tx, id, topicID string, r SendRequest) error {
	now := time.Now().UTC()
	_, err := tx.ExecContext(ctx, "INSERT INTO messages(id,topic_id,unique_id,request_hash,payload,run_at,status,last_error,response_body,created_at,updated_at,callback_url,callback_method,callback_headers) VALUES (?,?,?,'',?,?,'pending','','',?,?,'','','{}')", id, topicID, r.UniqueID, string(r.Payload), r.RunAt, now, now)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO message_waits VALUES (?,?)", id, r.RunAt)
	return err
}
func (s *Store) Cancel(ctx context.Context, env, id string) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		ref, err := one(ctx, tx, "SELECT topic_id FROM messages WHERE id=?", id)
		if err != nil {
			return err
		}
		if _, err = one(ctx, tx, "SELECT id FROM topics WHERE id=? AND environment=? FOR UPDATE", ref["topic_id"], env); err != nil {
			return err
		}
		m, err := one(ctx, tx, "SELECT * FROM messages WHERE id=? FOR UPDATE", id)
		if err != nil {
			return err
		}
		if Str(m["status"]) == "completed" && Str(m["result"]) == "cancelled" {
			return nil
		}
		if Str(m["status"]) != "pending" {
			return fmt.Errorf("%w: only pending messages can be cancelled", ErrConflict)
		}
		if _, err = tx.ExecContext(ctx, "UPDATE messages SET status='completed',result='cancelled',finished_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE id=?", id); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "DELETE FROM message_waits WHERE message_id=?", id)
		return err
	})
}

// Manual redelivery creates a new occurrence with an internal key; the original stays immutable.
func (s *Store) Redeliver(ctx context.Context, env, id string) (Message, error) {
	var newID string
	err := s.tx(ctx, func(tx *sql.Tx) error {
		ref, err := one(ctx, tx, "SELECT topic_id FROM messages WHERE id=?", id)
		if err != nil {
			return err
		}
		if _, err = one(ctx, tx, "SELECT id FROM topics WHERE id=? AND environment=? FOR UPDATE", ref["topic_id"], env); err != nil {
			return err
		}
		m, err := one(ctx, tx, "SELECT * FROM messages WHERE id=? FOR UPDATE", id)
		if err != nil {
			return err
		}
		if Str(m["status"]) != "completed" || Str(m["result"]) != "failed" {
			return fmt.Errorf("%w: only failed messages can be redelivered", ErrConflict)
		}
		if newID = Str(m["redelivered_as"]); newID != "" {
			return nil
		}
		newID = ID("msg_")
		if err = insertTimer(ctx, tx, newID, Str(m["topic_id"]), SendRequest{UniqueID: "__redelivery:" + id, Payload: []byte(Str(m["payload"])), RunAt: time.Now().UTC()}); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE messages SET redelivered_as=? WHERE id=?", newID, id)
		return err
	})
	if err != nil {
		return Message{}, err
	}
	return s.Message(ctx, env, newID)
}

// failedAttempt is 1-based. Topic validation bounds attempts at 20, so no duration overflow.
func RetryDelay(failedAttempt int64) time.Duration {
	return 5 * time.Second * time.Duration(int64(1)<<uint(failedAttempt-1))
}

func (e *Engine) claimTimer(ctx context.Context, id string) (*Delivery, error) {
	q := "SELECT m.id FROM message_waits w JOIN messages m ON m.id=w.message_id JOIN topics t ON t.id=m.topic_id WHERE w.available_at<=? AND t.enabled=TRUE AND m.status IN ('pending','delivering') AND m.lease_until IS NULL"
	args := []any{time.Now().UTC()}
	if id != "" {
		q += " AND m.id=?"
		args = append(args, id)
	}
	rows, err := Rows(ctx, e.Store.DB, q+" ORDER BY w.available_at,m.id LIMIT 32", args...)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		var out *Delivery
		err = e.Store.tx(ctx, func(tx *sql.Tx) error {
			m, err := one(ctx, tx, "SELECT * FROM messages WHERE id=? FOR UPDATE SKIP LOCKED", r["id"])
			if err == ErrNotFound {
				return nil
			}
			if err != nil {
				return err
			}
			if (Str(m["status"]) != "pending" && Str(m["status"]) != "delivering") || m["lease_until"] != nil {
				return nil
			}
			w, err := one(ctx, tx, "SELECT available_at FROM message_waits WHERE message_id=? FOR UPDATE", m["id"])
			if err == ErrNotFound {
				return nil
			}
			if err != nil {
				return err
			}
			now := time.Now().UTC()
			if Time(w["available_at"]).After(now) {
				return nil
			}
			t, err := scanTopic(tx.QueryRowContext(ctx, "SELECT "+topicColumns+" FROM topics WHERE id=?", m["topic_id"]))
			if err != nil {
				return err
			}
			if !t.Enabled {
				return nil
			}
			if _, err = tx.ExecContext(ctx, "DELETE FROM message_waits WHERE message_id=?", m["id"]); err != nil {
				return err
			}
			if Int(m["attempts"]) >= int64(t.MaxAttempts) {
				_, err = tx.ExecContext(ctx, "UPDATE messages SET status='completed',result='failed',finished_at=?,updated_at=? WHERE id=?", now, now, m["id"])
				return err
			}
			token := ID("lease_")
			until := now.Add(time.Duration(t.TimeoutSeconds*2+15) * time.Second)
			if _, err = tx.ExecContext(ctx, "UPDATE messages SET status='delivering',attempts=attempts+1,lease_token=?,lease_until=?,updated_at=? WHERE id=?", token, until, now, m["id"]); err != nil {
				return err
			}
			m["lease_token"] = token
			m["lease_until"] = until
			m["attempts"] = Int(m["attempts"]) + 1
			out = &Delivery{Message: m, Topic: t}
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
func finishTimerTx(ctx context.Context, tx *sql.Tx, m Row, maxAttempts int, status int, body, reason string, now time.Time) error {
	state, result := "completed", "success"
	var finished any = now
	if reason != "" {
		result = "failed"
	}
	if reason != "" && Int(m["attempts"]) < int64(maxAttempts) {
		state, result, finished = "delivering", "", nil
		if _, err := tx.ExecContext(ctx, "INSERT INTO message_waits VALUES (?,?) ON DUPLICATE KEY UPDATE available_at=VALUES(available_at)", m["id"], now.Add(RetryDelay(Int(m["attempts"])))); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, "UPDATE messages SET status=?,result=?,http_status=?,last_error=?,response_body=?,finished_at=?,updated_at=?,lease_until=NULL,lease_token='' WHERE id=?", state, result, status, reason, body, finished, now, m["id"])
	return err
}
func (e *Engine) finishTimer(ctx context.Context, d *Delivery, status int, body, reason string) error {
	return e.Store.tx(ctx, func(tx *sql.Tx) error {
		m, err := one(ctx, tx, "SELECT * FROM messages WHERE id=? FOR UPDATE", d.Message["id"])
		if err != nil {
			return err
		}
		if Str(m["status"]) != "delivering" || Str(m["lease_token"]) == "" || m["lease_token"] != d.Message["lease_token"] {
			return nil
		}
		return finishTimerTx(ctx, tx, m, d.Topic.MaxAttempts, status, body, reason, time.Now().UTC())
	})
}
func (e *Engine) recoverTimers(ctx context.Context, now time.Time) error {
	rows, err := Rows(ctx, e.Store.DB, "SELECT id FROM messages WHERE topic_id IS NOT NULL AND status='delivering' AND lease_until<=? LIMIT 100", now)
	if err != nil {
		return err
	}
	for _, r := range rows {
		err = e.Store.tx(ctx, func(tx *sql.Tx) error {
			m, err := one(ctx, tx, "SELECT * FROM messages WHERE id=? FOR UPDATE", r["id"])
			if err != nil {
				return err
			}
			if Str(m["status"]) != "delivering" || m["lease_until"] == nil || Time(m["lease_until"]).After(now) {
				return nil
			}
			var max int
			if err = tx.QueryRowContext(ctx, "SELECT max_attempts FROM topics WHERE id=?", m["topic_id"]).Scan(&max); err != nil {
				return err
			}
			return finishTimerTx(ctx, tx, m, max, 0, "", "Delivery lease expired; result unconfirmed", now)
		})
		if err != nil {
			return err
		}
	}
	return nil
}
