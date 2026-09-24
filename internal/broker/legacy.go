package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Row map[string]any

func Str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
func Int(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	}
	n, _ := strconv.ParseInt(Str(v), 10, 64)
	return n
}
func Bool(v any) bool { return v == true || Int(v) != 0 || v == "true" }
func Time(v any) time.Time {
	if t, ok := v.(time.Time); ok {
		return t
	}
	return time.Time{}
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func Rows(ctx context.Context, q queryer, query string, args ...any) ([]Row, error) {
	rows, e := q.QueryContext(ctx, query, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := []Row{}
	for rows.Next() {
		v := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range v {
			ptr[i] = &v[i]
		}
		if e = rows.Scan(ptr...); e != nil {
			return nil, e
		}
		m := Row{}
		for i, k := range cols {
			if b, ok := v[i].([]byte); ok {
				v[i] = string(b)
			}
			m[k] = v[i]
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
func one(ctx context.Context, q queryer, query string, args ...any) (Row, error) {
	r, e := Rows(ctx, q, query, args...)
	if e != nil {
		return nil, e
	}
	if len(r) == 0 {
		return nil, ErrNotFound
	}
	return r[0], nil
}
func (s *Store) Job(ctx context.Context, id int64) (Row, error) {
	return one(ctx, s.DB, "SELECT * FROM jobs WHERE id=?", id)
}
func (s *Store) Jobs(ctx context.Context) ([]Row, error) {
	return Rows(ctx, s.DB, "SELECT * FROM jobs ORDER BY created_at DESC,id DESC")
}
func (s *Store) Executions(ctx context.Context, id int64, limit int) ([]Row, error) {
	return Rows(ctx, s.DB, "SELECT * FROM execution_records WHERE job_id=? ORDER BY started_at DESC,id DESC LIMIT ?", id, limit)
}
func (s *Store) Changes(ctx context.Context, id int64, limit int) ([]Row, error) {
	return Rows(ctx, s.DB, "SELECT * FROM worker_status_changes WHERE job_id=? ORDER BY changed_at DESC,id DESC LIMIT ?", id, limit)
}
func HealthURL(raw string) string {
	u, e := url.Parse(raw)
	if e != nil {
		return raw
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/_health"
	return u.String()
}
func validateJob(j Row) error {
	t := Topic{Environment: Str(j["environment"]), Name: "job", URL: Str(j["execute_url"]), Method: Str(j["execute_method"]), Enabled: Bool(j["enabled"])}
	if e := json.Unmarshal([]byte(Str(j["execute_headers"])), &t.Headers); e != nil {
		return fmt.Errorf("Request headers must be a JSON object of strings")
	}
	if e := ValidateTopic(&t); e != nil {
		return e
	}
	if strings.TrimSpace(Str(j["name"])) == "" || len([]rune(Str(j["name"]))) > 255 {
		return fmt.Errorf("Name is required, maximum 255 characters")
	}
	if _, e := LegacyNext(Str(j["cron_expression"]), time.Now()); e != nil {
		return e
	}
	b, e := CanonicalJSON([]byte(Str(j["execute_body"])))
	if e != nil {
		return e
	}
	j["execute_body"] = string(b)
	return nil
}
func (s *Store) SaveJob(ctx context.Context, id int64, j Row) (int64, error) {
	if e := validateJob(j); e != nil {
		return 0, e
	}
	now := time.Now().UTC()
	var next any
	if Bool(j["enabled"]) {
		n, e := LegacyNext(Str(j["cron_expression"]), now)
		if e != nil {
			return 0, e
		}
		next = n
	}
	e := s.tx(ctx, func(tx *sql.Tx) error {
		if id > 0 {
			old, e := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", id)
			if e != nil {
				return e
			}
			if Str(old["managed_key"]) != "" {
				return fmt.Errorf("%w: managed jobs must use task API", ErrConflict)
			}
			if _, e = tx.ExecContext(ctx, "UPDATE messages SET status='cancelled',finished_at=? WHERE job_id=? AND job_version>0 AND status IN ('waiting','ready')", now, id); e != nil {
				return e
			}
			_, e = tx.ExecContext(ctx, "UPDATE jobs SET name=?,environment=?,cron_expression=?,execute_method=?,execute_url=?,execute_headers=?,execute_body=?,health_check_method='HEAD',health_check_url=?,health_check_headers=?,health_check_body='{}',enabled=?,updated_at=?,broker_next_at=?,broker_version=broker_version+1,broker_health_version=broker_health_version+1 WHERE id=?", j["name"], j["environment"], j["cron_expression"], j["execute_method"], j["execute_url"], j["execute_headers"], j["execute_body"], HealthURL(Str(j["execute_url"])), j["execute_headers"], Bool(j["enabled"]), now, next, id)
			if e != nil {
				return e
			}
		} else {
			r, e := tx.ExecContext(ctx, "INSERT INTO jobs(name,environment,cron_expression,execute_method,execute_url,execute_headers,execute_body,health_check_method,health_check_url,health_check_headers,health_check_body,enabled,worker_status,created_at,updated_at,broker_next_at) VALUES (?,?,?,?,?,?,?,'HEAD',?,?,'{}',?,'unknown',?,?,?)", j["name"], j["environment"], j["cron_expression"], j["execute_method"], j["execute_url"], j["execute_headers"], j["execute_body"], HealthURL(Str(j["execute_url"])), j["execute_headers"], Bool(j["enabled"]), now, now, next)
			if e != nil {
				return e
			}
			id, _ = r.LastInsertId()
		}
		row, e := one(ctx, tx, "SELECT * FROM jobs WHERE id=?", id)
		if e != nil {
			return e
		}
		return ensureJobMessage(ctx, tx, row)
	})
	return id, e
}
func (s *Store) Toggle(ctx context.Context, id int64) (Row, error) {
	e := s.tx(ctx, func(tx *sql.Tx) error {
		j, e := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", id)
		if e != nil {
			return e
		}
		if Str(j["managed_key"]) != "" {
			_, e = tx.ExecContext(ctx, "UPDATE jobs SET managed_paused=NOT managed_paused WHERE id=?", id)
			return e
		}
		j["enabled"] = !Bool(j["enabled"])
		j["broker_version"] = Int(j["broker_version"]) + 1
		var next any
		if Bool(j["enabled"]) {
			n, e := LegacyNext(Str(j["cron_expression"]), time.Now())
			if e != nil {
				return e
			}
			next = n
		}
		j["broker_next_at"] = next
		if _, e = tx.ExecContext(ctx, "UPDATE jobs SET enabled=?,broker_next_at=?,broker_version=?,updated_at=? WHERE id=?", j["enabled"], next, j["broker_version"], time.Now().UTC(), id); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "UPDATE messages SET status='cancelled',finished_at=? WHERE job_id=? AND job_version>0 AND status IN ('waiting','ready')", time.Now().UTC(), id); e != nil {
			return e
		}
		return ensureJobMessage(ctx, tx, j)
	})
	if e != nil {
		return nil, e
	}
	return s.Job(ctx, id)
}
func (s *Store) DeleteJob(ctx context.Context, id int64, name string) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		j, e := one(ctx, tx, "SELECT * FROM jobs WHERE id=? FOR UPDATE", id)
		if e != nil {
			return e
		}
		if Str(j["managed_key"]) != "" {
			return ErrConflict
		}
		if name != Str(j["name"]) {
			return fmt.Errorf("Job name confirmation does not match")
		}
		if Time(j["broker_busy_until"]).After(time.Now()) {
			return fmt.Errorf("%w: job running", ErrConflict)
		}
		for _, table := range []string{"execution_records", "worker_status_changes"} {
			if _, e = tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE job_id=?", id); e != nil {
				return e
			}
		}
		_, e = tx.ExecContext(ctx, "DELETE FROM jobs WHERE id=?", id)
		return e
	})
}
