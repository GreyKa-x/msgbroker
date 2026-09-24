package broker

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

//go:embed schema.sql
var schema string

//go:embed legacy.sql
var legacySchema string

type Store struct{ DB *sql.DB }

func Open(dsn string) (*Store, error) {
	cfg, e := mysql.ParseDSN(dsn)
	if e != nil {
		return nil, fmt.Errorf("invalid MYSQL_DSN")
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	db, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(3 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, fmt.Errorf("database unavailable")
	}
	return &Store{db}, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	c, e := s.DB.Conn(ctx)
	if e != nil {
		return e
	}
	defer c.Close()
	var locked int
	if e = c.QueryRowContext(ctx, "SELECT GET_LOCK('msgbroker_schema', 30)").Scan(&locked); e != nil || locked != 1 {
		return fmt.Errorf("migration lock unavailable")
	}
	defer c.ExecContext(context.Background(), "SELECT RELEASE_LOCK('msgbroker_schema')")
	for _, q := range strings.Split(legacySchema, ";") {
		if strings.TrimSpace(q) != "" {
			if _, e = c.ExecContext(ctx, q); e != nil {
				return e
			}
		}
	}
	if e = s.upgradeLegacy(ctx, c); e != nil {
		return e
	}
	for _, q := range strings.Split(schema, ";") {
		if strings.TrimSpace(q) != "" {
			if _, e = c.ExecContext(ctx, q); e != nil {
				return e
			}
		}
	}
	return nil
}
func (s *Store) tx(ctx context.Context, f func(*sql.Tx) error) error {
	t, e := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return e
	}
	defer t.Rollback()
	if e = f(t); e != nil {
		return e
	}
	return t.Commit()
}
func duplicate(e error) bool { var m *mysql.MySQLError; return errors.As(e, &m) && m.Number == 1062 }
func missing(e error) error {
	if errors.Is(e, sql.ErrNoRows) {
		return ErrNotFound
	}
	return e
}

const topicColumns = "id,environment,name,url,method,headers,timeout_seconds,max_attempts,enabled,health_url,health_status,last_checked_at,created_at"

type scanner interface{ Scan(...any) error }

func scanTopic(r scanner) (Topic, error) {
	var t Topic
	var h string
	e := r.Scan(&t.ID, &t.Environment, &t.Name, &t.URL, &t.Method, &h, &t.TimeoutSeconds, &t.MaxAttempts, &t.Enabled, &t.HealthURL, &t.HealthStatus, &t.LastCheckedAt, &t.CreatedAt)
	if e == nil {
		e = json.Unmarshal([]byte(h), &t.Headers)
	}
	return t, missing(e)
}
func (s *Store) Topic(ctx context.Context, env, name string) (Topic, error) {
	return scanTopic(s.DB.QueryRowContext(ctx, "SELECT "+topicColumns+" FROM topics WHERE environment=? AND name=?", env, name))
}
func (s *Store) Topics(ctx context.Context, env string) ([]Topic, error) {
	rows, e := s.DB.QueryContext(ctx, "SELECT "+topicColumns+" FROM topics WHERE environment=? ORDER BY name", env)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Topic{}
	for rows.Next() {
		t, e := scanTopic(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
func (s *Store) SaveTopic(ctx context.Context, t Topic) (Topic, error) {
	if e := ValidateTopic(&t); e != nil {
		return t, e
	}
	e := s.tx(ctx, func(tx *sql.Tx) error {
		old, e := scanTopic(tx.QueryRowContext(ctx, "SELECT "+topicColumns+" FROM topics WHERE environment=? AND name=? FOR UPDATE", t.Environment, t.Name))
		if e != nil && !errors.Is(e, ErrNotFound) {
			return e
		}
		if errors.Is(e, ErrNotFound) {
			t.ID = ID("top_")
			t.CreatedAt = time.Now().UTC()
			if t.Headers == nil {
				t.Headers = map[string]string{}
			}
			h, _ := json.Marshal(t.Headers)
			_, e = tx.ExecContext(ctx, "INSERT INTO topics ("+topicColumns+") VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,?)", t.ID, t.Environment, t.Name, t.URL, t.Method, h, t.TimeoutSeconds, t.MaxAttempts, t.Enabled, t.HealthURL, "unknown", t.CreatedAt)
			return e
		}
		t.ID = old.ID
		t.CreatedAt = old.CreatedAt
		if t.Headers == nil {
			t.Headers = old.Headers
		}
		h, _ := json.Marshal(t.Headers)
		_, e = tx.ExecContext(ctx, "UPDATE topics SET url=?,method=?,headers=?,timeout_seconds=?,max_attempts=?,enabled=?,health_url=?,health_version=health_version+1,health_streak=0 WHERE id=?", t.URL, t.Method, h, t.TimeoutSeconds, t.MaxAttempts, t.Enabled, t.HealthURL, t.ID)
		return e
	})
	if duplicate(e) {
		return t, ErrConflict
	}
	return t, e
}
func (s *Store) DeleteTopic(ctx context.Context, env, name string) error {
	return s.tx(ctx, func(t *sql.Tx) error {
		var id string
		if e := t.QueryRowContext(ctx, "SELECT id FROM topics WHERE environment=? AND name=? FOR UPDATE", env, name).Scan(&id); e != nil {
			return missing(e)
		}
		var n int
		if e := t.QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM messages WHERE topic_id=?)", id).Scan(&n); e != nil {
			return e
		}
		if n > 0 {
			return fmt.Errorf("%w: topic has retained records; pause instead", ErrConflict)
		}
		if _, e := t.ExecContext(ctx, "DELETE FROM health_changes WHERE topic_id=?", id); e != nil {
			return e
		}
		_, e := t.ExecContext(ctx, "DELETE FROM topics WHERE id=?", id)
		return e
	})
}
func Redacted(t Topic) Topic {
	h := map[string]string{}
	for k, v := range t.Headers {
		if strings.EqualFold(k, "Content-Type") || strings.EqualFold(k, "Accept") {
			h[k] = v
		} else {
			h[k] = "[redacted]"
		}
	}
	t.Headers = h
	return t
}
