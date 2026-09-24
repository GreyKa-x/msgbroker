package testutil

import (
	"context"
	"database/sql"
	"github.com/GreyKa-x/msgbroker/internal/broker"
	"github.com/go-sql-driver/mysql"
	"os"
	"testing"
)

func DB(t *testing.T) *broker.Store    { return db(t, true) }
func RawDB(t *testing.T) *broker.Store { return db(t, false) }
func db(t *testing.T, migrate bool) *broker.Store {
	t.Helper()
	raw := os.Getenv("TEST_MYSQL_DSN")
	if raw == "" {
		t.Skip("TEST_MYSQL_DSN required for database integration test")
	}
	cfg, e := mysql.ParseDSN(raw)
	if e != nil {
		t.Fatal(e)
	}
	cfg.DBName = ""
	admin, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	name := broker.ID("msgbroker_test_")
	if _, e = admin.Exec("CREATE DATABASE `" + name + "`"); e != nil {
		t.Fatal(e)
	}
	cfg.DBName = name
	s, e := broker.Open(cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.DB.Close(); admin.Exec("DROP DATABASE `" + name + "`"); admin.Close() })
	if migrate {
		if e = s.Migrate(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	return s
}
