package broker

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *Store) upgradeLegacy(ctx context.Context, c *sql.Conn) error {
	additions := map[string]map[string]string{
		"jobs":              {"environment": "VARCHAR(4) NULL", "managed_key": "VARCHAR(160) NULL", "managed_version": "INT DEFAULT 0", "managed_definition": "TEXT NULL", "managed_next_at": "DATETIME(6) NULL", "managed_paused": "BOOLEAN DEFAULT FALSE", "broker_next_at": "DATETIME(6) NULL", "broker_version": "INT NOT NULL DEFAULT 1", "broker_busy_until": "DATETIME(6) NULL", "broker_health_raw": "VARCHAR(16) NOT NULL DEFAULT 'unknown'", "broker_health_streak": "INT NOT NULL DEFAULT 0", "broker_health_version": "BIGINT NOT NULL DEFAULT 0"},
		"execution_records": {"response_body": "TEXT NULL", "delivery_id": "VARCHAR(32) NULL", "version": "INT DEFAULT 0", "scheduled_at": "DATETIME(6) NULL", "retry_at": "DATETIME(6) NULL", "attempts": "INT DEFAULT 0"},
	}
	for table, columns := range additions {
		for name, definition := range columns {
			var n int
			if e := c.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name=?", table, name).Scan(&n); e != nil {
				return e
			}
			if n == 0 {
				if _, e := c.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, name, definition)); e != nil {
					return e
				}
			}
		}
	}
	for _, index := range []struct{ table, column, name string }{{"jobs", "managed_key", "broker_managed_key"}, {"execution_records", "delivery_id", "broker_delivery_id"}} {
		var n int
		if e := c.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? AND column_name=? AND non_unique=0", index.table, index.column).Scan(&n); e != nil {
			return e
		}
		if n == 0 {
			if _, e := c.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD UNIQUE KEY %s (%s)", index.table, index.name, index.column)); e != nil {
				return e
			}
		}
	}
	// Additive operational metadata; retain names, IDs, headers, histories and legacy nullable fields.
	if _, e := c.ExecContext(ctx, "UPDATE jobs SET environment=CASE WHEN execute_url LIKE '%.test.%' OR name REGEXP '[(（]test[)）]$' THEN 'test' ELSE 'prod' END WHERE environment IS NULL"); e != nil {
		return e
	}
	for _, q := range []string{"ALTER TABLE jobs MODIFY name VARCHAR(255) NOT NULL", "ALTER TABLE execution_records MODIFY status ENUM('pending','success','failed') NOT NULL", "ALTER TABLE execution_records MODIFY finished_at DATETIME(6) NULL"} {
		if _, e := c.ExecContext(ctx, q); e != nil {
			return e
		}
	}
	return nil
}
