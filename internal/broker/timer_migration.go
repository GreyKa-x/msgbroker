package broker

import (
	"context"
	"database/sql"
)

// The dispatcher must be stopped during this state-machine migration.
func (s *Store) upgradeTimers(ctx context.Context, c *sql.Conn) error {
	columns := []struct{ name, definition string }{
		{"result", "VARCHAR(16) NOT NULL DEFAULT ''"},
		{"updated_at", "DATETIME(6) NULL"},
		{"redelivered_as", "VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin NULL"},
	}
	for _, col := range columns {
		var n int
		if err := c.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='messages' AND column_name=?", col.name).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			if _, err := c.ExecContext(ctx, "ALTER TABLE messages ADD COLUMN "+col.name+" "+col.definition); err != nil {
				return err
			}
		}
	}
	// Preserve due dates of retries; ready messages no longer have a wait row.
	queries := []string{
		"INSERT IGNORE INTO message_waits(message_id,available_at) SELECT id,IF(attempts=0,run_at,UTC_TIMESTAMP(6)) FROM messages WHERE topic_id IS NOT NULL AND status IN ('waiting','ready')",
		"UPDATE messages SET result=CASE status WHEN 'delivered' THEN 'success' WHEN 'failed' THEN 'failed' WHEN 'cancelled' THEN 'cancelled' ELSE '' END, status=CASE WHEN status IN ('delivered','failed','cancelled') THEN 'completed' WHEN status IN ('waiting','ready') AND attempts=0 THEN 'pending' ELSE 'delivering' END, updated_at=UTC_TIMESTAMP(6) WHERE topic_id IS NOT NULL AND status IN ('waiting','ready','delivered','failed','cancelled')",
	}
	for _, q := range queries {
		if _, err := c.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
