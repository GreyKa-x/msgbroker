CREATE TABLE IF NOT EXISTS topics (
 id VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 environment VARCHAR(4) NOT NULL,
 name VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 url TEXT NOT NULL, method VARCHAR(8) NOT NULL,
 headers TEXT NOT NULL, timeout_seconds INT NOT NULL, max_attempts INT NOT NULL,
 enabled BOOLEAN NOT NULL, health_url TEXT NOT NULL,
 health_status VARCHAR(16) NOT NULL DEFAULT 'unknown', health_raw VARCHAR(16) NOT NULL DEFAULT 'unknown',
 health_streak INT NOT NULL DEFAULT 0, health_version BIGINT NOT NULL DEFAULT 0,
 last_checked_at DATETIME(6) NULL, created_at DATETIME(6) NOT NULL,
 UNIQUE KEY topic_name(environment,name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS messages (
 id VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 topic_id VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin NULL,
 unique_id VARCHAR(160) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 request_hash CHAR(64) CHARACTER SET ascii NOT NULL,
 job_id INT NULL, job_version INT NOT NULL DEFAULT 0, record_id INT NULL,
 callback_url TEXT NOT NULL, callback_method VARCHAR(8) NOT NULL, callback_headers TEXT NOT NULL, preflight BOOLEAN NOT NULL DEFAULT FALSE,
 payload MEDIUMTEXT NOT NULL, run_at DATETIME(6) NOT NULL,
 status VARCHAR(16) NOT NULL, attempts INT NOT NULL DEFAULT 0,
 lease_token VARCHAR(40) NOT NULL DEFAULT '', lease_until DATETIME(6) NULL,
 http_status INT NOT NULL DEFAULT 0, last_error TEXT NOT NULL, response_body TEXT NOT NULL,
 created_at DATETIME(6) NOT NULL, finished_at DATETIME(6) NULL,
 FOREIGN KEY(topic_id) REFERENCES topics(id), FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE,
 UNIQUE KEY message_dedupe(topic_id,unique_id), KEY message_ready(status,created_at,id),
 KEY message_recovery(status,lease_until), KEY message_topic(topic_id,created_at,id), KEY message_job(job_id,status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS message_waits (
 message_id VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 available_at DATETIME(6) NOT NULL,
 FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE, KEY due(available_at,message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS health_changes (
 id BIGINT PRIMARY KEY AUTO_INCREMENT,
 topic_id VARCHAR(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 status VARCHAR(16) NOT NULL, checked_at DATETIME(6) NOT NULL,
 FOREIGN KEY(topic_id) REFERENCES topics(id), KEY health_history(topic_id,checked_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
