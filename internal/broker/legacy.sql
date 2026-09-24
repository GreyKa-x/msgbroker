CREATE TABLE IF NOT EXISTS jobs (
 id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, environment VARCHAR(4) NOT NULL,
 cron_expression VARCHAR(120) NOT NULL, execute_method VARCHAR(12) NOT NULL,
 execute_url TEXT NOT NULL, execute_headers TEXT NOT NULL, execute_body TEXT NOT NULL,
 health_check_method VARCHAR(12) NOT NULL, health_check_url TEXT NOT NULL,
 health_check_headers TEXT NOT NULL, health_check_body TEXT NOT NULL,
 enabled BOOLEAN NOT NULL, worker_status ENUM('unknown','healthy','unhealthy') NOT NULL DEFAULT 'unknown',
 last_checked_at DATETIME(6) NULL, created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
 managed_key VARCHAR(160) NULL UNIQUE, managed_version INT DEFAULT 0,
 managed_definition TEXT NULL, managed_next_at DATETIME(6) NULL, managed_paused BOOLEAN DEFAULT FALSE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS execution_records (
 id INT PRIMARY KEY AUTO_INCREMENT, job_id INT NOT NULL,
 status ENUM('pending','success','failed') NOT NULL, status_code INT NULL,
 error_message TEXT NULL, response_body TEXT NULL,
 started_at DATETIME(6) NOT NULL, finished_at DATETIME(6) NULL,
 delivery_id VARCHAR(32) NULL UNIQUE, version INT DEFAULT 0, scheduled_at DATETIME(6) NULL,
 retry_at DATETIME(6) NULL, attempts INT DEFAULT 0,
 FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS worker_status_changes (
 id INT PRIMARY KEY AUTO_INCREMENT, job_id INT NOT NULL,
 from_status ENUM('unknown','healthy','unhealthy') NOT NULL,
 to_status ENUM('unknown','healthy','unhealthy') NOT NULL,
 status_code INT NULL, error_message TEXT NULL, changed_at DATETIME(6) NOT NULL,
 FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
