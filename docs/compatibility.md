# JobScheduler compatibility and cutover

## Preserved

- Existing MySQL tables `jobs`, `execution_records`, `worker_status_changes`, their IDs, definitions, header/body JSON, health state and historical rows remain in place. Both the ordinary legacy schema and the managed-delivery schema are accepted. New blank installations create these same tables.
- Existing HTML form field names/routes and legacy JSON APIs; new/edit validation, delete-by-name confirmation, synchronous manual Run Once, toggle, health refresh, execution response display and status history.
- Existing HTML/CSS/JS retained, rendered by Go's Gonja engine. No Python process is needed. Small template adaptations replace request-method syntax and add navigation tabs; Name/Cron split row, full URL input and UUID cursor insertion remain.
- Cron: five or six fields, optional leading seconds, Asia/Shanghai, Monday=0, all day/weekday restrictions combined with AND. Named weekdays/ranges/steps and `last` day supported. Oracle fixtures use APScheduler 3 to check these rules. New cron editing continues using legacy semantics, not robfig's default weekday rules.
- Original jobs: HEAD `{execute URL}/_health` before each execution, request headers applied, only live 2xx health permits callback. One active execution per job, manual duplicate run rejected, pending obsolete cron runs skipped. Cron requests remain single-attempt to avoid changing existing business retry assumptions.
- Automatic health observations every minute, three consecutive identical results before changing stored state; manual refresh immediately updates it and clears the streak. `HEALTH_ALERT_AFTER_SECONDS` override retained. Preflight observations participate as before.
- UUID placeholders resolved once when creating each periodic occurrence; body/headers remain stable within that message. Explicit one-time API payloads are never templated.
- Seven-day completed execution record cleanup retained; pending records are not purged. Message records/dedupe keys are retained separately.
- Lark failure and worker-status change notifications use the existing optional webhook setting. Response previews are bounded and configured header values are redacted.

## Database changes

Startup serializes schema migration with a MySQL named lock. It adds `broker_*` progress, busy-lease and health sequencing fields to `jobs`; adds managed columns when absent; allows `pending` and nullable finish time in execution records; creates `topics`, `messages`, `message_waits`, `health_changes`. It does not rename/drop old columns/tables or reset data. Back up before applying schema changes.

An active periodic job holds one upcoming timed message. Its promotion and the creation of the next occurrence share a transaction locked on the job. Schedule edits invalidate older waiting/ready occurrences by version. A callback already in flight cannot be revoked. Callback network I/O happens after transaction commit.

Managed schedules retain their existing records. Outstanding managed `pending` records are imported into the new worker using the original delivery IDs; successful history is not replayed. They retain version/paused/enabled checks and their old callback format.

## Safe replacement procedure

1. Capture the actual deployed revision and a backup of its schema/data. Restore a copy to an isolated MySQL 8 test database, preserving representative ordinary/managed jobs and history.
2. Start Go with `DISPATCH_ENABLED=false` on that copy. This setting still applies schema migrations and allows explicit manual actions; it is not a read-only mode. Point callback URLs and webhook secrets at isolated test receivers before enabling dispatch.
3. Verify pages and API consumers, compare next trigger times, run controlled manual jobs, test one-time sends/cancellation/retries, and simulate restart. Confirm network reachability/credentials separately.
4. Stop the Python scheduler; allow or explicitly reconcile in-flight HTTP operations. Never overlap old and new schedulers on the same active DB. Take a final backup.
5. Deploy Go with the same DATABASE_URL, port, tokens, webhook and managed integration settings. Enable dispatch and verify histories and next occurrences. This production step requires separate user approval.
6. For rollback, stop Go first. Resolve pending Go deliveries so old callbacks cannot be unintentionally repeated. The ordinary pre-managed Python version cannot read a `pending` enum row: settle/reconcile all pending execution records before rollback, or roll back to the managed-aware release. Do not blindly run both versions.

Single-server deployment is supported. Database leases coordinate message claims across Go instances; health probing runs in each process and is intended for the initial single-instance deployment. Before a multi-replica rollout, centralize health scheduling so "three observations" continues to mean the same elapsed time. No exactly-once external side-effect guarantee and no ordered messaging are claimed.
