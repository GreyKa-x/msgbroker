# HTTP API

Use the existing `Authorization: Bearer <AGENT_API_TOKEN>` or `X-Agent-Token`. Configure this token in a real deployment. As in the old service, an unset token leaves the internal API open. These are administrator/service credentials with access to both environments; fine-grained tenant ACLs are not provided.

Environment is mandatory in the new path: `/openplatform/v1/environments/{test|prod}`. The following paths are relative to that prefix. Producer registration and an SDK are not required.

## Topic configuration

`PUT /topics/{name}` creates or replaces configuration:

```json
{
  "url": "https://callback.example.com/wakeup",
  "method": "POST",
  "headers": {"Authorization": "Bearer callback-credential"},
  "timeoutSeconds": 10,
  "maxAttempts": 16,
  "enabled": true
}
```

Name: 1–128 ASCII letters/digits/dot/underscore/hyphen, beginning with a letter or digit. Timeout: 1–120 seconds; max attempts: 1–20 including the first attempt. Headers omitted preserves existing headers; `{}` clears them. Header values are redacted in JSON reads. Optional `healthUrl` is probed with HEAD every minute and shown in the UI; it does not gate timed message delivery. Topic callback configuration is read when each attempt is claimed, permitting credential rotation and endpoint repair. Do not change a topic to represent a different business operation while old messages exist.

`GET /topics`, `GET /topics/{name}`, `DELETE /topics/{name}`. Deletion is rejected if retained message records reference it. Set `enabled=false` to pause future claims; in-flight requests cannot be recalled. Multiple topics may use the same callback URL. There is no broadcast or consumer SDK.

## Create a timed message

`POST /messages`

```json
{
  "topic": "agent.wakeup",
  "uniqueId": "wake_session001_video001",
  "runAt": "2026-09-25T10:00:00+08:00",
  "payload": {"sessionId": "session001", "requestId": "video001", "reason": "检查视频生成结果"}
}
```

All fields required. `uniqueId` is caller-owned (1–160 characters); preserve it across submission retries and business retries. The `__` prefix is reserved. `runAt` is RFC3339 with offset; UTC storage has microsecond precision, past times become eligible immediately. Payload must be a JSON object, at most 256 KiB; total request limit is 1 MiB.

```json
{"messageId":"msg_...","status":"pending"}
```

The unique constraint is `(topic_id, unique_id)`; topic identity includes the environment. Repeated submissions with the same key return success and the original message ID/current status, regardless of differences in valid payload/time parameters. The original payload and time are never overwritten, including after completion, failure or cancellation. A genuinely new message requires a new key. Dedupe records are not automatically purged. This submission idempotency is provided by this service, not a guarantee of RocketMQ message keys.

The callback body is exactly the payload's JSON value; no envelope or message-ID header is forced. Whitespace/key order may be normalized. Unlike periodic templates, literal UUID placeholders in one-time payloads are not expanded. Consumer deduplication is entirely the business's responsibility.

## Query, cancel, retry

- `GET /messages`: `topic`, `uniqueId` (exact), `uniqueIdPrefix` (literal prefix, including underscores), `status`, `result`, `limit` (1–200, default 100), `before` (RFC3339), `beforeId` (tie-breaker) filters. Returns `{"items":[...]}`. For next page pass the last item's `createdAt` and `id` as `before`/`beforeId`.
- `GET /messages/{messageId}`: state, original payload/time, attempts, next available time, last HTTP status/error/response. Environment checked.
- `POST /messages/{messageId}/cancel`: pending only. Duplicate cancellation succeeds; delivering or terminal states return 409. Cannot revoke a callback already claimed.
- `POST /messages/{messageId}/retry`: completed + failed only; creates a new message with a fresh attempt budget and an internal unique key. Returns `{ "messageId": "msg_...", "status": "pending" }`. Preserves the original failure and sets its `redeliveredAs`; repeating the same action returns that same new record. To redeliver again, use the latest failed record. Administrative action; may repeat business side effects.

States: `pending → delivering → completed`. Retrying stays `delivering`, with `availableAt` for the next attempt and no active lease while waiting. Completed records carry `result=success|failed|cancelled`. The failure queue is the filtered view `status=completed&result=failed`, exposed in the UI under 定时消息 → 失败队列.

2xx acknowledges success. All unsuccessful HTTP attempts, including 4xx, network failures and response-read failures, retry. Redirects are not followed. Topic `maxAttempts` defaults to **16 total attempts including the first**, configurable 1–20. After failed attempt n, wait `5 × 2^(n−1)` seconds; there is no delay cap. With 16 attempts, 15 waits sum to **45h 30m 35s**, plus request duration and scheduling latency. After the final failure, the message enters the failure queue. No automatic further retries.

One dispatcher checks due messages every second and claims only enough work for available HTTP delivery workers. `DELIVERY_WORKERS` sets the shared concurrency for timed and periodic deliveries (positive integer, default 1024); workers do not poll the database. No FIFO ordering is promised: an in-flight message does not block another when a worker is available; retry waits release workers. All workers busy or sustained overload can still delay delivery. `runAt` is earliest eligibility, not an exact execution guarantee. Disabled Topics retain messages without delivering them.

Timeout/response loss can cause duplicates. Each attempt has a lease token; stale workers cannot overwrite a later attempt. Expired leases consume an attempt and use the same exponential backoff and total budget. Receivers must handle duplicate business execution. Stop the old dispatcher before upgrading: startup migrates only Topic-backed message states; legacy periodic/managed states and retry policies remain unchanged.

## Old API retained

`GET /openplatform/v1/health`, `GET /openplatform/v1/jobs`, `GET /openplatform/v1/jobs/{id}`, `GET .../{id}/executions?limit=50`, `GET .../{id}/status-history?limit=50`, `POST .../{id}/refresh-health`, `POST .../{id}/toggle`, `POST .../{id}/run` retain the old response fields.

Managed schedule integration retains `PUT /openplatform/v1/managed-jobs/{key}`, list, `GET .../{key}/deliveries` and `POST .../{key}/pause?paused=true`. Existing `TASK_SCHEDULER_SOURCE`, `TASK_DELIVERY_URL`, `TASK_DELIVERY_TOKEN` remain applicable. Managed callbacks keep their old body (`task_id`, `delivery_id`, `version`, `scheduled_at`) and one-hour/five-attempt retry budget.
