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
  "maxAttempts": 5,
  "enabled": true
}
```

Name: 1–128 ASCII letters/digits/dot/underscore/hyphen, beginning with a letter or digit. Timeout: 1–120 seconds; max attempts: 1–20 including the first attempt. Headers omitted preserves existing headers; `{}` clears them. Header values are redacted in JSON reads. Topic callback configuration is read when each attempt is claimed, permitting credential rotation and endpoint repair. Do not change a topic to represent a different business operation while old messages exist.

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

All fields required. `uniqueId` is caller-owned (1–160 characters); preserve it across submission retries and business retries. The `__schedule:` prefix is reserved. `runAt` is RFC3339 with offset; UTC storage has microsecond precision, past times become eligible immediately. Payload must be a JSON object, at most 256 KiB; total request limit is 1 MiB.

```json
{"messageId":"msg_...","status":"waiting"}
```

The unique constraint is `(topic_id, unique_id)`; topic identity includes the environment. Same key and same normalized payload/time returns the existing message even after cancellation/completion. Different parameters return HTTP 409. JSON key order is ignored; JSON numeric token representations such as `1` versus `1.0` are distinct. Dedupe records are not automatically purged.

The callback body is exactly the payload's JSON value; no envelope or message-ID header is forced. Whitespace/key order may be normalized. Unlike periodic templates, literal UUID placeholders in one-time payloads are not expanded. Consumer deduplication is entirely the business's responsibility.

## Query, cancel, retry

- `GET /messages`: `topic`, `status`, `limit` (1–200, default 100), `before` (RFC3339), `beforeId` (tie-breaker) filters. Returns `{"items":[...]}`. For next page pass the last item's `createdAt` and `id` as `before`/`beforeId`.
- `GET /messages/{messageId}`: state, original payload/time, attempts, next available time, last HTTP status/error/response. Environment checked.
- `POST /messages/{messageId}/cancel`: waiting/ready only. Duplicate cancellation succeeds; delivering or terminal states return 409. Cannot revoke a callback already claimed.
- `POST /messages/{messageId}/retry`: failed only; resets attempt budget for the same immutable message, not a new send. Administrative action; may repeat business side effects.

States: `waiting → ready → delivering → delivered`; transient failure returns to `waiting`; retry exhaustion/permanent failure becomes `failed`; cancellation becomes `cancelled`.

2xx acknowledges success. Network/read failures, 408, 429 and 5xx retry; other responses fail. Redirects are not followed. Backoff is 5/10/20/40/80 seconds (capped at 80). Timeout/response loss can cause duplicates. Each claimed attempt gets a lease token; stale workers cannot overwrite a later attempt. Expired leases are retried up to the configured budget.

## Old API retained

`GET /openplatform/v1/health`, `GET /openplatform/v1/jobs`, `GET /openplatform/v1/jobs/{id}`, `GET .../{id}/executions?limit=50`, `GET .../{id}/status-history?limit=50`, `POST .../{id}/refresh-health`, `POST .../{id}/toggle`, `POST .../{id}/run` retain the old response fields.

Managed schedule integration retains `PUT /openplatform/v1/managed-jobs/{key}`, list, `GET .../{key}/deliveries` and `POST .../{key}/pause?paused=true`. Existing `TASK_SCHEDULER_SOURCE`, `TASK_DELIVERY_URL`, `TASK_DELIVERY_TOKEN` remain applicable. Managed callbacks keep their old body (`task_id`, `delivery_id`, `version`, `scheduled_at`) and one-hour/five-attempt retry budget.
