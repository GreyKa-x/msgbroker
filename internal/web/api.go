package web

import (
	"encoding/json"
	"fmt"
	"github.com/GreyKa-x/msgbroker/internal/broker"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func jobView(j broker.Row, detail bool) map[string]any {
	out := map[string]any{"id": j["id"], "name": j["name"], "environment": j["environment"], "cronExpression": j["cron_expression"], "executeMethod": j["execute_method"], "executeUrl": j["execute_url"], "enabled": broker.Bool(j["enabled"]), "workerStatus": j["worker_status"], "lastCheckedAt": j["last_checked_at"], "healthCheckUrl": j["health_check_url"], "createdAt": j["created_at"], "updatedAt": j["updated_at"]}
	if detail {
		headers := map[string]string{}
		json.Unmarshal([]byte(broker.Str(j["execute_headers"])), &headers)
		out["executeHeaders"] = broker.Redacted(broker.Topic{Headers: headers}).Headers
		var body any
		json.Unmarshal([]byte(broker.Str(j["execute_body"])), &body)
		out["executeBody"] = body
		out["healthCheckMethod"] = j["health_check_method"]
		out["healthCheckHeaders"] = out["executeHeaders"]
	}
	return out
}
func executionView(r broker.Row) map[string]any {
	return map[string]any{"id": r["id"], "jobId": r["job_id"], "status": r["status"], "statusCode": r["status_code"], "errorMessage": r["error_message"], "responseBody": r["response_body"], "startedAt": r["started_at"], "finishedAt": r["finished_at"]}
}
func changeView(r broker.Row) map[string]any {
	return map[string]any{"id": r["id"], "jobId": r["job_id"], "fromStatus": r["from_status"], "toStatus": r["to_status"], "statusCode": r["status_code"], "errorMessage": r["error_message"], "changedAt": r["changed_at"]}
}
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	p := parts(r, "/openplatform/v1")
	ctx := r.Context()
	if p[0] == "health" && r.Method == "GET" {
		write(w, 200, map[string]string{"status": "ok", "service": "jobscheduler"})
		return
	}
	if p[0] == "environments" {
		s.messagesAPI(w, r, p)
		return
	}
	if p[0] == "managed-jobs" {
		s.managedAPI(w, r, p)
		return
	}
	if p[0] != "jobs" {
		http.NotFound(w, r)
		return
	}
	if len(p) == 1 && r.Method == "GET" {
		jobs, e := s.Store.Jobs(ctx)
		if e != nil {
			fail(w, e)
			return
		}
		items := []any{}
		for _, j := range jobs {
			items = append(items, jobView(j, false))
		}
		write(w, 200, map[string]any{"items": items, "total": len(items)})
		return
	}
	if len(p) < 2 {
		http.NotFound(w, r)
		return
	}
	id, _ := strconv.ParseInt(p[1], 10, 64)
	j, e := s.Store.Job(ctx, id)
	if e != nil {
		fail(w, e)
		return
	}
	if len(p) == 2 && r.Method == "GET" {
		write(w, 200, jobView(j, true))
		return
	}
	if len(p) != 3 {
		http.NotFound(w, r)
		return
	}
	action := p[2]
	if r.Method == "GET" {
		limit := number(r.URL.Query().Get("limit"), 50)
		var rows []broker.Row
		var convert func(broker.Row) map[string]any
		if action == "executions" {
			rows, e = s.Store.Executions(ctx, id, limit)
			convert = executionView
		} else if action == "status-history" {
			rows, e = s.Store.Changes(ctx, id, limit)
			convert = changeView
		} else {
			http.NotFound(w, r)
			return
		}
		if e != nil {
			fail(w, e)
			return
		}
		items := []any{}
		for _, row := range rows {
			items = append(items, convert(row))
		}
		write(w, 200, map[string]any{"jobId": id, "items": items, "total": len(items)})
		return
	}
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	out := map[string]any{"ok": true, "jobId": id}
	switch action {
	case "refresh-health":
		var status string
		status, e = s.Engine.ProbeJob(ctx, id, true)
		out["workerStatus"] = status
	case "toggle":
		if broker.Str(j["managed_key"]) != "" {
			e = broker.ErrConflict
			break
		}
		j, e = s.Store.Toggle(ctx, id)
		if e == nil {
			out["enabled"] = broker.Bool(j["enabled"])
			out["workerStatus"] = j["worker_status"]
			out["saved"] = true
			out["scheduled"] = true
			out["message"] = "Configuration saved and schedule applied."
		}
	case "run":
		var rec broker.Row
		rec, e = s.Engine.RunJob(ctx, id)
		if e == nil {
			out["execution"] = executionView(rec)
			out["enabled"] = broker.Bool(j["enabled"])
			updated, err := s.Store.Job(ctx, id)
			if err == nil {
				out["workerStatus"] = updated["worker_status"]
			}
		}
	default:
		http.NotFound(w, r)
		return
	}
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 200, out)
}
func (s *Server) messagesAPI(w http.ResponseWriter, r *http.Request, p []string) {
	if len(p) < 3 || !broker.ValidEnvironment(p[1]) {
		fail(w, fmt.Errorf("invalid environment"))
		return
	}
	env := p[1]
	ctx := r.Context()
	if p[2] == "topics" {
		if len(p) == 3 && r.Method == "GET" {
			rows, e := s.Store.Topics(ctx, env)
			if e != nil {
				fail(w, e)
				return
			}
			for i := range rows {
				rows[i] = broker.Redacted(rows[i])
			}
			write(w, 200, map[string]any{"items": rows})
			return
		}
		if len(p) == 4 {
			switch r.Method {
			case "GET":
				t, e := s.Store.Topic(ctx, env, p[3])
				if e != nil {
					fail(w, e)
					return
				}
				write(w, 200, broker.Redacted(t))
				return
			case "PUT":
				var t broker.Topic
				if e := decode(r, &t); e != nil {
					fail(w, e)
					return
				}
				t.Environment = env
				t.Name = p[3]
				out, e := s.Store.SaveTopic(ctx, t)
				if e != nil {
					fail(w, e)
					return
				}
				write(w, 200, broker.Redacted(out))
				return
			case "DELETE":
				if e := s.Store.DeleteTopic(ctx, env, p[3]); e != nil {
					fail(w, e)
					return
				}
				write(w, 200, map[string]bool{"ok": true})
				return
			}
		}
	}
	if p[2] == "messages" {
		if len(p) == 3 {
			switch r.Method {
			case "POST":
				var req broker.SendRequest
				if e := decode(r, &req); e != nil {
					fail(w, e)
					return
				}
				m, e := s.Store.Send(ctx, env, req)
				if e != nil {
					fail(w, e)
					return
				}
				write(w, 200, map[string]any{"messageId": m.ID, "status": m.Status})
				return
			case "GET":
				rows, e := s.Store.Messages(ctx, env, filter(r))
				if e != nil {
					fail(w, e)
					return
				}
				write(w, 200, map[string]any{"items": rows})
				return
			}
		}
		if len(p) == 4 && r.Method == "GET" {
			m, e := s.Store.Message(ctx, env, p[3])
			if e != nil {
				fail(w, e)
				return
			}
			write(w, 200, m)
			return
		}
		if len(p) == 5 && r.Method == "POST" {
			var e error
			switch p[4] {
			case "cancel":
				e = s.Store.Cancel(ctx, env, p[3])
			case "retry":
				e = s.Store.Retry(ctx, env, p[3])
			default:
				http.NotFound(w, r)
				return
			}
			if e != nil {
				fail(w, e)
				return
			}
			write(w, 200, map[string]bool{"ok": true})
			return
		}
	}
	http.NotFound(w, r)
}
func filter(r *http.Request) broker.MessageFilter {
	q := r.URL.Query()
	before, _ := time.Parse(time.RFC3339Nano, q.Get("before"))
	return broker.MessageFilter{OnlyTimed: true, BeforeID: q.Get("beforeId"), Topic: q.Get("topic"), Status: q.Get("status"), ScheduleID: q.Get("jobId"), Limit: number(q.Get("limit"), 100), Before: before}
}
func (s *Server) managedAPI(w http.ResponseWriter, r *http.Request, p []string) {
	ctx := r.Context()
	if len(p) == 1 && r.Method == "GET" {
		out, e := s.Store.Managed(ctx)
		if e != nil {
			fail(w, e)
			return
		}
		write(w, 200, out)
		return
	}
	if len(p) < 2 {
		http.NotFound(w, r)
		return
	}
	key := p[1]
	if len(p) == 2 && r.Method == "PUT" {
		var req broker.ManagedSpec
		if e := decode(r, &req); e != nil {
			fail(w, e)
			return
		}
		out, e := s.Store.UpsertManaged(ctx, key, req)
		if e != nil {
			fail(w, e)
			return
		}
		write(w, 200, out)
		return
	}
	if len(p) == 3 && p[2] == "deliveries" && r.Method == "GET" {
		out, e := s.Store.ManagedDeliveries(ctx, key)
		if e != nil {
			fail(w, e)
			return
		}
		write(w, 200, out)
		return
	}
	if len(p) == 3 && p[2] == "pause" && r.Method == "POST" {
		v := r.URL.Query().Get("paused")
		pause, e := strconv.ParseBool(v)
		if e != nil {
			fail(w, fmt.Errorf("invalid paused"))
			return
		}
		if e = s.Store.PauseManaged(ctx, key, pause); e != nil {
			fail(w, e)
			return
		}
		write(w, 200, map[string]bool{"paused": pause})
		return
	}
	http.NotFound(w, r)
}
func htmlJob(j broker.Row) broker.Row {
	out := broker.Row{}
	for k, v := range j {
		out[k] = v
	}
	out["enabled"] = broker.Bool(j["enabled"])
	out["managed_paused"] = broker.Bool(j["managed_paused"])
	out["worker_status"] = map[string]any{"value": j["worker_status"]}
	return out
}
func htmlExecution(r broker.Row) broker.Row {
	out := broker.Row{}
	for k, v := range r {
		out[k] = v
	}
	out["status"] = map[string]any{"value": r["status"]}
	return out
}
func parseHeaders(raw string) (map[string]string, error) {
	h := map[string]string{}
	e := json.Unmarshal([]byte(strings.TrimSpace(raw)), &h)
	if h == nil && e == nil {
		e = fmt.Errorf("invalid headers")
	}
	return h, e
}
