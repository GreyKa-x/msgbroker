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

func (s *Server) html(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" && r.Method == "GET" {
		s.home(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/messages") || r.URL.Path == "/topics" {
		s.messagePages(w, r)
		return
	}
	p := parts(r, "/jobs")
	if r.URL.Path == "/jobs/new" && r.Method == "GET" {
		s.form(w, r, nil, "/jobs", "", 200)
		return
	}
	if r.URL.Path == "/jobs" && r.Method == "POST" {
		s.saveJob(w, r, 0)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/jobs/") || len(p) == 0 {
		http.NotFound(w, r)
		return
	}
	id, e := strconv.ParseInt(p[0], 10, 64)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	j, e := s.Store.Job(r.Context(), id)
	if e != nil {
		fail(w, e)
		return
	}
	if len(p) == 1 && r.Method == "POST" {
		s.saveJob(w, r, id)
		return
	}
	if len(p) == 4 && p[1] == "deliveries" && p[3] == "retry" && r.Method == "POST" {
		rec, _ := strconv.ParseInt(p[2], 10, 64)
		e = s.Store.RetryManaged(r.Context(), id, rec)
		if e != nil {
			fail(w, e)
			return
		}
		redirect(w, r, fmt.Sprintf("/jobs/%d/executions", id), "", "")
		return
	}
	if len(p) != 2 {
		http.NotFound(w, r)
		return
	}
	action := p[1]
	if r.Method == "GET" {
		switch action {
		case "edit":
			s.form(w, r, j, fmt.Sprintf("/jobs/%d", id), "", 200)
			return
		case "executions":
			runs, e := s.Store.Executions(r.Context(), id, 200)
			if e != nil {
				fail(w, e)
				return
			}
			out := []broker.Row{}
			for _, row := range runs {
				out = append(out, htmlExecution(row))
			}
			s.render(w, r, "executions.html", map[string]any{"job": htmlJob(j), "executions": out}, 200)
			return
		case "status-history":
			rows, e := s.Store.Changes(r.Context(), id, 200)
			if e != nil {
				fail(w, e)
				return
			}
			for _, row := range rows {
				row["changed_at"] = htmlValue(row["changed_at"])
				row["from_status"] = map[string]any{"value": row["from_status"]}
				row["to_status"] = map[string]any{"value": row["to_status"]}
			}
			s.render(w, r, "status_history.html", map[string]any{"job": htmlJob(j), "changes": rows}, 200)
			return
		}
		http.NotFound(w, r)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	to := "/"
	message := ""
	kind := "success"
	switch action {
	case "toggle":
		_, e = s.Store.Toggle(r.Context(), id)
	case "refresh-health":
		var status string
		status, e = s.Engine.ProbeJob(r.Context(), id, true)
		message = "Health status refreshed: " + status + "."
		if status != "healthy" {
			kind = "error"
		}
	case "run":
		var rec broker.Row
		rec, e = s.Engine.RunJob(r.Context(), id)
		to = fmt.Sprintf("/jobs/%d/executions", id)
		message = "Run triggered successfully."
		if e != nil || rec["status"] != "success" {
			kind = "error"
			message = "Run failed. View the execution record for details."
		}
	case "delete":
		e = s.Store.DeleteJob(r.Context(), id, r.FormValue("job_name_confirmation"))
	default:
		http.NotFound(w, r)
		return
	}
	if e != nil {
		fail(w, e)
		return
	}
	redirect(w, r, to, message, kind)
}
func (s *Server) form(w http.ResponseWriter, r *http.Request, j broker.Row, action, err string, status int) {
	ctx := map[string]any{"form_action": action, "error": err, "methods": []string{"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"}, "headers_enabled": false, "body_enabled": false}
	rows := []map[string]string{}
	if j != nil {
		ctx["job"] = htmlJob(j)
		h := map[string]string{}
		json.Unmarshal([]byte(broker.Str(j["execute_headers"])), &h)
		for k, v := range h {
			rows = append(rows, map[string]string{"key": k, "value": v})
		}
		ctx["headers_enabled"] = len(h) > 0
		b := strings.TrimSpace(broker.Str(j["execute_body"]))
		ctx["body_enabled"] = b != "" && b != "{}"
	}
	if len(rows) == 0 {
		rows = append(rows, map[string]string{"key": "", "value": ""})
	}
	ctx["header_rows"] = rows
	s.render(w, r, "job_form.html", ctx, status)
}
func (s *Server) saveJob(w http.ResponseWriter, r *http.Request, id int64) {
	j := broker.Row{}
	for _, key := range []string{"name", "environment", "cron_expression", "execute_method", "execute_url", "execute_headers", "execute_body"} {
		j[key] = r.FormValue(key)
	}
	j["enabled"] = r.FormValue("enabled") == "true"
	if id > 0 {
		j["id"] = id
	}
	if j["execute_headers"] == "" {
		j["execute_headers"] = "{}"
	}
	if j["execute_body"] == "" {
		j["execute_body"] = "{}"
	}
	_, e := s.Store.SaveJob(r.Context(), id, j)
	if e != nil {
		s.form(w, r, j, r.URL.Path, e.Error(), 400)
		return
	}
	redirect(w, r, "/", "", "")
}
func (s *Server) home(w http.ResponseWriter, r *http.Request) {
	jobs, e := s.Store.Jobs(r.Context())
	if e != nil {
		fail(w, e)
		return
	}
	hours := number(r.URL.Query().Get("hours"), 24)
	switch hours {
	case 1, 6, 12, 24, 72, 168:
	default:
		hours = 24
	}
	htmlJobs := []broker.Row{}
	byID := map[int64]broker.Row{}
	for _, j := range jobs {
		view := htmlJob(j)
		htmlJobs = append(htmlJobs, view)
		byID[broker.Int(j["id"])] = view
		if broker.Str(j["managed_key"]) != "" {
			var spec any
			json.Unmarshal([]byte(broker.Str(j["managed_definition"])), &spec)
			view["managed_schedule"] = spec
		}
	}
	rows, e := broker.Rows(r.Context(), s.Store.DB, "SELECT e.*,s.success_count,s.failed_count FROM execution_records e JOIN (SELECT job_id,MAX(id) latest_id,SUM(status='success') success_count,SUM(status='failed') failed_count FROM execution_records WHERE started_at>=? GROUP BY job_id) s ON s.latest_id=e.id ORDER BY e.started_at DESC", time.Now().UTC().Add(-time.Duration(hours)*time.Hour))
	if e != nil {
		fail(w, e)
		return
	}
	summaries := []map[string]any{}
	for _, row := range rows {
		v := htmlExecution(row)
		v["job"] = byID[broker.Int(row["job_id"])]
		summaries = append(summaries, map[string]any{"execution": v, "success_count": broker.Int(row["success_count"]), "failed_count": broker.Int(row["failed_count"])})
	}
	s.render(w, r, "jobs.html", map[string]any{"jobs": htmlJobs, "recent_execution_summaries": summaries, "window_hours": hours, "window_options": []int{1, 6, 12, 24, 72, 168}}, 200)
}
func (s *Server) messagePages(w http.ResponseWriter, r *http.Request) {
	environment := env(r)
	ctx := r.Context()
	tab := "messages"
	if r.URL.Path == "/topics" {
		tab = "topics"
	}
	var err error
	if r.Method == "POST" {
		if tab == "topics" {
			t := broker.Topic{Environment: environment, Name: r.FormValue("name"), URL: r.FormValue("url"), Method: r.FormValue("method"), Enabled: r.FormValue("enabled") == "true", HealthURL: r.FormValue("healthUrl"), TimeoutSeconds: number(r.FormValue("timeoutSeconds"), 10), MaxAttempts: number(r.FormValue("maxAttempts"), 16)}
			if h := r.FormValue("headers"); strings.TrimSpace(h) != "" {
				t.Headers, err = parseHeaders(h)
			}
			if err == nil {
				_, err = s.Store.SaveTopic(ctx, t)
			}
		} else if r.URL.Path == "/messages" {
			at, e := time.Parse(time.RFC3339Nano, r.FormValue("runAt"))
			err = e
			if err == nil {
				_, err = s.Store.Send(ctx, environment, broker.SendRequest{Topic: r.FormValue("topic"), UniqueID: r.FormValue("uniqueId"), RunAt: at, Payload: json.RawMessage(r.FormValue("payload"))})
			}
		} else {
			p := parts(r, "/messages")
			if len(p) != 2 {
				http.NotFound(w, r)
				return
			}
			switch p[1] {
			case "cancel":
				err = s.Store.Cancel(ctx, environment, p[0])
			case "retry":
				_, err = s.Store.Redeliver(ctx, environment, p[0])
			default:
				http.NotFound(w, r)
				return
			}
		}
		if err == nil {
			redirect(w, r, "/"+tab, "Saved successfully.", "success")
			return
		}
	}
	topics, e := s.Store.Topics(ctx, environment)
	if e != nil {
		fail(w, e)
		return
	}
	out := []any{}
	for _, t := range topics {
		out = append(out, mapJSON(broker.Redacted(t)))
	}
	data := map[string]any{"tab": tab, "topics": out, "edit": map[string]any{}}
	code := 200
	if err != nil {
		data["error"] = err.Error()
		code = 400
	}
	if tab == "topics" {
		if r.Method == "POST" && err != nil {
			data["edit"] = map[string]any{"name": r.FormValue("name"), "url": r.FormValue("url"), "method": r.FormValue("method"), "timeoutSeconds": r.FormValue("timeoutSeconds"), "maxAttempts": r.FormValue("maxAttempts"), "healthUrl": r.FormValue("healthUrl"), "enabled": r.FormValue("enabled") == "true"}
		}
		if name := r.URL.Query().Get("edit"); name != "" {
			t, e := s.Store.Topic(ctx, environment, name)
			if e != nil {
				fail(w, e)
				return
			}
			data["edit"] = mapJSON(broker.Redacted(t))
		}
		s.render(w, r, "topics.html", data, code)
		return
	}
	if r.Method == "GET" && r.URL.Path == "/messages/new" || r.Method == "POST" && r.URL.Path == "/messages" && err != nil {
		data["unique_id"] = broker.ID("request_")
		data["run_at"] = time.Now().Add(time.Minute).Format(time.RFC3339)
		data["payload"] = "{}"
		if r.Method == "POST" {
			data["unique_id"] = r.FormValue("uniqueId")
			data["run_at"] = r.FormValue("runAt")
			data["payload"] = r.FormValue("payload")
		}
		data["selected_topic"] = r.FormValue("topic")
		s.render(w, r, "message_form.html", data, code)
		return
	}
	f := filter(r)
	if r.URL.Path == "/messages/failed" {
		f.Status = "completed"
		f.Result = "failed"
		data["tab"] = "failed"
	}
	data["list_path"] = "/messages"
	if r.URL.Path == "/messages/failed" {
		data["list_path"] = "/messages/failed"
	}
	msgs, e := s.Store.Messages(ctx, environment, f)
	if e != nil {
		fail(w, e)
		return
	}
	views := []any{}
	for _, m := range msgs {
		if m.ScheduleID != "" {
			continue
		}
		v := mapJSON(m)
		var b strings.Builder
		var payload any
		json.Unmarshal(m.Payload, &payload)
		p, _ := json.MarshalIndent(payload, "", "  ")
		b.Write(p)
		v["payload_text"] = b.String()
		views = append(views, v)
	}
	data["messages"] = views
	data["topic_filter"] = f.Topic
	data["status_filter"] = f.Status
	if len(msgs) > 0 {
		data["next_before"] = msgs[len(msgs)-1].CreatedAt.Format(time.RFC3339Nano)
		data["next_id"] = msgs[len(msgs)-1].ID
	}
	s.render(w, r, "messages.html", data, code)
}
