package web

import (
	"context"
	"encoding/json"
	"github.com/GreyKa-x/msgbroker/internal/broker"
	"github.com/GreyKa-x/msgbroker/internal/testutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestTemplatesParseAndEscape(t *testing.T) {
	s, e := New(nil, nil, "")
	if e != nil {
		t.Fatal(e)
	}
	r := httptest.NewRequest("GET", "/jobs/new?environment=test", nil)
	w := httptest.NewRecorder()
	s.form(w, r, broker.Row{"name": "<script>alert(1)</script>", "id": int64(1), "environment": "test", "execute_headers": "{}", "execute_body": "{}", "enabled": true}, "/jobs/1", "", 200)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	text := w.Body.String()
	if strings.Contains(text, "<script>alert(1)</script>") || !strings.Contains(text, "&lt;script&gt;") {
		t.Fatal("unescaped job name")
	}
	for _, name := range []string{"messages.html", "message_form.html", "topics.html", "topic_form.html"} {
		w = httptest.NewRecorder()
		s.render(w, r, name, map[string]any{"topics": []any{}, "messages": []any{}, "edit": map[string]any{}, "tab": "messages"}, 200)
		if w.Code != 200 {
			t.Fatalf("%s: %s", name, w.Body.String())
		}
	}
}
func TestLegacyPagesAndAPIs(t *testing.T) {
	db := testutil.DB(t)
	engine := broker.NewEngine(db)
	s, e := New(db, engine, "secret")
	if e != nil {
		t.Fatal(e)
	}
	h := s.Handler()
	form := url.Values{"name": {"<script>test</script>"}, "environment": {"test"}, "cron_expression": {"0 3 * * 0"}, "execute_method": {"POST"}, "execute_url": {"http://127.0.0.1:9999/task"}, "execute_headers": {`{"Authorization":"Bearer sensitive"}`}, "execute_body": {`{"x":1}`}}
	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 303 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, path := range []string{"/?environment=test", "/jobs/1/edit", "/jobs/1/executions", "/jobs/1/status-history", "/messages?environment=test", "/topics?environment=test"} {
		w = httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/openplatform/v1/jobs", nil))
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	for _, path := range []string{"/openplatform/v1/jobs", "/openplatform/v1/jobs/1", "/openplatform/v1/jobs/1/executions", "/openplatform/v1/jobs/1/status-history"} {
		r = httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer secret")
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 || strings.Contains(w.Body.String(), "sensitive") {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
}
func TestMessageAPIIdempotenceAndEnvironment(t *testing.T) {
	db := testutil.DB(t)
	db.SaveTopic(context.Background(), broker.Topic{Name: "wake", Environment: "test", URL: "http://127.0.0.1:9999", Enabled: true})
	s, e := New(db, broker.NewEngine(db), "")
	if e != nil {
		t.Fatal(e)
	}
	h := s.Handler()
	body := `{"topic":"wake","uniqueId":"u1","runAt":"2030-01-01T00:00:00Z","payload":{"x":1}}`
	var first string
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("POST", "/openplatform/v1/environments/test/messages", strings.NewReader(body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		var m map[string]string
		json.Unmarshal(w.Body.Bytes(), &m)
		if i == 0 {
			first = m["messageId"]
		} else if first != m["messageId"] {
			t.Fatal("duplicate")
		}
	}
	r := httptest.NewRequest("GET", "/openplatform/v1/environments/prod/messages/"+first, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	s.render(w, httptest.NewRequest("GET", "/messages?environment=test", nil), "messages.html", map[string]any{"messages": []map[string]any{{"id": first, "payload_text": "<script>bad</script>", "status": "pending", "runAt": time.Now().String()}}, "tab": "messages", "topics": []any{}}, 200)
	if w.Code != 200 || strings.Contains(w.Body.String(), "<script>bad</script>") {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestRejectCrossOriginMutation(t *testing.T) {
	s, _ := New(nil, nil, "")
	r := httptest.NewRequest("POST", "http://internal/jobs/1/delete", nil)
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatal(w.Code)
	}
}

func TestTopicListAndSeparateForm(t *testing.T) {
	db := testutil.DB(t)
	s, err := New(db, broker.NewEngine(db), "")
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	get := func(path string) string {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		return w.Body.String()
	}
	post := func(form url.Values) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/topics", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	list := get("/topics?environment=test")
	if !strings.Contains(list, "/topics/new?environment=test") || strings.Contains(list, `name="name"`) {
		t.Fatal("topic list must link to a separate creation page")
	}
	if form := get("/topics/new?environment=test"); !strings.Contains(form, `name="name"`) || !strings.Contains(form, `name="environment" value="test"`) {
		t.Fatal("creation form or environment missing")
	}
	form := url.Values{"environment": {"test"}, "name": {"wake"}, "url": {"http://localhost/callback"}, "method": {"POST"}, "enabled": {"true"}, "headers": {`{"Authorization":"Bearer private-test-value"}`}}
	if w := post(form); w.Code != http.StatusSeeOther || !strings.Contains(w.Header().Get("Location"), "environment=test") {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	edit := get("/topics?environment=test&edit=wake")
	if !strings.Contains(edit, "编辑 Topic") || !strings.Contains(edit, "readonly") || strings.Contains(edit, "private-test-value") {
		t.Fatal("edit form missing or saved credentials exposed")
	}
	form.Set("editing", "true")
	form.Set("headers", "")
	form.Set("url", "invalid-url")
	if w := post(form); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), `value="invalid-url"`) || !strings.Contains(w.Body.String(), "编辑 Topic") {
		t.Fatalf("invalid edit must retain form: %d", w.Code)
	}
	form.Set("url", "http://localhost/updated")
	if w := post(form); w.Code != http.StatusSeeOther {
		t.Fatalf("edit: %d %s", w.Code, w.Body.String())
	}
	updated, err := db.Topic(context.Background(), "test", "wake")
	if err != nil || updated.URL != "http://localhost/updated" || updated.Headers["Authorization"] != "Bearer private-test-value" {
		t.Fatal("edit did not preserve callback configuration")
	}
	if strings.Contains(get("/topics?environment=prod"), "localhost/updated") {
		t.Fatal("topic leaked across environments")
	}
}

func TestManagedSchedulesRenderInLegacyList(t *testing.T) {
	db := testutil.DB(t)
	id, err := db.SaveJob(context.Background(), 0, broker.Row{"name": "Managed schedule", "environment": "test", "cron_expression": "0 0 * * *", "execute_method": "POST", "execute_url": "http://localhost/task", "execute_headers": "{}", "execute_body": "{}", "enabled": false})
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range []string{
		`{"schedule_type":"once","run_at":"2030-01-01T00:00:00Z","timezone":"Asia/Shanghai"}`,
		`{"schedule_type":"recurring","interval_type":"24h","run_at":"2030-01-01T00:00:00Z","timezone":"Asia/Shanghai"}`,
	} {
		if _, err = db.DB.Exec("UPDATE jobs SET managed_key='test:fixture',managed_definition=? WHERE id=?", spec, id); err != nil {
			t.Fatal(err)
		}
		s, err := New(db, broker.NewEngine(db), "")
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/?environment=test", nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "Asia/Shanghai") {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
	}
}

func TestJinjaFallbackValuesPreserveDisplay(t *testing.T) {
	s, err := New(nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		data map[string]any
		want []string
	}{
		{"executions.html", map[string]any{"job": htmlJob(broker.Row{"id": int64(1), "environment": "test", "name": "fixture"}), "executions": []broker.Row{htmlExecution(broker.Row{"status": "failed", "status_code": 503, "error_message": "callback unavailable", "finished_at": time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)})}}, []string{"<td>503</td>", "callback unavailable", "2026-09-24 00:00:00"}},
		{"topic_form.html", map[string]any{"tab": "topics", "edit": map[string]any{}}, []string{`value="10"`, `value="16"`}},
		{"message_form.html", map[string]any{"tab": "messages", "payload": "{}"}, []string{`name="payload" required>{}</textarea>`}},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		s.render(w, httptest.NewRequest("GET", "/?environment=test", nil), c.name, c.data, 200)
		if w.Code != 200 {
			t.Fatal(c.name, w.Body.String())
		}
		for _, want := range append(c.want, "<title>msgbroker</title>") {
			if !strings.Contains(w.Body.String(), want) {
				t.Fatalf("%s missing %q", c.name, want)
			}
		}
	}
}

func TestFailureQueueAndRedeliveryAPI(t *testing.T) {
	db := testutil.DB(t)
	ctx := context.Background()
	if _, err := db.SaveTopic(ctx, broker.Topic{Name: "wake", Environment: "test", URL: "http://127.0.0.1:9999", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	m, err := db.Send(ctx, "test", broker.SendRequest{Topic: "wake", UniqueID: "failed-key", RunAt: time.Now(), Payload: json.RawMessage(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.DB.Exec("UPDATE messages SET status='completed',result='failed',attempts=16,finished_at=UTC_TIMESTAMP(6) WHERE id=?", m.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = db.DB.Exec("DELETE FROM message_waits WHERE message_id=?", m.ID); err != nil {
		t.Fatal(err)
	}
	s, err := New(db, broker.NewEngine(db), "")
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	for _, path := range []string{"/messages/failed?environment=test", "/openplatform/v1/environments/test/messages?status=completed&result=failed"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), m.ID) {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	var newID string
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", "/openplatform/v1/environments/test/messages/"+m.ID+"/retry", nil))
		var out map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || out["status"] != "pending" || out["messageId"] == m.ID || out["messageId"] == "" {
			t.Fatal(w.Code, out)
		}
		if i == 0 {
			newID = out["messageId"]
		} else if newID != out["messageId"] {
			t.Fatal("retry action duplicated")
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/messages/failed?environment=test", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), newID) || !strings.Contains(w.Body.String(), "disabled") {
		t.Fatal(w.Code, w.Body.String())
	}
	// Changed parameters on a repeated submission must succeed and return the failed original.
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/openplatform/v1/environments/test/messages", strings.NewReader(`{"topic":"wake","uniqueId":"failed-key","runAt":"2030-01-01T00:00:00Z","payload":{"x":2}}`)))
	if w.Code != 200 || !strings.Contains(w.Body.String(), m.ID) {
		t.Fatal(w.Code, w.Body.String())
	}
}
