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
	for _, name := range []string{"messages.html", "topics.html"} {
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
	s.render(w, httptest.NewRequest("GET", "/messages?environment=test", nil), "messages.html", map[string]any{"messages": []map[string]any{{"id": first, "payload_text": "<script>bad</script>", "status": "waiting", "runAt": time.Now().String()}}, "tab": "messages", "topics": []any{}}, 200)
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
