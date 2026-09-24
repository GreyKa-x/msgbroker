package web

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/GreyKa-x/msgbroker/internal/broker"
	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/nikolalohinski/gonja/v2/loaders"
)

//go:embed templates/* static/*
var assets embed.FS

type Server struct {
	Store     *broker.Store
	Engine    *broker.Engine
	Token     string
	Templates map[string]*exec.Template
}

func New(s *broker.Store, e *broker.Engine, token string) (*Server, error) {
	sources := map[string]string{}
	entries, _ := assets.ReadDir("templates")
	for _, f := range entries {
		b, _ := assets.ReadFile("templates/" + f.Name())
		sources["/"+f.Name()] = string(b)
	}
	loader, err := loaders.NewMemoryLoader(sources)
	if err != nil {
		return nil, err
	}
	cfg := gonja.DefaultConfig.Inherit()
	cfg.AutoEscape = true
	out := &Server{s, e, token, map[string]*exec.Template{}}
	for name := range sources {
		t, err := exec.NewTemplate(name, cfg, loader, gonja.DefaultEnvironment)
		if err != nil {
			return nil, fmt.Errorf("template %s: %w", name, err)
		}
		out.Templates[strings.TrimPrefix(name, "/")] = t
	}
	return out, nil
}
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				slog.Error("handler panic", "path", r.URL.Path)
				http.Error(w, "Internal error", 500)
			}
		}()
		if r.URL.Path == "/healthy" || r.URL.Path == "/healthz" {
			ctx, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if s.Store.DB.PingContext(ctx) != nil {
				http.Error(w, "database unavailable", 503)
				return
			}
			write(w, 200, map[string]string{"status": "ok"})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/static/") {
			http.FileServerFS(assets).ServeHTTP(w, r)
			return
		}
		if v := r.URL.Query().Get("environment"); v != "" {
			if !broker.ValidEnvironment(v) {
				http.Error(w, "Environment must be prod or test", 400)
				return
			}
			http.SetCookie(w, &http.Cookie{Name: "job_environment", Value: v, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
		}
		// Preserve existing internal HTML access; all OpenPlatform JSON routes use the existing token contract.
		if strings.HasPrefix(r.URL.Path, "/openplatform/") && !s.authorized(r) {
			write(w, 401, map[string]string{"detail": "invalid or missing agent token"})
			return
		}
		if r.Method != "GET" && r.Method != "HEAD" {
			if origin := r.Header.Get("Origin"); origin != "" {
				u, er := url.Parse(origin)
				if er != nil || u.Host != r.Host {
					http.Error(w, "Cross-origin request rejected", 403)
					return
				}
			}
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
		if strings.HasPrefix(r.URL.Path, "/openplatform/") {
			s.api(w, r)
			return
		}
		s.html(w, r)
	})
}
func (s *Server) authorized(r *http.Request) bool {
	if s.Token == "" {
		return true
	}
	v := r.Header.Get("X-Agent-Token")
	if v == "" {
		v = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	return subtle.ConstantTimeCompare([]byte(v), []byte(s.Token)) == 1
}
func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, e error) {
	code := 400
	if errors.Is(e, broker.ErrNotFound) {
		code = 404
	} else if errors.Is(e, broker.ErrConflict) {
		code = 409
	}
	if code == 400 && !strings.Contains(e.Error(), "invalid") && !strings.Contains(e.Error(), "required") {
		slog.Warn("request failed", "error", e)
	}
	write(w, code, map[string]string{"detail": e.Error()})
}
func decode(r *http.Request, v any) error {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return fmt.Errorf("invalid JSON: %w", e)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return fmt.Errorf("invalid trailing JSON")
	}
	return nil
}
func env(r *http.Request) string {
	v := r.URL.Query().Get("environment")
	if v == "" {
		v = r.FormValue("environment")
	}
	if v == "" {
		if c, e := r.Cookie("job_environment"); e == nil {
			v = c.Value
		}
	}
	if !broker.ValidEnvironment(v) {
		v = "prod"
	}
	return v
}
func mapJSON(v any) map[string]any {
	b, _ := json.Marshal(v)
	m := map[string]any{}
	json.Unmarshal(b, &m)
	return m
}
func number(s string, def int) int {
	n, e := strconv.Atoi(s)
	if e != nil || n < 1 || n > 200 {
		return def
	}
	return n
}
func (s *Server) render(w http.ResponseWriter, r *http.Request, name string, ctx map[string]any, code int) {
	ctx["environment"] = env(r)
	ctx["toast"] = r.URL.Query().Get("toast")
	ctx["toast_type"] = r.URL.Query().Get("toast_type")
	if ctx["toast_type"] != "error" {
		ctx["toast_type"] = "success"
	}
	query := map[string]any{}
	for k, v := range r.URL.Query() {
		query[k] = v[0]
	}
	cookies := map[string]any{"job_environment": env(r)}
	ctx["request"] = map[string]any{"query_params": query, "cookies": cookies, "url": map[string]any{"path": r.URL.Path}}
	if ctx["tab"] == nil {
		ctx["tab"] = "jobs"
	}
	if ctx["title"] == nil {
		ctx["title"] = "msgbroker"
	}
	if ctx["job"] == nil {
		ctx["job"] = nil
	}
	text, e := s.Templates[name].ExecuteToString(exec.NewContext(ctx))
	if e != nil {
		slog.Error("render failed", "template", name, "error", e)
		http.Error(w, "Template error", 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	io.WriteString(w, text)
}
func redirect(w http.ResponseWriter, r *http.Request, to, message, kind string) {
	u, _ := url.Parse(to)
	q := u.Query()
	q.Set("environment", env(r))
	if message != "" {
		q.Set("toast", message)
		q.Set("toast_type", kind)
	}
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), 303)
}
func parts(r *http.Request, prefix string) []string {
	return strings.Split(strings.Trim(strings.TrimPrefix(path.Clean(r.URL.Path), prefix), "/"), "/")
}
