package broker

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func ID(prefix string) string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return prefix + hex.EncodeToString(b)
}
func ValidEnvironment(v string) bool { return v == "prod" || v == "test" }
func CanonicalJSON(b []byte) (json.RawMessage, error) {
	if len(b) > 256*1024 {
		return nil, fmt.Errorf("payload exceeds 256 KiB")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil || v == nil {
		return nil, fmt.Errorf("payload must be a JSON object")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, fmt.Errorf("trailing JSON data")
	}
	return json.Marshal(v)
}
func ValidateTopic(t *Topic) error {
	if !ValidEnvironment(t.Environment) || !namePattern.MatchString(t.Name) {
		return fmt.Errorf("invalid environment or topic name")
	}
	if t.Method == "" {
		t.Method = "POST"
	}
	t.Method = strings.ToUpper(t.Method)
	switch t.Method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
	default:
		return fmt.Errorf("invalid HTTP method")
	}
	for _, raw := range []string{t.URL, t.HealthURL} {
		if raw == "" && raw == t.HealthURL {
			continue
		}
		u, e := url.Parse(raw)
		if e != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
			return fmt.Errorf("invalid callback or health URL")
		}
	}
	if t.URL == "" {
		return fmt.Errorf("URL required")
	}
	if t.TimeoutSeconds == 0 {
		t.TimeoutSeconds = 10
	}
	if t.MaxAttempts == 0 {
		t.MaxAttempts = 5
	}
	if t.TimeoutSeconds < 1 || t.TimeoutSeconds > 120 || t.MaxAttempts < 1 || t.MaxAttempts > 20 {
		return fmt.Errorf("timeout must be 1..120 seconds; attempts 1..20")
	}
	for k, v := range t.Headers {
		if !httpToken(k) || strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("invalid header")
		}
		switch strings.ToLower(k) {
		case "host", "content-length", "transfer-encoding", "connection":
			return fmt.Errorf("reserved header")
		}
	}
	return nil
}
func httpToken(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
			return false
		}
	}
	return true
}
func fingerprint(r SendRequest) string {
	sum := sha256.Sum256(append([]byte(r.RunAt.UTC().Format(time.RFC3339Nano)+"\n"), r.Payload...))
	return hex.EncodeToString(sum[:])
}

// New rules use standard cron semantics (Sunday=0; restricted DOM/DOW use OR).
// Legacy APScheduler expressions must pass the explicit migration audit.
func Next(expr, zone string, after time.Time) (time.Time, error) {
	if zone == "" {
		zone = "Asia/Shanghai"
	}
	loc, e := time.LoadLocation(zone)
	if e != nil {
		return time.Time{}, fmt.Errorf("invalid timezone")
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 && len(fields) != 6 {
		return time.Time{}, fmt.Errorf("cron requires 5 or 6 fields")
	}
	p := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	s, e := p.Parse(expr)
	if e != nil {
		return time.Time{}, fmt.Errorf("invalid cron: %w", e)
	}
	n := s.Next(after.In(loc))
	if n.IsZero() {
		return n, fmt.Errorf("cron has no upcoming occurrence")
	}
	return n.UTC(), nil
}
func expandUUID(b json.RawMessage) json.RawMessage {
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	d.Decode(&v)
	var walk func(any) any
	walk = func(x any) any {
		switch y := x.(type) {
		case string:
			return strings.NewReplacer("{{UUID}}", ID(""), "{{uuid}}", ID(""), "__UUID__", ID("")).Replace(y)
		case map[string]any:
			for k, z := range y {
				y[k] = walk(z)
			}
		case []any:
			for i, z := range y {
				y[i] = walk(z)
			}
		}
		return x
	}
	out, _ := json.Marshal(walk(v))
	return out
}
