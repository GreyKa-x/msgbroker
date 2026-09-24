package broker

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"strconv"
	"strings"
	"time"
)

// APScheduler 3: Monday=0, and every field must match, including both day and weekday.
func LegacyNext(expr string, after time.Time) (time.Time, error) {
	f := strings.Fields(expr)
	if len(f) == 5 {
		f = append([]string{"0"}, f...)
	}
	if len(f) != 6 {
		return time.Time{}, fmt.Errorf("Cron expression must contain 5 or 6 fields")
	}
	lastDay := strings.EqualFold(f[3], "last")
	if lastDay {
		f[3] = "*"
	}
	days, e := legacyWeekdays(f[5])
	if e != nil {
		return time.Time{}, e
	}
	f[5] = "*"
	p := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	s, e := p.Parse(strings.Join(f, " "))
	if e != nil {
		return time.Time{}, e
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	n := after.In(loc)
	for i := 0; i < 366*8; i++ {
		n = s.Next(n)
		if n.IsZero() {
			break
		}
		if days[(int(n.Weekday())+6)%7] && (!lastDay || n.AddDate(0, 0, 1).Day() == 1) {
			return n.UTC(), nil
		}
		n = time.Date(n.Year(), n.Month(), n.Day()+1, 0, 0, 0, 0, loc).Add(-time.Second)
	}
	return time.Time{}, fmt.Errorf("cron has no upcoming occurrence")
}
func legacyWeekdays(raw string) ([7]bool, error) {
	var out [7]bool
	names := []string{"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
	number := func(v string) (int, error) {
		for i, n := range names {
			if strings.EqualFold(v, n) {
				return i, nil
			}
		}
		i, e := strconv.Atoi(v)
		if e != nil || i < 0 || i > 6 {
			return 0, fmt.Errorf("invalid weekday")
		}
		return i, nil
	}
	for _, part := range strings.Split(raw, ",") {
		step := 1
		bits := strings.Split(part, "/")
		if len(bits) > 2 {
			return out, fmt.Errorf("invalid weekday")
		}
		if len(bits) == 2 {
			var e error
			step, e = strconv.Atoi(bits[1])
			if e != nil || step < 1 || step > 7 {
				return out, fmt.Errorf("invalid weekday step")
			}
		}
		start, end := 0, 6
		if bits[0] != "*" {
			r := strings.Split(bits[0], "-")
			var e error
			start, e = number(r[0])
			if e != nil {
				return out, e
			}
			end = start
			if len(r) == 2 {
				end, e = number(r[1])
				if e != nil {
					return out, e
				}
			} else if len(r) > 2 {
				return out, fmt.Errorf("invalid weekday range")
			} else if len(bits) == 2 {
				end = 6
			}
		}
		if end < start {
			return out, fmt.Errorf("invalid weekday range")
		}
		for i := start; i <= end; i += step {
			out[i] = true
		}
	}
	return out, nil
}
