package broker

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")

type Topic struct {
	ID             string            `json:"id"`
	Environment    string            `json:"environment"`
	Name           string            `json:"name"`
	URL            string            `json:"url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers,omitempty"`
	TimeoutSeconds int               `json:"timeoutSeconds"`
	MaxAttempts    int               `json:"maxAttempts"`
	Enabled        bool              `json:"enabled"`
	HealthURL      string            `json:"healthUrl"`
	HealthStatus   string            `json:"healthStatus"`
	LastCheckedAt  *time.Time        `json:"lastCheckedAt"`
	CreatedAt      time.Time         `json:"createdAt"`
}
type SendRequest struct {
	Topic    string          `json:"topic"`
	UniqueID string          `json:"uniqueId"`
	RunAt    time.Time       `json:"runAt"`
	Payload  json.RawMessage `json:"payload"`
}
type Message struct {
	ID              string          `json:"id"`
	Environment     string          `json:"environment"`
	Topic           string          `json:"topic"`
	UniqueID        string          `json:"uniqueId"`
	ScheduleID      string          `json:"scheduleId,omitempty"`
	ScheduleVersion int             `json:"scheduleVersion,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	RunAt           time.Time       `json:"runAt"`
	AvailableAt     *time.Time      `json:"availableAt,omitempty"`
	Status          string          `json:"status"`
	Result          string          `json:"result,omitempty"`
	UpdatedAt       *time.Time      `json:"updatedAt,omitempty"`
	RedeliveredAs   string          `json:"redeliveredAs,omitempty"`
	Attempts        int             `json:"attempts"`
	LeaseToken      string          `json:"-"`
	LeaseUntil      *time.Time      `json:"leaseUntil,omitempty"`
	HTTPStatus      int             `json:"httpStatus,omitempty"`
	Error           string          `json:"error,omitempty"`
	Response        string          `json:"response,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
	FinishedAt      *time.Time      `json:"finishedAt,omitempty"`
}
type HealthChange struct {
	Status    string    `json:"status"`
	CheckedAt time.Time `json:"checkedAt"`
}
type MessageFilter struct {
	UniqueID       string
	UniqueIDPrefix string
	OnlyTimed      bool
	BeforeID       string
	Topic          string
	Status         string
	Result         string
	ScheduleID     string
	Limit          int
	Before         time.Time
}
