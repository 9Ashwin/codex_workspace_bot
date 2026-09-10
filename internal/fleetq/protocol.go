package fleetq

import "time"

// Message kinds used by FleetQ job subjects.
const (
	KindJobRequest = "job.request"
	KindJobResult  = "job.result"
	KindJobNotice  = "job.notice"
)

// Lifecycle statuses emitted in message metadata and status events.
const (
	StatusAccepted  = "accepted"
	StatusClaimed   = "claimed"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusDelivered = "delivered"
)

// NotifyRoute identifies an optional Feishu route on a target Bot.
type NotifyRoute struct {
	Machine string     `json:"machine"`
	Reply   ReplyRoute `json:"reply"`
}

// StatusEvent records a task lifecycle transition in the events stream.
type StatusEvent struct {
	Schema    int    `json:"schema"`
	Event     string `json:"event"`
	RequestID string `json:"request_id"`
	MessageID string `json:"message_id"`
	Machine   string `json:"machine"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	CreatedAt string `json:"created_at"`
}

// NewStatusEvent builds a lifecycle event without exposing message content.
func NewStatusEvent(requestID, messageID, machine, status string, cause error) StatusEvent {
	event := StatusEvent{
		Schema: Schema, Event: "job.status", RequestID: requestID, MessageID: messageID,
		Machine: machine, Status: status, CreatedAt: time.Now().Format(time.RFC3339),
	}
	if cause != nil {
		event.Error = cause.Error()
	}
	return event
}
