package fleetqhermes

import (
	"context"
	"testing"
	"time"

	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
)

type recordingClient struct {
	message fleetq.Message
}

func (c *recordingClient) Next(context.Context) (*fleetq.Delivery, error) {
	return nil, context.Canceled
}

func (c *recordingClient) Publish(_ context.Context, message fleetq.Message) error {
	c.message = message
	return nil
}

func (c *recordingClient) PublishStatus(context.Context, fleetq.StatusEvent) error {
	return nil
}

func TestSplitCommand(t *testing.T) {
	got, err := splitCommand(`/opt/hermes/bin/run --profile "default profile" 'queued task'`)
	if err != nil {
		t.Fatalf("splitCommand() error = %v", err)
	}
	want := []string{"/opt/hermes/bin/run", "--profile", "default profile", "queued task"}
	if len(got) != len(want) {
		t.Fatalf("splitCommand() = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("splitCommand()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestExecuteWithLeaseReturnsHandlerOutput(t *testing.T) {
	service := Service{Handler: `/bin/sh -c 'printf result-text'`}
	delivery := &fleetq.Delivery{Message: fleetq.Message{
		ID:   "job-1",
		Meta: map[string]any{"cwd": t.TempDir(), "timeout_s": 5},
	}}

	got, err := service.executeWithLease(context.Background(), delivery)
	if err != nil {
		t.Fatalf("executeWithLease() error = %v", err)
	}
	if got != "result-text" {
		t.Fatalf("executeWithLease() = %q, want %q", got, "result-text")
	}
}

func TestPublishResultPreservesRequestRoute(t *testing.T) {
	client := &recordingClient{}
	service := Service{Client: client, Machine: "linux"}
	request := fleetq.Message{
		ID:   "message-1",
		From: "mac-mini",
		Meta: map[string]any{"request_id": "request-1", "reply": fleetq.ReplyRoute{ReceiveID: "chat-1"}},
	}

	if err := service.publishResult(context.Background(), request, "done", nil); err != nil {
		t.Fatalf("publishResult() error = %v", err)
	}
	if client.message.ID != "request-1:result" {
		t.Errorf("result ID = %q, want %q", client.message.ID, "request-1:result")
	}
	if len(client.message.To) != 1 || client.message.To[0] != "mac-mini" {
		t.Errorf("result targets = %#v, want [mac-mini]", client.message.To)
	}
	if client.message.Meta["request_id"] != "request-1" {
		t.Errorf("result request_id = %#v, want request-1", client.message.Meta["request_id"])
	}
	if client.message.Meta["status"] != fleetq.StatusCompleted {
		t.Errorf("result status = %#v, want %s", client.message.Meta["status"], fleetq.StatusCompleted)
	}
	if client.message.CreatedAt == "" {
		t.Error("result CreatedAt is empty")
	}
	if _, err := time.Parse(time.RFC3339, client.message.CreatedAt); err != nil {
		t.Errorf("result CreatedAt = %q is not RFC3339: %v", client.message.CreatedAt, err)
	}
}
