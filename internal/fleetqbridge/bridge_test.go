package fleetqbridge

import (
	"context"
	"testing"

	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
)

type fakeSender struct{ receiveID, receiveType, text string }

type fakeResultLedger struct{ status string }

func (l *fakeResultLedger) BeginFleetQResult(context.Context, string, string, string, string, string, string) (bool, error) {
	if l.status == "" {
		l.status = "pending"
		return true, nil
	}
	return l.status == "pending", nil
}

func (l *fakeResultLedger) MarkFleetQResultSent(context.Context, string) error {
	l.status = "sent"
	return nil
}

func (l *fakeResultLedger) MarkFleetQResultUnknown(context.Context, string, string) error {
	l.status = "unknown"
	return nil
}

func (s *fakeSender) SendText(_ context.Context, receiveID, receiveType, text string) (string, error) {
	s.receiveID, s.receiveType, s.text = receiveID, receiveType, text
	return "om-result", nil
}

func TestDeliverResultUsesOriginalFeishuReplyRoute(t *testing.T) {
	sender := &fakeSender{}
	bridge := New(nil, nil, map[string]Sender{"app-1": sender}, nil)
	message := fleetq.Message{ID: "result-1", Text: "done", Meta: map[string]any{
		"request_id": "request-1",
		"reply":      map[string]any{"app_id": "app-1", "receive_id": "oc-chat", "receive_type": "chat_id"},
	}}
	if err := bridge.deliverResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if sender.receiveID != "oc-chat" || sender.receiveType != "chat_id" || sender.text == "" {
		t.Fatalf("sender=%#v", sender)
	}
	if err := bridge.deliverResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if sender.text != "[FleetQ 任务结果]\n任务 ID：request-1\ndone" {
		t.Fatalf("duplicate result was sent or text changed: %q", sender.text)
	}
}

func TestDeliverResultUsesLedgerAcrossBridgeInstances(t *testing.T) {
	sender := &fakeSender{}
	ledger := &fakeResultLedger{}
	message := fleetq.Message{ID: "result-1", Text: "done", Meta: map[string]any{
		"request_id": "request-1",
		"reply":      map[string]any{"app_id": "app-1", "receive_id": "oc-chat", "receive_type": "chat_id"},
	}}
	first := New(nil, nil, map[string]Sender{"app-1": sender}, nil, ledger)
	if err := first.deliverResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	second := New(nil, nil, map[string]Sender{"app-1": sender}, nil, ledger)
	if err := second.deliverResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if sender.text != "[FleetQ 任务结果]\n任务 ID：request-1\ndone" {
		t.Fatalf("ledger did not suppress duplicate: %q", sender.text)
	}
}

func TestDeliverNoticeUsesExplicitTargetRoute(t *testing.T) {
	sender := &fakeSender{}
	bridge := New(nil, nil, map[string]Sender{"app-2": sender}, nil)
	notice := fleetq.Message{ID: "request-1:notice:mac-mini", Text: "target update", Meta: map[string]any{
		"request_id": "request-1",
		"reply":      map[string]any{"app_id": "app-2", "receive_id": "oc-target", "receive_type": "chat_id"},
	}}
	if err := bridge.deliverNotice(context.Background(), notice); err != nil {
		t.Fatal(err)
	}
	if sender.receiveID != "oc-target" || sender.receiveType != "chat_id" || sender.text != "target update" {
		t.Fatalf("sender=%#v", sender)
	}
	if err := bridge.deliverNotice(context.Background(), notice); err != nil {
		t.Fatal(err)
	}
	if sender.text != "target update" {
		t.Fatalf("duplicate notice was sent: %q", sender.text)
	}
}

func TestResultMessageUsesRequestIDAndExecutor(t *testing.T) {
	bridge := &Bridge{machine: "mac-mini"}
	message, err := bridge.resultMessage(fleetq.Message{
		ID:   "message-1",
		From: "mac-air",
		Meta: map[string]any{"request_id": "request-1", "reply": map[string]any{"app_id": "app-1"}},
	}, "done", nil)
	if err != nil {
		t.Fatal(err)
	}
	if message.ID != "request-1:result" || message.From != "mac-mini" || message.To[0] != "mac-air" {
		t.Fatalf("result=%#v", message)
	}
	if message.Meta["status"] != fleetq.StatusCompleted || message.Meta["executor"] != "mac-mini" {
		t.Fatalf("result meta=%#v", message.Meta)
	}
}
