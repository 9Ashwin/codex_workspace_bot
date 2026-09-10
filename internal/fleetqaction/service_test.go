package fleetqaction

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kid0317/codex-workspace-bot/internal/codexapp"
	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
	"github.com/kid0317/codex-workspace-bot/internal/worker"
)

type fakePublisher struct{ spec fleetq.TaskSpec }

func (p *fakePublisher) PublishTask(_ context.Context, spec fleetq.TaskSpec) (fleetq.Message, error) {
	p.spec = spec
	return fleetq.Message{ID: "task-1", To: spec.To}, nil
}

func TestExecutePublishesFullAccessTaskWithReplyRoute(t *testing.T) {
	publisher := &fakePublisher{}
	service := Service{Publisher: publisher}
	arguments, _ := json.Marshal(map[string]any{"to": "mac-air", "cwd": "/tmp/work", "text": "run tests"})
	result, err := service.Execute(context.Background(), Route{AppID: "app-1", ChatType: "p2p", ChatID: "oc-chat", Reply: worker.ReplyTarget{ID: "ou-user", Type: "open_id"}}, codexapp.ToolCall{Arguments: arguments})
	if err != nil || !result.Success {
		t.Fatalf("Execute() result=%#v err=%v", result, err)
	}
	if publisher.spec.Sandbox != "danger-full-access" || publisher.spec.Reply.AppID != "app-1" || publisher.spec.Reply.ReceiveID != "ou-user" {
		t.Fatalf("published spec=%#v", publisher.spec)
	}
}

func TestExecuteReturnsAcceptedStatusAndOptionalNotifyRoute(t *testing.T) {
	publisher := &fakePublisher{}
	service := Service{Publisher: publisher}
	arguments, _ := json.Marshal(map[string]any{
		"to": "mac-mini", "cwd": "/tmp/work", "text": "run tests",
		"notify_target": true,
	})
	result, err := service.Execute(context.Background(), Route{AppID: "app-1", Reply: worker.ReplyTarget{ID: "ou-user", Type: "open_id"}}, codexapp.ToolCall{Arguments: arguments})
	if err != nil || !result.Success {
		t.Fatalf("Execute() result=%#v err=%v", result, err)
	}
	if publisher.spec.Notify == nil || publisher.spec.Notify.Machine != "mac-mini" || publisher.spec.Notify.Reply.ReceiveID != "ou-user" {
		t.Fatalf("published notify route=%#v", publisher.spec.Notify)
	}
	if len(result.ContentItems) != 1 || result.ContentItems[0].Text == "" || !strings.Contains(result.ContentItems[0].Text, `"status":"accepted"`) {
		t.Fatalf("accepted result=%#v", result)
	}
}
