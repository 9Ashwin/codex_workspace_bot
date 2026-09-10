package fleetqaction

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kid0317/codex-workspace-bot/internal/codexapp"
	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
	"github.com/kid0317/codex-workspace-bot/internal/worker"
)

type Publisher interface {
	PublishTask(context.Context, fleetq.TaskSpec) (fleetq.Message, error)
}

type Route struct {
	AppID, ChatType, ChatID string
	Reply                   worker.ReplyTarget
}

type Service struct{ Publisher Publisher }

func (s Service) Execute(ctx context.Context, route Route, call codexapp.ToolCall) (codexapp.ToolResult, error) {
	if s.Publisher == nil {
		return codexapp.ToolResult{Success: false}, fmt.Errorf("fleetq is unavailable")
	}
	var args struct {
		To      string `json:"to"`
		CWD     string `json:"cwd"`
		Text    string `json:"text"`
		Model   string `json:"model"`
		Timeout int    `json:"timeout_s"`
	}
	if err := json.Unmarshal(call.Arguments, &args); err != nil {
		return codexapp.ToolResult{Success: false}, fmt.Errorf("decode fleetq.task arguments: %w", err)
	}
	if strings.TrimSpace(args.To) == "" || strings.TrimSpace(args.CWD) == "" || strings.TrimSpace(args.Text) == "" {
		return codexapp.ToolResult{Success: false}, fmt.Errorf("fleetq.task requires to, cwd, and text")
	}
	message, err := s.Publisher.PublishTask(ctx, fleetq.TaskSpec{
		To: []string{args.To}, Text: args.Text, CWD: args.CWD, Model: args.Model, Timeout: args.Timeout,
		Sandbox: "danger-full-access",
		Reply:   fleetq.ReplyRoute{AppID: route.AppID, ReceiveID: route.Reply.ID, ReceiveType: route.Reply.Type, ChatType: route.ChatType, ChatID: route.ChatID},
	})
	if err != nil {
		return codexapp.ToolResult{Success: false}, err
	}
	result, _ := json.Marshal(map[string]any{"success": true, "task_id": message.ID, "to": message.To, "sandbox": "danger-full-access"})
	return codexapp.ToolResult{Success: true, ContentItems: []codexapp.ToolContentItem{{Type: "inputText", Text: string(result)}}}, nil
}
