package fleetqbridge

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kid0317/codex-workspace-bot/internal/codexapp"
	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
	"github.com/kid0317/codex-workspace-bot/internal/worker"
)

type CodexExecutor struct {
	processor *codexapp.Processor
	threads   *threadStore
}

func NewCodexExecutor(runtime *codexapp.Runtime) *CodexExecutor {
	threads := &threadStore{threads: make(map[string]string)}
	return &CodexExecutor{processor: &codexapp.Processor{Runtime: runtime, Store: threads}, threads: threads}
}

func (e *CodexExecutor) Execute(ctx context.Context, message fleetq.Message) (string, error) {
	meta := message.Meta
	cwd := expandWorkspacePath(stringValue(meta, "cwd"))
	if strings.TrimSpace(cwd) == "" {
		return "", fmt.Errorf("fleetq task %s has no cwd", message.ID)
	}
	model := stringValue(meta, "model")
	chatGroupID := "fleetq:" + message.ID
	batch := worker.Batch{
		ID:  message.ID,
		Key: worker.P2PKey(chatGroupID, "fleetq"),
		Runtime: worker.AppRuntime{
			ID: "fleetq", WorkspaceDir: cwd, Model: model, WorkspaceMode: "work",
		},
		Messages: []worker.Message{{
			ID: message.ID, ChatGroupID: chatGroupID, ChatType: "fleetq", ChatID: chatGroupID,
			Key: worker.P2PKey(chatGroupID, "fleetq"), Query: message.Text,
		}},
	}
	result, err := e.processor.Process(ctx, batch)
	if err != nil {
		return "", err
	}
	return result.FinalText, nil
}

func expandWorkspacePath(value string) string {
	value = os.ExpandEnv(value)
	if value == "~" || strings.HasPrefix(value, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			value = filepath.Join(home, strings.TrimPrefix(value, "~/"))
		}
	}
	return filepath.Clean(value)
}

type threadStore struct {
	mu      sync.Mutex
	threads map[string]string
}

func (s *threadStore) GetChatGroupThread(_ context.Context, id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.threads[id], nil
}

func (s *threadStore) SetThreadIfExpected(_ context.Context, id, expected, replacement string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.threads[id] != expected {
		return false, nil
	}
	s.threads[id] = replacement
	return true, nil
}

func (s *threadStore) GetChatGroupToolset(context.Context, string) (string, error) { return "", nil }
func (s *threadStore) SetChatGroupToolset(context.Context, string, string) error   { return nil }
