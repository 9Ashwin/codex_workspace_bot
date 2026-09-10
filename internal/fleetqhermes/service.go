package fleetqhermes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
)

var errLeaseLost = errors.New("fleetq Hermes lease lost")

// Client is the FleetQ capability required by the Hermes consumer.
type Client interface {
	Next(context.Context) (*fleetq.Delivery, error)
	Publish(context.Context, fleetq.Message) error
	PublishStatus(context.Context, fleetq.StatusEvent) error
}

// Service consumes FleetQ jobs and delegates them to a local Hermes handler.
type Service struct {
	Client     Client
	Machine    string
	Handler    string
	RetryDelay time.Duration
	Logger     *slog.Logger
}

// Run consumes jobs until the context is canceled.
func (s *Service) Run(ctx context.Context) error {
	if s.Client == nil {
		return errors.New("fleetq Hermes client is required")
	}
	if s.Machine == "" {
		return errors.New("fleetq Hermes machine is required")
	}
	if s.RetryDelay <= 0 {
		s.RetryDelay = 15 * time.Second
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	for {
		delivery, err := s.Client.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.Logger.Error("fleetq_hermes_consumer_failed", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(s.RetryDelay):
			}
			continue
		}
		if err := s.handle(ctx, delivery); err != nil {
			s.Logger.Error("fleetq_hermes_message_failed", "message_id", delivery.ID, "error", err)
			if nakErr := delivery.Nak(s.RetryDelay); nakErr != nil && ctx.Err() == nil {
				s.Logger.Error("fleetq_hermes_nak_failed", "message_id", delivery.ID, "error", nakErr)
			}
		}
	}
}

func (s *Service) handle(ctx context.Context, delivery *fleetq.Delivery) error {
	if delivery.Kind != fleetq.KindJobRequest {
		return delivery.Ack()
	}
	s.publishStatus(ctx, delivery.Message, fleetq.StatusClaimed, nil)
	s.publishStatus(ctx, delivery.Message, fleetq.StatusRunning, nil)
	result, executeErr := s.executeWithLease(ctx, delivery)
	if executeErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(executeErr, errLeaseLost) {
			return executeErr
		}
		result = "[执行失败] " + executeErr.Error()
	}
	if err := s.publishResult(ctx, delivery.Message, result, executeErr); err != nil {
		return err
	}
	status := fleetq.StatusCompleted
	if executeErr != nil {
		status = fleetq.StatusFailed
	}
	s.publishStatus(ctx, delivery.Message, status, executeErr)
	return delivery.Ack()
}

func (s *Service) executeWithLease(ctx context.Context, delivery *fleetq.Delivery) (string, error) {
	command, err := splitCommand(s.Handler)
	if err != nil {
		return "", err
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	interval := max(delivery.AckWaitDuration()/3, time.Second)
	lost := make(chan error, 1)
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := delivery.InProgress(); err != nil {
					select {
					case lost <- fmt.Errorf("%w: %v", errLeaseLost, err):
					default:
					}
					cancel()
					return
				}
			}
		}
	}()

	messagePath, err := writeMessageFile(delivery.Message)
	if err != nil {
		close(stop)
		return "", err
	}
	defer func() { _ = os.Remove(messagePath) }()
	meta := delivery.Meta
	cwd := expandPath(stringValue(meta, "cwd"))
	if cwd == "" {
		close(stop)
		return "", errors.New("fleetq Hermes task has no cwd")
	}
	info, err := os.Stat(cwd)
	if err != nil || !info.IsDir() {
		close(stop)
		return "", fmt.Errorf("fleetq Hermes cwd does not exist: %s", cwd)
	}
	timeout := time.Duration(intValue(meta, "timeout_s")) * time.Second
	if timeout <= 0 {
		timeout = time.Hour
	}
	workCtx, timeoutCancel := context.WithTimeout(workCtx, timeout)
	defer timeoutCancel()
	process := exec.CommandContext(workCtx, command[0], append(command[1:], messagePath)...)
	process.Dir = cwd
	process.Stdin = strings.NewReader(jsonString(delivery.Message))
	var stdout bytes.Buffer
	process.Stdout = io.MultiWriter(os.Stdout, &stdout)
	process.Stderr = os.Stderr
	process.Env = append(os.Environ(),
		"FLEETQ_MACHINE="+s.Machine,
		"FLEETQ_MSG_ID="+delivery.ID,
		"FLEETQ_MSG_PATH="+messagePath,
	)
	runErr := process.Run()
	close(stop)
	select {
	case leaseErr := <-lost:
		return "", leaseErr
	default:
	}
	if errors.Is(workCtx.Err(), context.DeadlineExceeded) {
		return "", fmt.Errorf("hermes task timed out after %s", timeout)
	}
	if runErr != nil {
		return "", runErr
	}
	return strings.TrimSpace(stdout.String()), nil
}

func (s *Service) publishResult(ctx context.Context, request fleetq.Message, text string, executeErr error) error {
	if strings.TrimSpace(request.From) == "" {
		return errors.New("fleetq request has no source machine")
	}
	requestID := stringValue(request.Meta, "request_id")
	if requestID == "" {
		requestID = request.ID
	}
	status := fleetq.StatusCompleted
	if executeErr != nil {
		status = fleetq.StatusFailed
	}
	result := fleetq.Message{
		Schema: fleetq.Schema, ID: requestID + ":result", From: s.Machine,
		To: []string{request.From}, Kind: fleetq.KindJobResult, Text: text,
		CreatedAt: time.Now().Format(time.RFC3339),
		Meta:      map[string]any{"request_id": requestID, "status": status, "executor": s.Machine, "reply": request.Meta["reply"]},
	}
	return s.Client.Publish(ctx, result)
}

func (s *Service) publishStatus(ctx context.Context, message fleetq.Message, status string, cause error) {
	if err := s.Client.PublishStatus(ctx, fleetq.NewStatusEvent(requestID(message), message.ID, s.Machine, status, cause)); err != nil && s.Logger != nil {
		s.Logger.Warn("fleetq_hermes_status_event_failed", "event", "fleetq_hermes_status_event_failed", "request_id", requestID(message), "status", status, "error", err)
	}
}

func requestID(message fleetq.Message) string {
	if value := stringValue(message.Meta, "request_id"); value != "" {
		return value
	}
	return message.ID
}

func writeMessageFile(message fleetq.Message) (string, error) {
	file, err := os.CreateTemp("", "fleetq-hermes-message-*.json")
	if err != nil {
		return "", err
	}
	path := file.Name()
	defer func() { _ = file.Close() }()
	data, err := json.Marshal(message)
	if err != nil {
		_ = os.Remove(path)
		return "", err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func splitCommand(input string) ([]string, error) {
	if strings.TrimSpace(input) == "" {
		return nil, errors.New("fleetq Hermes handler is required")
	}
	var result []string
	var current strings.Builder
	var quote rune
	escaped := false
	for _, char := range input {
		if escaped {
			current.WriteRune(char)
			escaped = false
			continue
		}
		if char == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if char == quote {
				quote = 0
			} else {
				current.WriteRune(char)
			}
			continue
		}
		switch char {
		case '\'', '"':
			quote = char
		case ' ', '\t', '\n':
			if current.Len() > 0 {
				result = append(result, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(char)
		}
	}
	if escaped || quote != 0 {
		return nil, errors.New("fleetq Hermes handler has an unclosed quote")
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result, nil
}

func expandPath(value string) string {
	value = os.ExpandEnv(value)
	if value == "~" || strings.HasPrefix(value, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			value = filepath.Join(home, strings.TrimPrefix(value, "~/"))
		}
	}
	return filepath.Clean(value)
}

func stringValue(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func intValue(values map[string]any, key string) int {
	value, _ := values[key].(float64)
	return int(value)
}

func jsonString(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}
