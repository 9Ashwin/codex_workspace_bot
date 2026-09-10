package fleetqbridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
)

var ErrNoReplyRoute = errors.New("fleetq result has no Feishu reply route")
var ErrLeaseLost = errors.New("fleetq lease lost")

type Executor interface {
	Execute(context.Context, fleetq.Message) (string, error)
}

type Sender interface {
	SendText(context.Context, string, string, string) (string, error)
}

type ResultLedger interface {
	BeginFleetQResult(context.Context, string, string, string, string, string, string) (bool, error)
	MarkFleetQResultSent(context.Context, string) error
	MarkFleetQResultUnknown(context.Context, string, string) error
}

type Bridge struct {
	client  *fleetq.Client
	execute Executor
	senders map[string]Sender
	logger  *slog.Logger
	ledger  ResultLedger
	seen    sync.Map
}

func New(client *fleetq.Client, execute Executor, senders map[string]Sender, logger *slog.Logger, ledgers ...ResultLedger) *Bridge {
	if logger == nil {
		logger = slog.Default()
	}
	var ledger ResultLedger
	if len(ledgers) > 0 {
		ledger = ledgers[0]
	}
	return &Bridge{client: client, execute: execute, senders: senders, logger: logger, ledger: ledger}
}

func (b *Bridge) Run(ctx context.Context) error {
	if b.client == nil || b.execute == nil {
		return errors.New("fleetq bridge is incomplete")
	}
	for {
		delivery, err := b.client.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			b.logger.Error("fleetq_consumer_failed", "event", "fleetq_consumer_failed", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if err := b.handle(ctx, delivery); err != nil {
			b.logger.Error("fleetq_message_failed", "event", "fleetq_message_failed", "message_id", delivery.ID, "kind", delivery.Kind, "error", err)
			_ = delivery.Nak(15 * time.Second)
		}
	}
}

func (b *Bridge) handle(ctx context.Context, delivery *fleetq.Delivery) error {
	switch delivery.Kind {
	case "job.request":
		result, err := b.executeWithLease(ctx, delivery)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, ErrLeaseLost) {
				return err
			}
			result = "[执行失败] " + err.Error()
		}
		if err := b.publishResult(ctx, delivery.Message, result, err); err != nil {
			return err
		}
		return delivery.Ack()
	case "job.result":
		if err := b.deliverResult(ctx, delivery.Message); err != nil {
			if errors.Is(err, ErrNoReplyRoute) {
				b.logger.Info("fleetq_result_without_feishu_route", "event", "fleetq_result_without_feishu_route", "message_id", delivery.ID)
				return delivery.Ack()
			}
			return err
		}
		return delivery.Ack()
	default:
		return delivery.Ack()
	}
}

func (b *Bridge) executeWithLease(ctx context.Context, delivery *fleetq.Delivery) (string, error) {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	interval := delivery.AckWaitDuration() / 3
	if interval < time.Second {
		interval = time.Second
	}
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
					case lost <- fmt.Errorf("%w: %v", ErrLeaseLost, err):
					default:
					}
					cancel()
					return
				}
			}
		}
	}()
	result, executeErr := b.execute.Execute(workCtx, delivery.Message)
	close(stop)
	select {
	case leaseErr := <-lost:
		return "", leaseErr
	default:
	}
	return result, executeErr
}

func (b *Bridge) publishResult(ctx context.Context, request fleetq.Message, text string, executeErr error) error {
	if strings.TrimSpace(request.From) == "" {
		return errors.New("fleetq request has no source machine")
	}
	status := "ok"
	if executeErr != nil {
		status = "error"
	}
	result := fleetq.Message{Schema: fleetq.Schema, ID: request.ID + ":result", From: request.From, To: []string{request.From}, Kind: "job.result", Text: text, CreatedAt: time.Now().Format(time.RFC3339), Meta: map[string]any{"request_id": request.ID, "status": status, "reply": request.Meta["reply"]}}
	return b.client.Publish(ctx, result)
}

func (b *Bridge) deliverResult(ctx context.Context, result fleetq.Message) error {
	requestID := stringValue(result.Meta, "request_id")
	if requestID == "" {
		requestID = result.ID
	}
	reply, ok := result.Meta["reply"].(map[string]any)
	if !ok {
		return ErrNoReplyRoute
	}
	appID := stringValue(reply, "app_id")
	sender := b.senders[appID]
	if sender == nil {
		b.seen.Delete(requestID)
		return fmt.Errorf("fleetq result app sender unavailable: %s", appID)
	}
	receiveID := stringValue(reply, "receive_id")
	receiveType := stringValue(reply, "receive_type")
	if receiveID == "" || receiveType == "" {
		b.seen.Delete(requestID)
		return ErrNoReplyRoute
	}
	text := fmt.Sprintf("[FleetQ 任务结果]\n任务 ID：%s\n%s", requestID, result.Text)
	if b.ledger != nil {
		shouldSend, err := b.ledger.BeginFleetQResult(ctx, requestID, result.ID, appID, receiveID, receiveType, text)
		if err != nil {
			return err
		}
		if !shouldSend {
			return nil
		}
	} else if _, loaded := b.seen.LoadOrStore(requestID, struct{}{}); loaded {
		return nil
	}
	if _, err := sender.SendText(ctx, receiveID, receiveType, text); err != nil {
		if b.ledger != nil {
			_ = b.ledger.MarkFleetQResultUnknown(ctx, requestID, err.Error())
		} else {
			b.seen.Delete(requestID)
		}
		return err
	}
	if b.ledger != nil {
		if err := b.ledger.MarkFleetQResultSent(ctx, requestID); err != nil {
			_ = b.ledger.MarkFleetQResultUnknown(ctx, requestID, err.Error())
			return err
		}
	}
	return nil
}

func stringValue(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}
