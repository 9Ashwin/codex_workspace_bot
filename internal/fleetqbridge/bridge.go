package fleetqbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kid0317/codex-workspace-bot/internal/fleetq"
)

// ErrNoReplyRoute indicates that a result cannot be sent to Feishu.
var ErrNoReplyRoute = errors.New("fleetq result has no Feishu reply route")

// ErrLeaseLost indicates that a task lost its JetStream lease during execution.
var ErrLeaseLost = errors.New("fleetq lease lost")

// Executor runs a FleetQ request on the current machine.
type Executor interface {
	Execute(context.Context, fleetq.Message) (string, error)
}

// Sender sends text through one configured Feishu App.
type Sender interface {
	SendText(context.Context, string, string, string) (string, error)
}

// ResultLedger persists idempotent Feishu delivery state.
type ResultLedger interface {
	BeginFleetQResult(context.Context, string, string, string, string, string, string) (bool, error)
	MarkFleetQResultSent(context.Context, string) error
	MarkFleetQResultUnknown(context.Context, string, string) error
}

// Bridge consumes FleetQ requests/results and connects them to local adapters.
type Bridge struct {
	client  *fleetq.Client
	machine string
	execute Executor
	senders map[string]Sender
	logger  *slog.Logger
	ledger  ResultLedger
	seen    sync.Map
}

// New creates a FleetQ bridge for one machine.
func New(client *fleetq.Client, execute Executor, senders map[string]Sender, logger *slog.Logger, ledgers ...ResultLedger) *Bridge {
	if logger == nil {
		logger = slog.Default()
	}
	var ledger ResultLedger
	if len(ledgers) > 0 {
		ledger = ledgers[0]
	}
	machine := ""
	if client != nil {
		machine = client.Machine()
	}
	return &Bridge{client: client, machine: machine, execute: execute, senders: senders, logger: logger, ledger: ledger}
}

// Run consumes FleetQ messages until the context is canceled.
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
	case fleetq.KindJobRequest:
		b.publishStatus(ctx, delivery.Message, fleetq.StatusClaimed, nil)
		b.publishStatus(ctx, delivery.Message, fleetq.StatusRunning, nil)
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
		status := fleetq.StatusCompleted
		if err != nil {
			status = fleetq.StatusFailed
		}
		b.publishStatus(ctx, delivery.Message, status, err)
		if err := b.publishTargetNotice(ctx, delivery.Message, result, err); err != nil {
			b.logger.Error("fleetq_target_notice_failed", "event", "fleetq_target_notice_failed", "message_id", delivery.ID, "error", err)
		}
		return delivery.Ack()
	case fleetq.KindJobResult:
		if err := b.deliverResult(ctx, delivery.Message); err != nil {
			if errors.Is(err, ErrNoReplyRoute) {
				b.logger.Info("fleetq_result_without_feishu_route", "event", "fleetq_result_without_feishu_route", "message_id", delivery.ID)
				return delivery.Ack()
			}
			return err
		}
		return delivery.Ack()
	case fleetq.KindJobNotice:
		if err := b.deliverNotice(ctx, delivery.Message); err != nil {
			if errors.Is(err, ErrNoReplyRoute) {
				b.logger.Info("fleetq_notice_without_feishu_route", "event", "fleetq_notice_without_feishu_route", "message_id", delivery.ID)
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
	result, err := b.resultMessage(request, text, executeErr)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, result)
}

func (b *Bridge) resultMessage(request fleetq.Message, text string, executeErr error) (fleetq.Message, error) {
	if strings.TrimSpace(request.From) == "" {
		return fleetq.Message{}, errors.New("fleetq request has no source machine")
	}
	requestID := requestID(request)
	status := fleetq.StatusCompleted
	if executeErr != nil {
		status = fleetq.StatusFailed
	}
	executor := b.machine
	if executor == "" {
		executor = request.From
	}
	result := fleetq.Message{Schema: fleetq.Schema, ID: requestID + ":result", From: executor, To: []string{request.From}, Kind: fleetq.KindJobResult, Text: text, CreatedAt: time.Now().Format(time.RFC3339), Meta: map[string]any{"request_id": requestID, "status": status, "executor": executor, "reply": request.Meta["reply"]}}
	return result, nil
}

func (b *Bridge) deliverResult(ctx context.Context, result fleetq.Message) error {
	requestID := requestID(result)
	reply, ok := replyRoute(result.Meta["reply"])
	if !ok {
		return ErrNoReplyRoute
	}
	text := fmt.Sprintf("[FleetQ 任务结果]\n任务 ID：%s\n%s", requestID, result.Text)
	return b.deliverToRoute(ctx, requestID, requestID, result.ID, reply, text)
}

func (b *Bridge) deliverNotice(ctx context.Context, notice fleetq.Message) error {
	requestID := requestID(notice)
	reply, ok := replyRoute(notice.Meta["reply"])
	if !ok {
		return ErrNoReplyRoute
	}
	dedupeID := requestID + ":notice:" + b.machine
	return b.deliverToRoute(ctx, requestID, dedupeID, notice.ID, reply, notice.Text)
}

func (b *Bridge) deliverToRoute(ctx context.Context, requestID, dedupeID, resultID string, reply fleetq.ReplyRoute, text string) error {
	sender := b.senders[reply.AppID]
	if sender == nil {
		b.seen.Delete(dedupeID)
		return fmt.Errorf("fleetq result app sender unavailable: %s", reply.AppID)
	}
	if reply.ReceiveID == "" || reply.ReceiveType == "" {
		b.seen.Delete(dedupeID)
		return ErrNoReplyRoute
	}
	if b.ledger != nil {
		shouldSend, err := b.ledger.BeginFleetQResult(ctx, dedupeID, resultID, reply.AppID, reply.ReceiveID, reply.ReceiveType, text)
		if err != nil {
			return err
		}
		if !shouldSend {
			return nil
		}
	} else if _, loaded := b.seen.LoadOrStore(dedupeID, struct{}{}); loaded {
		return nil
	}
	if _, err := sender.SendText(ctx, reply.ReceiveID, reply.ReceiveType, text); err != nil {
		if b.ledger != nil {
			_ = b.ledger.MarkFleetQResultUnknown(ctx, dedupeID, err.Error())
		} else {
			b.seen.Delete(dedupeID)
		}
		return err
	}
	if b.ledger != nil {
		if err := b.ledger.MarkFleetQResultSent(ctx, dedupeID); err != nil {
			_ = b.ledger.MarkFleetQResultUnknown(ctx, dedupeID, err.Error())
			return err
		}
	}
	b.publishStatusByID(ctx, requestID, resultID, fleetq.StatusDelivered, nil)
	return nil
}

func (b *Bridge) publishTargetNotice(ctx context.Context, request fleetq.Message, text string, executeErr error) error {
	notify, ok := notifyRoute(request.Meta["notify"])
	if !ok || notify.Machine == "" {
		return nil
	}
	requestID := requestID(request)
	status := fleetq.StatusCompleted
	if executeErr != nil {
		status = fleetq.StatusFailed
	}
	executor := b.machine
	if executor == "" {
		executor = request.From
	}
	notice := fleetq.Message{
		Schema: fleetq.Schema, ID: requestID + ":notice:" + notify.Machine, From: executor,
		To: []string{notify.Machine}, Kind: fleetq.KindJobNotice,
		Text:      fmt.Sprintf("[FleetQ 任务通知]\n任务 ID：%s\n状态：%s\n%s", requestID, status, text),
		CreatedAt: time.Now().Format(time.RFC3339),
		Meta:      map[string]any{"request_id": requestID, "status": status, "executor": executor, "reply": notify.Reply},
	}
	return b.client.Publish(ctx, notice)
}

func (b *Bridge) publishStatus(ctx context.Context, message fleetq.Message, status string, cause error) {
	b.publishStatusByID(ctx, requestID(message), message.ID, status, cause)
}

func (b *Bridge) publishStatusByID(ctx context.Context, requestID, messageID, status string, cause error) {
	if b.client == nil || requestID == "" || messageID == "" || b.machine == "" {
		return
	}
	if err := b.client.PublishStatus(ctx, fleetq.NewStatusEvent(requestID, messageID, b.machine, status, cause)); err != nil {
		b.logger.Warn("fleetq_status_event_failed", "event", "fleetq_status_event_failed", "request_id", requestID, "status", status, "error", err)
	}
}

func requestID(message fleetq.Message) string {
	if value := stringValue(message.Meta, "request_id"); value != "" {
		return value
	}
	return message.ID
}

func replyRoute(value any) (fleetq.ReplyRoute, bool) {
	data, err := json.Marshal(value)
	if err != nil {
		return fleetq.ReplyRoute{}, false
	}
	var route fleetq.ReplyRoute
	if err := json.Unmarshal(data, &route); err != nil || route.AppID == "" || route.ReceiveID == "" || route.ReceiveType == "" {
		return fleetq.ReplyRoute{}, false
	}
	return route, true
}

func notifyRoute(value any) (fleetq.NotifyRoute, bool) {
	data, err := json.Marshal(value)
	if err != nil {
		return fleetq.NotifyRoute{}, false
	}
	var route fleetq.NotifyRoute
	if err := json.Unmarshal(data, &route); err != nil || route.Machine == "" {
		return fleetq.NotifyRoute{}, false
	}
	if route.Reply.AppID == "" || route.Reply.ReceiveID == "" || route.Reply.ReceiveType == "" {
		return fleetq.NotifyRoute{}, false
	}
	return route, true
}

func stringValue(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}
