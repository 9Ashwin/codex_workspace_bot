package fleetq

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	Schema        = 1
	JobsStream    = "FLEETQ_JOBS"
	EventsStream  = "FLEETQ_EVENTS"
	JobsPrefix    = "fleetq.jobs."
	EventsSubject = "fleetq.events"
)

type Config struct {
	Machine   string
	NATSURL   string
	TokenEnv  string
	CredsEnv  string
	AckWait   time.Duration
	FetchWait time.Duration
	Machines  []string
}

type Message struct {
	Schema    int            `json:"schema"`
	ID        string         `json:"id"`
	From      string         `json:"from"`
	To        []string       `json:"to"`
	Kind      string         `json:"kind"`
	Text      string         `json:"text,omitempty"`
	CreatedAt string         `json:"created_at"`
	Meta      map[string]any `json:"meta,omitempty"`
}

type ReplyRoute struct {
	AppID       string `json:"app_id"`
	ReceiveID   string `json:"receive_id"`
	ReceiveType string `json:"receive_type"`
	ChatType    string `json:"chat_type"`
	ChatID      string `json:"chat_id"`
}

type TaskSpec struct {
	To      []string
	Text    string
	CWD     string
	Model   string
	Timeout int
	Sandbox string
	Reply   ReplyRoute
}

type Client struct {
	conn *nats.Conn
	js   nats.JetStreamContext
	cfg  Config
}

type Delivery struct {
	Message
	msg     *nats.Msg
	ackWait time.Duration
}

func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.Machine) == "" {
		return nil, errors.New("fleetq machine is required")
	}
	if strings.TrimSpace(cfg.NATSURL) == "" {
		return nil, errors.New("fleetq NATS URL is required")
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 2 * time.Hour
	}
	if cfg.FetchWait <= 0 {
		cfg.FetchWait = time.Second
	}
	options := []nats.Option{
		nats.Name("codex-workspace-bot-fleetq-" + cfg.Machine),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.Timeout(10 * time.Second),
	}
	if cfg.TokenEnv != "" {
		if token := os.Getenv(cfg.TokenEnv); token != "" {
			options = append(options, nats.Token(token))
		}
	}
	if cfg.CredsEnv != "" {
		if path := os.Getenv(cfg.CredsEnv); path != "" {
			options = append(options, nats.UserCredentials(path))
		}
	}
	conn, err := nats.Connect(cfg.NATSURL, options...)
	if err != nil {
		return nil, fmt.Errorf("fleetq connect NATS: %w", err)
	}
	js, err := conn.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("fleetq initialize JetStream: %w", err)
	}
	client := &Client{conn: conn, js: js, cfg: cfg}
	if err := client.ensureResources(); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

func (c *Client) Close() { c.conn.Close() }

func (c *Client) Publish(ctx context.Context, message Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if message.ID == "" {
		return errors.New("fleetq message id is required")
	}
	if message.Schema == 0 {
		message.Schema = Schema
	}
	if message.From == "" {
		message.From = c.cfg.Machine
	}
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal fleetq message: %w", err)
	}
	targets := message.To
	if len(targets) == 0 || slices.Contains(targets, "*") {
		targets = c.cfg.Machines
	}
	targets = uniqueTargets(targets)
	if len(targets) == 0 {
		return errors.New("fleetq message has no targets")
	}
	event, err := json.Marshal(map[string]any{"schema": Schema, "event": "message", "message": message})
	if err != nil {
		return fmt.Errorf("marshal fleetq event: %w", err)
	}
	if _, err := c.js.Publish(EventsSubject, event, nats.MsgId("event:"+message.ID), nats.ExpectStream(EventsStream), nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish fleetq event: %w", err)
	}
	for _, target := range targets {
		if target == "" {
			continue
		}
		if _, err := c.js.Publish(jobsPrefix(target), data, nats.MsgId("job:"+message.ID+":"+target), nats.ExpectStream(JobsStream), nats.Context(ctx)); err != nil {
			return fmt.Errorf("publish fleetq message to %s: %w", target, err)
		}
	}
	if err := c.conn.FlushWithContext(ctx); err != nil {
		return fmt.Errorf("flush fleetq publish: %w", err)
	}
	return nil
}

func (c *Client) PublishTask(ctx context.Context, spec TaskSpec) (Message, error) {
	if strings.TrimSpace(spec.Text) == "" {
		return Message{}, errors.New("fleetq task text is required")
	}
	if strings.TrimSpace(spec.CWD) == "" {
		return Message{}, errors.New("fleetq task cwd is required")
	}
	if spec.Timeout <= 0 {
		spec.Timeout = 3600
	}
	if spec.Sandbox == "" {
		spec.Sandbox = "danger-full-access"
	}
	id, err := newID()
	if err != nil {
		return Message{}, err
	}
	message := Message{Schema: Schema, ID: id, From: c.cfg.Machine, To: append([]string(nil), spec.To...), Kind: "job.request", Text: spec.Text, CreatedAt: time.Now().Format(time.RFC3339), Meta: map[string]any{
		"action": "codex.prompt", "cwd": spec.CWD, "sandbox": spec.Sandbox, "timeout_s": spec.Timeout,
		"reply": spec.Reply,
	}}
	if spec.Model != "" {
		message.Meta["model"] = spec.Model
	}
	message.Meta["request_id"] = message.ID
	if err := c.Publish(ctx, message); err != nil {
		return Message{}, err
	}
	return message, nil
}

func (c *Client) Next(ctx context.Context) (*Delivery, error) {
	consumer := consumerName(c.cfg.Machine)
	subscription, err := c.js.PullSubscribe(jobsPrefix(c.cfg.Machine), consumer, nats.BindStream(JobsStream))
	if err != nil {
		return nil, fmt.Errorf("fleetq subscribe consumer: %w", err)
	}
	defer subscription.Unsubscribe()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		messages, err := subscription.Fetch(1, nats.MaxWait(c.cfg.FetchWait))
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(messages) == 0 {
			continue
		}
		var message Message
		if err := json.Unmarshal(messages[0].Data, &message); err != nil {
			_ = messages[0].Term()
			return nil, fmt.Errorf("decode fleetq message: %w", err)
		}
		return &Delivery{Message: message, msg: messages[0], ackWait: c.cfg.AckWait}, nil
	}
}

func (d *Delivery) Ack() error { return d.msg.Ack() }

func (d *Delivery) Nak(delay time.Duration) error {
	if delay <= 0 {
		return d.msg.Nak()
	}
	return d.msg.NakWithDelay(delay)
}

func (d *Delivery) InProgress() error { return d.msg.InProgress() }

func (d *Delivery) Term() error { return d.msg.Term() }

func (d *Delivery) AckWaitDuration() time.Duration { return d.ackWait }

func jobsPrefix(machine string) string   { return JobsPrefix + machine }
func consumerName(machine string) string { return "fleetq-" + machine }

func uniqueTargets(targets []string) []string {
	result := make([]string, 0, len(targets))
	for _, target := range targets {
		if target != "" && !slices.Contains(result, target) {
			result = append(result, target)
		}
	}
	return result
}

func (c *Client) ensureResources() error {
	if _, err := c.js.StreamInfo(JobsStream); errors.Is(err, nats.ErrStreamNotFound) {
		if _, err := c.js.AddStream(&nats.StreamConfig{
			Name: JobsStream, Subjects: []string{JobsPrefix + ">"}, Storage: nats.FileStorage,
			Retention: nats.LimitsPolicy, MaxAge: 30 * 24 * time.Hour, MaxBytes: 512 << 20,
			Discard: nats.DiscardOld, Duplicates: 10 * time.Minute, Replicas: 1,
		}); err != nil {
			return fmt.Errorf("fleetq create jobs stream: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("fleetq jobs stream unavailable: %w", err)
	}
	if _, err := c.js.StreamInfo(EventsStream); errors.Is(err, nats.ErrStreamNotFound) {
		if _, err := c.js.AddStream(&nats.StreamConfig{
			Name: EventsStream, Subjects: []string{EventsSubject}, Storage: nats.FileStorage,
			Retention: nats.LimitsPolicy, MaxAge: 30 * 24 * time.Hour, MaxBytes: 128 << 20,
			Discard: nats.DiscardOld, Duplicates: 10 * time.Minute, Replicas: 1,
		}); err != nil {
			return fmt.Errorf("fleetq create events stream: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("fleetq events stream unavailable: %w", err)
	}
	if info, err := c.js.ConsumerInfo(JobsStream, consumerName(c.cfg.Machine)); err == nil {
		if info.Config.AckWait != c.cfg.AckWait || info.Config.MaxAckPending != 1 || info.Config.MaxDeliver != 5 {
			cfg := info.Config
			cfg.AckWait = c.cfg.AckWait
			cfg.MaxAckPending = 1
			cfg.MaxDeliver = 5
			if _, updateErr := c.js.UpdateConsumer(JobsStream, &cfg); updateErr != nil {
				return fmt.Errorf("fleetq update consumer: %w", updateErr)
			}
		}
		return nil
	} else if !errors.Is(err, nats.ErrConsumerNotFound) {
		return fmt.Errorf("fleetq consumer lookup: %w", err)
	}
	_, err := c.js.AddConsumer(JobsStream, &nats.ConsumerConfig{Durable: consumerName(c.cfg.Machine), FilterSubject: jobsPrefix(c.cfg.Machine), AckPolicy: nats.AckExplicitPolicy, AckWait: c.cfg.AckWait, MaxDeliver: 5, MaxAckPending: 1, DeliverPolicy: nats.DeliverAllPolicy, ReplayPolicy: nats.ReplayInstantPolicy})
	if err != nil {
		return fmt.Errorf("fleetq create consumer: %w", err)
	}
	return nil
}

func newID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	// NATS de-duplication only needs a process-unique ID here; timestamp plus
	// a monotonic-free random suffix is sufficient for personal-local use.
	return fmt.Sprintf("%d-%x", time.Now().UnixNano(), bytes), nil
}
