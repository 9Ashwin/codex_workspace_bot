# FleetQ Linux Hermes 接入指引

`fleetq-hermes` 是不依赖 Codex Workspace Bot 的独立消费者，适合运行在 Linux Hermes
节点。它只负责连接 Tailscale 网络中的 NATS JetStream、领取发给本机的 `job.request`，
调用本机 Hermes 命令，并把 stdout 发布为 `job.result`。

## 工作流

```text
mac-mini Bot → NATS JetStream → fleetq-hermes(linux)
                                      ↓
                                Hermes handler
                                      ↓
                         NATS job.result → mac-mini Bot → 飞书
```

Hermes 节点不需要安装 Codex，不需要运行 Bot，也不需要通过飞书 Bot 中转消息。

## 编译与运行

在 Linux 节点拉取代码并编译：

```bash
git clone https://github.com/9Ashwin/codex_workspace_bot.git ~/codex_workspace_bot
cd ~/codex_workspace_bot
git pull --rebase origin main
go build -o ~/.local/bin/fleetq-hermes ./cmd/fleetq-hermes
```

准备环境变量。`FLEETQ_NATS_URL` 必须使用 NATS 所在机器的 Tailscale 地址；凭据只放在
环境变量或 credentials 文件中，不要写入仓库：

```bash
export FLEETQ_MACHINE=linux
export FLEETQ_NATS_URL=nats://<mac-mini-tailscale-ip>:4222
export FLEETQ_NATS_CREDS=/path/to/fleetq-linux.creds
export FLEETQ_HERMES_HANDLER='/path/to/hermes-handler --fleetq'
fleetq-hermes
```

如果使用 token，把 `FLEETQ_NATS_CREDS` 换成 `FLEETQ_NATS_TOKEN`。两种认证方式只配置
一种，避免客户端认证配置冲突。

## Handler 合同

每个任务会以完整 JSON 同时提供给 handler：

- stdin：完整 FleetQ 消息 JSON。
- 最后一个命令行参数：临时 JSON 文件路径，文件内容同样是完整消息。
- 工作目录：`meta.cwd` 展开后的目录。
- `FLEETQ_MACHINE`：本机名。
- `FLEETQ_MSG_ID`：当前消息 ID。
- `FLEETQ_MSG_PATH`：临时 JSON 文件路径。

handler 的 stdout 会作为结果文本发布；stderr 会保留在本机服务日志中。退出码非 0 会
发布 `status=failed` 的结果并按 JetStream 重试，成功结果发布 `status=completed` 后才会
ack。消费者还会向 `FLEETQ_EVENTS` 发布 `claimed`、`running` 和终态事件。

请求中的 `meta.request_id` 和 `meta.reply` 会原样带入结果消息，mac-mini 上的 Bot 据此
把结果投递回原飞书会话。Handler 应按 `FLEETQ_MSG_ID` 或 `meta.request_id` 做幂等处理，
因为 AckWait 超时或进程崩溃时同一任务可能重新投递。

Bot 侧的 `fleetq.task` 默认只回传发起方会话；如果需要目标 Bot 也显示结果，使用
`notify_target=true`。服务端会从当前可信飞书路由生成 `meta.notify`，模型不能直接传入
任意 `app_id` 或 `receive_id`。

## systemd（可选）

将以下内容保存为 `/etc/systemd/system/fleetq-hermes.service`，把路径替换为 Linux 节点
实际路径：

```ini
[Unit]
Description=FleetQ Hermes consumer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=hermes
WorkingDirectory=/home/hermes/codex_workspace_bot
EnvironmentFile=/etc/fleetq-hermes.env
ExecStart=/home/hermes/.local/bin/fleetq-hermes
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

`/etc/fleetq-hermes.env` 示例：

```dotenv
FLEETQ_MACHINE=linux
FLEETQ_NATS_URL=nats://<mac-mini-tailscale-ip>:4222
FLEETQ_NATS_CREDS=/etc/fleetq/fleetq-linux.creds
FLEETQ_HERMES_HANDLER=/home/hermes/bin/hermes-fleetq-handler
```

启动并检查：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now fleetq-hermes
journalctl -u fleetq-hermes -f
```

部署前先确认 `tailscale ping <mac-mini>`，并确认 NATS 端口仅监听 Tailscale 私网地址。
`fleetq-hermes` 启动时会建立 NATS 连接并初始化所需的 JetStream stream/consumer；连接
失败会直接打印错误退出。
