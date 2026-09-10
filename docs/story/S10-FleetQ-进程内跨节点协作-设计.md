# S10 FleetQ 进程内跨节点协作

## 目标

将跨节点任务能力整合进 `codex_workspace_bot`，每台节点只运行一个 Bot 主进程：

```text
飞书消息 → Bot Worker → FleetQ/NATS job.request
                         ↓
                  目标节点 Bot 内部 consumer / Linux Hermes adapter
                         ↓                         ↓
                  同一 Codex App Server 执行      本机 Hermes handler
                         ↓
                  job.result → 发起 Bot → 原飞书会话
```

NATS Server 仍作为 `mac-mini` 上的独立基础设施进程运行；本 Story 消除的是每台节点额外的
`fleetq daemon`、`fleetq-handler` 进程，不嵌入 NATS Server。

## 范围

- 把 FleetQ 的 NATS JetStream 最小协议实现迁入 Bot 的 `internal/fleetq`。
- Bot 启动并管理本机任务 consumer，随主进程优雅停止和重连。
- 从 Bot 的 Codex 动态工具发布远程任务，默认 `danger-full-access`。
- 目标节点复用现有 Codex App Server，不另起 `codex exec` 或第二个 App Server。
- 对不运行 Codex Workspace Bot 的 Linux Hermes 节点，提供独立 `fleetq-hermes` consumer；
  它只依赖 `internal/fleetq` 和本机 handler，不依赖 Codex App Server。
- 任务结果回到发起节点，并根据不可变 `request_id` 投递到原飞书 `ReplyTarget`。
- `fleetq.task` 只返回 `accepted`；`completed/failed` 来自目标执行结果，`delivered` 来自
  发起 Bot 的飞书发送确认。
- 目标机飞书展示必须通过显式 `meta.notify` 路由生成独立 `job.notice`，通知失败不得重跑原任务。
- 任务/结果至少一次投递；NATS message ID、request ID 和本地持久化状态用于幂等。

## 不在范围

- 不把 NATS Server 嵌入 Bot。
- 不通过飞书 Bot 之间转发机器消息。
- 不改变现有普通飞书会话的 Worker 串行语义、App Server 生命周期或权限审批契约。
- 不再把独立 `agent-fleet-queue` 仓库作为运行时依赖；协议实现和 `fleetq-hermes` 入口统一
  在本仓库维护，旧仓库仅可作为历史迁移参考。

## 消息契约

`job.request` 的 `meta` 至少包含：

- `action=codex.prompt`
- `cwd`、`sandbox=danger-full-access`、`timeout_s`、可选 `model`
- `request_id`：Bot 内部稳定关联 ID
- `reply`：`app_id`、`receive_id`、`receive_type`、`chat_type`、`chat_id`，只用于发起节点回飞书
- `status=accepted`：仅表示任务已进入 NATS
- 可选 `notify`：由服务端从 `notify_target=true` 和当前可信飞书路由生成目标 Bot 的
  `machine + reply`；执行机通过独立 `job.notice` 投递，不与原任务 ack 绑定

`job.result` 回显 `request_id`、`executor`、`completed/failed` 状态、摘要和可选文件引用。
发起 Bot 成功发送到飞书后记录 `delivered`；目标通知单独 ack、重试和幂等，不能因为通知
失败重新执行原任务。

## 处理边界

- `internal/fleetq` 只负责 NATS 连接、stream/consumer、publish、pull、ack 和消息模型。
- FleetQ bridge 负责任务/结果路由，不依赖飞书 SDK。
- `cmd/fleetq-hermes` 负责非 Bot 节点的领取、租约续期、外部 handler 执行和结果发布，
  不引入 Codex 或飞书运行时依赖。
- `cmd/server` 负责把 bridge 接到已有 `runtime`、`outputs` 和配置。
- Codex 动态工具只做任务发布；不得让模型直接拼装 NATS ack token。
- 结果投递优先由发起节点使用创建任务时的 App sender 发送，避免目标节点没有原飞书 App 权限。

## 验收场景

1. `fleetq.task` 动态工具生成 `job.request`，默认 sandbox 为 `danger-full-access`，并保存原会话回复目标。
2. 目标 Bot 自动领取任务，复用唯一 Codex App Server，在指定 `cwd` 执行并发布 `job.result`。
3. 发起 Bot 收到 `job.result` 后把结果发回同一个飞书会话，重投不重复创建任务。
4. NATS 暂时不可用时 Bot 主进程仍能保持既有飞书服务，bridge 记录重连状态。
5. handler/turn 失败时发布失败结果，原任务不会静默丢失。
6. Bot 优雅退出时关闭 consumer，不留下额外 fleetq 进程。
7. Linux Hermes 只运行 `fleetq-hermes` 即可消费发给 `linux` 的任务，并把结果回传到原会话。

## 风险与决策

- `danger-full-access` 是个人自用部署的明确选择；仍保留显式 sandbox 字段以兼容旧消息。
- 任务执行使用现有 App Server 的 `thread/start` / `turn/start`，每个 FleetQ job 使用独立
  thread，不污染飞书频道 thread。
- 结果消息必须带 `request_id` 与回复目标；仅靠 `from` 机器名无法定位原飞书会话。
- 本地 MySQL 增加 FleetQ request/result 关联表，作为结果投递幂等和重启恢复的真相源。
