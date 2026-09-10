# FleetQ 协议

FleetQ 使用 NATS JetStream 传输任务，不通过飞书 Bot 互相转发机器消息。

## 消息方向

- `job.request`：发起机投递给目标机，`meta.request_id` 是稳定任务 ID。
- `job.result`：执行机投递回发起机，结果携带原始 `reply` 路由。
- `job.notice`：可选的目标机飞书通知，不参与任务执行。
- `job.status`：写入 `FLEETQ_EVENTS` 的生命周期事件。

## 状态语义

| 状态 | 含义 |
| --- | --- |
| `accepted` | NATS 已接受 `job.request`，不代表远程任务完成 |
| `claimed` | 目标消费者已领取消息 |
| `running` | 目标 handler/Codex 已开始执行 |
| `completed` | 执行机已发布成功结果 |
| `failed` | 执行机已发布失败结果；是否重试由 ack/lease 决定 |
| `delivered` | 发起机或通知目标的 Bot 已成功调用飞书发送接口 |

`fleetq.task` 是异步工具，立即返回只能使用 `accepted`。调用方必须等待后续
`job.result`，不能把入队成功当成执行完成。

## 回执路由

`job.request.meta.reply` 始终表示发起任务的飞书会话，目标执行机不应自行推断路由。
如果还要让目标 Bot 的飞书会话显示结果，受信任的发布端额外提供（对外 dynamic tool
只暴露 `notify_target=true`，由服务端生成此路由）：

```json
{
  "notify": {
    "machine": "mac-mini",
    "reply": {
      "app_id": "app-1",
      "receive_id": "oc-chat",
      "receive_type": "chat_id"
    }
  }
}
```

执行机先发布 `job.result`，再尽力发布一个独立的 `job.notice`。通知失败不能导致原
任务重新执行；目标 Bot 对 `job.notice` 单独 ack、重试和幂等。`request_id`、结果 ID 和
通知 ID 均不可变，处理器必须允许 JetStream 重投。

## 兼容性

新增状态、`executor` 和 `notify` 都位于已有消息的 `meta` 中，未知字段必须被旧版本
忽略，因此 `SCHEMA` 保持为 `1`。动态工具 Schema 变化会提升持久化 catalog version，
当前版本为 `s10-fleetq-v2`，旧 Codex Thread 会按 catalog 升级状态机重新建立。
