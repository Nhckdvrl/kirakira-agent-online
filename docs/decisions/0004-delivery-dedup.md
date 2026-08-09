# 0004 主动投递采用内容指纹与时间窗去重

- 状态：accepted

## 背景

进程可能在 Channel 已发送成功、但本地提交尚未完成时崩溃。重启后若没有持久标记，同一内容会重复推送。

## 决定

使用 `deliveries(session_key, delivery_key, sent_at)` 持久表。`delivery_key` 是消息内容指纹，发送前标记，
Channel 明确失败时撤销，成功后提交 Session、consume 和 cooldown。

```text
mark → dispatch → success commit
                ↘ explicit failure → unmark
```

## 取舍

mark 后、真正发送前崩溃可能漏发；发送后、本地提交前崩溃则能防止重复。主动消息更重视少打扰，因此选择
窗口内至多一次，不引入完整两阶段 outbox，也不宣称 exactly-once。

时间窗使用 delivery cooldown；超窗后相同内容仍可再次发送。
