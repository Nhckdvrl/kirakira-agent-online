---
name: codex-delegate
description: 把长代码库任务委托给本机 Codex CLI 后台执行。当用户说用 codex skill、codexskill、codex delegate、委托 codex、后台 codex、阻塞 codex exec、subagent 跑 codex 时使用。
metadata: {"skill": {"always": false, "requires": {"bins": ["codex"]}}}
---

# Codex Delegate

## 目标

把一个可以独立完成的长任务交给后台 subagent；subagent 内部用 `bash` 同步阻塞等待 `codex exec` 完成，最后由后台 subagent 把结果带回主会话。

## kirakira 工具映射

- 委托后台任务用 `spawn(mode="background", profile="scripting")`（不是 `run_in_background=true`）。
- 阻塞执行 shell 用 `bash`（不是 `shell`）；用 `auto_promote=false` 保持同步。
- **kirakira 的 `bash` 在 `auto_promote=false` 且不传 `timeout` 时只等 120s**，codex 是长任务，必须显式传大 `timeout`（例如 `timeout=21600`），否则会误判超时。
- 读写文件用 `write_file` / `read_file`（workspace 相对路径）。

## 流程

```
┌─ 主会话
│  ├─ bash(command="command -v codex && codex --version", auto_promote=false)
│  └─ spawn(mode="background", profile="scripting"|"general")
│     └─ 后台 subagent
│        ├─ bash 建工作目录：workdir=$(mktemp -d "$PWD/.kirakira/codex-XXXXXX")
│        ├─ write_file(<workdir>/prompt.txt)
│        ├─ bash(auto_promote=false, timeout=21600)
│        │  └─ codex exec --cd <repo> --output-last-message <workdir>/codex-result.md - < <workdir>/prompt.txt
│        │     └─ 阻塞等待完成
│        ├─ read_file(<workdir>/codex-result.md)
│        └─ read_file(<workdir>/codex-session.txt)
└─ subagent 完成后回灌结果
```

## 使用规则

1. 主 agent 必须先检查 `codex` 是否存在：`bash(command="command -v codex && codex --version", auto_promote=false)`。失败就直接告诉用户本机没有可用 Codex CLI，不要 spawn。不要把这个检查下放给 subagent。
2. 如果用户已给出 repo 路径，主 agent 不要为了理解任务先 `list_dir`、`read_file`、`bash find/grep` 探索该仓库；只把 repo 路径和任务目标原样传给 `spawn`。代码库探索由 Codex 完成。
3. 只有用户没给 repo 路径、路径明显缺失、或用户要求先确认路径时，主 agent 才做最小路径检查。
4. 主 agent 不要在 spawn task 里写“重点读取这些文件”“候选路径如下”这类预探索结果；除非用户原文明确指定文件。spawn task 只含用户给的 repo 路径、用户目标、输出要求和本技能的执行约束。
5. Codex prompt 必须要求 Codex 自己从整个 repo 发现入口、目录和相关文件，而不是沿主 agent 预选的文件列表工作。
6. 检查通过后，外层必须用 `spawn(mode="background")`，不要在主会话里直接跑长时间 `codex exec`。
7. subagent 的 `profile` 选 `scripting`；若任务还需联网调研，选 `general`。
8. subagent 内部调用 `bash` 时必须 `auto_promote=false` 并显式传大 `timeout`，不要 `run_in_background=true`。
9. `codex exec` 用 `--cd <repo>` 指定工作目录，不要依赖 shell 的 `cd` 状态。
10. 默认把任务说明写入 prompt 文件，再用 `codex exec --cd <repo> - < prompt.txt` 读取，避免引号/换行破坏 prompt。
11. 必须加 `--output-last-message <workdir>/codex-result.md`，完成后 `read_file` 这个文件作为主要结果；不要从散落日志里 grep。
12. 同时把 stdout+stderr `tee` 到 `<workdir>/codex-run.log`（`session id: ...` 在 stderr 里）。
13. 需要复用同一 Codex 会话时，从 `codex-run.log` 提取 session id 写入 `<workdir>/codex-session.txt`，并在回复里带回。
14. prompt、result、run log、session id 都放在 subagent 自建的 `<workdir>`（在 workspace 内，如 `.kirakira/codex-*`）；不要写 `/tmp`。

## 推荐调用形态

主会话：

```text
bash(command="command -v codex && codex --version", description="检查 codex", auto_promote=false)

spawn(
  task="用户给出的 repo 路径是 /path/to/repo；用户目标是：<原样概括用户目标>。不要预探索这个仓库，不要用主 agent 预选文件列表；Codex 必须把 /path/to/repo 当完整代码库，自行发现入口和相关文件。先用 bash 建工作目录 workdir=$(mktemp -d \"$PWD/.kirakira/codex-XXXXXX\")，把任务写进 <workdir>/prompt.txt，然后用 bash(auto_promote=false, timeout=21600) 阻塞执行 codex exec --cd /path/to/repo --output-last-message <workdir>/codex-result.md - < <workdir>/prompt.txt 2>&1 | tee <workdir>/codex-run.log。完成后 read_file(<workdir>/codex-result.md) 总结；从 codex-run.log 提取 session id 写入 codex-session.txt 并带回。",
  label="codex delegate",
  profile="scripting",
  mode="background"
)
```

subagent 内层阻塞执行：

```text
bash(
  command="set -o pipefail; codex exec --cd /path/to/repo --output-last-message <workdir>/codex-result.md - < <workdir>/prompt.txt 2>&1 | tee <workdir>/codex-run.log; sed -n 's/^session id: //p' <workdir>/codex-run.log | tail -1 > <workdir>/codex-session.txt",
  description="阻塞 codex",
  auto_promote=false,
  timeout=21600
)
```

续聊同一 Codex 会话：

```text
codex exec resume <session_id> --output-last-message <workdir>/codex-result-2.md - < <workdir>/prompt-2.txt
```

## 注意

- 不要给 `codex exec` 末尾加 `&`、`nohup`、`disown`，也不要 `run_in_background=true` 包装；本技能要求 `bash` 前台阻塞等完整结果。
- 用户消息里已有 repo 路径时，主 agent 不要提前读这个 repo 的文件来“了解一下”，直接委托。
- 不要把自己猜的入口文件、目录、搜索结果塞进 Codex prompt，会污染 Codex 对完整 repo 的自主分析。
- 不要轮询 `task_output`；本技能要求前台阻塞。
- `--last` 只适合人工临时用；自动化续聊必须传明确的 `<session_id>`。
