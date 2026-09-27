# Transport-agnostic LLM Agent Core

一个**与 IM 平台解耦**的群聊 Agent 核心：事件消费 → 上下文工程 → LLM 推理 → 回复发送，
全链路可用 Mock **离线跑通**；真实平台适配器不随仓库提供，实现一个接口即可接入。

- **传输层无关**：所有消息进出收口在 `Transport` 接口（5 个方法）+ 自带 `MockTransport`
- **上下文工程**：实体归一（一人多昵称 → 同一身份）、按需注入、mtime 缓存、上下文去重
- **会话管理**：按群复用 + 轮数阈值交接轮换 + 空闲归档（防上下文无限膨胀）
- **工具能力**：自带 `botkb` MCP server（知识检索 / 原文读取 / 取图 / 以图搜源）
- **工程兜底**：超时中断+轮换、重试上限、额度监控与熔断、白名单 fail-closed

结构总览见 [`ARCHITECTURE.md`](ARCHITECTURE.md)。

---

## 快速开始（离线演示，无需任何外部服务）

前置：Python 3.9+。

```bash
# 1) 准备运行时目录（示例数据 → 运行位置）
cp examples/bot_whitelist.json bot_whitelist.json
cp -r examples/BotMemory BotMemory

# 2) 投入一条合成 @ 事件
mkdir -p pending_replies
cp examples/events/at_demo_001.json pending_replies/

# 3) 跑一轮（LLM_STUB=1 = 离线模式，不调用真实模型）
LLM_STUB=1 python watcher/watcher.py process
```

Windows PowerShell：

```powershell
Copy-Item examples\bot_whitelist.json .
Copy-Item -Recurse examples\BotMemory .
New-Item -ItemType Directory -Force pending_replies | Out-Null
Copy-Item examples\events\at_demo_001.json pending_replies\
$env:LLM_STUB=1; python watcher\watcher.py process
```

预期输出（节选）：

```
[..] [watcher] 发现 1 个待处理事件，进入 3s 去重窗口...
[..] [watcher]   📦 移入处理队列: at_demo_001.json
[MockTransport] send_group_msg -> 【离线演示回复（LLM_STUB=1）】已收到：…
[..] [watcher]   ✅ 已发送 (1/1)
处理完成，共 1 个事件
```

发送记录同时写入 `logs/mock_transport.log`（JSONL）。
第二条示例事件（图片链路）在 `examples/events/at_demo_002.json`，同样方式投入即可。

## 接入真实 LLM（opencode）

1. 安装并启动 `opencode serve`（默认 `http://127.0.0.1:14104`）。
2. 在 opencode 配置中创建两个 agent：
   - `group-reply`：群聊回复（读记忆 + 生成回复文本；**不给终端/发送能力**）
   - `group-task`：任务委托执行者（可读文件 / 联网检索；同样无发送能力）
3. 不设 `LLM_STUB`（或设 0）运行：

```bash
python watcher/watcher.py process   # 手动跑一轮
python watcher/watcher.py start     # 常驻守护
python watcher/watcher.py status    # 查看队列
```

> 会话级权限 ruleset 在 `watcher/opencode_client.py` 中集中定义
> （外部目录白名单由 `AGENT_ALLOW_DIRS` 控制）。

## 接入真实平台（Transport）

1. 实现 `watcher/transport.py` 的 `Transport` 接口（参考 `MockTransport` 的接口形状）。
2. 凭据**只走外部注入**（环境变量 → 本地文件），示范见 `watcher/example_napcat_auth.py`。
3. 在 `get_transport()` 中注册你的适配器（或改自己的副本）。

> 注：作者自用的平台适配器（NapCat / OneBot 系）因平台 ToS 合规原因不随仓库提供。

## 工具能力（botkb MCP server）

`mcp-servers/botkb/server.py` 是一个 stdio MCP server（无第三方依赖）：

| 工具 | 作用 | 依赖 |
|---|---|---|
| `search_theory` | 知识库检索（评分 + 章节聚合） | `sample_kb/`（可替换为你的库） |
| `list_sources` / `read_source` | 列出 / 读取原文章节 | 组件的 `references/chapters/` |
| `view_image` | 读取本地归档的群图（MCP 图片内容块） | `images/`（`BOTKB_IMAGE_DIR`） |
| `trace_image` | 以图搜源（SauceNAO） | 可选 key（`SAUCENAO_API_KEY` 或 `saucenao_key.txt`） |

新增知识源 = 写一个 `*_search.py` 检索库 + 在 `TOOLS` 注册一个工具（架构不变）。
示例库结构（结构同构的无版权假数据）见 `sample_kb/`；快速自测：

```bash
python mcp-servers/botkb/kb_search.py --stats
python mcp-servers/botkb/kb_search.py "检索评分"
```

## 环境变量（主要）

| 变量 | 默认 | 说明 |
|---|---|---|
| `TRANSPORT` | `mock` | 传输适配器（本仓库内仅 mock 可用） |
| `LLM_STUB` | `0` | `1` = 离线演示（不调用 opencode） |
| `OPENCODE_SERVE_URL` | `http://127.0.0.1:14104` | LLM 层地址 |
| `OPENCODE_AGENT` / `OPENCODE_TASK_AGENT` | `group-reply` / `group-task` | agent 名 |
| `BOT_PENDING_DIR` / `BOT_LOG_DIR` | `<repo>/pending_replies` / `<repo>/logs` | 队列 / 日志目录 |
| `BOT_DEFAULT_GROUP` / `BOT_ADMIN_QQ` / `BOT_QQ` | 演示占位号码 | 默认群 / 管理员 / bot 自身 |
| `BOTKB_KB_DIR` / `BOTKB_IMAGE_DIR` | `<botkb>/sample_kb` / `<repo>/images` | 知识库 / 图片目录 |
| `AGENT_ALLOW_DIRS` | 空 | 追加 agent 可读目录（分号分隔） |

（其余配置项见各文件头部 `os.getenv` 默认值。）

## 数据与脱敏说明

本仓库是从作者自用的完整系统中**抽取、精简、脱敏**的副本：

- 不含任何真实账号 / 群号 / 聊天记录 / 凭据；
- 示例数据（名册、人格、知识库、事件）均为**结构同构的合成假数据**，
  其中的号码为演示占位值，非真实号码；
- 运行期产物（`pending_replies/`、`logs/`、`images/`、`BotMemory/`）已在 `.gitignore` 中。

## 依赖

- Python 3.9+（无第三方必装依赖）
- 可选：`Pillow`（`view_image` 图片处理）；opencode（真实 LLM 层；开发环境版本 1.18.x）

## 相关项目

- [`social-bot-persona-memory`](https://github.com/Yuki-0079/social-bot-persona-memory)
  —— 配套的「群成员语言风格蒸馏 + 五层记忆架构」系统（框架无关的 skill 形态）：
  本仓库上下文工程所消费的群记忆 / 名册数据，即由这类系统产出。

## License

[MIT](LICENSE)
