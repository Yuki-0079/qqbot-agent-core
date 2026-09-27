#!/usr/bin/env python3
"""
opencode HTTP API 客户端 — LLM 回复通道
==========================================================
通过本地 opencode serve（默认 http://127.0.0.1:14104，由 `opencode serve`
进程提供）的 HTTP API 完成推理调用：

调用链路:
  POST /session                          → 创建会话（带权限 ruleset）
  POST /session/{sid}/message            → 同步发送 prompt，返回完整回复（含 info/parts）
  GET  /global/health                    → 健康检查

设计:
  - 会话是真实 server 侧对象（消息追加，上下文由 server 维护）
  - 每群一个 session 复用，达轮数阈值交接轮换（防上下文无限膨胀）
  - 权限通过会话级 ruleset 控制（external_directory 等需显式 allow）
  - 离线演示：设 LLM_STUB=1 时不调用 opencode、返回合成回复（供 Mock 全链路演练）

用法:
  from opencode_client import call_opencode, serve_health, create_session, rotate_session
  reply = call_opencode(prompt, group_id, timeout=150)
"""

import json, os, time, uuid
import urllib.request
import urllib.error

# ==================== 配置 ====================

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(MODULE_DIR)
OPENCODE_SERVE_URL = os.getenv("OPENCODE_SERVE_URL", "http://127.0.0.1:14104")
OPENCODE_AGENT = os.getenv("OPENCODE_AGENT", "group-reply")
LOG_DIR = os.getenv("BOT_LOG_DIR", os.path.join(REPO_DIR, "logs"))

import re  # noqa: E402  （LLM_STUB 回显用）

# 会话映射持久化（{gid: {sid, ts, rounds}}）
SESSION_MAP_FILE = os.getenv("OC_SESSION_MAP_FILE", os.path.join(LOG_DIR, "oc_sessions.json"))

# 单次调用总超时（秒）
RUN_TIMEOUT = int(os.getenv("AUTO_REPLY_TIMEOUT", "150"))

# ==================== 权限 ruleset ====================
# 会话级权限：agent 只负责「读记忆 + 生成回复文本 + 写记忆」，发送由 watcher 完成
# → bash 全 deny（防 agent 自行调用 send 导致重复发群）。
# 2026 年事故记录：agent 被问到某个本地服务状态后，用 webfetch 反复访问本地回环服务
# （127.0.0.1 端口）陷入 76 步工具循环，每次调用 150s 超时 → watcher 轮换会话 →
# 旧 run 在 serve 里僵尸堆积 → 全链路瘫痪。群聊信息全部由 watcher 注入 prompt，
# agent 无需访问本地回环服务。
# 权限补充说明：
#  ① external_directory 放开「agent 可读的目录」——用户需求「让 bot 读本地文件结合内容回复」。
#     默认放开：仓库目录 + opencode 配置目录；如需追加，用环境变量 AGENT_ALLOW_DIRS
#     （分号分隔）。历史部署曾放开读全盘，仓库版默认收敛为白名单目录。
#  ② serena_execute_shell_command 全 deny —— serena 是独立于 bash 的 shell 通道，
#     会话级 bash deny 拦不住（frontmatter 里 allow）。实测 agent 曾借 serena 写入外部目录。
#  ③ 检索类能力走 botkb 知识检索 MCP server（只读）：仅放行 botkb 这一个 MCP server，
#     bash / serena / 本地回环 webfetch 的既有 deny 全部不动。
_HOME = os.path.expanduser("~")
ALLOW_DIRS = [REPO_DIR, os.path.join(_HOME, ".config", "opencode")]
ALLOW_DIRS += [p for p in os.getenv("AGENT_ALLOW_DIRS", "").split(";") if p]
_EXTERNAL_DIR_RULES = [
    {"permission": "external_directory", "pattern": os.path.join(d, "**"), "action": "allow"}
    for d in ALLOW_DIRS
]

PERMISSION_RULESET = _EXTERNAL_DIR_RULES + [
    # MCP 工具 permission 类别：两种格式——server 级 mcp__<server> 与裸工具名。
    # 两条都显式 allow，双保险防落 ask 卡死。
    # ⚠️ 关键认知：MCP 工具「是否挂载」只看 agent frontmatter（allow 才挂载）；
    # 会话级 ruleset 加 allow 无法把没挂载的工具补上。此处显式声明是为了：
    # ① 防会话级被 deny；② 与 botkb 的声明一致；③ server 级 + 裸工具名两种写法都覆盖。
    {"permission": "mcp__searxng", "pattern": "*", "action": "allow"},
    {"permission": "searxng_searxng_web_search", "pattern": "*", "action": "allow"},
    {"permission": "mcp__botkb", "pattern": "*", "action": "allow"},
    {"permission": "search_theory", "pattern": "*", "action": "allow"},
    # 索图/溯源通道（botkb 工具）
    {"permission": "view_image", "pattern": "*", "action": "allow"},
    {"permission": "trace_image", "pattern": "*", "action": "allow"},
    {"permission": "bash", "pattern": "*", "action": "deny"},
    {"permission": "serena_execute_shell_command", "pattern": "*", "action": "deny"},
    {"permission": "webfetch", "pattern": "http://127.0.0.1:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "http://localhost:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "http://0.0.0.0:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "https://127.0.0.1:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "https://localhost:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "*", "action": "allow"},
    {"permission": "skill", "pattern": "*", "action": "allow"},
    {"permission": "read", "pattern": "*", "action": "allow"},
    {"permission": "write", "pattern": "*", "action": "allow"},
    {"permission": "edit", "pattern": "*", "action": "allow"},
]


# ==================== 任务会话 ruleset ====================
# 任务会话 = 群会话委托的重活执行者（回顾历史记录/读文件分析/联网搜索整理）。
# 权限设计：能干活（读按白名单/写按规范/联网搜外网），不能乱说话（无 send 通道）。
# 与群会话的关系：群会话收紧（防事故复发），任务会话放开（干活的会话有干活的权限）。
TASK_PERMISSION_RULESET = _EXTERNAL_DIR_RULES + [
    # 联网搜索（任务会话的"联网检索"是核心职责）。
    # 同上：挂载与否由 agent frontmatter 决定，此处为显式声明 + 防 deny。
    # 同上：挂载与否由 group-task agent 的 frontmatter 决定，此处为显式声明 + 防 deny。
    {"permission": "mcp__searxng", "pattern": "*", "action": "allow"},
    {"permission": "searxng_searxng_web_search", "pattern": "*", "action": "allow"},
    # bash：不在此列条目（实测 2026-09-01：会话级 bash 条目会覆盖 frontmatter 的 bash
    # 白名单导致 bash 工具不可用）。bash 工具挂载与白名单由 group-task agent 的 frontmatter
    # 决定（只 allow 白名单脚本 + status）；非白名单命令无 bash 路径可走。
    # serena 通道一律堵死（2026-09-01 安全洞）
    {"permission": "serena_execute_shell_command", "pattern": "*", "action": "deny"},
    # webfetch：外网 allow，本地回环 deny（沿用事故修复，防 agent 直连本地服务）
    {"permission": "webfetch", "pattern": "http://127.0.0.1:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "http://localhost:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "http://0.0.0.0:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "https://127.0.0.1:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "https://localhost:*", "action": "deny"},
    {"permission": "webfetch", "pattern": "*", "action": "allow"},
    {"permission": "skill", "pattern": "*", "action": "allow"},
    {"permission": "read", "pattern": "*", "action": "allow"},
    {"permission": "write", "pattern": "*", "action": "allow"},
    {"permission": "edit", "pattern": "*", "action": "allow"},
]

# 任务会话使用的 agent
TASK_AGENT = os.getenv("OPENCODE_TASK_AGENT", "group-task")
# 任务会话最长执行预算（秒）：长文本分析等可能跑很久
TASK_RUN_TIMEOUT = int(os.getenv("TASK_RUN_TIMEOUT", "600"))


# ==================== 基础请求 ====================

def _http(method, path, body=None, timeout=RUN_TIMEOUT):
    """发 HTTP 请求，返回 (status, json_or_text)。异常已包装。"""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        OPENCODE_SERVE_URL + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw.strip() else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")[:500]
    except TimeoutError:
        raise TimeoutError(f"opencode serve 超时（{timeout}s）: {method} {path}")
    except Exception as e:
        raise RuntimeError(f"opencode serve 请求失败: {e}")


def _stub_enabled():
    """离线演示开关：LLM_STUB=1 时不调用 opencode（供 Mock 全链路演练/自动化验证）。"""
    return os.getenv("LLM_STUB", "0").strip().lower() not in ("", "0", "false", "no")


def _stub_reply(prompt, group_id):
    """合成回复：回显首条 @ 事件摘要，便于肉眼确认链路走通。"""
    m = re.search(r"【@事件 1】([^\n]{0,80})", prompt or "")
    return ("【离线演示回复（LLM_STUB=1）】已收到：" + (m.group(1).strip() if m else f"群 {group_id} 的消息")
            + "。当前未调用真实模型；设 LLM_STUB=0 并启动 opencode serve 后走真实链路。")


def serve_health():
    """检查 opencode serve 是否可达。返回 (ok, detail)。"""
    if _stub_enabled():
        return True, "stub（离线演示模式，未连接 opencode）"
    try:
        code, body = _http("GET", "/global/health", timeout=5)
        if code == 200 and isinstance(body, dict) and body.get("healthy"):
            return True, f"opencode {body.get('version')}"
        return False, f"HTTP {code}: {str(body)[:100]}"
    except Exception as e:
        return False, str(e)[:150]


# ==================== 会话管理 ====================

def _load_sessions():
    try:
        with open(SESSION_MAP_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_sessions(sessions):
    try:
        tmp = SESSION_MAP_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sessions, f, ensure_ascii=False)
        os.replace(tmp, SESSION_MAP_FILE)
    except Exception:
        pass


def create_session(group_id, title=None):
    """创建带权限 ruleset 的会话，返回 session_id。"""
    if title is None:
        title = f"group_{group_id}"
    code, body = _http("POST", "/session", {
        "title": title,
        "agent": OPENCODE_AGENT,
        "permission": PERMISSION_RULESET,
    }, timeout=30)
    if code != 200 or not isinstance(body, dict) or "id" not in body:
        raise RuntimeError(f"创建会话失败: HTTP {code} {str(body)[:200]}")
    return body["id"]


def rotate_session(group_id, sessions=None):
    """轮换群会话：创建新 session 并更新映射。返回新 session_id。"""
    if _stub_enabled():
        return f"stub-session-{group_id}"
    if sessions is None:
        sessions = _load_sessions()
    sid = create_session(group_id)
    sessions[str(group_id)] = {"sid": sid, "ts": int(time.time()), "rounds": 0}
    _save_sessions(sessions)
    return sid


def interrupt_session(sid):
    """
    中断 serve 端正在运行的 run。
    事故：客户端 150s 超时后轮换新会话，但 serve 端的 run 并未取消——
    僵尸 run 继续在 serve 里跑（甚至多个会话并发），把模型调用链路
    挤到全部超时 → 新调用也超时 → 无限堆积。超时后必须先 interrupt 清理。
    """
    if _stub_enabled():
        return True
    try:
        code, _ = _http("POST", f"/api/session/{sid}/interrupt", timeout=10)
        return code in (200, 204)
    except Exception:
        return False


# ==================== 任务会话（2026-09-01 新增） ====================
# 群会话 agent 判断重活 → 输出 [任务] 标记 → watcher 建任务会话异步执行 →
# 结果回传群会话加工。任务会话权限宽松（TASK_PERMISSION_RULESET），
# 用专用 task agent（无发消息能力）。

TASK_MAP_FILE = os.path.join(LOG_DIR, "task_sessions.json")


def _load_tasks():
    try:
        with open(TASK_MAP_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_tasks(tasks):
    try:
        tmp = TASK_MAP_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(tasks, f, ensure_ascii=False, indent=1)
        os.replace(tmp, TASK_MAP_FILE)
    except Exception:
        pass


def create_task_session(desc):
    """创建任务会话（宽松 ruleset + group-task agent），返回 session_id。"""
    title = f"task_{int(time.time())}_{desc[:20]}"
    code, body = _http("POST", "/session", {
        "title": title,
        "agent": TASK_AGENT,
        "permission": TASK_PERMISSION_RULESET,
    }, timeout=30)
    if code != 200 or not isinstance(body, dict) or "id" not in body:
        raise RuntimeError(f"创建任务会话失败: HTTP {code} {str(body)[:200]}")
    return body["id"]


def start_task_async(sid, task_prompt):
    """异步发起任务（prompt_async，204 立即返回，不阻塞）。"""
    code, _ = _http("POST", f"/session/{sid}/prompt_async", {
        "parts": [{"type": "text", "text": task_prompt}],
        "agent": TASK_AGENT,
    }, timeout=15)
    return code == 204


def _run_active(sid):
    """
    查 serve 的 session 运行状态（2026-09-11 新增）。
    返回 True=run 仍在跑(busy) / False=已结束 / None=查询失败(未知)。

    端点：GET /session/status → `{"<sid>": {"type": "busy"}}`；
    run 结束后该 sid **从字典里消失**（实测：0-15s 为 busy，18s 起消失）。
    这是判断「run 是否真的结束」的唯一可靠来源——message 的内容/条数都不行（见下）。
    """
    try:
        code, st = _http("GET", "/session/status", timeout=10)
    except Exception:
        return None
    if code != 200 or not isinstance(st, dict):
        return None
    entry = st.get(sid)
    if not entry:
        return False
    return str(entry.get("type", "")).lower() == "busy"


def _latest_assistant_text(msgs):
    """取最后一条「有文本」的 assistant 回复（末条若纯工具调用则往前找）。"""
    if not isinstance(msgs, list):
        return ""
    for m in reversed(msgs):
        info = m.get("info") or {}
        if info.get("role") != "assistant":
            continue
        texts = [p.get("text", "") for p in (m.get("parts") or [])
                 if p.get("type") == "text"]
        joined = "\n".join(t for t in texts if t).strip()
        if joined:
            return joined
    return ""


def poll_task_result(sid, timeout=TASK_RUN_TIMEOUT, poll_interval=10):
    """
    轮询任务会话直到 run 真正结束，返回 (ok, 最终 assistant 文本)。

    完成判据（2026-09-11 重写，修「任务结果回传被截断」bug）：
      · 主判据：`GET /session/status` 里该 sid 不再 busy（serve 的真实运行状态）。
      · 兜底：从未观察到 busy 时（任务比轮询间隔还快，或状态端点不可用），
        退回「文本连续 3 轮相同」。
      · 主判据必须**先见过 busy** 才生效，否则任务刚发起时（status 里还没出现）
        会被误判成「已完成」。

    ⚠️ 旧实现只看「最后文本连续 2 轮不变」——任务执行中的**中间过渡文本**完全能
    持续多轮不变。实测（2026-09-11）：消息数在 12s/15s 连续两轮都是 5、文本也稳定，
    但 serve 仍报 busy，17s 才真正结束。后果：中间过渡语被当成最终结果回传——
    实锤事故：任务会话成功产出完整报告，群里只收到
    "I have enough source material. Let me compile the report."（群会话据此说
    "外包的检索断了"），**任务成功了，回传丢了结果**。
    """
    deadline = time.time() + timeout
    last_text = ""
    stable_rounds = 0
    seen_busy = False

    while time.time() < deadline:
        active = _run_active(sid)
        if active is True:
            seen_busy = True

        try:
            code, msgs = _http("GET", f"/session/{sid}/message", timeout=10)
        except Exception:
            code, msgs = -1, None
        if code == 200:
            text = _latest_assistant_text(msgs)
            if text:
                if text == last_text:
                    stable_rounds += 1
                else:
                    last_text = text
                    stable_rounds = 1

        # 主判据：见过 busy 且现在不 busy → run 结束，取最终文本
        if seen_busy and active is False:
            if last_text:
                return True, last_text
            return False, "任务已结束但没有输出内容"

        # 兜底：从未见 busy（任务极快 / 状态端点不可用）→ 保留旧的文本稳定判据
        if not seen_busy and last_text and stable_rounds >= 3:
            return True, last_text

        time.sleep(poll_interval)

    if last_text:
        return True, last_text  # 有部分结果也返回
    return False, f"任务超时（{timeout}s）"


def get_session_id(group_id, sessions=None, force_new=False):
    """
    群会话 ID：默认复用映射中的 session；force_new 时轮换新建。
    返回 (session_id, sessions)。
    """
    if _stub_enabled():
        return f"stub-session-{group_id}", (sessions or {})
    if sessions is None:
        sessions = _load_sessions()
    gid = str(group_id)
    entry = sessions.get(gid)
    if not force_new and entry and entry.get("sid"):
        entry["ts"] = int(time.time())
        entry["rounds"] = entry.get("rounds", 0) + 1
        _save_sessions(sessions)
        return entry["sid"], sessions
    sid = create_session(gid)
    sessions[gid] = {"sid": sid, "ts": int(time.time()), "rounds": 1}
    _save_sessions(sessions)
    return sid, sessions


# ==================== 核心调用 ====================

def _extract_reply(body):
    """从 message 响应解析 assistant 回复文本。"""
    if not isinstance(body, dict):
        return ""
    info = body.get("info") or {}
    if info.get("role") == "assistant":
        parts = body.get("parts") or []
        texts = [p.get("text", "") for p in parts if p.get("type") == "text"]
        return "\n".join(t for t in texts if t).strip()
    return ""


def call_opencode(prompt, group_id, timeout=RUN_TIMEOUT):
    """
    核心调用：向群会话发送 prompt，同步等待并返回回复文本。

    Args:
        prompt: 完整 prompt（本地组装，含 persona/上下文/@事件）
        group_id: 群号（会话复用键）
        timeout: 总超时秒数

    Returns:
        str: 回复文本

    Raises:
        RuntimeError: serve 不可达 / 会话错误 / 模型错误
        TimeoutError: 超时
    """
    if _stub_enabled():
        return _stub_reply(prompt, group_id)
    sessions = _load_sessions()
    sid, sessions = get_session_id(group_id, sessions)
    try:
        code, body = _http("POST", f"/session/{sid}/message", {
            "parts": [{"type": "text", "text": prompt}],
            "agent": OPENCODE_AGENT,
        }, timeout=timeout)
    except TimeoutError:
        # 2026-09-01：超时后必须先 interrupt 取消 serve 端 run，再轮换会话。
        # 否则僵尸 run 堆积（旧 run 继续跑），serve 并发被占满 → 后续调用全超时。
        interrupted = interrupt_session(sid)
        sessions = _load_sessions()
        rotate_session(group_id, sessions)
        if interrupted:
            raise TimeoutError(
                f"opencode 调用超时（{timeout}s），已中断旧 run 并轮换会话")
        raise
    if code == 200:
        reply = _extract_reply(body)
        if reply:
            return reply
        # 空回复但成功 → 可能无回复
        return ""
    # 404 = Session not found（serve 重启/数据目录切换后旧 session 失效）：
    # 轮换新会话并立即重试一次（避免 requeue 等下一轮）
    if code == 404 and "not found" in str(body).lower():
        sid2, sessions = get_session_id(group_id, sessions, force_new=True)
        try:
            code2, body2 = _http("POST", f"/session/{sid2}/message", {
                "parts": [{"type": "text", "text": prompt}],
                "agent": OPENCODE_AGENT,
            }, timeout=timeout)
        except TimeoutError:
            interrupt_session(sid2)  # 2026-09-01：同上，超时先清 run
            raise
        if code2 == 200:
            reply = _extract_reply(body2)
            if reply:
                return reply
            return ""
        raise RuntimeError(f"opencode 调用失败(重试后): HTTP {code2} {str(body2)[:300]}")
    # 其他失败：轮换会话（下轮干净上下文重试），并抛异常
    sessions = _load_sessions()
    rotate_session(group_id, sessions)
    raise RuntimeError(f"opencode 调用失败: HTTP {code} {str(body)[:300]}")


# ==================== 交接支持 ====================

def summarize_session(group_id, instruction, timeout=RUN_TIMEOUT):
    """
    在群会话内发起总结（交接文档生成）。
    用当前 session 生成总结文本（不轮换，由调用方决定轮换时机）。
    """
    if _stub_enabled():
        return "（stub）本轮对话摘要占位。"
    sessions = _load_sessions()
    gid = str(group_id)
    entry = sessions.get(gid)
    if not entry or not entry.get("sid"):
        raise RuntimeError(f"群 {gid} 无活动会话，无法总结")
    code, body = _http("POST", f"/session/{entry['sid']}/message", {
        "parts": [{"type": "text", "text": instruction}],
        "agent": OPENCODE_AGENT,
    }, timeout=timeout)
    if code == 200:
        return _extract_reply(body)
    raise RuntimeError(f"总结失败: HTTP {code} {str(body)[:300]}")


if __name__ == "__main__":
    # 自检模式
    ok, detail = serve_health()
    print(f"serve health: {ok} | {detail}")
    if ok:
        test_gid = f"selfcheck_{uuid.uuid4().hex[:6]}"
        try:
            reply = call_opencode("回复：opencode 通道自检成功。", test_gid)
            print(f"reply: {reply[:200]}")
        except Exception as e:
            print(f"call failed: {e}")
