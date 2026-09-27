#!/usr/bin/env python3
"""
@事件消费守护 — 事件队列 → 上下文工程 → LLM → 回复发送
==============================================================
从 pending_replies/ 目录消费 @事件（JSON 文件），带 debounce 去重。
多个 @ 合为一次 LLM 调用处理，回复经 Transport 发送。

与具体 IM 平台解耦：所有消息收发经 transport.py 的 Transport 接口；
仓库自带 MockTransport 供本地跑通演示。真实平台适配器（如 NapCat/OneBot）
不随仓库提供——接入方式与凭据注入示范见 README 与 example_napcat_auth.py。

用法:
  python watcher.py start      ← 前台守护运行
  python watcher.py process    ← 手动处理一轮
  python watcher.py status     ← 查看队列状态
"""

import json, os, sys, time, glob, re, threading, ast, uuid
from datetime import datetime, timedelta

# 昵称规范化（实体归一：多昵称 → 同一身份）
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nickname import normalize as _normalize_nick

# LLM 层：opencode HTTP API 客户端
from opencode_client import (
    call_opencode, serve_health, summarize_session,
    rotate_session, get_session_id,
    create_task_session, start_task_async, poll_task_result,
    interrupt_session, _load_tasks, _save_tasks,
)

# 响应白名单（群 + 用户；配置见 bot_whitelist.json，热更新）
from whitelist import is_group_allowed, is_user_allowed, allowed_groups

# 传输层：消息进出统一走 Transport 接口（默认 Mock；真实适配器不随仓库提供）
from transport import get_transport

# ==================== 路径与配置 ====================
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(MODULE_DIR)              # 仓库根目录（数据目录都在根下）
PENDING_DIR = os.getenv("BOT_PENDING_DIR", os.path.join(REPO_DIR, "pending_replies"))
ARCHIVE_DIR = os.path.join(PENDING_DIR, "archive")
PROCESSING_DIR = os.path.join(PENDING_DIR, "processing")
LOG_DIR = os.getenv("BOT_LOG_DIR", os.path.join(REPO_DIR, "logs"))
WATCHER_LOCK_FILE = os.path.join(LOG_DIR, "watcher.lock")

DEBOUNCE_SEC = int(os.getenv("WATCHER_DEBOUNCE", "3"))         # 去重窗口(秒)（2026-08-08: 10s→3s，加快响应；连续 @ 拆批由独立 conv 承担，无副作用）
POLL_INTERVAL = int(os.getenv("WATCHER_POLL", "5"))             # 轮询间隔(秒)
RUN_TIMEOUT = int(os.getenv("AUTO_REPLY_TIMEOUT", "150"))       # LLM 调用总超时(秒)
# 2026-08-08: 300s→120s。实测 stuck conv 会吃满超时（用户干等 5 分钟）；
# 缩短后快速失败 → 轮换新会话重试（全新 conv 实测 8-21s 秒回）
# 2026-08-10: 120s→150s。超时链统一到 150s（serve run 超时 WORKBUDDY_RUN_TIMEOUT_MS 同步 150s），
# 略微放宽给 reasoning 思考留余量；超时后仍走「降思考强度 + 轮换会话」自动兜底。
# 2026-08-24：重试上限。长耗时任务会反复 150s 超时 → 轮换会话（丢上下文）
# → 重新入队 → 新会话从头重跑 → 无限循环（事故实锤：同事件 3 个会话先后重跑）。
# retry 计数在 _requeue_event 时 +1，process_once 加载时检查 retry >= MAX 直接放弃归档。
MAX_REQUEUE_RETRIES = int(os.getenv("WATCHER_MAX_REQUEUE_RETRIES", "2"))  # 最多完整处理 2 次，第 3 次尝试放弃

# 回复发送：走 Transport 接口（见 transport.py；真实适配器的端点/凭据由其自行加载）

MAX_MSG_LEN = int(os.getenv("MAX_MSG_LEN", "300"))              # 单条消息长度限制

# 默认群（演示用占位号码，非真实 QQ 号）
DEFAULT_GROUP_ID = os.getenv("BOT_DEFAULT_GROUP", "100000001")

# ==================== 管理权限 ====================
# ADMIN_QQ = bot 建立者/管理者（演示用占位号码）。两层用途：
#   1) 扮演层：示例作品角色卡激活且管理员在场 → 注入「制作人（P）」身份（对 P 说话）
#   2) 权限层：管理命令（检查上下文等）仅管理员可触发，私聊回复
ADMIN_QQ = os.getenv("BOT_ADMIN_QQ", "100000003")
ADMIN_NICK = "维护者"
PRODUCER_CARD_PREFIX = "card_demo_"      # 示例角色卡前缀（P 概念适用；其他卡不注入）

# bot 自己的标识（演示用占位号码；@ 解析 / 名册注入等处排除 bot 自身）
BOT_QQ = os.getenv("BOT_QQ", "100000002")

# 会话策略：2026-08-08 起每次调用生成全新 conversation_id（serve 同 conv 二次调用
# 必卡死，runState 不复位；不再复用）。
# 2026-08-17 实测：同 conv 连续 5 次调用正常且记忆累积（bug 已不再复现）→ 恢复复用；
# 达 CONV_MAX_ROUNDS 轮后生成「短期交接记忆」并轮换新会话（防上下文无限膨胀）。
CONVERSATION_TTL = int(os.getenv("CONVERSATION_TTL", "1800"))   # 保留（兼容），实际不再使用
CONV_MAX_ROUNDS = int(os.getenv("CONV_MAX_ROUNDS", "15"))       # 每群每个 conv 最大轮数，达阈值后交接轮换
CONV_IDLE_SECONDS = int(os.getenv("CONV_IDLE_SECONDS", "10800"))  # 对话不活跃超过该秒数（3h）也触发交接归档
CONV_IDS_FILE = os.path.join(LOG_DIR, "conv_ids.json")          # 会话 ID 持久化（watcher 重启保留记忆）
HANDOFF_TITLE = "## 短期交接记忆（上一段对话的整理文档，你的历史记忆，回答相关话题时可引用）"
_last_conv = {}  # group_id -> (conversation_id, timestamp, rounds)
_conv_ids_persisted = {}  # 重启恢复用：{gid: {cid, ts, rounds}}
_EPOCH = int(time.time())

# ==================== 任务会话状态（2026-09-01 新增） ====================
# 单任务串行：同一时间只跑一个任务（用户决策 A）。任务状态持久化 task_sessions.json
# （client 侧 _load_tasks/_save_tasks），watcher 重启后恢复未完成任务继续轮询。
_active_task = None  # {task_id, gid, sid, desc, status, created}
TASK_MARK_RE = re.compile(r'\[任务\]\s*(.+?)(?:\n|$)', re.S)  # agent 输出 [任务] <描述>
TASK_EXPLICIT_RE = re.compile(r'^\s*#任务\s+(.+)', re.S)      # 群友显式触发 #任务 <描述>

# 自维护问答历史（2026-08-08 加入，弥补新 conv 丢失的跨消息记忆）：
# 每群记录最近 N 轮 (清洗后问题, 回复)，构建 prompt 时注入最近 M 轮。
# 持久化到 logs/conv_memory.json（watcher 重启不丢）。
CONV_MEM_FILE = os.getenv("CONV_MEM_FILE", os.path.join(LOG_DIR, "conv_memory.json"))
CONV_MEM_MAX_ROUNDS = int(os.getenv("CONV_MEM_MAX_ROUNDS", "6"))    # 每群最多保留轮数
CONV_MEM_INJECT = int(os.getenv("CONV_MEM_INJECT", "3"))            # prompt 注入轮数
_conv_memory = {}  # group_id -> list[(question, reply, ts)]

# 日文假名段剥离（2026-08-09 加入）：注入「你的历史回复」时清洗日语，
# 防止旧日语口癖回复（ふん…ねー 等）作为自我复读源污染当前角色扮演。
_JP_KANA_RE = re.compile(r"[ぁ-んァ-ヶー〜]+")


def _strip_jp(text):
    """剥离回复文本中的日文假名段（保留汉字/中文，用于注入层清洗）"""
    return _JP_KANA_RE.sub("…", text) if text else text

# 显式长期记忆（2026-08-09 加入）：bot 主动甄别"值得记住的内容"，回复中输出
# [记忆:xxx] 标记 → watcher 静默写入 Notes_{group}.md（可读可查可验证）。
# 与问答历史分工：问答历史=短期自动（最近对话）；Notes=长期显式（事实/约定/偏好）。
BOTMEMORY_DIR = os.path.join(REPO_DIR, "BotMemory")
NOTES_MAX_PER_REPLY = int(os.getenv("NOTES_MAX_PER_REPLY", "3"))    # 每轮最多记录条数
NOTES_MAX_LEN = int(os.getenv("NOTES_MAX_LEN", "100"))              # 单条最长字数
NOTES_INJECT = int(os.getenv("NOTES_INJECT", "10"))                 # prompt 注入最近条数

# 近期群消息注入条数（10→30：与上游事件管线的上下文条数对齐）。
# 注意方向：deduped_ctx 为「最新在前」（上游管线拉取历史消息即此序），
# 切片必须 [:N] 取最新 N 条；若用 [-N:] 在总量超过 N 时会取到最旧的 N 条。
CTX_INJECT_N = int(os.getenv("CTX_INJECT_N", "30"))                 # prompt 注入近期上下文条数

for d in [PENDING_DIR, ARCHIVE_DIR, PROCESSING_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)


def _log(msg, tag="watcher"):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] [{tag}] {msg}"
    print(line, flush=True)
    try:
        logfile = os.path.join(LOG_DIR, "watcher.log")
        with open(logfile, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ==================== 队列操作 ====================

# 已成功处理的事件文件名（2026-08-09 防循环兜底）：
# 归档失败（rename 被拦截/权限）时事件文件会残留队列导致无限重复处理刷屏，
# 记录"已处理过"的文件名，下次扫描直接跳过——即使再发生归档失败，最多处理一次。
# 2026-08-09 12:15 修复：持久化到 processed.json（原为纯内存，watcher 重启后清空
# 导致残留文件全部重放 = "一启动就开始循环"，见消息循环事故复盘）。
_PROCESSED_FILE = os.path.join(LOG_DIR, "processed.json")
_PROCESSED_KEEP_DAYS = 30


def _load_processed():
    """启动时加载已处理事件记录；只保留近 30 天（文件名含日期 at_YYYYMMDD_*）"""
    try:
        with open(_PROCESSED_FILE, "r", encoding="utf-8") as f:
            names = json.load(f)
        cutoff = (datetime.now() - timedelta(days=_PROCESSED_KEEP_DAYS)).strftime("%Y%m%d")
        return {n for n in names if len(n) >= 13 and n[3:13] >= cutoff}
    except Exception:
        return set()


def _save_processed():
    """已处理记录落盘（每次处理后调用，防进程重启丢失）"""
    try:
        with open(_PROCESSED_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(_processed_ok), f, ensure_ascii=False)
    except Exception:
        pass


def _mark_processed(fp):
    """标记事件已处理并落盘（归档失败也会被记录，防复活）"""
    _processed_ok.add(os.path.basename(fp))
    _save_processed()


def _cleanup_stale_pending():
    """启动时归档上次会话已处理的残留文件（processed.json 有记录），
    防止重启后残留事件重放刷屏"""
    try:
        stale = [f for f in _scan_pending() if os.path.basename(f) in _processed_ok]
        for f in stale:
            _archive_event(f)
        if stale:
            _log(f"  🧹 启动清理已处理残留 {len(stale)} 个")
    except Exception as e:
        _log(f"  ⚠️ 启动残留清理异常: {e}")


_processed_ok = _load_processed()


def _scan_pending():
    files = glob.glob(os.path.join(PENDING_DIR, "at_*.json"))
    files.sort(key=lambda f: os.path.getmtime(f))
    return files


def _load_event(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def _archive_event(filepath):
    if not os.path.exists(filepath):
        return
    dest = os.path.join(ARCHIVE_DIR, os.path.basename(filepath))
    for attempt in range(3):
        try:
            os.rename(filepath, dest)
            return
        except FileNotFoundError:
            _log(f"  ⚠️ 归档跳过（源已被其他实例处理）: {os.path.basename(filepath)}")
            return
        except PermissionError:
            if attempt < 2:
                time.sleep(0.5)
                continue
            # 2026-08-09 12:15：重试耗尽仍被锁（杀软实时防护/外部句柄）→
            # 事件已处理完毕，直接删除残留，防止 debounce/重启复活导致重复回复刷屏。
            # （若连删除也被拒，残留文件靠 processed.json 记录防复活）
            try:
                os.remove(filepath)
                _log(f"  🗑️ 归档被拒且重试耗尽，已删除已处理残留: {os.path.basename(filepath)}")
            except Exception as e2:
                _log(f"  ❌ 归档失败且删除失败: {os.path.basename(filepath)}（{e2}）——残留靠 processed.json 防复活")
        except Exception as e:
            _log(f"  ❌ 归档失败: {os.path.basename(filepath)}（{e}）——残留靠 processed.json 防复活")


def _requeue_event(filepath):
    if not os.path.exists(filepath):
        return
    base = os.path.basename(filepath)
    dest = os.path.join(PENDING_DIR, base)
    # 2026-08-24：重试计数。每次重新入队 retry+1，process_once 加载时检查超限放弃，
    # 防长耗时任务"超时→轮换会话→从头重跑"无限循环（事故：同事件 3 会话先后重跑刷屏）。
    try:
        ev = _load_event(filepath)
        ev["retry"] = int(ev.get("retry", 0)) + 1
        tmp = filepath + ".retry_tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(ev, f, ensure_ascii=False)
        os.replace(tmp, filepath)
        _log(f"  🔄 重新入队: {base} (retry={ev['retry']}/{MAX_REQUEUE_RETRIES})")
    except Exception as e:
        _log(f"  ⚠️ 重试计数写入失败（仍重新入队）: {base}（{e}）")
    try:
        os.rename(filepath, dest)
    except Exception as e:
        _log(f"  ⚠️ 重新入队失败: {e}")


# ==================== Transport 发送 ====================

_transport = None


def _t():
    """Transport 单例。默认 MockTransport（本地演示）；真实平台适配器
    （如 NapCat/OneBot）不随仓库提供 —— 见 README「接入真实平台」。"""
    global _transport
    if _transport is None:
        _transport = get_transport()
    return _transport


def _split_message(text, max_len=MAX_MSG_LEN):
    """按自然边界拆分长消息"""
    if len(text) <= max_len:
        return [text]
    segments = []
    remaining = text.strip()
    while remaining:
        if len(remaining) <= max_len:
            segments.append(remaining)
            break
        chunk = remaining[:max_len]
        for sep in ["\n\n", "\n", "。", "！", "？", "；", "，", "、"]:
            pos = chunk.rfind(sep)
            if pos > max_len // 3:
                segments.append(remaining[:pos + len(sep)])
                remaining = remaining[pos + len(sep):].strip()
                break
        else:
            segments.append(chunk.strip())
            remaining = remaining[max_len:].strip()
    return segments


def _resolve_at_target(target, registry=None):
    """解析 [at:xxx] 目标 → 账号。数字直接返回；昵称查 registry 反查；查不到返回 None。
    模型输出 [at:昵称或账号] 标记 → 转成平台可识别的 CQ:at 码。

    ⚠️ 修复记录：**增加 bot 自身排除**。
    背景：`_resolve_persona_target`（切扮演）早已排除 bot，但本函数从未加同样排除
    → `[at:某称呼]` 会命中 bot 在 registry 里的遗留别名，即 **@ 到 bot 自己**；
    数字自 @ 同样无意义。
    **通用教训：同一个排除规则必须在所有相关代码路径上一起加，否则混淆会从另一扇门进来。**
    """
    target = target.strip()
    if not target:
        return None
    if target.isdigit():
        # bot 自己：@ 自己没有意义（会真的在群里 @ 到自己）
        return None if target == BOT_QQ else target
    if registry is None:
        try:
            with open(os.path.join(PERSONA_DIR, "registry.json"), encoding="utf-8") as f:
                registry = json.load(f)
        except Exception:
            return None
    if not isinstance(registry, dict):
        return None
    # 反查：昵称精确或包含匹配（优先精确）。排除 bot 自己（见 docstring）
    for qq, aliases in registry.items():
        if str(qq) == BOT_QQ or not isinstance(aliases, list):
            continue
        if target in aliases:
            return str(qq)
    for qq, aliases in registry.items():
        if str(qq) == BOT_QQ or not isinstance(aliases, list):
            continue
        if any(target in a for a in aliases if isinstance(a, str)):
            return str(qq)
    return None


_AT_MARK_RE = re.compile(r"\[at:([^\]]{1,30})\]")
_registry_cache = None  # 昵称→账号映射缓存


def _get_registry():
    global _registry_cache
    if _registry_cache is None:
        try:
            with open(os.path.join(PERSONA_DIR, "registry.json"), encoding="utf-8") as f:
                _registry_cache = json.load(f)
        except Exception:
            _registry_cache = {}
    return _registry_cache


def _convert_at_marks(text):
    """把回复中的 [at:xxx] 标记转为 CQ:at 码。

    2026-09-13 改：**解析失败时剥掉标记，而不是原样保留**。
    原行为（保留原标记）会把 `[at:某位群友]` 这类未解析标记**当普通文本发到群里**
    （实测泄漏过：未解析的 [at:…] 标记原样出现在群消息里）。
    标记是给机器看的，泄漏到群聊纯属噪音 → 改为静默剥除。

    注意：`[at:]` 解析到 bot 自己时现在返回 None（见 `_resolve_at_target`），
    因此同样走"剥除"分支，不会在群里 @ 到 bot 自己。
    """
    registry = _get_registry()

    def _repl(m):
        qq = _resolve_at_target(m.group(1), registry)
        if qq is None:
            _log(f"  ⚠️ [at:{m.group(1)}] 无法解析为群成员，已剥除（避免标记泄漏到群）")
            return ""
        return f"[CQ:at,qq={qq}] "

    return _AT_MARK_RE.sub(_repl, text)


def _send_reply(group_id, text):
    """分段发送回复到群。返回 (ok_count, fail_count)"""
    if not text or not text.strip():
        return 0, 0
    # —— 发送层保护（2026-08-17）：非白名单群拒绝发送（事件入口 + 队列过滤的兜底）——
    if not is_group_allowed(group_id):
        _log(f"  🛡️ 拦截发送到非白名单群 {group_id}（白名单: {allowed_groups()}）")
        return 0, 0
    text = _convert_at_marks(text)
    segments = _split_message(text)
    ok = 0
    fail = 0
    for i, seg in enumerate(segments, 1):
        result = _t().send_group_msg(group_id, seg)
        if result.get("status") == "ok":
            ok += 1
            _log(f"  ✅ 已发送 ({i}/{len(segments)})")
        else:
            fail += 1
            _log(f"  ❌ 发送失败 ({i}/{len(segments)}): {str(result.get('error',''))[:100]}")
    return ok, fail


# ==================== 任务会话流程（2026-09-01 新增） ====================
# 架构（用户确认）：群会话 agent 判断重活 → 输出 [任务] 标记 →
# watcher 建任务会话（宽松 ruleset + group-task agent）→ prompt_async 异步跑 →
# 后台轮询（600s）→ 结果回传群会话 agent 加工（决策 B）→ 发群。
# 单任务串行（决策 A）：_active_task 非 None 时新任务告知稍等。
# 不做取消机制（决策 A）：任务一旦发起跑到完成/超时。

def _extract_task_desc(reply):
    """从群会话 agent 回复中提取 [任务] 标记描述。无标记返回 None。"""
    m = TASK_MARK_RE.search(reply or "")
    if m:
        return m.group(1).strip()
    return None


def _task_prompt(task_desc, gid, requester):
    """组装任务会话 prompt：任务描述 + 群背景 + 规范引用。"""
    return "\n".join([
        f"【任务委托】来自群 {gid}（发起人: {requester}）的任务：",
        task_desc,
        "",
        "【背景】群聊上下文可读 BotMemory/ 了解（Detailed/ 今日记录、Slang、Persona 等）。",
        "【规范】写文件前先读工作区 AGENT_WRITE_GUIDE.md（如已配置）。",
        "【输出】完成后只输出任务结果（结论先行、信息完整）。失败则如实说明原因。",
        "你只输出结果，不要尝试发送任何消息。",
    ])


def _start_task(gid, task_desc, requester):
    """启动任务：占位回复 → 建任务会话 → 异步发起 → 后台轮询。"""
    global _active_task
    if _active_task is not None:
        _send_reply(gid, "上一个任务还在处理中，稍等一下再试~")
        return False
    try:
        task_id = f"task_{int(time.time())}"
        sid = create_task_session(task_desc)
        if not start_task_async(sid, _task_prompt(task_desc, gid, requester)):
            _log(f"  ❌ 任务异步发起失败: {sid}")
            _send_reply(gid, "任务启动失败，稍后再试吧~")
            return False
        task = {"task_id": task_id, "gid": str(gid), "sid": sid,
                "desc": task_desc[:50], "requester": requester,
                "status": "running", "created": int(time.time())}
        tasks = _load_tasks()
        tasks[task_id] = task
        _save_tasks(tasks)
        _active_task = task
        _log(f"  🚀 任务已启动: {task_id} (群{gid}, sid={sid}) desc={task_desc[:40]}")
        _send_reply(gid, "收到，正在处理，稍等~")
        threading.Thread(target=_task_runner,
                         args=(task_id, gid, sid, task_desc, requester),
                         daemon=True).start()
        return True
    except Exception as e:
        _log(f"  ❌ 任务启动异常: {e}")
        _send_reply(gid, "任务启动失败，稍后再试吧~")
        return False


def _task_runner(task_id, gid, sid, task_desc, requester):
    """后台线程：轮询任务结果 → 回传群会话加工 → 发群 → 清状态。"""
    _log(f"  🔄 任务 {task_id} 开始执行（预算 600s），后台轮询中...")
    ok, result = poll_task_result(sid, timeout=600)
    try:
        if not ok:
            interrupt_session(sid)
            _send_reply(gid, f"任务没跑完（{result}），可以换个说法再试~")
            _log(f"  ⏰ 任务 {task_id} 未完成: {result}")
            return
        _log(f"  📦 任务 {task_id} 完成，回传群会话加工...")
        polished = _polish_task_result(gid, task_desc, result)
        if polished:
            _send_reply(gid, polished)
            _log(f"  ✅ 任务结果已发送 (群{gid})")
        else:
            _send_reply(gid, result[:MAX_MSG_LEN * 3])
            _log("  ⚠️ 加工失败，原样发送任务结果")
    except Exception as e:
        _log(f"  ❌ 任务 {task_id} 收尾异常: {e}")
        _send_reply(gid, f"任务结果获取异常：{e}")
    finally:
        _finish_task(task_id)


def _polish_task_result(gid, task_desc, result):
    """决策 B：任务结果回传群会话 agent，以当前 persona 组织成群聊回复。
    失败返回 None（调用方原样发送）。"""
    try:
        prompt = (
            "【任务结果回传】你之前判断这是重活并委托执行，现在结果回来了。\n\n"
            "注意：这不是新的 @ 消息处理，**不要执行 6 步流程、不要加载 skill、"
            "不要写任何记忆文件（Core.md/Detailed/Topics/Users 都不要动）、"
            "不要输出 📬/✅/TASK_COMPLETE 等状态标记**。\n\n"
            "原任务：\n"
            f"{task_desc}\n\n"
            "执行结果：\n"
            f"{result[:3000]}\n\n"
            "请以你的风格（当前 persona/群聊习惯）把结果组织成适合直接发到群里的回复。"
            "结论先行，口语自然，不要复述任务过程。**只输出要发到群里的回复内容本身。**"
        )
        reply = call_opencode(prompt, str(gid), timeout=RUN_TIMEOUT)
        reply = (reply or "").strip()
        reply = _strip_status_block(reply)
        reply = re.sub(r'^(回复id[:：]?\d+[,，:：、\s]*)+', '', reply).strip()
        return reply or None
    except Exception as e:
        _log(f"  ⚠️ 回传加工失败: {e}")
        return None


def _finish_task(task_id):
    """任务结束：清内存状态 + 更新持久化状态。"""
    global _active_task
    if _active_task and _active_task.get("task_id") == task_id:
        _active_task = None
    tasks = _load_tasks()
    if task_id in tasks:
        tasks[task_id]["status"] = "done"
        _save_tasks(tasks)


def _resume_tasks():
    """watcher 启动恢复：status=running 的任务重新轮询（防重启丢任务）。"""
    tasks = _load_tasks()
    for tid, t in tasks.items():
        if t.get("status") == "running":
            _log(f"  🔄 恢复任务 {tid}: {t.get('desc','')[:40]}")
            _active_task = t
            threading.Thread(target=_task_runner,
                             args=(tid, t["gid"], t["sid"],
                                   t.get("desc", ""), t.get("requester", "?")),
                             daemon=True).start()


def _try_task_command(events):
    """#任务 显式触发（本地指令，watcher 正则判别）：提取描述直接建任务。
    返回 (remaining_events, handled_count)。"""
    remaining = []
    handled = 0
    for fp, ev in events:
        msg = str(ev.get("message", ""))
        m = TASK_EXPLICIT_RE.search(msg)
        if m:
            desc = m.group(1).strip()
            _log(f"  📋 显式任务指令: {desc[:50]}")
            _start_task(ev.get("group_id"), desc, ev.get("nickname", "?"))
            handled += 1
        else:
            remaining.append((fp, ev))
    return remaining, handled


def _send_private(qq, text):
    """分段发送私聊消息（管理命令用，2026-08-09 v1.11）。返回 (ok_count, fail_count)"""
    if not text or not text.strip():
        return 0, 0
    segments = _split_message(text)
    ok = 0
    fail = 0
    for i, seg in enumerate(segments, 1):
        result = _t().send_private_msg(qq, seg)
        if result.get("status") == "ok":
            ok += 1
            _log(f"  📨 私聊已发送 ({i}/{len(segments)})")
        else:
            fail += 1
            _log(f"  ❌ 私聊发送失败 ({i}/{len(segments)}): {str(result.get('error',''))[:100]}")
    return ok, fail


# ==================== Persona 注入 ====================

def _persona_state_file(group_id=None):
    """扮演状态文件路径。group_id 提供 → DEFAULT_{gid}.txt（按群分离，2026-08-17）；
    group_id 为空 → DEFAULT.txt（兼容旧调用，一般不触发）。"""
    persona_dir = os.path.join(BOTMEMORY_DIR, "Persona")
    if group_id:
        return os.path.join(persona_dir, f"DEFAULT_{group_id}.txt")
    return os.path.join(persona_dir, "DEFAULT.txt")


def _get_target_id(group_id=None):
    """读取当前激活的扮演目标 id。group_id 提供 → 按群状态（互不干涉）。"""
    path = _persona_state_file(group_id)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _load_persona_json(target_id):
    botmemory_dir = os.path.join(REPO_DIR, "BotMemory")
    persona_dir = os.path.join(botmemory_dir, "Persona")
    json_path = os.path.join(persona_dir, f"{target_id}.persona.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _load_system_prompt_snippet(group_id=None):
    target_id = _get_target_id(group_id)
    if not target_id:
        return None
    persona = _load_persona_json(target_id)
    if persona:
        snippet = persona.get("system_prompt_snippet", "")
        if snippet:
            return snippet
    botmemory_dir = os.path.join(REPO_DIR, "BotMemory")
    persona_dir = os.path.join(botmemory_dir, "Persona")
    md_path = os.path.join(persona_dir, f"{target_id}.md")
    if os.path.exists(md_path):
        try:
            with open(md_path, "r", encoding="utf-8") as f:
                content = f.read()
            m = re.search(r"## 模仿指南\s*```\s*(.*?)\s*```", content, re.DOTALL)
            if m:
                return m.group(1).strip()
        except Exception:
            pass
    return None


def _group_or_global_file(basename, group_id=None, ext=".md"):
    """按群文件优先（{basename}_{gid}{ext}），缺失回退全局（{basename}{ext}）。
    2026-08-17：群俚语/成员共识按群分离，新群无自己的蒸馏文件时继承全局。"""
    if group_id:
        p = os.path.join(BOTMEMORY_DIR, f"{basename}_{group_id}{ext}")
        if os.path.exists(p):
            return p
    return os.path.join(BOTMEMORY_DIR, f"{basename}{ext}")


def _load_slang_summary(max_entries=6, group_id=None):
    """读取群俚语词条摘要（词 + 含义一句话）。按群文件 Slang_{gid}.md 优先，缺失回退 Slang.md。"""
    path = _group_or_global_file("Slang", group_id)
    if not os.path.exists(path):
        return ""
    try:
        with open(path, encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return ""
    # 解析 "### 词条" 块：标题行 + 含义行
    lines_out = []
    current = None
    for ln in content.splitlines():
        if ln.startswith("### "):
            current = ln[4:].strip()
        elif current and ln.strip().startswith("- **含义**"):
            meaning = ln.split("**", 2)[-1].strip(" :")
            if not meaning or meaning.startswith("{"):  # 跳过占位/空条目
                current = None
                continue
            lines_out.append(f"- {current}：{meaning}")
            current = None
            if len(lines_out) >= max_entries:
                break
    return "\n".join(lines_out)


def _load_standard_users_summary(group_id=None):
    """读取群成员共识（稳定称呼）。按群文件 StandardUsers_{gid}.md 优先，缺失回退全局。"""
    path = _group_or_global_file("StandardUsers", group_id)
    if not os.path.exists(path):
        return ""
    try:
        with open(path, encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return ""
    lines_out = []
    current_qq = None
    for ln in content.splitlines():
        if ln.startswith("## "):
            current_qq = ln[3:].strip()
        elif current_qq and ln.strip().startswith("- 稳定称呼"):
            names = ln.split(":", 1)[-1].strip()
            lines_out.append(f"- {current_qq}：{names}")
            current_qq = None
    return "\n".join(lines_out) if lines_out else ""


# ==================== 群成员名册注入（2026-09-12 新增）====================
# 背景：群友把群名片当状态栏用，名片极端易变（实测 21 账号中 12 个有多昵称，
#   个别成员名片变体多达 12 种），而事件里的 nickname 就是群名片
#   → 模型每轮看到的名字都可能不同 → 搞混群员 / 分不清谁在 @ 自己。
#
# 依据：BotMemory/Members_{gid}.json（build_member_roster.py 生成，
#   合并平台实时名册 + 历史名片变体 + Users.md aliases + 人工 StandardUsers）。
#   由离线名册管线定期刷新。
#
# 注入策略（按需，控 token）：活跃度 Top ROSTER_TOP_N + 本批事件/上下文中出现的人。
ROSTER_TOP_N = 12
ROSTER_ALIAS_MAX = 4          # 每人最多展示几个曾用名片


# ==================== 角色名册注入（2026-09-12 新增）====================
# 目的：给"认出图中角色"提供**本地锚点**，取代原先的「看图→猜→联网搜」流程。
#
# 原流程的缺陷（2026-09-12 实测）：
#   ① 模型不知道本地卡库有哪些角色 → 无法系统性覆盖 card_*.persona.json；
#   ② **确认偏误**：先猜一个答案、再搜它的特征 → 搜到的当然符合 →
#      把搜索结果当成自己判断的佐证（循环论证），误认时无法自我发现；
#   ③ 每次都要联网搜，慢且贵。
#
# 数据源：BotMemory/CharacterRoster.json（build_character_roster.py 生成，
#   从卡库 + 角色知识库知识库抽取外观）。**别名来自 _load_card_index()**（与切换逻辑同源，
#   以 card_id join），避免两处维护同一份别名而漂移。
#
# 注入策略：**仅本轮窗口内有图时**注入 → 无图零开销。
_character_roster_cache = {}


def _load_character_roster():
    """读 CharacterRoster.json，带 mtime 缓存。无则 None。"""
    path = os.path.join(BOTMEMORY_DIR, "CharacterRoster.json")
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None
    hit = _character_roster_cache.get("data")
    if hit and hit[0] == mtime:
        return hit[1]
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        chars = data.get("characters") or {}
    except Exception:
        chars = {}
    _character_roster_cache["data"] = (mtime, chars)
    return chars or None


def _character_roster_prompt(max_aliases=4):
    """构建角色名册注入块（紧凑单行制）。无名册文件时返回 ""。"""
    chars = _load_character_roster()
    if not chars:
        return ""
    try:
        card_index = _load_card_index()
    except Exception:
        card_index = {}

    lines = []
    for cid, r in chars.items():
        name = r.get("name") or cid
        bits = [name]
        if r.get("work"):
            bits.append(f"《{r['work']}》")
        if r.get("appearance"):
            bits.append(str(r["appearance"]))
        if r.get("features"):
            bits.append(str(r["features"]))
        # 别名：取 CARD_ALIASES 里该卡的别名（与扮演切换同源），去掉与 name 重复的
        al = [a for a in (card_index.get(cid) or []) if a and a != name]
        if al:
            bits.append("群里叫: " + "/".join(al[:max_aliases]))
        lines.append("- " + " ｜ ".join(bits))
    return "\n".join(lines)


def _load_member_roster(group_id):
    """读名册 JSON；无则 None（调用方降级到旧的 StandardUsers 摘要）。"""
    path = os.path.join(BOTMEMORY_DIR, f"Members_{group_id}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        members = data.get("members")
        return members if isinstance(members, dict) and members else None
    except Exception:
        return None


def _member_roster_prompt(group_id, events, deduped_ctx=None):
    """构建「群成员名册」注入块。无名册文件时返回 ""（调用方回退旧摘要）。"""
    members = _load_member_roster(group_id)
    if not members:
        return ""

    # 本批涉及的人（事件发送者 + 上下文发言者）——优先注入，避免被 Top N 挤掉
    involved = []
    for _, ev in events:
        uid = str(ev.get("user_id") or "")
        if uid and uid not in involved:
            involved.append(uid)
    for c in (deduped_ctx or [])[:CTX_INJECT_N]:
        cu = str(c.get("user_id") or "")
        if cu and cu not in involved:
            involved.append(cu)

    # 排序：本批涉及者优先，其余按发言量降序
    def rank(qq):
        r = members.get(qq) or {}
        return (0 if qq in involved else 1, -(r.get("msg_count") or 0))

    # ⚠️ 跳过外群成员：Users.md 是全局文件，会把别的群的人带入本群名册
    #   （实测会因全局文件把外群成员带进小群名册）→ 不注入，避免 bot 误以为他们在本群。
    #   但**本批消息确实提到的**人仍注入（以防平台名册暂缺某人）。
    pool = [qq for qq, r in members.items()
            if not r.get("foreign") or qq in involved]
    selected = sorted(pool, key=rank)[:ROSTER_TOP_N]

    bot_qq = BOT_QQ
    lines = []
    for qq in selected:
        r = members.get(qq) or {}
        name = r.get("stable") or ""

        # bot 自己**保留在名册里**：群友会用各种昵称称呼它、
        # 也会讨论它或引用它的话，这些别名是理解群聊所必需的。
        # 但要**双标注**：① 群友可能怎么叫它 ② 它就是你（与 `@你` 的写法对齐，
        # 否则模型不知道名册里的 bot 账号和消息里的「@你」是同一个账号）。
        if qq == bot_qq:
            pool = []
            # 群名片优先：群里对它的惯用称呼比平台昵称更常用
            if r.get("card_now"):
                pool.append(str(r["card_now"]))
            if name:
                pool.append(name)
            pool.extend([a for a in (r.get("aliases") or []) if a])
            seen, aka = set(), []
            for a in pool:
                a = str(a).strip()
                if a and a not in seen:
                    seen.add(a)
                    aka.append(a)
            shown = " / ".join(aka[:ROSTER_ALIAS_MAX + 2])
            lines.append(f"- 本 bot（你自己）({qq})"
                         + (f" | 群友可能叫：{shown}" if shown else "")
                         + " | 被 @ 时显示为「你」")
            continue

        label = f"{name}({qq})" if name else f"({qq})"
        bits = [label]

        def _ok(v):
            """过滤 StandardUsers 里的占位符与与 label 重复的值"""
            v = str(v or "").strip()
            return v and v not in ("—", "-", "无", "N/A") and v != name

        if _ok(r.get("identity")):
            bits.append(str(r["identity"]).strip())
        # role 仅在身份未体现时补（避免「群主 | 群主」重复）
        if r.get("role") and r["role"] != "成员" and not _ok(r.get("identity")):
            bits.append(str(r["role"]))
        if _ok(r.get("note")):
            bits.append(str(r["note"]).strip())
        # 曾用名片：排除当前稳定称呼自身，取高频前几个
        aliases = [a for a in (r.get("aliases") or []) if a and a != name]
        if aliases:
            bits.append("曾用名片: " + " / ".join(aliases[:ROSTER_ALIAS_MAX]))
        lines.append("- " + " | ".join(bits))
    return "\n".join(lines)


# ==================== 歌词检索（2026-09-05 新增） ====================
# 背景：此前歌词靠 lorebook 静态条目 + 手工 hook 表关键词触发，覆盖只有
# 25/117 首，且 hook 归属写错会导致「贴 A 歌的歌词、注入 B 歌的歌词」
# （曾出现「贴 A 歌歌词、注入 B 歌」类归属事故）。现改为真检索。
#
# 架构：内容与归属解耦
#   · 歌词内容 → lyrics_index.json（build_lyrics_index.py 构建，可扩源）
#   · 归属判断 → 角色卡 song_meta（ip + char + 自己的曲目列表）
#
# 实现要点：watcher 侧预检索——命中结果直接拼进 prompt，模型不需要调用
# 任何工具，因此不触碰下方 build_context 中「禁止模型调用工具/访问本地
# 服务」的安全护栏（那条护栏是 2026-08-09 事故后加的，不动）。

LYRIC_SEARCH_LIMIT = 3        # 最多注入几条命中
LYRIC_SEARCH_MIN_SCORE = 45   # 低于此分视为噪声，不注入
LYRIC_SNIPPET_CHARS = 170     # 命中片段最大长度


def _search_lyrics_for_message(message_text, group_id=None, lore_entries=None):
    """统一歌词检索。命中则注入 prompt，模型据此作答。

    返回 [{'title','artist','char','ip','own','label','snippet','body_cn',...}, ...]
    其中 own/label 是归属判定结果：
      own   → 当前角色自己的歌
      ip    → 同作品其他角色的歌
      other → 作品外的歌（网易云曲库等）
    """
    if not message_text:
        return []
    try:
        if REPO_DIR not in sys.path:
            sys.path.insert(0, REPO_DIR)
        import lyrics_search
    except Exception as e:
        _log(f"  歌词检索模块加载失败: {e}")
        return []

    try:
        hits = lyrics_search.search(
            message_text, limit=LYRIC_SEARCH_LIMIT * 2,
            min_score=LYRIC_SEARCH_MIN_SCORE)
    except Exception as e:
        _log(f"  歌词检索异常: {e}")
        return []
    if not hits:
        return []

    # ---- 归属判定：读当前角色的 song_meta ----
    persona = None
    try:
        tid = _get_target_id(group_id)
        if tid:
            persona = _load_persona_json(tid)
    except Exception:
        persona = None
    sm = (persona or {}).get("song_meta") or {}
    my_titles = set(sm.get("my_song_titles_norm") or [])
    my_ip = sm.get("ip", "")
    my_char = sm.get("char", "")

    # lorebook 已覆盖的歌名 → 跳过，避免同一首歌重复注入两遍
    covered = set()
    for e in (lore_entries or []):
        nm = e.get("name", "")
        if nm:
            covered.add(lyrics_search.norm(nm))

    out = []
    for h in hits:
        s = h["song"]
        tnorm = lyrics_search.norm(s["title"])
        # 归属判定：精确归一化匹配 + 前缀匹配（Remix/变体，如
        # 「メクルメ (PAS TASTA Remix)」也是广自己的歌，只是 remix 版）
        own_flag = False
        if my_titles:
            if tnorm in my_titles:
                own_flag = True
            else:
                for mt in my_titles:
                    if mt and tnorm.startswith(mt) and len(tnorm) - len(mt) <= 40:
                        own_flag = True
                        break
        if own_flag:
            own, label = "own", "你的歌"
        elif s.get("ip") and my_ip and s.get("ip") == my_ip:
            own, label = "ip", f"同作品（{my_ip}）其他角色的歌"
        else:
            own, label = "other", "作品外的歌"

        # 该歌已由 lorebook 注入 → 不重复
        if tnorm and any(tnorm in c for c in covered):
            continue

        out.append({
            "title": s["title"],
            "artist": s["artist"],
            "char": s.get("char", ""),
            "ip": s.get("ip", ""),
            "album": s.get("album", ""),
            "own": own,
            "label": label,
            "score": h["score"],
            "kind": h["kind"],
            "snippet": (h.get("snippet") or "")[:LYRIC_SNIPPET_CHARS],
            "body_cn": (s.get("body_cn") or "")[:LYRIC_SNIPPET_CHARS],
        })
        if len(out) >= LYRIC_SEARCH_LIMIT:
            break
    return out


def _song_meta_summary(group_id=None):
    """读取当前角色卡 song_meta，生成「你的曲目」摘要（bot 判断歌词归属用）。
    无 song_meta 的角色卡返回空。"""
    try:
        tid = _get_target_id(group_id)
        if not tid:
            return ""
        persona = _load_persona_json(tid)
        if not persona:
            return ""
        sm = persona.get("song_meta") or {}
        songs = sm.get("my_songs") or []
        if not songs:
            return ""
        titles = "、".join(s.get("title", "?") for s in songs)
        ip = sm.get("ip", "")
        char = sm.get("char", "")
        return (f"- 所属 IP：{ip}\n"
                f"- 你的角色：{char}\n"
                f"- 你的全部歌曲（{len(songs)} 首）：{titles}\n"
                f"- 共通曲/全员合唱曲也算你的歌；你歌曲的 Remix 版本也算你的歌。"
                f"其余歌曲（包括同 IP 其他角色的歌）不是你的。")
    except Exception:
        return ""


def _load_lorebook_for_message(message_text, group_id=None):
    target_id = _get_target_id(group_id)
    if not target_id or not message_text:
        return []
    persona = _load_persona_json(target_id)
    if not persona:
        return []
    entries = persona.get("lorebook", {}).get("entries", [])
    # 2026-08-09：合并手动打捞/自动挖掘的话题库 Interests_{QQ}.json
    # （独立于 persona，重跑不丢；与 persona lorebook 同管线注入）
    try:
        ipath = os.path.join(BOTMEMORY_DIR, f"Interests_{target_id}.json")
        if os.path.exists(ipath):
            with open(ipath, encoding="utf-8") as f:
                idata = json.load(f)
            for c in idata.get("interests", []):
                # 2026-08-09 增强：①keys 过滤单字（"消"会误触发"消息"）②content 带 sample
                # 典型消息（注入信息量：summary 是"是什么"，sample 是真实语境）③relation
                # 个人关系（"A 喜欢 B"这类命题——bot 既知道话题是什么、也知道对象与它的关系）
                keys = [k for k in c.get("keys", [c.get("topic", "")]) if len(str(k)) >= 2]
                if not keys:
                    continue
                content = f"{c.get('topic')}（{c.get('type','')}）: {c.get('summary','')}"
                if c.get("relation") and c.get("relation") != "泛提及":
                    content += f" 本人{c.get('relation')}"
                if c.get("sample"):
                    content += f" 例：「{c.get('sample')}」"
                entries.append({
                    "keys": keys,
                    "content": content,
                    "category": "关键话题",
                })
    except Exception:
        pass
    if not entries:
        return []
    msg_lower = message_text.lower()
    matched = []
    for entry in entries:
        keys = entry.get("keys", [])
        # 2026-08-09 防御：历史遗留/生成 bug 可能把 keys 序列化成 str
        # （如 "['话题A', '角色名']"），迭代字符串会变成单字匹配——
        # 导致单字误命中大量条目（任一含该字的条目都会触发）。
        # str 则尝试解析回 list，解析失败按单个完整字符串处理。
        if isinstance(keys, str):
            try:
                keys = ast.literal_eval(keys)
            except Exception:
                keys = [keys]
            if not isinstance(keys, list):
                keys = [keys]
        if any(kw.lower() in msg_lower for kw in keys):
            matched.append(entry)
    # 角色知识库注入（Knowledge/characters/*.md，离线管线生成）
    # 命中角色名关键词时注入该页梗概（前 600 字），控制 token 不过载。
    # 匹配优先级：当前模仿对象的名字 > 消息中出现的人物名 > 作品名
    kb_matches = _load_character_kb_for_message(message_text)
    matched.extend(kb_matches)
    return matched[:6]


# 角色知识库缓存
_character_kb_cache = None
_character_kb_cache_mtime = None

def _load_character_kb():
    """懒加载角色知识库（Knowledge/characters/），文件变更时自动刷新"""
    global _character_kb_cache, _character_kb_cache_mtime
    kb_dir = os.path.join(BOTMEMORY_DIR, "Knowledge", "characters")
    idx_path = os.path.join(kb_dir, "index.json")
    if not os.path.exists(idx_path):
        return {}
    mtime = os.path.getmtime(idx_path)
    if _character_kb_cache is not None and _character_kb_cache_mtime == mtime:
        return _character_kb_cache
    try:
        with open(idx_path, encoding="utf-8") as f:
            index = json.load(f)
        kb = {}
        for item in index:
            fpath = os.path.join(kb_dir, item["file"])
            if os.path.exists(fpath):
                with open(fpath, encoding="utf-8") as fh:
                    kb[item["title"]] = {
                        "file": item["file"],
                        "title": item["title"],
                        "category": item.get("category", ""),
                        "content": fh.read(),
                    }
        _character_kb_cache = kb
        _character_kb_cache_mtime = mtime
        _log(f"  角色知识库加载: {len(kb)} 页")
    except Exception as e:
        _log(f"  角色知识库加载失败: {e}")
        _character_kb_cache = {}
    return _character_kb_cache


def _load_character_kb_for_message(message_text):
    """消息中出现角色名时，注入对应知识页梗概（前 600 字）"""
    if not message_text:
        return []
    kb = _load_character_kb()
    if not kb:
        return []
    msg_lower = message_text.lower()
    # 标题别名映射：中文名/常见简称 → 知识库 title（示例；按你的库替换）
    alias_map = {
        "演示甲": "演示甲", "甲": "演示甲", "alpha": "演示甲",
        "演示乙": "演示乙", "乙": "演示乙", "beta": "演示乙",
    }
    matched = []
    seen = set()
    for alias, title in alias_map.items():
        if alias.lower() in msg_lower and title not in seen:
            page = kb.get(title)
            if not page:
                continue
            seen.add(title)
            # 取梗概：标题后 600 字（跳过版权头，从"登场版本/基本资料"附近开始）
            content = page["content"]
            start = content.find("登场版本")
            if start == -1:
                start = content.find("简介")
            if start == -1:
                start = 0
            digest = content[start:start + 600]
            matched.append({
                "keys": [alias],
                "content": f"[角色知识库] {title}：{digest}",
                "category": "知识库",
            })
    return matched[:2]


# ==================== 构建 prompt ====================

def _bracket_end(text, i):
    """从 i（指向 '['）扫描到匹配的 ']'，正确处理嵌套 [ ] 结构。
    （2026-08-16：原 text.find(']') 遇嵌套会截错段——引用壳内容含
    [回复id:xxx]/[图片:xxx] 等内层标记时，切分必须按括号深度走）
    """
    depth = 0
    for k in range(i, len(text)):
        c = text[k]
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                return k
    return -1


# ==================== 图片引用（索图通道，2026-09-10 路线 A 实装）====================

IMG_REF_FILE = os.path.join(REPO_DIR, "bot_img_ref.json")
IMG_REF_TOOL = "view_image"
IMG_TRACE_TOOL = "trace_image"   # 2026-09-13 溯源通道（SauceNAO 以图搜图）

# ==================== SauceNAO 额度监控（2026-09-13）====================
# 背景：SauceNAO 只在**响应里**给额度（header.long_remaining 等），本地不查询就拿不到实时值。
#   botkb（另一个进程）每次查询后把快照写进 SAUCENAO_QUOTA_FILE，watcher 读它做两件事：
#     ① 额度告急/用尽时**私聊提醒维护者**（阈值可配；每天每档只提醒一次）
#     ② 额度用尽时**往 prompt 注入提示**，让模型照实说"今天查不了"，不白调一次工具
#
# ⚠️ 这是**上一次查询的快照**，不是实时值 —— 因此：
#   · 超过 SAUCENAO_QUOTA_TTL 秒的快照不再用于注入提示（宁可放过，不可误拦：
#     若额度其实已恢复，误拦会让模型拒绝本可用的功能，代价比"少提示一次"大）
#   · 提示措辞带时间戳并留"可试一次"的出口，由模型自己判断
SAUCENAO_QUOTA_FILE = os.path.join(BOTMEMORY_DIR, "SauceNAOQuota.json")
SAUCENAO_NOTICE_STATE = os.path.join(LOG_DIR, "saucenao_notice_state.json")
# 阈值：剩余 <= 该值时私聊提醒维护者。0 = 额度已用尽（单独提醒一次）
SAUCENAO_WARN_THRESHOLDS = [10, 5, 0]
# 快照有效期（秒）。超过则不再据此注入"额度用尽"提示。
SAUCENAO_QUOTA_TTL = int(os.getenv("SAUCENAO_QUOTA_TTL", str(4 * 3600)))


def _saucenao_quota_state():
    """读额度快照（botkb 写）。返回 dict；读不到返回 {}。"""
    try:
        with open(SAUCENAO_QUOTA_FILE, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _saucenao_quota_fresh(st, now=None):
    """快照是否新鲜（未超 TTL）。"""
    try:
        age = (now or time.time()) - float(st.get("updated_ts") or 0)
        return 0 <= age <= SAUCENAO_QUOTA_TTL
    except (TypeError, ValueError):
        return False


def _saucenao_quota_prompt_line():
    """额度用尽 / key 失效时给模型的提示行；无需提示时返回 ""。

    ⚠️ 两者必须分开措辞（2026-09-13 踩坑）：最初把"任何客户端错误"都当成额度耗尽，
       结果 key 无效也会让模型以为"额度用尽"，且要等一次成功调用才清除 → 长期误拦。
    """
    st = _saucenao_quota_state()
    if not st or not _saucenao_quota_fresh(st):
        return ""
    when = str(st.get("updated_at") or "")[11:16] or "之前"
    if st.get("exhausted"):
        return (f"   ⚠️ **SauceNAO 溯源额度已用尽**（最近一次查询 {when} 确认）。"
                f"现在不要把 {IMG_TRACE_TOOL} 当作可用工具：群友问图片来源/画师时，"
                "直接照实说「今天查不了图源」，**不要编造出处**。"
                "（若距上面时间已过很久、你判断额度可能已恢复，可以试一次；"
                "失败就照实说明，不要重试。）")
    if st.get("auth_error"):
        # key 失效是配置问题，不会自己好——不要建议"试一次"
        return (f"   ⚠️ **溯源功能当前不可用**（API key 无效，最近一次查询 {when} 发现）。"
                "群友问图片来源/画师时，照实说这个功能暂时用不了，**不要编造出处**；"
                "也可以提一句「请维护者检查配置」。")
    return ""


def _maybe_notify_saucenao_quota():
    """额度告急/用尽 → 私聊提醒维护者。每档每天只提醒一次。

    调用点：主循环每轮（事件被处理时）。开销 = 一次小文件读，可忽略。
    """
    st = _saucenao_quota_state()
    if not st or not _saucenao_quota_fresh(st):
        return
    left = st.get("long_remaining")
    try:
        left = int(left)
    except (TypeError, ValueError):
        return
    if not st.get("exhausted") and left > max(SAUCENAO_WARN_THRESHOLDS):
        return

    today = time.strftime("%Y-%m-%d")
    state = {}
    try:
        with open(SAUCENAO_NOTICE_STATE, encoding="utf-8") as f:
            state = json.load(f) or {}
    except Exception:  # noqa: BLE001
        state = {}
    if not isinstance(state, dict) or state.get("date") != today:
        state = {"date": today, "notified": []}
    done = set(state.get("notified") or [])

    # 找出本次触及的所有未提醒档位，只发**最紧急**的那一档（避免跳变时连发多条）
    hit = [t for t in sorted(SAUCENAO_WARN_THRESHOLDS) if t not in done and left <= t]
    if not hit:
        return
    threshold = hit[0]
    if threshold <= 0:
        head = "🔴 SauceNAO 溯源额度**已用尽**"
        tail = "群友问图片来源时会照实说查不了。额度按 24 小时滚动恢复，无需处理。"
    else:
        head = f"⚠️ SauceNAO 溯源额度**告急**：24 小时内仅剩 {left} 次"
        tail = "再查几次就会用尽。若想省着用，可后续加同图结果缓存。"

    msg = (f"{head}（上限 {st.get('long_limit')} 次/24h）。\n"
           f"{tail}\n"
           f"（快照时间 {str(st.get('updated_at'))[11:16]}｜"
           f"短时额度 {st.get('short_remaining')}/{st.get('short_limit')}）")
    ok, fail = _send_private(ADMIN_QQ, msg)
    if ok:
        # 只发最紧急那一档，但把本次触及的**所有**档位都标记为已提醒 ——
        # 否则额度跳变（如直接从 12 → 3）时，下一轮会为 10 档再发一条重复消息。
        done.update(hit)
        state["notified"] = sorted(done)
        try:
            tmp = SAUCENAO_NOTICE_STATE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False)
            os.replace(tmp, SAUCENAO_NOTICE_STATE)
        except Exception as e:  # noqa: BLE001
            _log(f"⚠️ 额度提醒状态写盘失败: {type(e).__name__}: {e}")
        _log(f"  📨 SauceNAO 额度提醒已私聊发送（阈值 {threshold}，剩余 {left}）")
    else:
        _log(f"  ⚠️ SauceNAO 额度提醒发送失败（剩余 {left}）—— 轮次内不重试，下轮再判")


def _img_ref_enabled(group_id):
    """索图通道开关（`bot_img_ref.json`，按群；`"*"` 为全局默认键）。

    缺失 → 默认**开启**（2026-09-10 实装）。理由：图片清单只在消息里**真有图**时
    才注入（无图零开销），取图也只有模型主动调用时才付费，属于"默认可用、
    按需付费"。若要全局关闭写 `{"*": false}`。
    """
    try:
        if os.path.exists(IMG_REF_FILE):
            with open(IMG_REF_FILE, encoding="utf-8") as f:
                cfg = json.load(f) or {}
            if str(group_id) in cfg:
                return bool(cfg[str(group_id)])
            if "*" in cfg:
                return bool(cfg["*"])
    except Exception:
        pass
    return True


class _ImgRefs:
    """图片引用收集器：file_id → 稳定编号，供「图片清单」注入 prompt。

    同一个 file_id 多次出现只占一个编号（跨 @ 消息与历史上下文共享），
    这样模型看到的 `[图片#1]` 含义在整个 prompt 内一致。
    """

    __slots__ = ("index", "items")

    def __init__(self):
        self.index = {}   # file_id -> idx
        self.items = []   # [(idx, file_id, desc)]

    def add(self, file_id, desc=""):
        """登记并返回编号；无 file_id 时返回 0（表示"无法索取的图"）。"""
        fid = (file_id or "").strip()
        if not fid:
            return 0
        if fid in self.index:
            i = self.index[fid]
            if desc and not self.items[i - 1][2]:
                self.items[i - 1] = (i, fid, desc)
            return i
        i = len(self.items) + 1
        self.index[fid] = i
        self.items.append((i, fid, desc or ""))
        return i

    def __len__(self):
        return len(self.items)


_IMG_TAG_RE = re.compile(r"^图片:([^()]*)(?:\([^)]*\))?$")
_IMG_DESC_RE = re.compile(r"^图片内容(?:\(([^)]*)\))?[:：]?(.*)$", re.S)


def _parse_img_seg(seg):
    """解析方括号内的图片段。返回 (file_id, desc)；不是图片段则返回 None。

    支持三种形态：
      · `图片:xxx.jpg`                       ← 上游管线的原始标记（识图开关关闭时）
      · `图片:xxx.jpg` + 括号里的相对路径      ← 同上（`[图片:x](_images/x)`）
      · `图片内容(id=xxx.jpg): 描述`          ← m3 识别后的形态（2026-09-10 起保留 id）
      · `图片内容: 描述`                      ← 旧形态（无 id，无法索图）
    """
    m = _IMG_TAG_RE.match(seg)
    if m:
        return (m.group(1).strip(), "")
    m = _IMG_DESC_RE.match(seg)
    if m:
        inner, desc = (m.group(1) or ""), (m.group(2) or "")
        fid = ""
        if inner:
            for part in inner.split(","):
                part = part.strip()
                if part.startswith("id="):
                    fid = part[3:].strip()
                elif "=" not in part and not fid:
                    fid = part
        return (fid, desc.strip())
    return None


# ---- 索图前置：把清单里的图补成本地副本（2026-09-10）----
# 为什么需要：历史上下文里的图片标记只带 file_id、没有本地文件；
# 而 botkb.view_image 只读本地归档、不联网。缺副本时预取一次。
IMAGE_ARCHIVE_DIR = os.getenv("BOT_IMAGE_DIR", os.path.join(REPO_DIR, "images"))
IMG_REF_PREFETCH_MAX = int(os.getenv("IMG_REF_PREFETCH_MAX", "3"))
IMG_FETCH_TIMEOUT = 20


def _ensure_local_image(file_id):
    """确保归档里有这张图。返回本地路径；失败返回 None（静默，不拖垮回复链路）。

    具体取图方式由 Transport 决定（真实适配器 = get_image + 缓存/URL 兜底；
    Mock = 生成占位图，保证离线演示可走通）。
    """
    try:
        base = os.path.basename(str(file_id).replace("\\", "/").strip())
        if not base:
            return None
        return _t().fetch_image(base, IMAGE_ARCHIVE_DIR)
    except Exception as e:
        _log(f"  ⚠️ 预取图片失败 {file_id}: {e}")
    return None


def _prefetch_images(refs, limit=None):
    """为清单里的图补齐本地副本（最多 limit 张，按清单顺序=相关性顺序）。返回成功数。"""
    if refs is None or not len(refs):
        return 0
    lim = IMG_REF_PREFETCH_MAX if limit is None else limit
    ok = 0
    for _i, fid, _desc in refs.items[:lim]:
        if _ensure_local_image(fid):
            ok += 1
    if ok:
        _log(f"  🖼️ 索图预取：{ok}/{min(lim, len(refs))} 张已有本地副本")
    return ok


def _at_display_name(qq, group_id=None):
    """把 @ 的 QQ 号转成可读名。返回 None 表示保留原号。

    2026-09-12（身份识别修复）：原实现把 `@\\d+` 整个删除，导致模型
    **看不到消息里 @ 了谁**——例如「@某人@bot 长官」经清洗后
    只剩「长官」，模型无从判断谁被点名、自己是否被点名。
    现改为：
      · bot 自己（BOT_QQ）        → 「你」
      · 名册/StandardUsers 有记录 → 「稳定称呼(QQ号)」← 名+号同时给，消歧最稳
      · 未知账号                  → None（保留裸 QQ 号，至少可辨识）
    """
    qq = str(qq)
    if qq == BOT_QQ:
        return "你"
    try:
        from nickname import normalize as _nz
        name = _nz(qq, fallback=None, group_id=group_id)
    except Exception:
        name = None
    if name:
        return f"{name}({qq})"
    return None


def _clean_msg(text, refs=None, group_id=None):
    """
    清洗消息中的格式标记，防止模型复述（如 '[图片:xxx.jpg]'）。
    只作用于进模型前的 prompt 文本；归档保留原始格式（供 BotMemory 等使用）。

    - @QQ号        → 转成「稳定称呼(QQ号)」；bot 自己 → 「你」；未知账号保留原号
                     （2026-09-12 改：原为直接删除，导致模型看不到谁被 @）
    - [回复id:xxx] → 删除（引用关系被降级为无意义 id）
    - [回复昵称: 内容] → 提取「内容」（递归清洗，支持嵌套 [图片] 等）
    - [图片:xxx]   → [图片#N]（传了 refs 收集器时；否则沿用旧的 [图片]）
    - [图片内容(id=x): desc] → [图片#N]（desc 记入清单；旧形态无 id → [图片]）
    - [CQ:xxx]     → [表情]
    - [卡片消息]/[合并转发] 等 → 保留类型名

    2026-09-10（索图通道）：`refs` 是 `_ImgRefs` 收集器。传入时图片被归一化为
    `[图片#N]` 并把 `file_id`/描述登记进清单，模型得以按编号索图（botkb.view_image）。
    不传 refs 时行为与旧版一致（向后兼容：`_extract_notes` 等调用点不受影响）。

    2026-09-12（身份识别）：`group_id` 用于按群解析 @ 的稳定称呼。不传时
    退回"未知账号保留裸 QQ 号"行为（不删、也不转名），仍与旧调用点兼容。
    """
    if not text:
        return ""
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == '[':
            j = _bracket_end(text, i)
            if j == -1:                     # 未闭合的 '['，剩余全部按普通文本
                out.append(text[i:])
                break
            seg = text[i + 1:j]
            if seg.startswith('图片'):
                parsed = _parse_img_seg(seg)
                if parsed is not None:
                    fid, desc = parsed
                    idx = refs.add(fid, desc) if refs is not None else 0
                    out.append(f'[图片#{idx}]' if idx else '[图片]')
                    i = j + 1
                    continue
            if seg.startswith('回复id:'):
                i = j + 1
                continue                    # 删除
            if seg.startswith(('回复bot:', '引用bot:')):
                # 2026-08-16：被引用者是 bot 自己 → 只提示不给全文（8/8 复述修复的
                # 延续；bot 上轮回复内容由 conv_memory「本群近期问答」注入提供）
                out.append('[引用的是你的上一条回复]')
                i = j + 1
                continue
            if seg.startswith('回复') and ':' in seg:
                # 2026-08-16 精细修复：8/8 一刀切整体删除误伤了群友引用。
                # bot 引用已由上方分支处理，此处均为群友引用 → 提取内容保留
                # （递归清洗内部的 [图片] 等标记，防格式垃圾进 prompt）。
                inner = seg.split(':', 1)[1]
                cleaned = _clean_msg(inner, refs).strip()
                if cleaned:
                    out.append('[' + cleaned + ']')
                i = j + 1
                continue
            out.append('[' + seg + ']')     # 其他段原样保留（如 [卡片消息]）
            i = j + 1
        else:
            out.append(c)
            i += 1
    txt = ''.join(out)
    # 2026-09-12（身份识别修复）：原为 `re.sub(r'@\d+', '', txt)` 直接删除 →
    # 模型看不到消息里 @ 了谁。现改为转成可读名（已知→稳定称呼(QQ号)；bot→你；未知→保留号）。
    # 注意：这里用函数替换，故不能再用 re.sub 的字符串替换。
    txt = re.sub(r'@(\d{5,12})',
                 lambda m: (lambda d: f"@{d}" if d else f"@{m.group(1)}")(_at_display_name(m.group(1), group_id)),
                 txt)
    # 图片标记后紧跟的本地相对路径（如 [图片#1](_images/x.jpg) / [图片](_images/x.jpg)）→ 去掉
    txt = re.sub(r'(\[图片[^\]]*\])\([^)]*\)', r'\1', txt)
    txt = re.sub(r'\[图片:[^\]]*\]', '[图片]', txt)
    txt = re.sub(r'\[CQ:[^\]]*\]', '[表情]', txt)
    txt = re.sub(r'\[文件:[^\]]*\]', '[文件]', txt)
    txt = re.sub(r'\[合并转发[^\]]*\]', '[合并转发]', txt)
    # 号码+图片后缀 的无意义标记串（形如「号码.jpg」），删除
    txt = re.sub(r'\b\d{6,12}\s*(?:jpe?g|png|gif|webp)\b', '', txt)
    txt = re.sub(r'\s{2,}', ' ', txt).strip()
    return txt


def _build_prompt(events, deduped_ctx=None):
    """events: list of (filepath, event_dict) → 完整 prompt
    （2026-08-17：扮演状态/俚语/成员共识按群注入，互不干涉）"""
    lines = []
    gid = events[0][1].get("group_id") if events else None
    # 2026-09-10：图片引用收集器。贯穿 @ 事件与历史上下文，保证 [图片#N] 编号一致；
    # 清单稍后统一注入（索图通道，botkb.view_image）。
    img_refs = _ImgRefs() if _img_ref_enabled(gid) else None
    # 预扫描：本轮窗口里是否**真的**有图片标记。必须在解析前判定——工具段在 prompt 前部，
    # 那时收集器还是空的，用 len(img_refs) 判断会永远为假（2026-09-10 踩过）。
    # 还要叠加开关：索图关闭时不提任何索图提示，完全回退旧行为。
    has_img = img_refs is not None and (
        any("[图片" in (ev.get("message") or "") for _, ev in events)
        or any("[图片" in (c.get("text") or "") for c in (deduped_ctx or [])))
    # 2026-08-17：短期交接记忆注入（上段对话摘要，新会话冷启动也有记忆）
    if gid:
        handoff = _load_handoff(str(gid))
        if handoff:
            lines.append(HANDOFF_TITLE)
            lines.append(handoff)
            lines.append("")
    # 2026-08-10：脱离扮演后 DEFAULT.txt 为空 → 不注入角色人格，
    # 首行提示同步条件化，防止模型在无人格时自行捏造角色。
    if _get_target_id(gid):
        lines.append("你是一个群聊的自动回复助手。请以当前激活的角色人格回复下面的 @ 消息。")
    else:
        lines.append("你是一个群聊的自动回复助手（当前未启用任何角色人格）。"
                     "请以你自己的正常风格、像群友闲聊一样回复下面的 @ 消息。")
    lines.append("")

    # 禁止工具探索。事故：agent 被问到某个本地服务状态后尝试自行查询，
    # 陷入 76 步工具循环导致全链路超时瘫痪。
    # 群聊上下文已全部由 watcher 注入，agent 唯一职责 = 直接生成回复文本。
    # 被拒绝后不得绕过（防 bash→webfetch 通道转移）。
    # 2026-09-01 21:05 增补：重活识别——需要查记录/读群文件/联网搜索的长任务，
    #   输出 [任务] 标记委托任务会话执行（不自己硬闯）。
    lines.append("【规则】你只需要直接回复群聊消息。所需上下文已全部提供：")
    # 白名单化：检索类能力通过 botkb MCP server 开放给 bot 主动调用
    # （全部只读）。其余工具的禁令全部保留。
    # 加入 searxng 联网搜索（规则 1），并同步放开规则 2 对它的封锁。
    #   背景：此前群会话被本段规则禁止直搜（规则1 只列三个 + 规则2 禁"其余 MCP 工具"），
    #   联网只能走规则 5 的 [任务] 委托；而任务会话当时 frontmatter deny 了 searxng，
    #   两条路同时断 → bot 对外只能声称"只有三个只读接口"。
    #   实测（2026-09-11 三组对照）：规则 1 列出 searxng 后模型即正常调用，
    #   仅改 opencode 权限而不改本段**完全无效**——模型严格服从这里的清单。
    #   ⚠️ 加工具/删工具必须同时改规则 1、规则 2（必要时规则 5），否则自相矛盾。
    # 2026-09-13：标题去掉「均只读」—— trace_image 会把图片上传到 SauceNAO（出网），
    #   严格说不是只读；同时避免与规则 2 的措辞冲突（改工具清单必须同步改 1、2）。
    lines.append("1. 【允许的工具】")
    # （可选知识源示例：若接入歌词类模块，在此追加对应工具说明，并在 botkb server 中注册——
    #   新增知识源 = *_search.py + TOOLS 注册，见 mcp-servers/botkb/server.py。）
    lines.append("   · search_theory（知识检索）：群里聊到知识库覆盖的概念或创作话题、"
                 "想引用得准时调用（示例库见 mcp-servers/botkb/sample_kb/）；"
                 "支持中文概念名和英文术语，没命中就换同义词再试。"
                 "注意：检索结果是资料，回答时仍用你当前角色的口吻消化，别切成讲课模式。")
    lines.append("   · searxng_searxng_web_search（联网搜索）：需要查实时信息/出处/事实核对时"
                 "调用，入参 query。拿到结果后**自己消化成群聊口吻**，不要把链接列表直接倒出来；"
                 "结果不理想就换个更具体的关键词再搜（可多次）。"
                 "只做轻量检索；需要多源交叉核对、成篇整理的深活仍按规则 5 委托。")
    # 2026-09-10：索图工具（仅当本轮窗口真的有图时才提示，避免空转）
    if img_refs is not None and has_img:
        lines.append(f"   · {IMG_REF_TOOL}（查看群图原图）：消息里的 [图片#N] 只是引用编号，"
                     "图片内容不会自动给你。当问题涉及**画面或图里的文字**、而「图片清单」里"
                     "该图没有描述或描述不够时，调用它并传入清单里的 file_id 取原图。"
                     "一次可只取最相关的一两张，不要无差别把清单里的图全取一遍。")
        # 2026-09-13（溯源通道）：SauceNAO 以图搜图。与 view_image 职责互补：
        #   view_image = 把图给模型看；trace_image = 拿图去查来源。
        #   2026-09-13 补能力边界（实测）：Pixiv/Twitter/deviantArt 等索引**不返回角色名**，
        #   只有 Danbooru 类索引才有 characters/material → 「图里是谁」不保证答得出。
        #   这一步必须写清，否则模型会把"查到 Pixiv 链接"错当成"确认了角色"。
        # ⚠️ 2026-09-13 修一处自相矛盾（隔离实验实测出的行为）：
        #   原文开头写「问的**不是**「图里画了什么」，而是「…是哪个角色」」——"图里是谁"
        #   与"图里画了什么"语义极近，模型会把它归入后者而不用本工具；
        #   原文结尾又写「认角色仍以角色名册为主，本工具只作补充证据」——**直接指令模型
        #   不要用溯源认角色**。两处叠加，导致实测「问角色」时模型 100% 不调溯源。
        #   用户定调（2026-09-13）：**改成条件性交叉验证——由 agent 先判断有没有这个需求**，
        #   不是每次都查（额度 100 次/24h，无脑查会烧光且打扰群聊节奏）。
        lines.append(f"   · {IMG_TRACE_TOOL}（以图搜图）：拿图片本身去 SauceNAO 反查，"
                     "返回作品名/角色名/作者/原图链接。"
                     "两类问题**都可以**用它：①「这图哪来的 / 出处 / 画师是谁」→ 直接查；"
                     "②「图里是谁 / 哪个角色」→ **先自己对照名册判断，再决定要不要查**："
                     "名册里有**唯一且明确**的匹配就正常答（不必查）；"
                     "名册里**找不到匹配**、**多个候选难取舍**、或群友**要你确认/给证据**时，"
                     "查一次取证——命中 Danbooru 时会返回角色名，那是**独立于你目测的硬证据**，"
                     "能钉死或推翻你的判断。是否要查由你自己判断，别每次都查。"
                     "⚠️ 结果已分档：标【高可信】的可据此回答；标【偏低(仅供参考)】"
                     "只能措辞为「可能/像是」，**不要断言**。"
                     "⚠️ 查不到角色名是常态（Pixiv/Twitter 等索引不返回角色名，实测多数群图"
                     "甚至低于判定线）——**这不能证明你的判断错了**，照旧按名册 + 画面作答。"
                     "额度有限（约每 12 秒一次、每天 100 次）：一次只查最相关的一张，"
                     "同一张图不重复查；已自动过滤成人向来源，结果比预期少属正常。")
        # 2026-09-13：额度用尽时把"别白调"的提示紧跟在工具说明后面
        #   （位置很重要——写在工具 bullet 之后，模型决定是否调用时才会看到）
        _quota_line = _saucenao_quota_prompt_line()
        if _quota_line:
            lines.append(_quota_line)
    lines.append("   以上工具都可多次调用。")
    lines.append("2. 除上述工具外，不要调用任何其他工具/访问任何本地服务"
                 "（bash、其余未列出的 MCP 工具、本地服务端口等）。")
    lines.append("3. 如果你的某个工具调用被拒绝或无法访问，不要尝试其他方式绕过，"
                 "直接向群友说明无法获取该信息。")
    lines.append("4. 如果群友询问导出/任务/后台状态等需要外部服务的信息，"
                 "直接告知「请去问维护者」，不要自己去查。")
    lines.append("5. 【重活识别】如果群友的任务需要查历史记录、下载读取群文件、"
                 "或多源交叉核对/成篇整理的长文本处理，不要自己尝试执行——直接输出一行："
                 "「[任务] <一句话任务描述>」，然后结束回复。"
                 "watcher 会安排执行，完成后把结果交回给你组织成回复。"
                 "（轻量联网检索已由规则 1 的 searxng 工具覆盖，自己搜即可，不必委托。）"
                 "普通问题直接正常回复。")
    lines.append("")

    # 风格指导注入
    snippet = _load_system_prompt_snippet(gid)
    if snippet:
        lines.append("## 角色人格（当前激活）")
        lines.append(snippet)
        lines.append("")

    # 歌曲归属元数据注入（2026-09-06）：角色卡 song_meta → bot 判断「是不是我的歌」
    # 与 search_lyrics 返回的 [IP/角色] 标注配合使用
    song_meta_block = _song_meta_summary(gid)
    if song_meta_block:
        lines.append("## 你的曲目（判断歌词归属的依据）")
        lines.append(song_meta_block)
        lines.append("")

    # 制作人身份注入：示例作品角色卡激活且管理员在场时，
    # 把管理员识别为制作人（P）——角色按「对制作人的称呼」与他对话，其他群友按一般同学/粉丝。
    # 注：管理员=bot 建立者，此身份同时用于管理命令权限（见 _try_admin_command）。
    producer_block = _producer_prompt(events)
    if producer_block:
        lines.append("## 当前对话对象（重要）")
        lines.append(producer_block)
        lines.append("")

    # 群俚语词典注入（2026-08-09：补回旧版 skill 的 prompt 结构——迁移时丢失）
    # 只注入词条摘要（词+含义），控制 prompt 长度
    slang_block = _load_slang_summary(group_id=gid)
    if slang_block:
        lines.append("## 群俚语词典（按需使用）")
        lines.append(slang_block)
        lines.append("")

    # 群成员名册注入（2026-09-12 升级：原为「群成员共识」摘要）
    # 旧实现只注入 StandardUsers 手写的「QQ号：称呼」（仅 9 条、无身份/别名历史），
    # 新名册合并平台实时数据 + 历史名片变体 + 人工字段，覆盖全群。
    # 名册缺失时回退旧摘要（向后兼容）。
    users_block = _member_roster_prompt(gid, events, deduped_ctx)
    if users_block:
        lines.append("## 群成员名册（判断「谁是谁」的唯一依据）")
        lines.append("群友常改群名片，**同一个人可能有多个名字**。判断身份以本表为准，"
                     "同一 QQ 号换了名片仍是同一人：")
        lines.append(users_block)
        lines.append("（消息里的 `@称呼(QQ号)` 已按本表换算；表外的人按名片原样显示。）")
        lines.append("")
    else:
        # 回退：旧的手写共识摘要
        users_block = _load_standard_users_summary(group_id=gid)
        if users_block:
            lines.append("## 群成员共识（稳定称呼）")
            lines.append(users_block)
            lines.append("")

    # lorebook 关键词注入
    all_msg_text = " ".join(ev.get("message", "") for _, ev in events)
    lore_matches = _load_lorebook_for_message(all_msg_text, group_id=gid)
    if lore_matches:
        lines.append("## 额外参考（关键词触发）")
        for entry in lore_matches:
            kw = ", ".join(entry["keys"][:3])
            lines.append(f"- 「{kw}」→ {entry['content']}")
        lines.append("")

    # 歌词检索注入（2026-09-05 引入；2026-09-06 默认关闭）
    # 改为 bot 主动调用 search_lyrics MCP 工具（更灵活：可换关键词多轮试错）。
    # 预检索保留为应急开关：设环境变量 LYRIC_PREINJECT=1 重新启用
    # （适用场景：bot 模型工具调用不稳定时退回旧模式）。
    if os.getenv("LYRIC_PREINJECT", "0") == "1":
        lyric_hits = _search_lyrics_for_message(
            all_msg_text, group_id=gid, lore_entries=lore_matches)
        if lyric_hits:
            lines.append("## 歌词检索结果（自动匹配，非你记忆中的内容）")
            for h in lyric_hits:
                head = f"- 【{h['label']}】{h['title']}"
                if h["artist"] and h["artist"] != h["title"]:
                    head += f" — {h['artist']}"
                if h["album"] and h["album"] != h["title"]:
                    head += f"（{h['album']}）"
                lines.append(head)
                if h["snippet"]:
                    lines.append(f"  命中片段：…{h['snippet']}…")
                if h["body_cn"]:
                    lines.append(f"  中文：{h['body_cn']}")
            lines.append("  注：以上是歌词库检索到的资料，不是你『想起来』的。"
                         "若是别人的歌，照实说不是自己的即可，不要假装唱过。")
            lines.append("")

    # @ 事件详情
    lines.append("## 当前 @ 事件")
    for i, (_, ev) in enumerate(events, 1):
        gid = ev.get("group_id", "?")
        raw_nick = ev.get("nickname", "?")
        uid = ev.get("user_id", "?")
        # 2026-09-12：传入 gid，让规范化走本群名册（修前因路径 bug 恒失效）
        nick = _normalize_nick(str(uid), fallback=raw_nick, group_id=str(gid))
        msg = _clean_msg(ev.get("message", ""), img_refs, group_id=str(gid))
        lines.append(f"【@事件 {i}】群 {gid} | {nick}({uid}): {msg}")
    lines.append("")

    # 全局上下文（已去重，最新在前 → 取前 CTX_INJECT_N 条）
    if deduped_ctx:
        lines.append(f"## 近期上下文（共 {len(deduped_ctx)} 条）")
        for c in deduped_ctx[:CTX_INJECT_N]:
            c_nick = _normalize_nick(str(c.get('user_id', '?')), fallback=c.get('nickname', '?'),
                                     group_id=str(gid))
            c_txt = _clean_msg(c.get('text', ''), img_refs, group_id=str(gid))
            if not c_txt:
                continue
            lines.append(f"- {c_nick}({c.get('user_id', '?')}): {c_txt}")
        lines.append("")

    # 2026-09-10：图片清单（索图通道）。只在本窗口内真有图片时注入 → 无图零开销。
    if img_refs:
        _prefetch_images(img_refs)   # 补本地副本，保证 view_image 取得到
        lines.append("## 图片清单（本窗口内出现的图片）")
        for i, fid, desc in img_refs.items:
            line = f"- [图片#{i}] file_id={fid}"
            line += f"　描述：{desc}" if desc else "　（未识别，无描述）"
            lines.append(line)
        lines.append("")
        lines.append("提示：如果某张图的描述不足以回答**关于画面/文字**的问题"
                     "（比如问「这是什么/写了什么/什么角色」，而描述里没有），"
                     f"调用 {IMG_REF_TOOL} 工具并把上面对应的 file_id 原样传入取原图查看，"
                     "然后照常回复。取到图后直接基于图片内容回答，"
                     "不要说「我看不到图片」；确实取不到时才照实说明。")
        lines.append("")

        # 2026-09-12：角色名册（仅本轮有图时注入 → 无图零开销）。
        # 给"认出图中角色"提供本地对照表，取代「看图→猜→联网搜」的循环论证流程。
        char_block = _character_roster_prompt()
        if char_block:
            lines.append("## 角色名册（判断图中动漫角色时对照用）")
            lines.append("本群可扮演的角色及其外观特征：")
            lines.append(char_block)
            lines.append("（这是本地卡库 + 官方资料的对照表，比你自己猜或联网搜更可靠。）")
            lines.append("")

    # 本群近期问答（自维护历史，2026-08-08：补回新 conv 丢失的 bot 上轮回复可见性）
    mem_block = _conv_memory_prompt(gid)
    if mem_block:
        lines.append(mem_block)
        lines.append("")

    # 显式长期记忆（2026-08-09：bot 主动记录的 Notes 文档）
    notes_block = _notes_prompt(gid)
    if notes_block:
        lines.append(notes_block)
        lines.append("")

    lines.append("## 回复要求")
    # 2026-09-07：显式注入配图模式档位（off=不发图 / auto=情绪自选 / always=每次必配）。
    # 让模型明确当前参数，也便于在会话记录里核对参数是否传入。
    img_mode = _get_image_mode()
    lines.append(f"（当前配图模式：{img_mode} = {_IMAGE_MODE_CN.get(img_mode, img_mode)}，"
                 f"按下方规则 8 执行）")
    lines.append("1. 直接回复上面列出的 @ 消息，语气贴合角色人格")
    lines.append("2. 群俚语可适度使用（如适用）")
    lines.append("3. 一次回复不超过 3 条消息，每条控制在 300 字以内")
    lines.append("4. 只输出要发送给群聊的回复内容本身，不要输出任何解释、前缀或格式标记"
                 "（消息中的 [图片]、[图片#N]、@QQ号、[表情] 等是系统标记，不要复述它们）")
    lines.append("5. 如果无需回复或没有把握，输出「（无回复）」")
    lines.append("6. 消息中的引用内容（如「回复xxx: ...」）是历史消息摘录，用概括语言指代"
                 "（如「你引用那条」），不得逐字复述其中的数字、QQ号或标记")
    lines.append("7. 若对话中出现值得长期记住的内容（群友重要信息、被要求的规则、项目状态等），"
                 "在回复末尾单独一行输出 [记忆:内容]（一行，100 字内，最多 3 条；没有则不要输出）。"
                 "该标记不会发送给群，只用于 bot 自己的记忆文档")
    # 规则 8（配图指令，2026-09-06 三档化：off 禁止 / auto 自选 / always 每次必配）
    # img_mode 已在「## 回复要求」头部读取（同函数作用域）
    if img_mode == "off":
        lines.append("8. 【禁止配图】当前模式不发送角色表情图，不要在回复中输出 [表情:xx] 标记")
    elif img_mode == "always":
        lines.append("8. 【每次回复必须配图】在回复末尾单独一行输出 [表情:情感] 标记（如 [表情:害羞]、"
                     "[表情:生气]、[表情:开心]、[表情:撒娇]、[表情:困惑]、[表情:震惊]、[表情:自信]、"
                     "[表情:困扰]、[表情:欣慰]、[表情:哭]），情感贴合本条回复的情绪，最多 1 个。"
                     "该标记不会发送给群，bot 会换成对应的角色表情图片；即使忘了输出，系统也会自动补图")
    else:  # auto（默认）
        lines.append("8. 若当前角色有人物表情图素材（示例作品角色卡），可以在回复末尾单独一行输出"
                     " [表情:情感] 标记配合情绪表达（如 [表情:害羞]、[表情:生气]、[表情:开心]、"
                     "[表情:撒娇]、[表情:困惑]、[表情:震惊]、[表情:自信]、[表情:困扰]、[表情:欣慰]、"
                     "[表情:哭]），最多 1 个。该标记不会发送给群，bot 会换成对应的角色表情图片")
    lines.append("9. 若需要 @ 群友（如点名回应某人），在回复中需要的位置输出 [at:QQ号] 或 [at:昵称]"
                 " 标记（如 [at:100000003] 或 [at:某个群友的昵称]），bot 会把它转成真正的 @ 消息；"
                 "昵称优先用「群成员共识」里的稳定称呼或 @ 事件里的昵称。该标记不会以文本形式发送。"
                 "不需要 @ 时不要输出该标记")
    # 2026-09-10（索图通道）：图片真实性约束（仅在窗口里有图时才加）
    if has_img:
        lines.append("10. 关于群友发的图：你没有自动看到图片内容。需要看时按上面的提示调用 "
                     f"{IMG_REF_TOOL} 取原图（或依赖「图片描述」）。**没看过的图绝对不要描述其画面"
                     "内容**，尤其不要因为群里上下文像在聊某张图就顺着编——就说你这边看不到图即可")
        # 2026-09-12（角色识别 L3）：认出角色时的措辞与行为约束。
        # 设计依据：① 用户定位是「被问到才说」（群友问起时才需要认）；② 误认的伤害
        # > 不认出的伤害（误认是以确定语气输出错误）；③ 「猜+搜自己猜测」是循环论证，
        # 必须从 prompt 层切断；④ 有误触发先例（"模仿我"曾误切某群友），图片不确定性更高。
        # 2026-09-13 修订（用户定调：条件性交叉验证，由 agent 自行判断需求）：
        #   原 ③ 只写"不要为确认猜测去联网搜索"，未说明 trace_image 算不算搜索 →
        #   模型可能连带回避合法取证。现明确区分，并把"该不该查"的判断权交给模型。
        lines.append(f"11. 【认出图中角色时】（只有当群友问起图里是谁/什么角色时才用得上）"
                     "判断依据有两个，**先看名册，再自己决定要不要取证**："
                     "① 对照上方「角色名册」的外观特征（发色/瞳色/发型/服饰/萌点）；"
                     "② 名册里**没有匹配**、或**多个候选难以取舍**、或你**准备用肯定语气**"
                     f"作答、或群友**要你确认**时 → 调 {IMG_TRACE_TOOL} 查一次取证"
                     "（命中 Danbooru 会给出角色名，是独立于你目测的硬证据）。"
                     "名册有唯一明确匹配时不必查——**是否要查由你自己判断，别每次都查**"
                     "（有频率限制和日额度）。"
                     "措辞规则："
                     "① **不要主动宣布**这是谁，被问到才说；"
                     "② 有把握就直接说角色名；把握一般就只说作品（如「看画风是某作品的角色」）；"
                     "没把握就直说看不准——**宁可说不准，也不要硬套名册里的角色**；"
                     "取证结果**查不到角色名是常态**（多数群图不在溯源收录范围），"
                     "**这不代表你的判断错了**，照旧按名册 + 画面作答即可；"
                     "两者结论**矛盾**时照实并列（「名册看像 A，但以图搜图查到 B」），"
                     "不要挑一个当定论；"
                     "③ **不要为了确认自己的猜测去联网搜索**（搜「XX 发色」只会返回符合你"
                     "猜测的结果，那是循环论证）；"
                     f"**注意 {IMG_TRACE_TOOL} 不属于此类**——它拿图片本身去查索引库，"
                     "结果不受你的猜测影响，可以放心用来交叉验证；"
                     "④ 认出角色**不等于**要切换扮演——绝不因看图改变你当前的说话人格；"
                     "⑤ 你正在扮演某角色时，看到该角色的图不要自称「这是我」"
                     "（角色不认识自己），自然带过即可")

    return "\n".join(lines)


# ==================== 上下文去重 ====================

def _dedup_context(events):
    seen = set()
    merged = []
    for _, ev in events:
        for c in ev.get("context", []):
            uid = str(c.get("user_id", ""))
            txt = c.get("text", "")
            if uid == BOT_QQ:
                continue
            if f"@{BOT_QQ}" in txt:
                continue
            key = (uid, txt)
            if key not in seen:
                seen.add(key)
                merged.append(c)
    return merged


# ==================== 会话管理（opencode 版） ====================
# 会话映射持久化在 oc_sessions.json（client 管理）。watcher 层只负责：
#   1. 轮数阈值/不活跃检查 → 触发交接（summarize_session）→ 轮换（rotate_session）
#   2. 每次调用前取/更新会话（get_session_id 自动复用 + 轮数 +1）

def _load_conv_ids():
    """（兼容 stub）opencode 版会话映射由 client 管理，此处返回空。"""
    return {}


def _save_conv_ids():
    """（兼容 stub）opencode 版由 client 持久化。"""
    pass


def _load_handoff(group_id):
    """读本群短期交接记忆摘要；无则空字符串。"""
    try:
        with open(os.path.join(LOG_DIR, f"conv_handoff_{group_id}.json"), encoding="utf-8") as f:
            d = json.load(f)
        return d.get("summary", "")
    except Exception:
        return ""


def _rotate_with_handoff(gid, min_rounds=3):
    """群会话触发交接（达轮数阈值/对话不活跃）：让模型整理交接文档 → 存盘 → 轮换新会话。
    对话过短（rounds < min_rounds）跳过总结直接轮换（不浪费调用）；
    总结失败也照常轮换（不阻塞回复链路），新会话靠 conv_memory/注入兜底。"""
    try:
        from opencode_client import _load_sessions
        sessions = _load_sessions()
        entry = sessions.get(str(gid))
        rounds = entry.get("rounds", 0) if entry else 0
    except Exception:
        rounds = 0
    if rounds >= min_rounds:
        try:
            summary = summarize_session(
                gid,
                "请把本次会话中与群友的对话整理成一份交接文档（300-500字，简体中文，markdown 列表）：\n"
                "1. 重要话题：本段聊过哪些主题（每条一两句话）\n"
                "2. 群友信息：聊到的群友兴趣、偏好、习惯或约定\n"
                "3. 待续事项：没聊完或可能继续的话题\n"
                "4. 你的结论：你回复过的重要观点或立场\n"
                "直接输出文档正文，不要客套。",
                timeout=RUN_TIMEOUT)
            if summary and summary.strip():
                handoff = {"gid": gid, "time": datetime.now().isoformat(timespec="seconds"),
                           "summary": summary.strip()[:3000]}
                fp = os.path.join(LOG_DIR, f"conv_handoff_{gid}.json")
                tmp = fp + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(handoff, f, ensure_ascii=False, indent=1)
                os.replace(tmp, fp)
                _log(f"  📝 短期交接文档已生成 (群{gid}, {rounds}轮)")
        except Exception as e:
            _log(f"  ⚠️ 交接文档生成失败: {e}")
    try:
        rotate_session(gid)
    except Exception as e:
        _log(f"  ⚠️ 会话轮换失败: {e}")
    _log(f"  🔄 群{gid} 交接轮换（rounds={rounds}）")


def _get_conversation_id(group_id, force_new=False):
    """群会话 ID（opencode 版）：委托 client 管理，返回 session_id 字符串。
    默认复用（保持对话记忆）；force_new 时轮换新会话。
    轮数计数由 client 维护（rounds 字段，用于 CONV_MAX_ROUNDS 阈值交接）。"""
    sid, _ = get_session_id(group_id, force_new=force_new)
    return sid


_conv_ids_persisted = _load_conv_ids()


# ==================== 问答历史记忆 ====================

def _load_conv_memory():
    """从磁盘加载问答历史（容错：文件缺失/损坏则从空开始）"""
    global _conv_memory
    try:
        with open(CONV_MEM_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _conv_memory = {
                k: v[-CONV_MEM_MAX_ROUNDS:]
                for k, v in data.items() if isinstance(v, list)
            }
    except Exception:
        _conv_memory = {}


def _save_conv_memory():
    """问答历史落盘（单文件、量小，每次处理后写）"""
    try:
        with open(CONV_MEM_FILE, "w", encoding="utf-8") as f:
            json.dump(_conv_memory, f, ensure_ascii=False)
    except Exception:
        pass


def _append_conv_memory(group_id, question, reply):
    """记录一轮问答（question/reply 均为清洗后文本），超上限裁旧"""
    try:
        rounds = _conv_memory.setdefault(str(group_id), [])
        rounds.append([question, reply, time.time()])
        if len(rounds) > CONV_MEM_MAX_ROUNDS:
            del rounds[:-CONV_MEM_MAX_ROUNDS]
        _save_conv_memory()
    except Exception:
        pass


def _conv_memory_prompt(group_id):
    """生成问答历史注入段（最近 CONV_MEM_INJECT 轮，空则返回 ""）"""
    rounds = _conv_memory.get(str(group_id), [])
    if not rounds:
        return ""
    lines = ["## 本群近期问答（你的历史回复，仅作指代背景，不要复述其中内容）"]
    for q, a, _ in rounds[-CONV_MEM_INJECT:]:
        lines.append(f"- 用户: {(q or '')[:100]}")
        lines.append(f"- 你: {_strip_jp((a or '')[:150])}")
    return "\n".join(lines) + "\n"


# ==================== 显式长期记忆（Notes） ====================

_NOTE_RE = re.compile(r"\[记忆[:：]([^\]]{1,200})\]")

def _extract_notes(reply):
    """从回复中提取 [记忆:xxx] 标记（支持多条）。
    返回 (剥离标记后的回复, 记忆列表)。清洗 + 限长 + 限条数，超限丢弃。"""
    notes = []

    def _repl(m):
        txt = _clean_msg(m.group(1)).strip()
        # 含方括号说明嵌套/截断异常（正常记忆内容不应含 []），丢弃防垃圾回灌
        if txt and len(txt) <= NOTES_MAX_LEN and "[" not in txt and "]" not in txt:
            notes.append(txt)
        return ""

    clean = _NOTE_RE.sub(_repl, reply)
    return clean, notes[:NOTES_MAX_PER_REPLY]


def _append_notes(group_id, notes, author="?"):
    """静默追加到 BotMemory/Notes_{group_id}.md（追加式、可读、带时间戳）"""
    if not notes:
        return
    try:
        path = os.path.join(BOTMEMORY_DIR, f"Notes_{group_id}.md")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"\n## {datetime.now():%Y-%m-%d %H:%M}（{author}）\n")
            for n in notes:
                f.write(f"- {n}\n")
    except Exception:
        pass


def _notes_prompt(group_id, limit=NOTES_INJECT):
    """读取长期记忆文档最近 limit 条，生成注入段（无文档/无条目返回 ""）"""
    path = os.path.join(BOTMEMORY_DIR, f"Notes_{group_id}.md")
    if not os.path.exists(path):
        return ""
    try:
        with open(path, encoding="utf-8") as f:
            raw = [ln.rstrip("\n") for ln in f if ln.strip()]
    except Exception:
        return ""
    items = [ln[2:].strip() for ln in raw if ln.startswith("- ")]
    if not items:
        return ""
    items = items[-limit:]
    lines = ["## 长期记忆（你主动记录的群内事实/约定，回答时可按需引用）"]
    lines.extend(f"- {i}" for i in items)
    return "\n".join(lines) + "\n"


# ==================== 切换模仿对象指令（2026-08-09 方案A） ====================
# 群友 @bot 说"模仿XX/学XX说话/以后像XX一样/切换模仿对象"时，本地执行切换：
# 解析目标 → 查 registry.json → 校验风格档案 → 原子写 DEFAULT.txt → 群内回执。
# 纯 watcher 本地逻辑，不依赖 gateway/模型工具能力。

PERSONA_DIR = os.path.join(BOTMEMORY_DIR, "Persona")
DEFAULT_PERSONA_FILE = os.path.join(PERSONA_DIR, "DEFAULT.txt")
REGISTRY_FILE = os.path.join(PERSONA_DIR, "registry.json")

# 切换指令目标模式（参考旧版 qq-autoreply skill Step 3.6）
# 目标字符集排除冒号/分号/顿号，防止「模仿某人：评价电影」把内容吞进目标
# v3：新增英文名模式（含空格的名字，允许 2-3 个词）
_SWITCH_TARGET_PATTERNS = [
    re.compile(r'(?:切换|换成|改成|开始)?模仿\s*[:：]?\s*([^\s，。！？!?：:；;、]{1,20})'),
    re.compile(r'(?:切换|换成|改成|开始)?模仿\s*([A-Za-z][A-Za-z\'\-]*(?:\s+[A-Za-z][A-Za-z\'\-]*){1,2})'),
    # 2026-09-11："一下/学学" 移进非捕获前缀——原写法 `学\s*(目标)` 会把"一下"吞进目标
    # （改前实测：「学一下某名字说话」会把前缀吞进目标 → 精确层失手 → 截胡其它目标）
    re.compile(r'学\s*(?:一下|学学|学)?\s*([^\s，。！？!?：:；;、]{1,20})\s*(?:说话|讲话|口吻|语气)'),
    re.compile(r'以后\s*(?:就)?像\s*([^\s，。！？!?：:；;、]{1,20})\s*一样'),
]
# 查询类（无目标，要求列名单）
_SWITCH_QUERY_PATTERN = re.compile(r'(?:模仿|风格|人格).{0,6}(?:名单|列表|对象|谁)|^(?:切换|换|看看|列出).{0,4}(?:模仿|风格)')
# 否定/闲聊排除（"不要模仿XX"、"你觉得模仿XX怎么样"不触发）
_SWITCH_NEGATE = re.compile(r'(?:不要|别|不用|不必|不想)[\s]*(?:模仿|学|像)')
_SWITCH_WEAK_QUESTION = re.compile(r'[？?吗怎么]')
_SWITCH_STRONG = re.compile(r'(?:切换|换成|改成|变成|变为|扮演|cos|以后|开始|请|帮我)')
_SWITCH_NOISE_WORDS = {"对象", "风格", "人格", "一下", "看看", "谁"}
# 2026-08-09 v2：宽松意图模式（正则精确模式未命中时兜底）——覆盖
# "换成XX口吻/变成XX/扮演XX/以XX风格/用XX语气"等句式。
# 触发条件较严：要求消息开头 4 字内出现意图动词，降低「这电影在模仿某人」类误触。
# 2026-08-09 v4：扮演/cos/变成 组后缀可选（"扮演XX"直接成立），
# 前缀支持"现在尝试/试着/来/先"等（修复真实事件「很好，现在尝试扮演XX」）。
# v5：前缀放宽为"前 8 字内出现意图动词"（允许"很好，现在尝试扮演"这类前置修饰），
#     误触由 _SWITCH_DISCUSS 讨论排除 + 弱指令疑问句保护兜底。
_SWITCH_INTENT_PATTERNS = [
    # 扮演组：后缀可选，动词本身即强指令
    re.compile(r'^.{0,8}?(?:扮演|cos|cosplay|变为|变成)\s*([^\s，。！？!?：:；;、💀]{1,20})'),
    # 换风格组：需后缀（"换成XX口吻"）
    re.compile(r'^.{0,8}?(?:换成|改成|以|用)\s*([^\s，。！？!?：:；;、💀]{1,20})\s*(?:的)?(?:口吻|风格|语气|样子|声线|人格|说话|讲话)'),
    # 学一下组
    # 2026-09-11：目标改非贪婪 + 后台词由可选改必需——原写法后缀可选且目标贪婪，
    # 「学一下XX说话」会把「说话」一起吞进目标（意图模式排在精确模式之后，
    # 精确模式已覆盖该句式，此处为双保险）。
    re.compile(r'^.{0,8}?(?:学一下|学学)\s*([^\s，。！？!?：:；;、💀]{1,20}?)\s*(?:说话|讲话|口吻|语气)'),
]
# 讨论句式排除（"这电影就是在模仿X"/"他在电影里扮演XX"→ 走 AI 不切换）
_SWITCH_DISCUSS = re.compile(r'(?:就是在|是在|在|就是|又|也|都|想|要|别|不要|他|她|电影|剧|作品|视频|有人|别人)(?:在|里|中|的)?(?:模仿|扮演|cos)')
# 目标里的 emoji/符号清洗（"扮演XX💀"→"XX"）
_SWITCH_EMOJI = re.compile(r'[\U0001F300-\U0001FAFF\u2600-\u27BF\ufe0f\u200d]+')
# 目标尾缀剥离（"XX说话"→"XX"、"YY的风格"→"YY"）
# 2026-09-11：补裸「的」——意图模式 `(?:换成|以|用)\s*(目标)\s*(?:的)?(?:口吻|…)` 的
# 目标捕获是贪婪的，「换成XX的口吻」会连「的」一起吞进目标，精确层因此失手
# （改前实测：目标带「的」→ 子串层命中他人短别名，误切换）。
_SWITCH_SUFFIX = re.compile(
    r'(?:说话|讲话|口吻|语气|一样|的样子|的风格|风格|人格|如何|怎么样|吧|呢|了|啊|呀|的)+$')


def _strip_target_suffix(target):
    """剥离目标尾缀，返回清洗后的目标"""
    t = (target or "").strip()
    prev = None
    while prev != t:  # 反复剥离（"XX说话的风格"→"XX"）
        prev = t
        t = _SWITCH_SUFFIX.sub("", t).strip()
    return t


def _load_registry():
    """读取 Persona/registry.json：{qq: [别名...]}"""
    try:
        with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ───────── 角色表情图发送（2026-08-09：强化角色扮演表达）─────────
# 素材：角色表情图目录（build_img_index.py 扫描生成索引）
# 机制：模型回复中输出 [表情:情感] 标记（如 [表情:害羞]）→ watcher 解析 →
#       从当前角色的图库匹配情感 → 随机选图 → Transport 发送图片消息。
# 索引：BotMemory/ImgIndex.json {roles: {role_id: {all: [...], emotions: {emo: [...]}}}}
IMG_INDEX_FILE = os.path.join(BOTMEMORY_DIR, "ImgIndex.json")
EMOJI_MARK_RE = re.compile(r"\[表情[:：]([^\]]{1,10})\]")
# 当前角色 id → 素材角色 id（DIR_TO_ROLE 的逆映射，构建索引时已对齐）
_IMG_INDEX_CACHE = None


def _load_img_index():
    global _IMG_INDEX_CACHE
    if _IMG_INDEX_CACHE is None:
        try:
            with open(IMG_INDEX_FILE, "r", encoding="utf-8") as f:
                _IMG_INDEX_CACHE = json.load(f)
        except Exception:
            _IMG_INDEX_CACHE = {"roles": {}}
    return _IMG_INDEX_CACHE


def _pick_emotion_image(role_id, emotion):
    """按角色+情感选图。返回图片绝对路径或 None。"""
    idx = _load_img_index()
    roles = idx.get("roles", {})
    # role_id 可能是 card_demo_alpha（角色卡 id）或 alpha（索引 id）
    rid = role_id.replace("card_demo_", "").replace("card_", "")
    r = roles.get(rid)
    if not r:
        return None
    pool = r.get("emotions", {}).get(emotion) or r.get("emotions", {}).get(_emotion_synonym(emotion))
    if not pool:
        pool = r.get("all", [])  # 无情感匹配 → 随机整库
    if not pool:
        return None
    import random
    return random.choice(pool)


def _emotion_synonym(emo):
    """情感近义词兜底（害羞→可爱等）"""
    SYN = {"害羞": "可爱", "生气": "愤怒", "开心": "高兴", "震惊": "惊讶"}
    return SYN.get(emo, "")


def _extract_emotion_mark(reply):
    """从回复中提取 [表情:情感] 标记。返回 (清洗后的回复, 情感列表)"""
    marks = []

    def _repl(m):
        e = m.group(1).strip()
        if e and len(e) <= 10:
            marks.append(e)
        return ""

    clean = EMOJI_MARK_RE.sub(_repl, reply)
    return clean, marks


def _strip_status_block(text):
    """剥离 agent 给 watcher 的内部"任务完成状态块"（见 qq-autoreply SKILL 退出条件）。

    该块形如：
        📬 已处理 N 条 @消息
        ✅ Core.md + Detailed/ 已更新
          ├ Topics.md: 已更新/无需变更
          └ Users.md: 已更新/无需变更
        <!-- TASK_COMPLETE -->
    是给 watcher 的完成信号，不应发到群（否则群里会看到"自检"噪音）。
    只在出现明确锚点（📬 已处理 / TASK_COMPLETE）时才剥离，避免误删正常对话。
    """
    lines = text.splitlines()
    out = []
    in_block = False
    for ln in lines:
        s = ln.strip()
        if not in_block and "📬 已处理" in s:
            in_block = True
            continue
        if in_block:
            if "TASK_COMPLETE" in s:
                in_block = False
            continue
        if "TASK_COMPLETE" in s:  # 异常兜底：单独出现终止符
            continue
        out.append(ln)
    return "\n".join(out).strip()


def _send_emotion_image(group_id, role_id, emotion):
    """发送角色表情图。返回 (ok: bool, detail: str)"""
    # —— 发送层保护（2026-08-17）：非白名单群拒绝发送 ——
    if not is_group_allowed(group_id):
        _log(f"  🛡️ 拦截表情图发送到非白名单群 {group_id}")
        return False, "group not allowed"
    img = _pick_emotion_image(role_id, emotion)
    if not img:
        return False, f"无素材（{role_id}/{emotion}）"
    result = _t().send_group_image(group_id, img)
    if result.get("status") == "ok":
        return True, f"已发送 {os.path.basename(img)}"
    return False, str(result.get("error", "?"))[:100]


# ───────── 配图模式三档调节（2026-09-06）─────────
# off   = 完全不配图（模型被禁止输出 [表情:情感]，watcher 也跳过发图）
# auto  = 按内容情绪自选（默认，现行行为：模型输出 [表情:情感] 才发）
# always= 每次回复都配图（有标记按标记发；无标记也从角色图库随机补一张）
# 优先级：BotMemory/image_mode.txt（热生效，管理员命令写入）> 环境变量 IMAGE_MODE > auto
# 注意：配图仍只走 ImgIndex.json 白名单通道，三档只调节"倾向"，不放开路径发送权限。
IMAGE_MODE_FILE = os.path.join(BOTMEMORY_DIR, "image_mode.txt")
IMAGE_MODES = ("off", "auto", "always")
_IMAGE_MODE_CN = {"off": "完全不配图", "auto": "按内容情绪自选配图", "always": "每次回复都配图"}


def _get_image_mode():
    raw = ""
    try:
        if os.path.exists(IMAGE_MODE_FILE):
            with open(IMAGE_MODE_FILE, "r", encoding="utf-8") as f:
                raw = f.read().strip().lower()
    except Exception:
        raw = ""
    if raw not in IMAGE_MODES:
        raw = os.getenv("IMAGE_MODE", "auto").strip().lower()
    return raw if raw in IMAGE_MODES else "auto"


def _set_image_mode(mode):
    """写入配图模式（原子写，受限环境降级为直接覆写）。返回是否成功。"""
    if mode not in IMAGE_MODES:
        return False
    try:
        tmp = IMAGE_MODE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(mode)
        try:
            os.replace(tmp, IMAGE_MODE_FILE)
        except OSError:
            # 某些环境（沙箱/安全软件）对"替换已存在文件"受限 → 退化为直接覆写。
            # 本文件仅 watcher 单进程写、读取极快，覆写窗口可忽略。
            with open(IMAGE_MODE_FILE, "w", encoding="utf-8") as f:
                f.write(mode)
            try:
                os.remove(tmp)
            except OSError:
                pass
        return True
    except Exception as e:
        _log(f"❌ 配图模式写入失败: {e}")
        return False


# ───────── 角色卡索引 ─────────
# 角色卡 = Persona/card_*.persona.json（示例：card_demo_alpha.persona.json）。
# 加载机制与真人档案完全相同（watcher 按 {id}.persona.json 读取），
# 只需让切换指令能解析到角色卡 ID。别名来源：
#   1) 文件名中文段（card_演示甲_20260101 → 演示甲）
#   2) json name 字段
#   3) 内置译名映射（CARD_ALIASES，示例如下——替换成你自己的卡即可）
CARD_ALIASES = {
    "alpha": ["演示甲", "甲", "alpha"],
    "beta": ["演示乙", "乙", "beta"],
}


def _load_card_index():
    """扫描 Persona/card_*.persona.json，返回 {card_id: [别名...]}
    别名来源：文件名中文段 + json name + 内置译名映射 + md 标题 + knowledge 全名"""
    index = {}
    if not os.path.isdir(PERSONA_DIR):
        return index
    for f in os.listdir(PERSONA_DIR):
        if not (f.startswith("card_") and f.endswith(".persona.json")):
            continue
        cid = f[:-len(".persona.json")]  # card_demo_alpha
        aliases = []
        # 1) 文件名中文段
        m = re.search(r'card_(.+?)_\d{8}', f)
        if m:
            name_seg = m.group(1)
            if re.search(r'[\u4e00-\u9fff]', name_seg):
                aliases.append(name_seg)
        # 2) json name 字段
        try:
            with open(os.path.join(PERSONA_DIR, f), encoding="utf-8") as fh:
                card = json.load(fh)
            nm = str(card.get("name", "")).strip()
            if nm and nm != "Unnamed":
                aliases.append(nm)
        except Exception:
            pass
        # 3) 内置译名映射（按文件名特征匹配）
        cid_lower = cid.lower()
        for k, v in CARD_ALIASES.items():
            if k in cid_lower:
                aliases.extend(v)
                break
        # 4) 关联 md 文件标题（"# 角色卡: X" 行）
        md_path = os.path.join(PERSONA_DIR, cid + ".md")
        if os.path.exists(md_path):
            try:
                with open(md_path, encoding="utf-8") as fh:
                    for ln in fh:
                        mt = re.match(r'#+\s*(?:角色卡|PERSONA)\s*[:：]?\s*(.+)', ln.strip())
                        if mt:
                            t = mt.group(1).strip()
                            if t and t.lower() not in [a.lower() for a in aliases]:
                                aliases.append(t)
                            break
            except Exception:
                pass
        # 注意：不自动从 knowledge_* 提取人名——知识库含配角/无关人物名
        # （如小说配角名），自动提取会产生噪声别名和误匹配。
        # 别名以手工精选的 CARD_ALIASES 为准。
        # 去重（保序，限长）
        seen = set()
        uniq = []
        for a in aliases:
            a = a.strip()
            if not a or a.lower() in seen or len(a) > 24:
                continue
            seen.add(a.lower())
            uniq.append(a)
        if uniq:
            index[cid] = uniq
    return index


def _resolve_persona_target(target, registry, card_index=None):
    """把用户输入解析为 (target_id, 匹配别名, 歧义候选)。

    三层判定（2026-09-11 重构，修复同姓氏截胡）：
      ① 精确层——全部来源（真人 + 角色卡）里别名与目标完全一致；
         但若命中别名同时是**他人**更长别名的前缀（= 该称呼不足以唯一定位，
         如裸姓氏之于其全名），则判歧义，交调用方反问澄清。
      ② 模糊层 A——别名 ⊂ 目标（用户多打了字），取**最长**别名：
         别名越长越具体，避免短姓氏抢答。
      ③ 模糊层 B——目标 ⊂ 别名（用户打了简称），
         取**最短**别名（最接近用户所想）。A/B 内并列即歧义。

    返回值：
      target_id        — 可直接作文件名前缀；None 表示未命中或歧义
      alias            — 命中的别名
      ambiguous        — None = 未歧义；否则 [(target_id, 展示名), ...]（≥2 项）

    历史沿革：原实现为「真人精确 → 真人子串 → 卡精确 → 卡子串」，两个缺陷——
    ①精确层在跨来源时**不是**统一优先（真人子串先于卡精确）；②子串层按
    card_index 的 dict 顺序返回首个命中，不按匹配质量择优，导致短别名恒压长别名。现改为「精确层统一 → 模糊层择优」。
    """
    t = (target or "").strip()
    if not t:
        return None, None, None
    if t.isdigit():
        if t == BOT_QQ:
            return None, None, None
        return t, t, None

    if card_index is None:
        card_index = _load_card_index()

    # 候选池：真人优先（保持"真人精确优先于角色卡"的既有语义），排除 bot 自己
    # （模仿自己没有意义，且 bot 的长别名会截胡角色卡短别名）
    pool = []
    for qq, aliases in registry.items():
        if qq == BOT_QQ:
            continue
        for a in aliases:
            if a:
                pool.append((qq, a))
    for cid, aliases in card_index.items():
        for a in aliases:
            if a:
                pool.append((cid, a))
    tl = t.lower()

    def _best_name(owner):
        """取某 owner 的展示名：首个含中文的别名（= 卡 json name / CARD_ALIASES 首位全名），
        无中文则取首个别名。**不可按最长取**——会挑中外号（外号可能比要展示的全名更长）。"""
        cands = [a for o, a in pool if o == owner]
        if not cands:
            return owner
        cn = [a for a in cands if re.search(r'[\u4e00-\u9fff]', a)]
        return (cn or cands)[0]

    def _ambiguous(owners):
        """按 owner 去重生成歧义候选 [(owner, 展示名)]"""
        seen, out = set(), []
        for o in owners:
            if o in seen:
                continue
            seen.add(o)
            out.append((o, _best_name(o)))
        return out if len(out) >= 2 else None

    # ① 精确层（全部来源统一）
    exact = [(o, a) for o, a in pool if a.lower() == tl]
    if exact:
        o1, a1 = exact[0]
        # 前缀歧义：命中的别名是不是别人更长名字的开头？
        #   短别名是他人更长名字的前缀 → 歧义（不足以唯一定位）
        #   罗马音连写属内嵌、非独立称呼 → 不误判为前缀歧义
        # ⚠️ 仅对 **≥2 字** 别名检测：单字别名（星/赤/青/广）本就依赖精确层兜底，
        #    其"前缀关系"多属偶然（单字恰为更长别名的开头），
        #    判歧义会无端打扰（2026-09-11 测试实测误判后收紧）。
        related = []
        if len(a1) >= 2:
            related = [o for o, a in pool
                       if o != o1 and len(a) > len(a1) and a.lower().startswith(a1.lower())]
        if related:
            amb = _ambiguous([o1] + related)
            if amb:
                return None, None, amb
        return o1, a1, None

    # ② 模糊层 A：别名 ⊂ 目标（用户多打了字）→ 最长别名优先
    # 模糊层一律要求目标与别名均 ≥2 字（2026-08-09 12:19 事故修复的约束，不可放宽）：
    # 单字 target 曾被包含命中他人长别名；单字别名也曾被更长别名误命中。单字别名只保留精确层命中（"星"/"广"/"赤" 仍可精确切换）。
    cand_a = [(o, a) for o, a in pool
              if len(t) >= 2 and len(a) >= 2 and a.lower() in tl]
    if cand_a:
        best = max(len(a) for _, a in cand_a)
        top = [(o, a) for o, a in cand_a if len(a) == best]
        if len({o for o, _ in top}) == 1:
            return top[0][0], top[0][1], None
        return None, None, _ambiguous([o for o, _ in top])

    # ③ 模糊层 B：目标 ⊂ 别名（用户打了简称）→ 最短别名优先
    cand_b = [(o, a) for o, a in pool
              if len(t) >= 2 and len(a) >= 2 and tl in a.lower()]
    if cand_b:
        best = min(len(a) for _, a in cand_b)
        top = [(o, a) for o, a in cand_b if len(a) == best]
        if len({o for o, _ in top}) == 1:
            return top[0][0], top[0][1], None
        return None, None, _ambiguous([o for o, _ in top])

    return None, None, None


def _ambiguous_reply_text(target_text, ambiguous):
    """歧义反问回执（2026-09-11）"""
    names = [nm for _, nm in (ambiguous or [])]
    return (f"「{target_text}」对应好几个角色：" + "、".join(names) +
            "～ 说全名我再切哦")


def _persona_exists(qq):
    """校验目标是否有风格档案（.persona.json 或 .md）"""
    return os.path.exists(os.path.join(PERSONA_DIR, f"{qq}.persona.json")) or \
           os.path.exists(os.path.join(PERSONA_DIR, f"{qq}.md"))


def _persona_list_text(registry, limit=10):
    """生成可模仿名单文本（真人 + 角色卡，排除 bot 自己）"""
    names = []
    for q, al in registry.items():
        if q == BOT_QQ:
            continue  # 不列出 bot 自己
        if _persona_exists(q):
            names.append(al[0] if al else q)
    card_index = _load_card_index()
    for cid, aliases in card_index.items():
        # 角色卡用中文名或 name 字段展示
        cn = next((a for a in aliases if re.search(r'[\u4e00-\u9fff]', a)), aliases[0])
        names.append(f"{cn}（角色）")
    if len(names) > limit:
        # 真人优先，角色卡截断
        real = [n for n in names if not n.endswith("（角色）")]
        cards = [n for n in names if n.endswith("（角色）")]
        names = real[:limit] + cards[:max(0, limit - len(real))]
    return "、".join(names) if names else "（暂无可用档案）"


def _switch_persona(group_id, target_text, requester):
    """执行切换（真人或角色卡）。返回 (ok, 回执文本)"""
    registry = _load_registry()
    card_index = _load_card_index()
    tid, alias, amb = _resolve_persona_target(target_text, registry, card_index)
    if amb:
        return False, _ambiguous_reply_text(target_text, amb)
    if not tid:
        return False, "没搞明白应该模仿谁～ 目前可模仿的：" + _persona_list_text(registry)
    if not _persona_exists(tid):
        return False, f"「{alias or tid}」还没有风格档案，暂时模仿不了"
    try:
        state_path = _persona_state_file(group_id)  # 2026-08-17：按群状态文件
        tmp = state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(tid)
        os.replace(tmp, state_path)  # 原子写入，防半截
    except Exception as e:
        _log(f"❌ 切换模仿写入失败: {e}")
        return False, "切换失败了（写入出错），稍后再试"
    is_card = tid.startswith("card_")
    _log(f"🔀 切换模仿对象: {requester} 要求 → {alias or tid} ({tid}){'（角色卡）' if is_card else ''}")
    return True, f"好，已切换成模仿「{alias or tid}」说话~"


def _producer_prompt(events):
    """示例作品角色卡激活 + 管理员（ADMIN_QQ）在场 → 返回制作人身份注入文本，否则 None。
    （2026-08-09 v1.11：让角色卡扮演把 bot 建立者识别为制作人 P）
    （2026-08-17：扮演状态按群 → 取事件所属群的激活角色）"""
    gid = events[0][1].get("group_id") if events else None
    target_id = _get_target_id(gid) or ""
    if not target_id.startswith(PRODUCER_CARD_PREFIX):
        return None
    if not any(str(ev.get("user_id")) == ADMIN_QQ for _, ev in events):
        return None
    return (f"群友 {ADMIN_QQ}（{ADMIN_NICK}）是你的制作人（P）——负责你的制作人。"
            f"按角色设定中「对制作人的称呼」称呼他，以与制作人相处的语气与他对话；"
            f"其他群友按一般同学/粉丝对待。")


def _admin_context_report(group_id=None):
    """管理命令「检查上下文」报告（私聊发送，不含敏感信息）"""
    target_id = _get_target_id(group_id) or "(无)"
    name, ver, lb_count = target_id, "?", 0
    persona = _load_persona_json(target_id) if target_id != "(无)" else None
    if persona:
        name = persona.get("name", target_id)
        ver = persona.get("version", "?")
        lb = persona.get("lorebook", {})
        entries = lb.get("entries", []) if isinstance(lb, dict) else []
        lb_count = len(entries)
    # conv_memory 本群状态（最近一轮注入轮数）
    mem_rounds = "—"
    try:
        mem_file = CONV_MEM_FILE
        with open(mem_file, encoding="utf-8") as f:
            mem = json.load(f)
        # 结构: {group_id: [(q, a, ts), ...]} 或类似——只读最近一条的时间
        if isinstance(mem, dict):
            for gid, rounds in mem.items():
                if isinstance(rounds, list) and rounds:
                    mem_rounds = f"{len(rounds)} 轮"
                    break
    except Exception:
        pass
    return "\n".join([
        f"📋 上下文检查 @ {datetime.now().strftime('%H:%M:%S')}",
        f"🎭 当前角色: {target_id}（{name} v{ver}）",
        f"📚 lorebook: {lb_count} 条（含歌词条目，关键词触发）",
        f"🧠 会话记忆: conv_memory.json 按群注入最近 3 轮（当前 {mem_rounds}）",
        "📝 Notes: 注入最近 10 条 ｜ 🏷️ 群俚语/群成员共识: 已注入",
        "💡 更多命令: 「当前角色」查看角色、「检查上下文」本报告",
    ])


def _admin_current_persona(group_id=None):
    """管理命令「当前角色」"""
    target_id = _get_target_id(group_id) or "(无)"
    name = target_id
    persona = _load_persona_json(target_id) if target_id != "(无)" else None
    if persona:
        name = persona.get("name", target_id)
    return f"🎭 当前角色: {target_id}（{name}）"


_ADMIN_NEGATE = re.compile(r"(不|别|不用|无需|别去|不要)")
_ADMIN_CMD_RE = re.compile(r"(检查|查看|看下|看看).{0,4}(上下文|记忆|prompt|提示词|状态)")
_ADMIN_WHO_RE = re.compile(r"(当前角色|在模仿谁|现在.{0,3}(什么角色|扮演|模仿).{0,3}(谁|人)|你现在是谁)")


def _admin_image_mode(msg):
    """配图模式管理命令（2026-09-06 三档调节）。返回回复文本或 None（非本命令）。
    仅由 _try_admin_command 中「配图」显式锚定（开头或含"配图模式"）后调用。
    切换：配图模式 不配图/关闭 → off；每次都配/强制 → always；自动/自选 → auto。
    查询：单独说"配图"或"配图模式"。"""
    low = msg.lower()
    if re.search(r"(不配|关闭|关掉|禁止|无图|off)", low):
        mode = "off"
    elif re.search(r"(每次|每条|always|必配|强制|都配)", low):
        mode = "always"
    elif re.search(r"(自动|自选|auto|按情绪|情绪)", low):
        mode = "auto"
    else:
        cur = _get_image_mode()
        return (f"当前配图模式：{cur}（{_IMAGE_MODE_CN[cur]}）\n"
                "三档：off=完全不配图 / auto=按内容情绪自选（默认） / always=每次回复都配图\n"
                "切换示例：「配图模式 每次都配」「配图模式 不配图」「配图模式 自动」")
    if _set_image_mode(mode):
        return f"配图模式已切换：{mode}（{_IMAGE_MODE_CN[mode]}），立即生效，无需重启"
    return "配图模式切换失败（写入出错），请查日志"


def _try_admin_command(events):
    """
    管理命令（2026-08-09 v1.11）：仅 bot 建立者 ADMIN_QQ 可触发，私聊回复。
    基础版命令：检查上下文 / 当前角色。疑问句与否定句不触发。
    返回 (remaining_events, handled_count)：
      remaining_events — 未匹配管理命令、继续走 AI 的事件
      handled_count   — 已本地处理的管理命令事件数
    """
    if not events:
        return [], 0
    remaining = []
    handled = 0
    for fp, ev in events:
        uid = str(ev.get("user_id", ""))
        msg = _clean_msg(ev.get("message", "") or "")
        if uid != ADMIN_QQ or not msg:
            remaining.append((fp, ev))
            continue
        # 配图模式命令（2026-09-06）：显式锚定（"配图"开头或含"配图模式"），
        # 需先于下方通用否定/疑问拦截——"配图模式 不配图"这类切换指令本身带否定词
        # 2026-09-07：回执发到触发所在群（原私聊回执不易被注意到，误以为没生效）
        if re.match(r"^\s*配图", msg) or "配图模式" in msg:
            img_reply = _admin_image_mode(msg)
            if img_reply:
                _log(f"🛡️ 管理命令: {ADMIN_NICK} 触发「{msg[:30]}」→ 群回执")
                _send_reply(ev.get("group_id"), img_reply)
                handled += 1
                continue
        if _ADMIN_NEGATE.search(msg) or msg.endswith(("吗", "？", "?")):
            remaining.append((fp, ev))
            continue
        reply = None
        if _ADMIN_CMD_RE.search(msg):
            reply = _admin_context_report(ev.get("group_id"))
        elif _ADMIN_WHO_RE.search(msg):
            reply = _admin_current_persona(ev.get("group_id"))
        if reply:
            _log(f"🛡️ 管理命令: {ADMIN_NICK} 触发「{msg[:30]}」→ 私聊报告")
            _send_private(ADMIN_QQ, reply)
            handled += 1
        else:
            remaining.append((fp, ev))
    return remaining, handled


# ───────── 脱离扮演指令（2026-08-10 新增）─────────
# 群友 @bot 说"不要模仿了/解除扮演/恢复正常/回到你自己"等 → 清空 DEFAULT.txt，
# 回到无人格本我（与切换对称：任何群友可触发，本地执行不耗 AI）。
# 设计原则（承袭切换指令 v5.1 的踩坑经验）：
#   - L0 讨论/疑问无条件拦截（"他在电影里扮演X"不触发）
#   - L2 带目标解除依赖当前状态（"不要模仿XX"仅在当前正模仿XX时解除，
#     否则走 AI 拒绝——防止"别切换成它"被误读为"解除当前"）
#   - L1 无目标解除全部 $ 锚定句尾，防止吞后续实质内容
_UNROLE_DISCUSS = re.compile(
    r'(?:他|她|它|他们|这|电影|剧|片|作品|视频|有人|别人|那边|里面|剧情)'
    r'.{0,4}(?:扮演|模仿|演|恢复正常|解除|别装了|不演了|别cos了)'
)
# L1-① 动词+扮演词族（宾语必选，防"解除"空触发）
_UNROLE_VERB = re.compile(
    r'(?:解除|退出|停止|结束|取消|关闭|关掉|收起|撤掉)'
    r'\s*(?:角色扮演|扮演|模仿模式|模仿|表演|演出|角色卡|角色|cosplay|cos|play)'
    r'\s*(?:模式|状态)?\s*(?:了|吧)?[\s，。！？!?]*$'
)
# L1-② 否定+动词（"不要模仿了"/"别装了"/"不用学了"）
_UNROLE_NEG = re.compile(
    r'^(?:不要|别|不用|不必|不|莫)'
    r'\s*(?:再|继续)?\s*(?:扮演|模仿|学|cos|cosplay|装)'
    r'(?:了|吧|了啦)?[\s，。！？!?]*$'
)
# L1-③ 恢复词族（"恢复正常"/"回到你自己"/"恢复出厂设置"）
_UNROLE_BACK = re.compile(
    r'^(?:恢复|回到|变回|退回|回归)'
    r'\s*(?:正常|正常模式|本我|本体|你自己|自己|本来的样子|原来的样子|原样|原状|初始状态|出厂设置|默认)'
    r'(?:说话|模式)?(?:了|吧)?[\s，。！？!?]*$'
)
# L1-④ 短词族（精确枚举，比通用正则可控）
_UNROLE_SHORT = re.compile(
    r'^(?:别装了|不装了|别演了|不演了|别再演了|下戏了|收工了|戏演完了|别cos了|不cos了)'
    r'[\s，。！？!?]*$'
)
# L2 带目标解除（B 类）："不要模仿XX了"/"别演YY了"/"不要模仿XX说话"
# 目标解析失败（如"不要模仿了"→ 目标"了"被 _strip_target_suffix 剥空）→ 降级 A 类
_UNROLE_TARGET = re.compile(
    r'^(?:不要|别|不用|不必|先别|暂时别|停止|别再)'
    r'\s*(?:再|继续)?\s*(?:扮演|模仿|cos|cosplay|演|装|学)'
    r'\s*([^\s，。！？!?：:；;、]{1,20})'
    r'\s*(?:了|吧|说话|讲话|语气|口吻)?[\s，。！？!?]*$'
)


def _undo_persona(group_id, current, requester):
    """执行脱离扮演：清空该群的 DEFAULT_{gid}.txt（原子写），回到无人格本我。回执已发送。
    （2026-08-17：按群状态文件，只解除当前群）"""
    display = current or ""
    if current:
        p = _load_persona_json(current)
        if p and p.get("name"):
            display = p["name"]
    try:
        state_path = _persona_state_file(group_id)
        tmp = state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("")  # 清空 = 脱离扮演（_get_target_id 返回 "" → 不注入角色人格）
        os.replace(tmp, state_path)
    except Exception as e:
        _log(f"❌ 解除扮演写入失败: {e}")
        _send_reply(group_id, "解除失败（写入出错），稍后再试")
        return
    if current:
        _log(f"🎭 脱离扮演: {requester} 要求 → 已从「{display}」({current}) 解除")
        _send_reply(group_id, f"好，已从「{display}」解除扮演，回归本bot～")
    else:
        _log(f"🎭 脱离扮演: {requester} 要求 → 本来就没在扮演")
        _send_reply(group_id, "现在本来就没在扮演呢～")


def _try_undo_persona(events):
    """检测 @ 事件中的「脱离扮演/解除模仿」指令（2026-08-10 新增）。
    任何群友可触发；解除 = 清空 DEFAULT.txt。与 _try_switch_persona 对称：
    在 process_once 中先于切换执行，未命中的事件原样返回。
    返回 (remaining_events, handled_count)：
      remaining_events — 未匹配解除指令、继续走后续流程的事件
      handled_count   — 已本地处理的解除指令事件数
    """
    if not events:
        return [], 0
    group_id = events[0][1].get("group_id")
    remaining = []
    handled = 0
    for fp, ev in events:
        msg = _clean_msg(ev.get("message", "") or "")
        requester = ev.get("nickname", "?")
        if not msg:
            remaining.append((fp, ev))
            continue
        # L0 前置排除：讨论句式 / 疑问句 → 走正常 AI
        if _UNROLE_DISCUSS.search(msg) or _SWITCH_WEAK_QUESTION.search(msg):
            remaining.append((fp, ev))
            continue
        current = _get_target_id(group_id)  # 2026-08-17：按群扮演状态
        # L2 带目标解除（B 类）——先于 L1 检查，它也覆盖"不要模仿了"类短句
        m = _UNROLE_TARGET.search(msg)
        if m:
            target_raw = _SWITCH_EMOJI.sub("", m.group(1).strip()).strip()
            target_raw = _strip_target_suffix(target_raw)
            # 自指映射（承袭切换指令 12:19 方案）："不要模仿我"→ 请求者本人
            if target_raw in ("我", "我自己", "本人"):
                requester_id = str(ev.get("user_id", "") or "").strip()
                if requester_id and requester_id != BOT_QQ:
                    target_raw = requester_id
            tid, _, amb = _resolve_persona_target(target_raw, _load_registry(), _load_card_index())
            if amb:
                # 裸姓氏歧义（"不要模仿某姓氏了"）——候选含当前扮演目标
                # 时按该目标解除（用户本来就是指它）；否则走 AI（与"带目标但当前
                # 不是该目标"一致，不擅自解除）
                if current and any(o == current for o, _ in amb):
                    _undo_persona(group_id, current, requester)
                    handled += 1
                    continue
                remaining.append((fp, ev))
                continue
            if tid is None:
                # 目标无法解析（"不要模仿了"→ 目标"了"被剥空）→ 降级无目标解除
                _undo_persona(group_id, current, requester)
                handled += 1
                continue
            if current and tid == current:
                # 当前正好模仿该目标 → 解除
                _undo_persona(group_id, current, requester)
                handled += 1
                continue
            # 带目标但当前不是该目标（或当前无扮演）→ 走 AI（拒绝，保持现状）
            remaining.append((fp, ev))
            continue
        # L1 无目标解除（A 类）
        if (_UNROLE_VERB.search(msg) or _UNROLE_NEG.search(msg)
                or _UNROLE_BACK.search(msg) or _UNROLE_SHORT.search(msg)):
            _undo_persona(group_id, current, requester)
            handled += 1
            continue
        remaining.append((fp, ev))
    return remaining, handled


def _try_switch_persona(events):
    """
    检测 @ 事件中的切换模仿指令（2026-08-09 修复：只处理匹配指令的事件，
    批次内其他事件返回给调用方继续走 AI 流程，防止切换后吞掉同批消息）。
    返回 (remaining_events, handled_count)：
      remaining_events — 未匹配指令、需要继续走 AI 的事件
      handled_count   — 已本地处理的指令事件数（>0 表示有切换/查询发生）
    """
    if not events:
        return [], 0
    group_id = events[0][1].get("group_id")
    remaining = []
    handled = 0
    for fp, ev in events:
        msg = _clean_msg(ev.get("message", "") or "")
        requester = ev.get("nickname", "?")
        if not msg or _SWITCH_NEGATE.search(msg):
            remaining.append((fp, ev))
            continue

        # 1) 目标模式匹配（精确 → 宽松意图，两级）
        matched_target = None
        matched_span = None
        for pat in _SWITCH_TARGET_PATTERNS:
            m = pat.search(msg)
            if m:
                matched_target = m.group(1).strip()
                matched_span = m.span()
                break
        if not matched_target:
            for pat in _SWITCH_INTENT_PATTERNS:
                m = pat.search(msg)
                if m:
                    matched_target = m.group(1).strip()
                    matched_span = m.span()
                    break

        if matched_target:
            # 解析成功 → 切换（强指令直接切换；弱指令遇疑问句视为闲聊跳过）
            matched_target = _strip_target_suffix(matched_target)
            matched_target = _SWITCH_EMOJI.sub("", matched_target).strip()  # 去 emoji（"筱泽广💀"→"筱泽广"）
            # 自指映射（2026-08-09 12:19，用户方案）：
            # target 整体为"我/我自己/本人"时，语义 = 模仿消息来源用户本人。
            # 不做词库包含匹配——单字 target 包含命中任意含"我"的别名
            # （含「我」字的长别名被单字 target 包含命中）是误判根源（实锤）。
            # 含「我」字的长昵称整体 ≠ "我"，不受影响，仍走昵称匹配。
            if matched_target in ("我", "我自己", "本人"):
                requester_id = str(ev.get("user_id", "") or "").strip()
                if requester_id and requester_id != BOT_QQ:
                    if _persona_exists(requester_id):
                        matched_target = requester_id  # 映射为来源用户 QQ（isdigit 直输路径）
                    else:
                        _send_reply(group_id, f"「{requester}」还没有风格档案，暂时模仿不了自己啦～")
                        handled += 1
                        continue
            registry = _load_registry()
            card_index = _load_card_index()
            qq, _, amb = _resolve_persona_target(matched_target, registry, card_index)
            strong = bool(_SWITCH_STRONG.search(msg))
            # 疑问句/讨论句式无条件拦截（2026-08-09 v5：即使含"扮演/cos"强指令词，
            # "你想扮演XX吗"是询问、"他在电影里扮演XX"是讨论，都不切换）
            if _SWITCH_WEAK_QUESTION.search(msg):
                remaining.append((fp, ev))  # 疑问句 → 走正常 AI
                continue
            if _SWITCH_DISCUSS.search(msg):
                remaining.append((fp, ev))  # 讨论句式 → 走正常 AI
                continue
            if amb:
                # 2026-09-11：同姓氏歧义（"模仿花海"）→ 反问澄清，不擅自切换。
                # 位置在疑问/讨论拦截之后——"你想扮演花海吗"仍走 AI 而非被反问。
                _send_reply(group_id, _ambiguous_reply_text(matched_target, amb))
                handled += 1
                continue
            if not qq:
                # 目标无法解析：
                # - 强指令（切换/换成/变成/扮演等）→ 引导回复名单
                # - 弱指令（如"以轻松的风格写文案"的"以"）→ 静默走 AI，不打扰
                if matched_target in _SWITCH_NOISE_WORDS or _SWITCH_QUERY_PATTERN.search(msg):
                    _send_reply(group_id, "目前可模仿的：" + _persona_list_text(registry))
                    handled += 1
                    continue
                if strong:
                    _send_reply(group_id, "没搞明白应该模仿谁～ 目前可模仿的：" + _persona_list_text(registry))
                    handled += 1
                    continue
                remaining.append((fp, ev))  # 弱指令且目标不可解析 → 走正常 AI
                continue
            ok, text = _switch_persona(group_id, matched_target, requester)
            _send_reply(group_id, text)
            handled += 1
            # 2026-08-09 修复：指令消息可能附带实质请求（如"模仿某群友说话，发表对
            # 甜品罐头的意见"）——剥离指令短语后如有剩余内容，继续走 AI，
            # 模型会以新 persona 回答（旧版 agent 行为：切换 + 回复）。
            if matched_span:
                rest = (msg[:matched_span[0]] + msg[matched_span[1]:]).strip(" ，。！？!?、")
                rest = re.sub(r'[，。！？!?、]{2,}', lambda m: m.group(0)[0], rest)  # 合并连续标点
                # 2026-08-09 v2：剥离后可能残留指令尾缀（"以后像某群友一样说话"→"说话"），清洗
                rest = re.sub(r'^(?:说|讲|回|回复|帮我|然后|再|接着|顺便|话)+', '', rest).strip(" ，。！？!?、：:；;")
                if len(rest) >= 4:
                    ev2 = dict(ev)
                    ev2["message"] = rest
                    remaining.append((fp, ev2))
                    _log(f"  ➡️ 指令消息附带内容「{rest[:30]}」继续走 AI")
            continue

        # 2) 无目标 + 查询类（"模仿对象是谁/列一下可模仿的"）
        if _SWITCH_QUERY_PATTERN.search(msg):
            _send_reply(group_id, "目前可模仿的：" + _persona_list_text(_load_registry()))
            handled += 1
            continue

        remaining.append((fp, ev))

    return remaining, handled


# ==================== 执行一轮处理 ====================

_processing_lock = False


def process_once():
    """扫描 pending → debounce → LLM 调用 → 发送 → 归档。返回处理数。"""
    global _processing_lock
    if _processing_lock:
        return 0
    files = _scan_pending()
    # 防循环兜底：跳过已成功处理过的残留文件（归档失败场景）
    skipped = [os.path.basename(f) for f in files if os.path.basename(f) in _processed_ok]
    files = [f for f in files if os.path.basename(f) not in _processed_ok]
    if skipped:
        _log(f"  ⏭️ 跳过已处理残留 {len(skipped)} 个: {skipped[:3]}{'...' if len(skipped) > 3 else ''}")
    if not files:
        return 0
    _processing_lock = True
    try:
        # 2026-09-13：额度监控（每轮检查一次，开销 = 一次小文件读）。
        # 放在这里而不是轮询空转时——只有真在处理事件时才需要判断是否该提醒/注入。
        try:
            _maybe_notify_saucenao_quota()
        except Exception as e:  # noqa: BLE001
            _log(f"  ⚠️ 额度提醒检查异常（不影响处理）: {type(e).__name__}: {e}")
        files = [f for f in _scan_pending() if os.path.exists(f)]
        files = [f for f in files if os.path.basename(f) not in _processed_ok]
        if not files:
            return 0

        _log(f"发现 {len(files)} 个待处理事件，进入 {DEBOUNCE_SEC}s 去重窗口...")

        # Debounce
        stable_start = time.time()
        last_count = len(files)
        while time.time() - stable_start < DEBOUNCE_SEC:
            time.sleep(POLL_INTERVAL)
            # 2026-08-09 12:15 修复：过滤已处理残留后再比较/赋值。
            # 原实现 files = current 用未过滤全量覆盖已过滤列表，
            # 归档失败残留文件会在"检测到新事件"时复活 → 重复处理/重复回复刷屏。
            current = [f for f in _scan_pending() if os.path.basename(f) not in _processed_ok]
            if len(current) > last_count:
                _log(f"  检测到新事件 ({len(current)}个)，重置去重计时")
                last_count = len(current)
                files = current
                stable_start = time.time()

        files = [f for f in files if os.path.exists(f)]
        if not files:
            _log("  文件已被其他实例处理，跳过")
            return 0

        events = [(f, _load_event(f)) for f in files]

        # —— 重试超限兜底（2026-08-24）：长耗时任务反复超时重入队 → 无限重跑循环。
        # retry >= MAX_REQUEUE_RETRIES 的事件直接放弃（归档+标记），不再尝试处理。
        exhausted = []
        kept = []
        for fp, ev in events:
            r = int(ev.get("retry", 0) or 0)
            if r >= MAX_REQUEUE_RETRIES:
                exhausted.append((fp, ev, r))
            else:
                kept.append((fp, ev))
        if exhausted:
            for fp, ev, r in exhausted:
                _log(f"  ❌ 重试超限（retry={r}>={MAX_REQUEUE_RETRIES}），放弃处理: {os.path.basename(fp)}"
                     f"（{ev.get('nickname','?')}: {str(ev.get('message',''))[:40]}）")
                if os.path.exists(fp):
                    _archive_event(fp)
                _mark_processed(fp)
        events = kept
        if not events:
            _log("  本轮事件全部重试超限，已放弃")
            return 0

        # —— 群白名单兜底（2026-08-17）：非白名单群的事件直接归档丢弃（防事件入口漏过滤/残留重放）——
        allowed_events = []
        for fp, ev in events:
            gid = ev.get("group_id")
            if not is_group_allowed(gid):
                _log(f"  🚫 丢弃非白名单群 {gid} 事件 {os.path.basename(fp)}（白名单: {allowed_groups()}）")
                if os.path.exists(fp):
                    _archive_event(fp)
                _mark_processed(fp)
            else:
                allowed_events.append((fp, ev))
        events = allowed_events
        if not events:
            _log("  本轮事件全部为非白名单群，已丢弃")
            return 0

        # 移入 processing/
        processed_paths = []
        for fp, _ in events:
            base = os.path.basename(fp)
            dest = os.path.join(PROCESSING_DIR, base)
            try:
                os.rename(fp, dest)
                processed_paths.append(dest)
                _log(f"  📦 移入处理队列: {base}")
            except OSError as e:
                # 2026-08-09 12:15：补日志（原实现静默，事故排查困难）
                _log(f"  ⚠️ 移入处理队列失败，留在原目录: {base}（{e}）")
                processed_paths.append(fp)
        events = [(processed_paths[i], ev) for i, (_, ev) in enumerate(events)]

        groups = set(ev.get("group_id") for _, ev in events)
        names = [ev.get("nickname", "?") for _, ev in events]
        _log(f"群: {groups}, 来自: {', '.join(names)}")

        # 脱离扮演指令（2026-08-10 新增：任何群友可触发，本地执行，不依赖 gateway）
        # 先于切换执行——"不要模仿了"类指令在此消费，未命中的事件传给切换/管理/AI
        remaining_events, undo_count = _try_undo_persona(events)
        # 切换模仿对象指令（本地执行，不依赖 gateway；2026-08-09 方案A）
        # 2026-08-09 修复：只处理匹配指令的事件，批次内其他事件继续走 AI
        # （切换后新 persona 生效，剩余事件以新风格回复，不吞消息）
        remaining_events, handled_count = _try_switch_persona(remaining_events)
        handled_count += undo_count
        # 管理命令（2026-08-09 v1.11）：仅管理员触发，私聊回复——放在切换之后，
        # 只处理未被切换指令消费的剩余事件
        remaining_events, admin_count = _try_admin_command(remaining_events)
        handled_count += admin_count
        if admin_count > 0:
            _log(f"  🛡️ 已本地处理 {admin_count} 个管理命令事件")
        # 显式任务指令 #任务 <描述>（2026-09-01）：watcher 正则判别，直接建任务
        remaining_events, task_count = _try_task_command(remaining_events)
        handled_count += task_count
        if task_count > 0:
            _log(f"  📋 已本地处理 {task_count} 个显式任务指令")
        if handled_count > 0:
            _log(f"  🔀 已本地处理 {handled_count} 个本地指令事件（脱离扮演/切换模仿）")
        if not remaining_events:
            # 全部事件都是指令 → 归档后结束本轮
            for fp, _ in events:
                if os.path.exists(fp):
                    _archive_event(fp)
                _mark_processed(fp)  # 2026-08-09：归档失败也记录，防残留复活
            return len(events)
        if handled_count > 0:
            # 有指令事件被本地处理：归档指令事件，剩余事件继续走 AI
            remaining_paths = {fp for fp, _ in remaining_events}
            for fp, _ in events:
                if fp not in remaining_paths:
                    if os.path.exists(fp):
                        _archive_event(fp)
                    _mark_processed(fp)  # 2026-08-09：归档失败也记录，防残留复活
            events = remaining_events
            groups = set(ev.get("group_id") for _, ev in events)
            names = [ev.get("nickname", "?") for _, ev in events]
            _log(f"  ➡️ 剩余 {len(events)} 个事件继续走 AI: 群 {groups}, 来自 {', '.join(names)}")

        # 检查 opencode serve 可用性
        ok, detail = serve_health()
        if not ok:
            _log(f"❌ opencode serve 不可达: {detail}（离线演示请设 LLM_STUB=1，见 README）")
            # 重新入队，下次再试
            for fp, _ in events:
                _requeue_event(fp)
            return 0

        # 按群分组（2026-08-17：扮演状态/上下文按群分离 → 多群事件分群处理，互不干涉）
        by_group = {}
        for fp, ev in events:
            by_group.setdefault(ev.get("group_id"), []).append((fp, ev))
        if len(by_group) > 1:
            _log(f"  🧩 事件跨 {len(by_group)} 个群，按群分批处理: {sorted(str(g) for g in by_group)}")

        for gid, g_events in by_group.items():
            # 构建 prompt（按群注入 persona/俚语/成员共识/问答历史/Notes）
            all_ctx = _dedup_context(g_events)
            prompt = _build_prompt(g_events, deduped_ctx=all_ctx)
            # 2026-08-17：对话不活跃超过阈值 → 先交接归档再轮换新会话
            # （opencode 版：会话 ts 由 client 的 oc_sessions.json 维护）
            try:
                from opencode_client import _load_sessions
                _entry = _load_sessions().get(str(gid))
                if _entry and (time.time() - _entry.get("ts", 0) > CONV_IDLE_SECONDS):
                    _log(f"  ⏱️ 群{gid} 对话不活跃超过 {CONV_IDLE_SECONDS}s，触发交接归档")
                    _rotate_with_handoff(str(gid))
            except Exception:
                pass
            conv_id = _get_conversation_id(str(gid))

            _log(f"调用 opencode (群{gid}, session={conv_id}, timeout={RUN_TIMEOUT}s)...")
            try:
                reply = call_opencode(prompt, str(gid), timeout=RUN_TIMEOUT)
            except TimeoutError:
                _log(f"⏰ opencode 调用超时 ({RUN_TIMEOUT}s)")
                # 会话上下文过大可能导致持续超时：强制轮换会话，下轮用干净上下文重试
                try:
                    rotate_session(str(gid))
                except Exception as e:
                    _log(f"  ⚠️ 会话轮换失败: {e}")
                _log("  🔁 已轮换会话（下轮使用干净上下文重试）")
                for fp, _ in g_events:
                    _requeue_event(fp)
                continue
            except Exception as e:
                _log(f"❌ opencode 调用失败: {e}")
                # 超时文本同样触发轮换
                if "timed out" in str(e).lower() or "timeout" in str(e).lower():
                    try:
                        rotate_session(str(gid))
                    except Exception:
                        pass
                    _log("  🔁 已轮换会话（超时兜底）")
                for fp, _ in g_events:
                    _requeue_event(fp)
                continue

            reply = (reply or "").strip()

            # —— 2026-09-01：重活识别。agent 输出 [任务] 标记 → 委托任务会话执行
            # （不自己硬闯查记录/读群文件/联网搜索）。_start_task 内部处理单任务串行。
            task_desc = _extract_task_desc(reply)
            if task_desc:
                _log(f"  📋 识别到重活标记: {task_desc[:50]}")
                requester = g_events[0][1].get("nickname", "?")
                _start_task(gid, task_desc, requester)
                # 事件已消费（任务启动或告知忙），归档，不发标记文本
                for fp, _ in g_events:
                    if os.path.exists(fp):
                        _archive_event(fp)
                    _mark_processed(fp)
                continue

            # 2026-08-17：会话达轮数阈值 → 生成短期交接记忆并轮换新会话
            try:
                from opencode_client import _load_sessions
                _entry = _load_sessions().get(str(gid))
                if _entry and _entry.get("rounds", 0) >= CONV_MAX_ROUNDS:
                    _rotate_with_handoff(str(gid))
            except Exception:
                pass
            # 剥离模型模仿消息格式输出的 "回复id123" 前缀（模型常犯，不依赖指令自觉）
            reply = re.sub(r'^(回复id[:：]?\d+[,，:：、\s]*)+', '', reply).strip()
            # 提取显式长期记忆标记 [记忆:xxx]（2026-08-09：静默写入 Notes 文档，不随回复发送）
            reply, notes = _extract_notes(reply)
            if notes:
                _log(f"  🧠 提取长期记忆 {len(notes)} 条")
            # 提取角色表情标记 [表情:情感]（2026-08-09：强化角色扮演表达，不随文本回复发送）
            reply, emo_marks = _extract_emotion_mark(reply)
            if emo_marks:
                _log(f"  🎭 提取表情标记 {len(emo_marks)} 条: {emo_marks}")
            # 2026-08-22：剥离 agent 给 watcher 的内部"任务完成状态块"
            # （📬 已处理 N 条 @消息 / ✅ Core.md + Detailed/ 已更新 / <!-- TASK_COMPLETE -->）
            # 此块是给 watcher 的完成信号，不应发到群，否则群里会看到"自检"噪音
            reply = _strip_status_block(reply)
            _log(f"💬 回复生成: {reply[:100]}{'...' if len(reply) > 100 else ''}")

            # 发送回复
            no_reply_marker = "（无回复）"
            reply_ok = False
            if reply and reply != no_reply_marker and not reply.startswith("无回复"):
                ok_c, fail_c = _send_reply(gid, reply)
                reply_ok = ok_c > 0
                _log(f"发送结果: ok={ok_c} fail={fail_c}")
                # 记录问答历史（2026-08-08：补回新 conv 丢失的记忆；
                # 问题用清洗后文本，防止格式垃圾进历史）
                if ok_c > 0:
                    q_text = _clean_msg(g_events[0][1].get("message", ""))
                    _append_conv_memory(gid, q_text, reply)
                    # 显式长期记忆落盘（2026-08-09：author = 触发本轮的群友）
                    _append_notes(gid, notes,
                                  author=g_events[0][1].get("nickname", "?"))
            else:
                _log("  ⏭️ 无回复（跳过发送）")

            # 发送角色表情图（2026-09-06 三档配图模式：off=不发 / auto=按标记 / always=每次必配）
            # 2026-09-06 补充门控：文本回复实际发出才配图（原逻辑"无回复"时仍可能发孤立图片）
            img_mode = _get_image_mode()
            if img_mode != "auto":
                _log(f"  🎛️ 配图模式（{img_mode}）"
                     + (f"，回复已发出" if reply_ok else "，无回复未配图"))
            if img_mode != "off" and reply_ok:
                cur_role = _get_target_id(gid) or ""  # 2026-08-17：按群角色
                # 角色卡 id 形如 card_demo_alpha / card_demo_beta，
                # 素材索引按角色 id 组织；无素材的卡则跳过
                if emo_marks:
                    for emo in emo_marks[:3]:  # 最多发 3 张
                        ok_img, detail = _send_emotion_image(gid, cur_role, emo)
                        _log(f"  🖼️ 表情图 [{emo}]: {'✅ ' + detail if ok_img else '⏭️ ' + detail}")
                elif img_mode == "always":
                    # always 档：模型没出标记也从角色图库随机补一张（空情感 → 整库随机）
                    ok_img, detail = _send_emotion_image(gid, cur_role, "")
                    _log(f"  🖼️ 强制配图（always 档）: {'✅ ' + detail if ok_img else '⏭️ ' + detail}")

            # 归档
            for fp, _ in g_events:
                if os.path.exists(fp):
                    _archive_event(fp)
            # 标记已处理（即使归档失败，下次扫描也跳过——防循环刷屏）
            for fp, _ in g_events:
                _mark_processed(fp)  # 2026-08-09：改为落盘版本，重启不丢

        return len(events)
    finally:
        _processing_lock = False


# ==================== CLI ====================

def _is_watcher_running():
    # 2026-08-24：去掉"锁文件时间戳 5 分钟过期"判断——锁只在启动时写一次、主循环不刷新，
    # 正常运行 5 分钟后锁必"过期" → 活着的实例被误判为不存在 → 新实例绕过检查启动 → 双实例。
    # 改为纯进程级检查：PID 存活且 cmdline 匹配 watcher 即视为运行中。
    # （旧实例即使阻塞在 150s 超时调用里进程也活着，能正确拦下重复启动）
    if not os.path.exists(WATCHER_LOCK_FILE):
        return False
    try:
        with open(WATCHER_LOCK_FILE, "r") as f:
            content = f.read().strip()
        if ":" in content:
            pid_str, _ts_str = content.split(":", 1)
            pid = int(pid_str)
        else:
            pid = int(content)
        r = subprocess_run(["tasklist", "/FO", "CSV", "/NH", "/FI", f"PID eq {pid}"])
        if f'"{pid}"' in r.stdout and "python.exe" in r.stdout.lower():
            pr = subprocess_run(
                ['powershell', '-Command',
                 f'Get-CimInstance Win32_Process -Filter "ProcessId={pid}" | Select-Object -ExpandProperty CommandLine'])
            cmdline = pr.stdout.strip().lower()
            if "watcher" in cmdline:
                return True
            _log(f"PID {pid} 不是 watcher_oc 进程 (cmdline 不匹配)")
        else:
            _log(f"PID {pid} 不存在或非 python 进程")
    except Exception as e:
        _log(f"锁文件解析异常: {e}")
    try:
        os.remove(WATCHER_LOCK_FILE)
    except Exception:
        pass
    return False


def subprocess_run(args):
    import subprocess
    return subprocess.run(args, capture_output=True, text=True, timeout=5)


def _migrate_legacy_persona_state():
    """2026-08-17：扮演状态按群分离。旧全局 DEFAULT.txt → DEFAULT_{DEFAULT_GROUP_ID}.txt（只迁移一次）。"""
    legacy = os.path.join(PERSONA_DIR, "DEFAULT.txt")
    target = _persona_state_file(DEFAULT_GROUP_ID)
    if os.path.exists(legacy) and not os.path.exists(target):
        try:
            with open(legacy, "r", encoding="utf-8") as f:
                content = f.read()
            tmp = target + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp, target)
            _log(f"🔁 扮演状态迁移: DEFAULT.txt → DEFAULT_{DEFAULT_GROUP_ID}.txt"
                 f"（{content.strip() or '(空)'}）")
        except Exception as e:
            _log(f"⚠️ 扮演状态迁移失败: {e}")


def _heartbeat_daemon():
    """独立心跳线程（2026-09-01 新增）。

    事故：process_once 阻塞在 150s 超时的 opencode 调用上时，主循环不写心跳
    → 外部守护脚本按心跳判定误判 watcher 离线 → 反复尝试重启（被 lock 挡住）。
    daemon 线程每 10s 写一次心跳，长调用期间心跳持续更新，不再误判。
    """
    hb_path = os.path.join(LOG_DIR, "heartbeat.txt")
    while True:
        try:
            with open(hb_path, "w", encoding="utf-8") as f:
                f.write(datetime.now().isoformat())
        except Exception:
            pass
        time.sleep(10)


def cmd_start():
    import subprocess
    if _is_watcher_running():
        _log("⚠️ 已有 watcher_oc 实例在运行，跳过")
        return
    threading.Thread(target=_heartbeat_daemon, daemon=True).start()
    _migrate_legacy_persona_state()  # 2026-08-17：旧全局扮演状态 → 默认群
    _load_conv_memory()  # 启动时加载问答历史（watcher 重启不丢）
    _cleanup_stale_pending()  # 2026-08-09：清理上次会话已处理的残留文件（防重启重放刷屏）
    _resume_tasks()  # 2026-09-01：恢复未完成任务（防重启丢任务）
    with open(WATCHER_LOCK_FILE, "w") as f:
        f.write(f"{os.getpid()}:{datetime.now().strftime('%Y%m%d%H%M%S')}")

    _log("🧩 @事件消费守护启动")
    _log(f"   队列目录: {PENDING_DIR}")
    _log(f"   去重窗口: {DEBOUNCE_SEC}s")
    _log(f"   轮询间隔: {POLL_INTERVAL}s")

    ok, detail = serve_health()
    _log(f"   opencode serve: {'✅ ' + detail if ok else '❌ ' + detail}")
    _log(f"   {'='*40}")

    while True:
        try:
            count = process_once()
            if count > 0:
                _log(f"本轮处理完成，共 {count} 个事件\n")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            _log("🛑 收到停止信号")
            break
        except Exception as e:
            _log(f"❌ 主循环异常: {e}，继续运行...")
            time.sleep(POLL_INTERVAL)


def cmd_status():
    files = _scan_pending()
    archived = glob.glob(os.path.join(ARCHIVE_DIR, "at_*.json"))
    ok, detail = serve_health()
    print(f"📊 @事件队列状态")
    print(f"   队列目录: {PENDING_DIR}")
    print(f"   待处理: {len(files)} 个")
    for f in files:
        ev = _load_event(f)
        msg = ev.get("message", "")[:50]
        print(f"     ├ {ev.get('nickname','?')}({ev.get('user_id','?')}) @群{ev.get('group_id','?')}: {msg}")
        print(f"     └ {os.path.basename(f)}")
    print(f"   已归档: {len(archived)} 个")
    print(f"   opencode serve: {'✅ 运行中' if ok else '❌ 不可达'} ({detail})")


def cmd_process():
    count = process_once()
    print(f"处理完成，共 {count} 个事件")


def print_help():
    print(__doc__)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_help()
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "start":
        cmd_start()
    elif cmd == "status":
        cmd_status()
    elif cmd == "process":
        cmd_process()
    elif cmd in ("--help", "-h"):
        print_help()
        sys.exit(0)
    else:
        print(f"未知命令: {cmd}")
        print_help()
        sys.exit(2)
