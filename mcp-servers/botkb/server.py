#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
botkb —— 知识检索 MCP Server（stdio，无第三方依赖）
========================================================
统一暴露 bot 可用的只读知识检索工具，设计为**可拓展的多知识源服务**：

  工具 1  search_theory   知识库检索（示例库见 sample_kb/，引擎 = kb_search.py）
  工具 2  list_sources / read_source   原著全文（chapters/，可选）
  工具 3  view_image      按需取群图原图（MCP 返回 image content block）
  工具 4  trace_image     以图搜图 / 反查图片来源（SauceNAO，唯一出网工具）
（search_lyrics 为可选模块：存在 lyrics_search.py 时自动注册。）

额度监控：trace_image 每次响应都把额度快照**原子写**到
BotMemory/SauceNAOQuota.json，供 watcher（另一进程）读取 →
① 额度告急时私聊提醒维护者 ② 用尽时往 prompt 注入提示。
⚠️ 快照是「上次查询时」的值，不是实时值；读方（watcher）自行按 TTL 判新鲜度。

新增知识源 = 写一个 *_search.py 检索库 + 在 TOOLS 注册一个工具，server 架构不变。

安全性：
  · 除 trace_image 外全部工具只读本地（查本地索引/JSON），无网络、无 shell —— 无注入面
  · trace_image 仅向 saucenao.com 发 HTTPS POST（上传本地图 + api_key），
    key 走环境变量 SAUCENAO_API_KEY 或仓库根目录 saucenao_key.txt；无 key 时功能自动禁用
  · view_image/trace_image 只读**单个目录**（IMAGE_ARCHIVE_DIR，见 BOTKB_IMAGE_DIR），
    入参经 basename 清洗 + realpath 前缀校验，防路径穿越；不写任何文件
  · 权限在 watcher 侧会话 ruleset（mcp__botkb）显式 allow

协议：MCP 2024-11-05（newline-delimited JSON-RPC over stdio）
  stdout 只输出 JSON-RPC 消息；一切日志走 stderr（stdout 污染会打挂握手）

在 opencode 配置里注册（把路径换成你的实际路径）：
  "botkb": {
    "type": "local",
    "command": ["<python>", "<repo>/mcp-servers/botkb/server.py"],
    "enabled": true
  }
"""
import base64
import io
import json
import os
import sys

# stdout 必须是 UTF-8 且无缓冲干扰（Windows 默认 GBK 会打挂协议）
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", write_through=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", write_through=True)

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(os.path.dirname(MODULE_DIR))
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)

import kb_search  # noqa: E402

try:  # 可选知识源：存在 lyrics_search.py 时一并注册（未提供则跳过）
    import lyrics_search  # noqa: E402
except ImportError:
    lyrics_search = None

SERVER_NAME = "botkb"
SERVER_VERSION = "2.3.0"

# ==================== view_image 配置（路线 A，2026-09-10）====================
# 参数：常规图 = 长边上限 1024；长图 = 宽度下限 800（严禁按长边缩放，会压到不可读）
IMAGE_ARCHIVE_DIR = os.getenv("BOTKB_IMAGE_DIR", os.path.join(REPO_DIR, "images"))
IMG_UPLOAD_MAXSIDE = int(os.getenv("BOTKB_IMG_MAXSIDE", "1024"))
IMG_UPLOAD_MIN_WIDTH = int(os.getenv("BOTKB_IMG_MIN_WIDTH", "800"))
IMG_UPLOAD_MAX_PIXELS = int(os.getenv("BOTKB_IMG_MAX_PIXELS", "2000000"))
IMG_LONG_RATIO = float(os.getenv("BOTKB_IMG_LONG_RATIO", "3.0"))
IMG_LONG_MIN_HEIGHT = int(os.getenv("BOTKB_IMG_LONG_MIN_HEIGHT", "2500"))
IMG_JPEG_Q = int(os.getenv("BOTKB_IMG_JPEG_Q", "88"))
IMG_MAX_FILESIZE = int(os.getenv("BOTKB_IMG_MAX_BYTES", str(12 * 1024 * 1024)))
IMG_ALLOW_EXT = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")

# ==================== trace_image 配置（SauceNAO 以图搜图，2026-09-13）====================
# 目的：群友问「这张图哪来的 / 图里是谁」时，用**以图搜图**找出处。
#   —— 与 view_image 的区别：view_image 是「把图给模型看」，本工具是「拿图去查来源」。
#   —— 与 searxng 的区别：searxng 的 images 类是「用文字搜图」，做不到反查（详见 skill）。
#
# ⚠️ 网络：本工具是 botkb 里**唯一会出网**的工具（仅连 saucenao.com，HTTPS）。
#    rest 工具仍全部只读本地。已实测：saucenao.com 可达（假 api_key 返回其业务 JSON
#    「The anonymous account type does not permit API usage.」= 网络通、API 层可达，只差真 key）。
#
# key 来源（按序，任一即可）：
#   ① 环境变量 SAUCENAO_API_KEY
#   ② 仓库根目录 saucenao_key.txt（内容就是 key 本身，一行）
#   key 缺失 → 工具返回明确提示，不报错、不影响其它工具。
#
# 限额（免费账号，保守设定）：进程内最小间隔 + 连续失败熔断，避免触发封禁。
# ⚠️ API 状态：SauceNAO 官方声明 API 处于 **beta**，可能不兼容地变更（原话：
#    "api is in beta, so while we will try to maintain backwards compatibility,
#     there is the potential for future breakage. Use with care."）
#    → 所有响应字段访问一律 .get() 防御式解析，绝不假设字段必存在。
#    官方文档：https://saucenao.com/user.php?page=search-api
SAUCENAO_URL = os.getenv("SAUCENAO_URL", "https://saucenao.com/search.php")
SAUCENAO_KEY_FILE = os.getenv("SAUCENAO_KEY_FILE",
                              os.path.join(REPO_DIR, "saucenao_key.txt"))
SAUCENAO_MIN_INTERVAL = float(os.getenv("SAUCENAO_MIN_INTERVAL", "12"))
SAUCENAO_TIMEOUT = int(os.getenv("SAUCENAO_TIMEOUT", "40"))
SAUCENAO_NUMRES = int(os.getenv("SAUCENAO_NUMRES", "8"))
# 2026-09-13 实测修正（依官方文档）：
#   · db=999 = 搜索全部索引（文档明示这是"搜全部"的正规写法，比 dbmask 位掩码稳妥）
#   · dedupe=2 = 官方默认的"全部去重方法（含按系列名）"（显式写出，避免默认值变更）
#   · hide=2 = 隐藏「明确或疑似 explicit」的结果。
#     **为什么必须显式传 hide**：实测发现官方示例返回的 E-Hentai(H 站) 结果在默认
#     hide=0 下 `hidden` 字段仍为 0（hide 参数只控制 hidden 字段的**语义**，不是过滤器）
#     → 原先「查 hidden 字段过滤 R18」的写法在默认参数下**完全失效**。
#
#     **为什么是 2 而不是 3**（同图实测对比，决定性证据）：
#       hide=3 → hidden 分布 {0:3, 1:5}，把 4 条正常 Twitter 画师图也隐藏了（过度过滤，
#                Twitter 是画师发图主阵地，误杀代价大）
#       hide=2 → hidden 分布 {0:7, 1:1}，精确拦掉 Kemono（成人归档站），保留正常图
#     → hide=2 拦住明确/疑似 R18 且不误杀；配合下方来源库名兜底构成两层防护。
SAUCENAO_DB = os.getenv("SAUCENAO_DB", "999")
SAUCENAO_HIDE = int(os.getenv("SAUCENAO_HIDE", "2"))
SAUCENAO_DEDUPE = int(os.getenv("SAUCENAO_DEDUPE", "2"))
# 相似度判定（2026-09-13 重设计）：
#   官方在 header 里给 `minimum_similarity`，并说明：结果 header 的 similarity
#   低于该值即"unlikely，正常情况下会被隐藏"→ **这才是官方判定线**。
#   原先硬编码的 60 是拍脑袋的值，可能误杀真实结果，已弃用为**可选的人工下限**：
#   仅当 SAUCENAO_SIM_FLOOR > 0 时才额外生效（取 max(官方线, floor)）。
SAUCENAO_SIM_FLOOR = float(os.getenv("SAUCENAO_SIM_FLOOR", "0"))
# 官方 minimum_similarity 字段缺失/不可解析时的兜底判定线。
#   API 处于 beta（见上方声明），字段可能在未来变更 → 不能退化成「不过滤，全通过」，
#   否则会把噪声结果当成候选展示。仅在取不到官方值时启用，不是日常主逻辑。
SAUCENAO_SIM_FALLBACK = float(os.getenv("SAUCENAO_SIM_FALLBACK", "50"))
# 展示分档（给模型判断依据，不是过滤线）：>= 高分档视为"高可信"
SAUCENAO_SIM_CONFIDENT = float(os.getenv("SAUCENAO_SIM_CONFIDENT", "80"))
SAUCENAO_BREAK_AFTER = int(os.getenv("SAUCENAO_BREAK_AFTER", "3"))
SAUCENAO_BREAK_SECONDS = int(os.getenv("SAUCENAO_BREAK_SECONDS", "600"))
# 免费账号额度（实测 account_type=1：short_limit=4/30s，long_limit=100/24h）。
# 仅用于提示；真实剩余量以每次响应 header 的 *_remaining 为准。
SAUCENAO_LOW_QUOTA = int(os.getenv("SAUCENAO_LOW_QUOTA", "10"))

# 额度状态落盘（2026-09-13）：SauceNAO 只在**响应里**给额度，本地无查询就拿不到实时值。
# 把「最近一次查询看到的额度」写到文件，供 watcher 读取（两个独立进程）→
# watcher 据此 (a) 在额度告急时私聊提醒维护者，(b) 在额度用尽时往 prompt 里加提示。
# ⚠️ 这是**上一次查询的快照**，不是实时值；时间戳必须一起写，读方自行判新鲜度。
SAUCENAO_QUOTA_FILE = os.getenv(
    "SAUCENAO_QUOTA_FILE",
    os.path.join(REPO_DIR, "BotMemory", "SauceNAOQuota.json"))

TOOLS = [
    {
        "name": "search_lyrics",
        "description": (
            "检索歌词库（可选模块；未配置时该工具不注册）。"
            "输入歌名或任意歌词片段（日文/中文/英文均可，容忍空耳与漏字）。"
            "返回命中歌曲的：歌名、歌手、所属作品/角色、命中片段。"
            "用途：群友贴歌词接梗、问某首歌、想确认某句歌词出自哪首歌时调用。"
            "建议：没命中就换更短的连续片段再试（取你认为最独特的一句），"
            "或改用歌名/罗马音。本工具只读、无副作用，可放心多次调用。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "歌名或歌词片段。歌词片段建议至少 7 个日文/汉字符或 12 个英文字符，太短会查不到",
                },
                "ip": {
                    "type": "string",
                    "description": "可选。限定作品（示例值）",
                    "enum": ["demo"],
                },
                "limit": {
                    "type": "integer",
                    "description": "可选。最多返回几条（默认 3，最大 10）",
                    "minimum": 1,
                    "maximum": 10,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_theory",
        "description": (
            "检索知识库（示例库见 sample_kb/：理论库 + 写作库两个演示组件）。"
            "输入概念名、含概念的问句或原文片段（支持中文概念与英文术语）。"
            "返回：条目标题、来源组件、概念列表、正文节选、适用场景。"
            "用途：群聊出现知识库覆盖的概念或创作话题、想引用准确保说时调用。"
            "注意：检索结果是摘录；若摘录不足以支撑回答、或需要核对原文，"
            "可再调 list_sources（列原文章节）与 read_source（读原文全文）兜底。"
            "没命中就换同义词或英文术语再试。只读、可多次调用。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "概念名、含概念的问句或原文片段",
                },
                "kb": {
                    "type": "string",
                    "description": "可选。限定知识库：theory（示例·理论库）| writing（示例·写作库）。默认全部",
                    "enum": ["theory", "writing"],
                },
                "limit": {
                    "type": "integer",
                    "description": "可选。最多返回几条（默认 3，最大 8）",
                    "minimum": 1,
                    "maximum": 8,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "list_sources",
        "description": (
            "列出已配备原著全文（chapters/）的组件与章节清单。"
            "不带参数返回组件汇总（组件+章节数）；带 component 返回该组件全部章节文件名与标题。"
            "用途：需要引原著原文、或 search_theory 摘录不足以回答时，先看有哪些章节可读，"
            "再用 read_source 取指定章原文。只读。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "kb": {
                    "type": "string",
                    "description": "可选。theory | writing。默认全部",
                    "enum": ["theory", "writing"],
                },
                "component": {
                    "type": "string",
                    "description": "可选。组件名（如 demo-book-a）。给定则列出该组件全部章节文件",
                },
            },
        },
    },
    {
        "name": "read_source",
        "description": (
            "读取某组件某章的原著全文（PDF/EPUB 提取，含少量 OCR 噪声可容忍）。"
            "query 填章节标题/关键词，或 file 直接填 list_sources 列出的文件名（更精确）。"
            "返回该章完整原文（单文件超 20000 字符会截断并注明）。"
            "用途：需要核对原文、知识库条目覆盖不足、或想引用原文时——"
            "这是摘录检索之上的最强兜底。多候选时会列出供你选择精确对象。只读。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "章节标题或关键词（示例：「第一章 检索入门」）",
                },
                "component": {
                    "type": "string",
                    "description": "可选。限定组件（如 demo-book-a）。多组件同名关键词时建议带上",
                },
                "file": {
                    "type": "string",
                    "description": "可选。精确文件名（来自 list_sources），此时 query 可省略",
                },
            },
        },
    },
    {
        "name": "view_image",
        "description": (
            "查看群聊消息里某张图片的**原图**。"
            "当消息中出现 [图片#N] 标记、而「图片清单」里该图没有描述、"
            "或描述不足以回答关于画面/文字的问题时，调用本工具取原图查看。"
            "file_id 从「图片清单」里原样复制（形如 32 位十六进制 + 扩展名，如 3F2A…9C.jpg），"
            "不要自己编造。返回的是图片本身（必要时已按可读性上限降采样；超长图默认只给顶部，"
            "返回文本会注明）。若本地无该图副本会明确说明。"
            "用途：群友发图问「这是什么 / 写了什么 / 什么角色 / 什么梗」，"
            "或清单里的描述不足以回答时。"
            "重要：取到图后请直接基于图片内容回答，不要说你「看不到图片」或「无法查看图片」。"
            "只读、无副作用，可多次调用。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_id": {
                    "type": "string",
                    "description": "「图片清单」里给出的 file_id（含扩展名），原样复制，不要编造",
                },
                "region": {
                    "type": "string",
                    "description": ("可选。超长图的取图范围：top=只取顶部一块（默认，受像素上限约束）；"
                                    "full=尽力返回完整长图（更贵、可能很慢）。普通图无需传。"),
                    "enum": ["top", "full"],
                },
            },
            "required": ["file_id"],
        },
    },
    {
        "name": "trace_image",
        "description": (
            "【以图搜图】把群图拿去 SauceNAO 溯源。"
            "**两类问题都可以用它**：① 「这图哪来的 / 出处是什么 / 画师是谁」；"
            "② 「图里是谁 / 是哪个角色」—— 用于**交叉验证**你目测的判断。"
            "file_id 从「图片清单」里原样复制（与 view_image 用同一个 file_id）。"
            "**这是查来源的工具，不是看图的工具** —— 想「看清画面/读图里的字」"
            "要用 view_image（两者配合：先看，再查证）。"
            "⚠️ 能力边界（2026-09-13 实测）：\n"
            "  · 「角色名 characters / 作品名 material」**只有命中 Danbooru 系列索引时才有**；"
            "Pixiv / Twitter / deviantArt 等索引只返回标题、作者、原图链接，**不含角色名**。"
            "  · 所以**查不到角色名是常态**（实测多数群图甚至低于判定线）——"
            "**这不代表你的判断错了**，照旧按外观判断作答即可；\n"
            "  · 主攻动漫插画类；实景照片、截图、群友原创图命中率低。\n"
            "  · 结果已按官方判定线过滤并标注分档：【高可信】(≥80%) 可据此回答；"
            "【偏低(仅供参考)】只能措辞为「可能/像是」，**不得断言**。\n"
            "  · **若溯源结论与你的外观判断矛盾，照实并列两者**"
            "（「看着像 A，但搜到的索引标的是 B」），不要挑一个当定论。\n"
            "  · 已自动过滤 R18/成人向来源，可能因此少几条结果，属正常。\n"
            "注意：① 有频率限制（约每 12 秒一次），同一张图不要重复调；"
            "② 24 小时额度有限（免费账号约 100 次），别对每张图都无差别地查。"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_id": {
                    "type": "string",
                    "description": "「图片清单」里给出的 file_id（含扩展名），原样复制，不要编造",
                },
            },
            "required": ["file_id"],
        },
    },
]


# 可选工具：模块缺失时从 TOOLS 中剔除（保持「可拓展知识源」设计）
if lyrics_search is None:
    TOOLS = [t for t in TOOLS if t["name"] != "search_lyrics"]


def _log(msg):
    print(f"[botkb-mcp] {msg}", file=sys.stderr)


# ==================== 工具实现 ====================

def fmt_lyrics_hits(hits):
    if not hits:
        return "（歌词库中未找到匹配。建议：换更长的连续歌词片段，或改用歌名再试）"
    out = [f"命中 {len(hits)} 首："]
    for i, h in enumerate(hits, 1):
        s = h["song"]
        tags = []
        if s.get("ip"):
            tags.append(f"IP:{s['ip']}")
        if s.get("char"):
            tags.append(f"角色:{s['char']}")
        elif s.get("ip"):
            tags.append("共通曲/全员")
        head = f"{i}. 《{s['title']}》 — {s['artist']}"
        if tags:
            head += f"  [{', '.join(tags)}]"
        out.append(head)
        out.append(f"   匹配方式: {h['kind']}  相关度: {h['score']}")
        if h.get("snippet"):
            out.append(f"   歌词片段: …{h['snippet'][:150]}…")
        if s.get("body_cn"):
            out.append(f"   中文翻译: {s['body_cn'][:150]}")
    return "\n".join(out)


def fmt_theory_hits(hits, peers=None, peer_chapter=""):
    """格式化命中列表。peers 非空时附加同章词条一览（跳过已在主列表的条目）。

    展示加厚：正文 600 字符（此前 260）/ 适用 300（此前 180），
    增显示 id 与 confidence——方便追查与评估可靠性。
    """
    if not hits:
        return ("（知识库中未找到匹配。建议：换同义词、更通用的上位概念、"
                "或英文术语再试）")
    out = [f"命中 {len(hits)} 条："]
    seen = set()
    for i, h in enumerate(hits, 1):
        e = h["entry"]
        seen.add(e["id"])
        out.append(f"{i}. {e['heading'] or e['chapter'] or e['id']}"
                   f"  （{e['component_cn']} · {'理论库' if e['kb'] == 'theory' else '写作库'}）")
        out.append(f"   相关度: {h['score']}（{h['kind']}） id={e['id']}")
        if e["confidence"]:
            out.append(f"   置信: {e['confidence']}")
        if e["concepts"]:
            out.append(f"   概念: {'、'.join(str(c) for c in e['concepts'][:10])}")
        if e["text"]:
            out.append(f"   正文: {e['text'][:600]}{'…' if len(e['text']) > 600 else ''}")
        if e["scope_note"]:
            out.append(f"   适用: {e['scope_note'][:300]}")

    if peers is not None:
        extra = [p for p in peers if p["id"] not in seen]
        out.append("")
        if extra:
            out.append(f"◆ 同章词条（「{peer_chapter}」在库内还收录 {len(extra)} 条）：")
            for p in extra:
                head = p["heading"] or p["chapter"] or p["id"]
                line = f"   · {head}  (id={p['id']})"
                if p["concepts"]:
                    line += f"  概念: {'、'.join(str(c) for c in p['concepts'][:6])}"
                out.append(line)
                if p["text"]:
                    t = p["text"]
                    out.append(f"      正文: {t[:200]}{'…' if len(t) > 200 else ''}")
        else:
            out.append(f"◆ 章节说明：「{peer_chapter}」在库内仅收录上方案中条目，无其他同章词条。")
    return "\n".join(out)


def fmt_sources_summary(sources):
    """按组件汇总章节文件。sources 为 list_sources 返回。"""
    if not sources:
        return "（当前没有库配置原著全文 chapters/）"
    by_comp = {}
    for s in sources:
        by_comp.setdefault((s["component"], s["component_cn"], s["kb"]), []).append(s)
    out = []
    for (comp, cn, kb), items in sorted(by_comp.items()):
        out.append(f"■ {cn}（{comp} · {'理论库' if kb == 'theory' else '写作库'}）— {len(items)} 章节")
        for it in items:
            out.append(f"   · {it['file']}: {' / '.join(it['labels'])}")
    return "\n".join(out)


def fmt_source_candidates(hits):
    """resolve_source 多候选时的清单输出。hits=[(score, src)]"""
    out = [f"匹配到 {len(hits)} 个章节，请用 read_source 指定其一（可带 file 参数精确指定）："]
    for i, (s, src) in enumerate(hits, 1):
        out.append(f"{i}. {src['component_cn']} — {' / '.join(src['labels'])}")
        out.append(f"   score={s}  file={src['file']}")
    return "\n".join(out)


# ==================== view_image 实现（路线 A，2026-09-10）====================

def _clean_file_id(raw):
    """清洗模型给的 file_id：去引号、去路径前缀、去 query，只留 basename。"""
    s = str(raw or "").strip().strip('"').strip("'")
    if not s:
        return ""
    s = s.replace("\\", "/")
    s = s.split("?")[0].split("#")[0]
    return os.path.basename(s)


def _resolve_image(file_id):
    """file_id → 归档中的绝对路径；找不到返回 None（无扩展名时尝试补全）。"""
    base = _clean_file_id(file_id)
    if not base or base in (".", ".."):
        return None
    root = os.path.realpath(IMAGE_ARCHIVE_DIR)

    def _ok(p):
        rp = os.path.realpath(p)
        return rp.startswith(root + os.sep) and os.path.isfile(rp)

    p = os.path.join(IMAGE_ARCHIVE_DIR, base)
    if _ok(p):
        return os.path.realpath(p)
    if "." not in base:
        for ext in IMG_ALLOW_EXT:
            p2 = os.path.join(IMAGE_ARCHIVE_DIR, base + ext)
            if _ok(p2):
                return os.path.realpath(p2)
    return None


def _preprocess_image(path, region="top"):
    """读取 + 降采样 + 编码。返回 (b64, mime, meta)；异常向上抛。

    缓存参数一致：
      · 基准：长边缩到 IMG_UPLOAD_MAXSIDE
      · **宽度下限优先（硬要求）**：若长边规则会把**宽度**压到 IMG_UPLOAD_MIN_WIDTH 以下，
        则改用宽度定标（`scale = MIN_WIDTH / w0`），放弃长边约束。
        依据：实测宽度 ≥500px 可读、**429px 起会编造**，而 860×1920 这类手机截图按长边规则
        会掉到 460px 宽——恰好落在不可读区（§11.2-④）。
      · 超长图（h/w ≥ IMG_LONG_RATIO 且 h ≥ IMG_LONG_MIN_HEIGHT）同理，缩放后受像素上限约束
      · 像素软上限 IMG_UPLOAD_MAX_PIXELS：超限时常规图继续缩、长图 top 模式裁顶部
      · GIF/多帧 → 首帧；带 alpha → 合成白底（JPEG 不支持透明）
    """
    from PIL import Image
    with Image.open(path) as im:
        fmt = (im.format or "?")
        try:
            n_frames = getattr(im, "n_frames", 1) or 1
        except Exception:
            n_frames = 1
        try:
            im.seek(0)
        except Exception:
            pass
        w0, h0 = im.size
        is_long = (h0 > w0) and (h0 / max(w0, 1) >= IMG_LONG_RATIO) and (h0 >= IMG_LONG_MIN_HEIGHT)

        longside = max(w0, h0)
        scale = (IMG_UPLOAD_MAXSIDE / longside) if longside > IMG_UPLOAD_MAXSIDE else 1.0
        # ★ 宽度下限优先：宁可突破长边上限，也不让宽度掉到可读阈值以下
        if w0 >= IMG_UPLOAD_MIN_WIDTH and w0 * scale < IMG_UPLOAD_MIN_WIDTH:
            scale = IMG_UPLOAD_MIN_WIDTH / float(w0)

        # alpha → 白底合成（避免 JPEG 出现黑块）
        if im.mode in ("RGBA", "LA", "P"):
            rgba = im.convert("RGBA")
            bg = Image.new("RGB", rgba.size, (255, 255, 255))
            bg.paste(rgba, mask=rgba.split()[-1])
            work = bg
        elif im.mode != "RGB":
            work = im.convert("RGB")
        else:
            work = im.copy()

        nw0, nh0 = work.size
        if scale != 1.0:
            nw = max(1, int(round(nw0 * scale)))
            nh = max(1, int(round(nh0 * scale)))
            work = work.resize((nw, nh), Image.LANCZOS)
        else:
            nw, nh = nw0, nh0

    cropped = False
    if nw * nh > IMG_UPLOAD_MAX_PIXELS:
        if is_long and region != "full":
            keep_h = max(1, IMG_UPLOAD_MAX_PIXELS // nw)
            if keep_h < nh:
                work = work.crop((0, 0, nw, keep_h))
                nh = keep_h
                cropped = True
        elif not is_long:
            f = (IMG_UPLOAD_MAX_PIXELS / float(nw * nh)) ** 0.5
            nw2 = max(1, int(nw * f))
            nh2 = max(1, int(nh * f))
            work = work.resize((nw2, nh2), Image.LANCZOS)
            nw, nh = nw2, nh2
        else:  # 长图 + full：仍然裁到硬上限的 2 倍（防止极端图把会话库撑爆）
            hard = IMG_UPLOAD_MAX_PIXELS * 2
            if nw * nh > hard:
                keep_h = max(1, hard // nw)
                work = work.crop((0, 0, nw, keep_h))
                nh = keep_h
                cropped = True

    buf = io.BytesIO()
    work.save(buf, format="JPEG", quality=IMG_JPEG_Q, optimize=True)
    data = buf.getvalue()
    return base64.b64encode(data).decode("ascii"), "image/jpeg", {
        "fmt": fmt, "orig_wh": (w0, h0), "out_wh": (nw, nh),
        "is_long": is_long, "cropped": cropped, "bytes": len(data), "frames": n_frames,
    }


def _view_image(file_id, region="top"):
    region = "full" if str(region).strip().lower() == "full" else "top"
    p = _resolve_image(file_id)
    if not p:
        return {"content": [{"type": "text", "text": (
            f"本地归档里没有这张图片（file_id={file_id}）。可能是这张图的副本没有被保存下来。"
            "请照实告诉群友你看不到这张图，不要凭空推测画面内容。")}], "isError": False}
    try:
        size = os.path.getsize(p)
        if size > IMG_MAX_FILESIZE:
            return {"content": [{"type": "text", "text": (
                f"这张图体积过大（{size // 1024 // 1024} MB），超出可取范围。")}], "isError": False}
        b64, mime, meta = _preprocess_image(p, region=region)
    except Exception as e:  # noqa: BLE001
        _log(f"view_image 处理失败 {p}: {e}")
        return {"content": [{"type": "text", "text": (
            f"读取这张图片时出错（{type(e).__name__}）。请照实说明无法查看，不要凭空推测内容。")}],
            "isError": False}

    w0, h0 = meta["orig_wh"]
    nw, nh = meta["out_wh"]
    notes = [f"图片 {os.path.basename(p)}（原图 {w0}×{h0}，"
             f"{meta['fmt']}{'，' + str(meta['frames']) + ' 帧取首帧' if meta['frames'] > 1 else ''}）。"]
    if (nw, nh) != (w0, h0):
        notes.append(f"为控制体积已缩放为 {nw}×{nh}。")
    if meta["cropped"]:
        notes.append(f"⚠️ 这是**超长图**，只返回了**顶部 {nh}px**（原图高 {h0}px）。"
                     "若你需要的文字在下方，说明你看不到下面部分——不要编造。")
    elif meta["is_long"]:
        notes.append("这是一张超长图，已按可读宽度缩放并完整返回。")
    notes.append(f"（{meta['bytes'] // 1024} KB）")
    return {
        "content": [
            {"type": "text", "text": "".join(notes)},
            {"type": "image", "data": b64, "mimeType": mime},
        ]
    }


# ==================== trace_image：SauceNAO 以图搜图（2026-09-13）====================

_sauce_last_ts = 0.0          # 上次请求时间（限流）
_sauce_fail_streak = 0        # 连续失败计数（熔断）
_sauce_break_until = 0.0      # 熔断截止时间


def _saucenao_key():
    """取 SauceNAO API key：环境变量 → 本地文件。都没有返回 ''。"""
    k = (os.getenv("SAUCENAO_API_KEY") or "").strip()
    if k:
        return k
    try:
        with open(SAUCENAO_KEY_FILE, encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    return ln
    except Exception:  # noqa: BLE001
        pass
    return ""


def _classify_sauce_error(msg):
    """把 SauceNAO 的业务错误消息归类 → 'auth' | 'quota' | 'other'。

    ⚠️ 2026-09-13 踩坑修正：最初 `_save_quota` 用 `status<0 且有 message` 就判"额度耗尽"，
       结果**把 key 无效也判成了额度耗尽** —— 而假"额度用尽"会让模型长期拒绝使用溯源
       （除非出现一次成功调用才清除），比"不提示"危害大得多。
       → 必须按**消息内容**区分，与 _trace_image 里的判定用同一个函数，避免两处漂移。
    """
    low = str(msg or "").lower()
    if not low:
        return "other"
    # 认证类：API key 缺失/无效（实测文案 "The anonymous account type does not permit API usage."）
    if "anonymous" in low or "api usage" in low or ("key" in low and
                                                    ("invalid" in low or "not permit" in low)):
        return "auth"
    # 额度类
    if (("search" in low and ("limit" in low or "out of" in low))
            or "quota" in low or "too many" in low or "out of searches" in low
            or "limit" in low):
        return "quota"
    return "other"


def _save_quota(hdr, status=None, message=""):
    """把本次响应的额度快照写盘，供 watcher 读取（2026-09-13）。

    ⚠️ 四条纪律：
      ① **绝不能让落盘失败影响工具本身** —— 全函数 try/except 兜住，只记 stderr 日志。
      ② **必须原子写**（临时文件 + os.replace）—— watcher 是另一个进程，会在任意时刻读，
         直接覆盖可能出现半截 JSON。
      ③ **必须写时间戳** —— 这是"上一次查询时的快照"，不是实时值；读方据此判断新鲜度。
      ④ **`exhausted` 只认额度类错误** —— 见 _classify_sauce_error 的踩坑说明。
    """
    if not isinstance(hdr, dict) or not hdr:
        return
    try:
        import datetime
        import time

        def _int(v):
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        lr = _int(hdr.get("long_remaining"))
        kind = _classify_sauce_error(message) if message else "other"
        # status<0 也可能是"图片有问题"等无关错误；只有 quota 类才算额度耗尽。
        # 另外 long_remaining<=0 是额度耗尽的**直接证据**，优先采信。
        exhausted = (lr is not None and lr <= 0) or (kind == "quota")
        rec = {
            "long_remaining": lr,
            "long_limit": _int(hdr.get("long_limit")),
            "short_remaining": _int(hdr.get("short_remaining")),
            "short_limit": _int(hdr.get("short_limit")),
            "status": status,
            "message": str(message or ""),
            "error_kind": kind,
            "exhausted": bool(exhausted),
            # key 无效 ≠ 额度耗尽：单独标记，两者对模型/维护者的含义完全不同
            "auth_error": bool(kind == "auth"),
            "updated_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "updated_ts": time.time(),
        }
        d = os.path.dirname(SAUCENAO_QUOTA_FILE)
        if d and not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        tmp = SAUCENAO_QUOTA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=1)
        os.replace(tmp, SAUCENAO_QUOTA_FILE)
    except Exception as e:  # noqa: BLE001
        _log(f"额度快照落盘失败（不影响工具）: {type(e).__name__}: {e}")


def _fmt_trace(payload, file_id, elapsed):
    """把 SauceNAO 返回的 JSON 整理成给模型看的文本。

    相似度判定（2026-09-13 依官方文档重写）：
      官方在 header 给 `minimum_similarity`，并说明结果 similarity 低于它即
      「unlikely，正常情况下会被隐藏」→ 用它作为**官方判定线**，
      不再用原先拍脑袋的硬编码阈值做过滤。
      展示时按量级分档，把判断依据交给模型（配合 prompt 里的措辞纪律）。
    """
    h = payload.get("header") or {}
    results = payload.get("results") or []

    # 官方判定线。取不到时退化为兜底值（不是 0 —— 见 SAUCENAO_SIM_FALLBACK 注释）
    min_sim = SAUCENAO_SIM_FALLBACK
    try:
        _ms = h.get("minimum_similarity")
        if _ms is not None:
            min_sim = float(_ms)
    except (TypeError, ValueError):
        pass
    # 可选的人工下限（默认 0 = 不启用）
    cutoff = max(min_sim, SAUCENAO_SIM_FLOOR)

    # 额度提示（免费账号额度很小，实测 long_limit=100/24h、short_limit=4/30s）
    quota_note = ""
    try:
        lr = h.get("long_remaining")
        ll = h.get("long_limit")
        if lr is not None and int(lr) <= SAUCENAO_LOW_QUOTA:
            quota_note = (f"（注意：溯源额度将尽，24 小时剩余 {lr}/{ll} 次，"
                          "请只在群友明确要求查来源时使用）")
    except (TypeError, ValueError):
        pass

    if not results:
        return ("没查到这张图的出处。可能是原创图、截图、或不在 SauceNAO 收录范围内"
                "（它主攻动漫/插画类）。照实告诉群友「没找到确切出处」，不要猜。"
                + quota_note)

    picked = []
    for r in results:
        rh = r.get("header") or {}
        rd = r.get("data") or {}
        # R18 过滤（双保险）：
        #   ① hide=3 已让 hidden 字段有意义 → 非 0 表示该站点不会展示此文件，跳过
        #   ② 补充按来源库名兜一层（防 hide 参数行为变更；目标是群聊场景）
        if rh.get("hidden"):
            continue
        iname = str(rh.get("index_name") or "").lower()
        if any(k in iname for k in ("e-hentai", "exhentai", "nhentai", "h-misc")):
            continue
        try:
            sim = float(rh.get("similarity") or 0)
        except (TypeError, ValueError):
            sim = 0.0
        if sim < cutoff:
            continue
        picked.append((sim, rh, rd))

    if not picked:
        return ("查到了候选但相似度都低于 SauceNAO 的判定线（%.1f%%），不足以确认出处。"
                "照实说「可能查不到确切出处」，**不要**把低相似度结果当成事实。%s"
                % (cutoff, quota_note))

    picked.sort(key=lambda x: -x[0])
    lines = [f"SauceNAO 以图搜图结果（{len(picked)} 条达到判定线 %.1f%%，耗时 %.1fs）："
             % (cutoff, elapsed)]
    for i, (sim, rh, rd) in enumerate(picked[:4], 1):
        tag = "高可信" if sim >= SAUCENAO_SIM_CONFIDENT else "偏低(仅供参考)"
        bits = [f"相似度 {sim:.1f}% [{tag}]"]
        for label, keys in (("作品", ("material", "source", "eng_name", "jp_name")),
                            ("角色", ("characters",)),
                            ("标题", ("title",)),
                            ("作者", ("author_name", "creator", "member_name"))):
            for k in keys:
                v = rd.get(k)
                if v:
                    if isinstance(v, (list, tuple)):
                        v = " / ".join(str(x) for x in v if x)
                    if v:
                        bits.append(f"{label}={v}")
                    break
        if rh.get("index_name"):
            bits.append(f"来源库={rh['index_name']}")
        lines.append(f"{i}. " + " ｜ ".join(str(b) for b in bits))
        urls = rd.get("ext_urls") or []
        if urls:
            lines.append(f"   原图页：{urls[0]}")

    top = picked[0][0]
    if top >= SAUCENAO_SIM_CONFIDENT:
        lines.append("提示：最高相似度已达高可信档，可以据此回答，但仍以「作品/角色」"
                     "字段实际有值为准；字段为空说明该来源库不提供，只能给链接。")
    else:
        lines.append("提示：**没有一条达到高可信档（≥%.0f%%）** —— 只能措辞为"
                     "「可能/像是」，不要断言出处，也不要编造角色名。" % SAUCENAO_SIM_CONFIDENT)
    if quota_note:
        lines.append(quota_note)
    return "\n".join(lines)


def _trace_image(file_id):
    """file_id → 本地图 → SauceNAO 反查 → 文本结果。"""
    global _sauce_last_ts, _sauce_fail_streak, _sauce_break_until
    import time
    import urllib.request
    import uuid

    key = _saucenao_key()
    if not key:
        return {"content": [{"type": "text", "text": (
            "以图搜图功能尚未配置（缺 SauceNAO API key）。"
            "请在 https://saucenao.com/user.php?page=search-api 免费注册获取 key，"
            "写入 <repo>/saucenao_key.txt（或设 SAUCENAO_API_KEY 环境变量）。"
            "现在请照实告诉群友：这个功能还没开。")}], "isError": False}

    p = _resolve_image(file_id)
    if not p:
        return {"content": [{"type": "text", "text": (
            f"本地没有这张图的副本（{_clean_file_id(file_id)}），无法溯源。")}], "isError": False}

    now = time.time()
    if now < _sauce_break_until:
        return {"content": [{"type": "text", "text": (
            f"溯源服务暂时不可用（连续失败已熔断，约 {int(_sauce_break_until - now)} 秒后恢复）。"
            "请照实说明暂时查不了。")}], "isError": False}
    wait = SAUCENAO_MIN_INTERVAL - (now - _sauce_last_ts)
    if wait > 0:
        return {"content": [{"type": "text", "text": (
            f"溯源有频率限制，请 {int(wait) + 1} 秒后再试（同一张图不必重复查）。")}], "isError": False}

    size = os.path.getsize(p)
    if size > IMG_MAX_FILESIZE:
        return {"content": [{"type": "text", "text": (
            f"这张图体积过大（{size // 1024 // 1024} MB），超出可上传范围。")}], "isError": False}

    boundary = uuid.uuid4().hex
    try:
        with open(p, "rb") as f:
            raw = f.read()
    except Exception as e:  # noqa: BLE001
        _log(f"trace_image 读图失败 {p}: {e}")
        return {"content": [{"type": "text", "text": "读取本地图片失败。"}], "isError": False}

    def field(k, v):
        return (f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n").encode()

    body = b"".join([
        field("api_key", key),
        field("output_type", "2"),
        field("db", SAUCENAO_DB),
        field("hide", str(SAUCENAO_HIDE)),
        field("dedupe", str(SAUCENAO_DEDUPE)),
        field("numres", str(SAUCENAO_NUMRES)),
        (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
         f"filename=\"{os.path.basename(p)}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
         ).encode() + raw + b"\r\n",
        f"--{boundary}--\r\n".encode(),
    ])

    _sauce_last_ts = now
    t0 = time.time()
    txt = ""
    try:
        req = urllib.request.Request(SAUCENAO_URL, data=body, headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=SAUCENAO_TIMEOUT) as r:
            txt = r.read().decode("utf-8", "replace")
        payload = json.loads(txt)
    except urllib.error.HTTPError as e:
        # ⚠️ 2026-09-13 实测：SauceNAO 的**业务错误也是 HTTP 403 + JSON body**，例如
        #   {"header":{"status":-1,"message":"The anonymous account type does not permit API usage."}}
        #   → 绝不能当成"网络失败"一律吞掉，否则 key 无效/额度耗尽时无法定位。
        #   区分三种 403：① 业务 JSON（key/额度问题）② Cloudflare 挑战（HTML）③ 其它
        try:
            txt = e.read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            txt = ""
        payload = None
        try:
            payload = json.loads(txt)
        except Exception:  # noqa: BLE001
            payload = None
        if isinstance(payload, dict) and isinstance(payload.get("header"), dict):
            msg = payload["header"].get("message") or "未知业务错误"
            kind = _classify_sauce_error(msg)
            _log(f"trace_image 业务错误(HTTP {e.code}): [{kind}] {msg}")
            # 额度快照落盘：这一支必须写，否则"额度耗尽"这个最关键的状态读不到
            _save_quota(payload["header"],
                        status=payload["header"].get("status"), message=msg)
            if kind == "auth":
                hint = ("（原因：API key 无效或未生效 —— 请确认 saucenao_key.txt 里是"
                        "注册后获得的 API key，不是用户名/密码）")
            elif kind == "quota":
                hint = "（原因：查询额度已用完，需等待额度恢复）"
            else:
                hint = ""
            return {"content": [{"type": "text", "text": (
                f"溯源服务拒绝请求：{msg}{hint}。请照实说明暂时查不了，不要编造出处。")}],
                "isError": False}
        if "just a moment" in txt.lower() or "cloudflare" in txt.lower():
            _log(f"trace_image 被 Cloudflare 挑战拦截 (HTTP {e.code})")
            return {"content": [{"type": "text", "text": (
                "溯源服务被反爬拦截（Cloudflare 挑战），暂时查不了。"
                "请照实说明，不要编造出处。")}], "isError": False}
        _sauce_fail_streak += 1
        if _sauce_fail_streak >= SAUCENAO_BREAK_AFTER:
            _sauce_break_until = time.time() + SAUCENAO_BREAK_SECONDS
            _sauce_fail_streak = 0
        _log(f"trace_image HTTP {e.code}: {txt[:200]}")
        return {"content": [{"type": "text", "text": (
            f"溯源请求失败（HTTP {e.code}）。请照实说明暂时查不了，不要编造出处。")}],
            "isError": False}
    except Exception as e:  # noqa: BLE001
        _sauce_fail_streak += 1
        if _sauce_fail_streak >= SAUCENAO_BREAK_AFTER:
            _sauce_break_until = time.time() + SAUCENAO_BREAK_SECONDS
            _sauce_fail_streak = 0
        _log(f"trace_image 失败 {file_id}: {type(e).__name__}: {e}")
        return {"content": [{"type": "text", "text": (
            f"溯源请求失败（{type(e).__name__}）。请照实说明暂时查不了，不要编造出处。")}],
            "isError": False}

    _sauce_fail_streak = 0
    # 业务状态码（官方文档）：
    #   status == 0  成功
    #   status  > 0  服务端错误（failed descriptor gen / failed query 等）→ 可重试，计入熔断
    #   status  < 0  客户端错误（bad image / out of searches 额度耗尽 等）→ 重试无用，不计熔断
    #   注意原文：「Not all errors are properly tagged yet, and some may exit the api.」
    #   → 不能只依赖 status，下面文字也要兜。
    hdr = payload.get("header") or {}
    try:
        st = int(hdr.get("status"))
    except (TypeError, ValueError):
        st = 0
    if st != 0:
        msg = str(hdr.get("message") or "未知错误")
        kind = _classify_sauce_error(msg)      # 'auth' | 'quota' | 'other'（与 _save_quota 同源）
        quota = (kind == "quota")
        _log(f"trace_image 业务错误 status={st}: [{kind}] {msg}")
        # 额度快照落盘（含"额度耗尽"这一支 —— watcher 靠它注入提示）
        _save_quota(hdr, status=st, message=msg if quota else "")
        if st < 0 and quota:
            # 额度耗尽：最常见的客户端错误，给出可操作提示
            left = hdr.get("long_remaining")
            return {"content": [{"type": "text", "text": (
                f"溯源额度已用完（{msg}；24 小时剩余 {left} 次）。"
                "请照实告诉群友今天查不了图了，**不要编造出处**。")}], "isError": False}
        if st < 0:
            return {"content": [{"type": "text", "text": (
                f"这张图无法溯源（{msg}）——多半是格式/内容问题，不是服务故障。"
                "请照实说明查不到，不要编造出处。")}], "isError": False}
        _sauce_fail_streak += 1
        if _sauce_fail_streak >= SAUCENAO_BREAK_AFTER:
            _sauce_break_until = time.time() + SAUCENAO_BREAK_SECONDS
            _sauce_fail_streak = 0
        return {"content": [{"type": "text", "text": (
            f"溯源服务返回错误：{msg}。请照实说明暂时查不了，不要编造出处。")}],
            "isError": False}

    # status == 0：成功。落盘额度快照（watcher 靠它判断"是否该提醒/是否该注入提示"）
    _save_quota(hdr, status=0)
    return {"content": [{"type": "text",
                         "text": _fmt_trace(payload, file_id, time.time() - t0)}]}


def call_tool(name, args):
    if name == "search_lyrics":
        if lyrics_search is None:
            return {"content": [{"type": "text", "text": "歌词库未配置（可选模块 lyrics_search.py 不存在）"}], "isError": False}
        query = str(args.get("query", "")).strip()
        if not query:
            return {"content": [{"type": "text", "text": "query 不能为空"}], "isError": True}
        ip = args.get("ip") or None
        limit = args.get("limit") or 3
        try:
            limit = max(1, min(10, int(limit)))
        except (TypeError, ValueError):
            limit = 3
        try:
            hits = lyrics_search.search(query, ip=ip, limit=limit)
        except Exception as e:
            _log(f"歌词检索异常: {e}")
            return {"content": [{"type": "text", "text": f"检索失败: {e}"}], "isError": True}
        return {"content": [{"type": "text", "text": fmt_lyrics_hits(hits)}]}

    if name == "search_theory":
        query = str(args.get("query", "")).strip()
        if not query:
            return {"content": [{"type": "text", "text": "query 不能为空"}], "isError": True}
        kb = args.get("kb") or None
        if kb not in (None, "", "theory", "writing"):
            kb = None
        limit = args.get("limit") or 3
        try:
            limit = max(1, min(8, int(limit)))
        except (TypeError, ValueError):
            limit = 3
        try:
            hits = kb_search.search(query, kb=kb, limit=limit)
        except Exception as e:
            _log(f"知识检索异常: {e}")
            return {"content": [{"type": "text", "text": f"检索失败: {e}"}], "isError": True}
        # 命中含 chapter 标签时，附加同章词条聚合（回答"哪些词条属于这个 chapter"）
        peers, peer_chapter = None, ""
        for h in hits:
            if h["kind"] == "chapter":
                peers = kb_search.same_chapter_entries(h["entry"], kb=kb)
                peer_chapter = h["entry"].get("chapter", "")
                break
        return {"content": [{"type": "text",
                             "text": fmt_theory_hits(hits, peers=peers, peer_chapter=peer_chapter)}]}

    if name == "list_sources":
        kb = args.get("kb") or None
        if kb not in (None, "", "theory", "writing"):
            kb = None
        component = args.get("component") or None
        try:
            sources = kb_search.list_sources(kb=kb, component=component)
        except Exception as e:
            _log(f"list_sources 异常: {e}")
            return {"content": [{"type": "text", "text": f"读取失败: {e}"}], "isError": True}
        return {"content": [{"type": "text", "text": fmt_sources_summary(sources)}]}

    if name == "read_source":
        query = str(args.get("query", "")).strip()
        file_ = str(args.get("file", "")).strip()
        component = args.get("component") or None
        if not query and not file_:
            return {"content": [{"type": "text",
                                 "text": "query 与 file 至少提供一个"}], "isError": True}
        try:
            sources = kb_search.list_sources(kb=None, component=component)
            src = None
            if file_:
                matched = [s for s in sources if s["file"] == file_]
                if not matched:
                    return {"content": [{"type": "text", "text": f"未找到文件 {file_}（可用 list_sources 查看清单）"}], "isError": True}
                src = matched[0]
                text, err = kb_search.read_chapter(src)
            else:
                hits = kb_search.resolve_source(query, kb=None, limit=8)
                hits = [h for h in hits if not component or h[1]["component"] == component]
                if not hits:
                    return {"content": [{"type": "text",
                                         "text": f"未在原著原文中找到「{query}」相关章节（知识库条目检索可继续用 search_theory；或用 list_sources 查看可读章节）"}], "isError": False}
                top = hits[0][0]
                best = [h for h in hits if h[0] == top]
                if len(best) > 1:
                    return {"content": [{"type": "text",
                                         "text": fmt_source_candidates(hits)}], "isError": False}
                src = best[0][1]
                text, err = kb_search.read_chapter(src)
            if err:
                return {"content": [{"type": "text", "text": err}], "isError": True}
            head = f"【{src['component_cn']} · {src['file']}】\n\n"
            return {"content": [{"type": "text", "text": head + text}]}
        except Exception as e:
            _log(f"read_source 异常: {e}")
            return {"content": [{"type": "text", "text": f"读取失败: {e}"}], "isError": True}

    if name == "view_image":
        file_id = str(args.get("file_id", "")).strip()
        if not file_id:
            return {"content": [{"type": "text",
                                 "text": "file_id 不能为空（从「图片清单」里原样复制）"}], "isError": True}
        return _view_image(file_id, region=args.get("region") or "top")

    if name == "trace_image":
        file_id = str(args.get("file_id", "")).strip()
        if not file_id:
            return {"content": [{"type": "text",
                                 "text": "file_id 不能为空（从「图片清单」里原样复制）"}], "isError": True}
        return _trace_image(file_id)

    return {"content": [{"type": "text", "text": f"未知工具: {name}"}], "isError": True}


# ==================== JSON-RPC 循环 ====================

def reply(msg_id, result):
    # flush=True 必须有：write_through 只旁路 TextIOWrapper 缓冲，
    # 底层 BufferedWriter（8KB）仍会扣住响应 → 客户端握手超时（2026-09-06 实测踩坑）
    print(json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": result}, ensure_ascii=False),
          flush=True)


def reply_error(msg_id, code, message):
    print(json.dumps({"jsonrpc": "2.0", "id": msg_id,
                      "error": {"code": code, "message": message}}, ensure_ascii=False),
          flush=True)


def main():
    _log(f"启动（{SERVER_NAME} v{SERVER_VERSION}，工具: {[t['name'] for t in TOOLS]}）")
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as e:
            _log(f"无法解析的消息: {e}: {line[:120]}")
            continue

        method = msg.get("method", "")
        msg_id = msg.get("id")

        if msg_id is None:
            continue

        if method == "initialize":
            pv = (msg.get("params") or {}).get("protocolVersion", "2024-11-05")
            reply(msg_id, {
                "protocolVersion": pv,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            })
        elif method == "tools/list":
            reply(msg_id, {"tools": TOOLS})
        elif method == "tools/call":
            params = msg.get("params") or {}
            reply(msg_id, call_tool(params.get("name", ""), params.get("arguments") or {}))
        elif method == "ping":
            reply(msg_id, {})
        else:
            reply_error(msg_id, -32601, f"Method not found: {method}")
    _log("stdin 关闭，退出")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        _log(f"致命错误: {e}")
        sys.exit(1)
