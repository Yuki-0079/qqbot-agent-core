#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识库检索库
==================================
数据源（只读）：$BOTKB_KB_DIR（默认 <botkb>/sample_kb）下各组件目录的
  references/knowledge-base.json

数据形态：每个组件一个 JSON 数组，条目字段：
  id / domain / chapter / heading / concepts[] / text / confidence / scope_note /
  critique_application

匹配策略（知识条目是离散概念，与歌词检索相反）：
  ① query 完整包含某 concept / heading（"什么是XX" → 命中概念名）→ 强信号
  ② query 本身就是某 concept / heading（精确或子串）→ 强信号
  ③ chapter 章节归属命中（query 是章节标题或其片段）→ 中强信号
  ④ text 正文子串命中（query 是原文引用片段）→ 中信号
  ⑤ scope_note / critique_application 命中 → 弱信号（适用场景级）

用法（库）:
    from kb_search import search, load_kbs
    hits = search("检索评分", kb="theory")

用法（命令行）:
    python kb_search.py "检索评分"
    python kb_search.py --stats
"""
import glob
import json
import os
import re
import sys
from datetime import datetime

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(os.path.dirname(MODULE_DIR))
KB_DIR = os.getenv("BOTKB_KB_DIR", os.path.join(MODULE_DIR, "sample_kb"))

# 知识库 → 组件根目录。新增一个库 = 加一行
# （组件目录下放 references/knowledge-base.json）
KB_ROOTS = {
    "theory": os.path.join(KB_DIR, "theory"),
    "writing": os.path.join(KB_DIR, "writing"),
}

# 特殊源（文件布局与标准组件不同，单独注册）：
#   示例：本仓库在 writing 库附赠一个「单文件源」（无组件目录层级，
#   条目无 heading、用 concepts[0] 兜底，certainty 字段 ≈ confidence）
EXTRA_FILES = {
    "writing": [
        ("writing", "writing-single", "写作示例（单文件源）",
         os.path.join(KB_DIR, "writing-single.json")),
    ],
}

_PUNCT = re.compile(
    r"[\s　\r\n\t,.\-_/\\!?！？…~～·、。，．'\"“”‘’()（）\[\]【】<>《》♪♡♥:：;；？?]+"
)


def norm(s):
    if not s:
        return ""
    return _PUNCT.sub("", str(s)).lower()


# ============ 加载（懒加载 + mtime 刷新）============
_cache = {"mtime": None, "entries": None}


def _component_cn(component_dir):
    """从组件 SKILL.md frontmatter description 提取中文名（优先取《…》书名）。"""
    skill_md = os.path.join(component_dir, "SKILL.md")
    try:
        with open(skill_md, encoding="utf-8") as f:
            head = f.read(2000)
        m = re.search(r"description:\s*>-?\s*\n((?:\s+.*\n)+)", head)
        desc = m.group(1) if m else ""
        if not desc:
            m2 = re.search(r"description:\s*(.+)", head)
            desc = m2.group(1) if m2 else ""
        books = re.findall(r"《(.+?)》", desc)
        if books:
            return books[0], desc.strip().splitlines()[0].strip()
        return "", desc.strip().splitlines()[0].strip() if desc.strip() else ""
    except Exception:
        return "", ""


def load_kbs(force=False):
    """加载全部知识条目。返回 [{"kb","component","component_cn","id","chapter",
    "heading","concepts","text","scope_note","critique_application"}, ...]"""
    all_mtime = 0
    paths = []
    for kb, root in KB_ROOTS.items():
        for p in glob.glob(os.path.join(root, "*", "references", "knowledge-base.json")):
            paths.append((kb, p))
            try:
                all_mtime = max(all_mtime, os.path.getmtime(p))
            except OSError:
                pass
    for kb, comp, cn_fixed, p in EXTRA_FILES.get("writing", []):
        if os.path.exists(p):
            paths.append((kb, p, comp, cn_fixed))
            try:
                all_mtime = max(all_mtime, os.path.getmtime(p))
            except OSError:
                pass
    if not paths:
        return []
    if not force and _cache["entries"] is not None and _cache["mtime"] == all_mtime:
        return _cache["entries"]

    entries = []
    for item in paths:
        if len(item) == 4:
            kb, path, fixed_comp, fixed_cn = item
            component, cn, desc = fixed_comp, fixed_cn, ""
        else:
            kb, path = item
            comp_dir = os.path.dirname(os.path.dirname(path))
            component = os.path.basename(comp_dir)
            cn, desc = _component_cn(comp_dir)
        try:
            with open(path, encoding="utf-8") as f:
                items = json.load(f)
        except Exception as e:
            print(f"[kb_search] 加载失败 {path}: {e}", file=sys.stderr)
            continue
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            concepts = it.get("concepts", []) or []
            entries.append({
                "kb": kb,
                "component": component,
                "component_cn": cn or component,
                "component_desc": desc,
                "id": it.get("id", ""),
                "chapter": it.get("chapter", ""),
                # 无 heading 的条目 → 用首个概念/章节兜底
                "heading": it.get("heading", "") or (concepts[0] if concepts else it.get("chapter", "")),
                "concepts": concepts,
                "text": it.get("text", "") or "",
                "scope_note": it.get("scope_note", "") or "",
                "critique_application": it.get("critique_application", "") or "",
                "confidence": it.get("confidence", "") or it.get("certainty", ""),
            })

    # 预归一化
    for e in entries:
        e["heading_n"] = norm(e["heading"])
        e["chapter_n"] = norm(e["chapter"])
        e["concepts_n"] = [norm(c) for c in e["concepts"] if c]
        e["text_n"] = norm(e["text"])
        e["scope_n"] = norm(e["scope_note"] + " " + e["critique_application"])

    _cache["entries"] = entries
    _cache["mtime"] = all_mtime
    return entries


# ============ 评分 ============

def _score_entry(e, q, q_n):
    """返回 (score, kind) 或 None。

    知识检索与歌词相反的一半：query 往往是"提到概念名的一句话"，
    所以「条目的概念出现在 query 里」是主匹配方向。
    """
    if not q_n:
        return None
    best = None  # (score, kind)

    def consider(score, kind):
        nonlocal best
        if best is None or score > best[0]:
            best = (score, kind)

    # ① query 完整包含某概念名（query 是提到该概念的句子/问题）
    for c in e["concepts_n"]:
        if c and len(c) >= 2 and c in q_n:
            consider(90, "concept")
            break
    # ② query 完整包含 heading
    if e["heading_n"] and len(e["heading_n"]) >= 3 and e["heading_n"] in q_n:
        consider(88, "heading")

    # ③ query 本身就是概念名（精确/子串）
    if q_n in e["concepts_n"]:
        consider(95, "concept-exact")
    if e["heading_n"]:
        if q_n == e["heading_n"]:
            consider(95, "heading-exact")
        elif len(q_n) >= 2 and (q_n in e["heading_n"] or e["heading_n"] in q_n):
            consider(72, "heading")

    # ④ chapter 章节归属命中（搜章节标题 = 定位整章专题）。
    #    权重语义：低于 concept/heading（那是精确到条目的人工标注），
    #    但高于 text/scope——chapter 是"整章专题"的归属标签，定向性比正文
    #    偶然出现的词强。一条章内常有 2-4 条共享同章节名 → 同分并列，
    #    正好把整章资料都带出来（专题检索的预期行为）。
    if e["chapter_n"] and len(q_n) >= 2:
        if e["chapter_n"] in q_n:
            consider(86, "chapter")  # query 完整提到章节标题（如「第八章 官僚制的反生产」）
        elif q_n in e["chapter_n"] or e["chapter_n"] in q_n:
            consider(76, "chapter")  # query 是章节标题片段 / 双向包含

    # ⑤ 正文命中（query 是原文片段引用）
    if e["text_n"] and len(q_n) >= 4 and q_n in e["text_n"]:
        consider(60, "text")

    # ⑥ 适用场景命中
    if e["scope_n"] and len(q_n) >= 4 and q_n in e["scope_n"]:
        consider(55, "scope")

    return best


def search(query, kb=None, limit=5):
    """检索知识条目。

    query : 概念名 / 含概念的问题 / 原文片段
    kb    : "theory" | "writing" | None(全部)
    limit : 最多返回条数
    """
    entries = load_kbs()
    if not entries or not query:
        return []
    q_n = norm(query)
    if not q_n:
        return []

    scored = []
    for e in entries:
        if kb and e["kb"] != kb:
            continue
        r = _score_entry(e, query, q_n)
        if r:
            scored.append((r[0], r[1], e))
    scored.sort(key=lambda x: -x[0])

    out, seen = [], set()
    for score, kind, e in scored:
        if e["id"] in seen:
            continue
        seen.add(e["id"])
        out.append({"entry": e, "score": score, "kind": kind})
        if len(out) >= limit:
            break
    return out


# ============ chapter 同章聚合 ============

_CHAPTER_SPLIT = re.compile(r"[/／、,，;；|]+")


def chapter_tags(e):
    """把条目 chapter 字段拆成单章标签列表（原始串按分隔符切、逐段 norm）。

    跨章合写条目（如 '第七章 X / 第八章 Y'）会拆出多个标签——这类条目
    同时归属多个章节，聚合时任一标签命中即视为同章。
    """
    raw = str(e.get("chapter", "") or "")
    tags = []
    for seg in _CHAPTER_SPLIT.split(raw):
        seg_n = norm(seg)
        if len(seg_n) >= 2:
            tags.append(seg_n)
    return tags


def same_chapter_entries(e, kb=None, limit=20):
    """返回与 e 共享任一单章标签的库内其他条目（不含 e 自身）。

    用途：检索命中某章节标签后，把该章在库内收录的全部词条一并带出
    （'哪些词条属于这个 chapter'）。同书同章的 chapter 写法一致时
    标签精确相等即可命中；跨章合写条目通过拆分标签参与各章聚合。
    """
    tags = set(chapter_tags(e))
    if not tags:
        return []
    out = []
    for o in load_kbs():
        if kb and o["kb"] != kb:
            continue
        if o["id"] == e["id"]:
            continue
        if tags & set(chapter_tags(o)):
            out.append(o)
            if len(out) >= limit:
                break
    return out


# ============ 原著原文 chapters（read_source 数据源）============
# 约定：组件 references/chapters/*.txt，首行为标签行（多标签用 ' / ' 分隔，
# 对齐该组件 knowledge-base 的 chapter 字段写法），其后为原著正文（OCR/EPUB 提取）。
# 示例库：Component A 附带 chapters/ 演示 read_source。

_ch_cache = {"mtime": None, "items": None}


def load_chapters(force=False):
    """扫描各组件 references/chapters/*.txt。

    返回 [{"kb","component","component_cn","file","path","labels","labels_n","size"}, ...]
    """
    paths = []
    all_mtime = 0
    for kb, root in KB_ROOTS.items():
        for f in glob.glob(os.path.join(root, "*", "references", "chapters", "*.txt")):
            paths.append((kb, f))
            try:
                all_mtime = max(all_mtime, os.path.getmtime(f))
            except OSError:
                pass
    if not paths:
        return []
    if not force and _ch_cache["items"] is not None and _ch_cache["mtime"] == all_mtime:
        return _ch_cache["items"]

    items = []
    for kb, f in paths:
        comp_dir = os.path.dirname(os.path.dirname(os.path.dirname(f)))
        component = os.path.basename(comp_dir)
        cn, _ = _component_cn(comp_dir)
        try:
            with open(f, encoding="utf-8") as fh:
                head = fh.readline().strip()
            size = os.path.getsize(f)
        except OSError as e:
            print(f"[kb_search] 章节文件读取失败 {f}: {e}", file=sys.stderr)
            continue
        labels = [s.strip() for s in re.split(r"\s*/\s*", head) if s.strip()]
        items.append({
            "kb": kb,
            "component": component,
            "component_cn": cn or component,
            "file": os.path.basename(f),
            "path": f,
            "labels": labels,
            "labels_n": [norm(lb) for lb in labels if norm(lb) and len(norm(lb)) >= 2],
            "size": size,
        })
    _ch_cache["items"] = items
    _ch_cache["mtime"] = all_mtime
    return items


def list_sources(kb=None, component=None):
    """列出配置了原著原文（chapters/）的组件与章节文件清单。"""
    out = []
    for it in load_chapters():
        if kb and it["kb"] != kb:
            continue
        if component and it["component"] != component:
            continue
        out.append(it)
    return out


def resolve_source(query, kb=None, limit=8):
    """把查询（章名/标题/书内片段）解析到章节文件。

    匹配：query 与标签 norm 后双向子串/包含。权重：
      精确相等 100 / 标签⊂query（query 提到完整章节标签）95 /
      query⊂标签或双向片段 86（与知识条目 chapter 匹配语义一致）
    返回 [(score, src), ...] 降序。
    """
    q_n = norm(query or "")
    if not q_n:
        return []
    scored = []
    for src in load_chapters():
        if kb and src["kb"] != kb:
            continue
        best = None
        for lb in src["labels_n"]:
            if not lb:
                continue
            s = None
            if q_n == lb:
                s = 100
            elif len(lb) >= 2 and lb in q_n:
                s = 95
            elif len(q_n) >= 2 and (q_n in lb or lb in q_n):
                s = 86
            if s and (best is None or s > best):
                best = s
        if best:
            scored.append((best, src))
    scored.sort(key=lambda x: -x[0])
    return [(s, it) for s, it in scored[:limit]]


def read_chapter(src, max_chars=20000):
    """读章节文件全文（含首行标签）。超长截断并注明。返回 (text, err)。"""
    try:
        with open(src["path"], encoding="utf-8") as fh:
            text = fh.read()
    except OSError as e:
        return None, f"读取失败: {e}"
    if len(text) > max_chars:
        text = text[:max_chars] + f"\n…（原文共 {len(text)} 字符，已截断前 {max_chars}，可要求续读）"
    return text, None


def stats():
    entries = load_kbs()
    by_kb = {}
    for e in entries:
        by_kb.setdefault(e["kb"], set()).add(e["component"])
    total_chars = sum(len(e["text"]) for e in entries)
    print(f"组件 {sum(len(v) for v in by_kb.values())} 个 / 条目 {len(entries)} 条 / "
          f"正文 {total_chars:,} 字符")
    for kb, comps in sorted(by_kb.items()):
        n = sum(1 for e in entries if e["kb"] == kb)
        print(f"  [{kb}] {len(comps)} 组件 {n} 条: {', '.join(sorted(comps))}")


# ============ CLI ============

if __name__ == "__main__":
    args = sys.argv[1:]
    if "--stats" in args:
        stats()
        sys.exit(0)
    if not args:
        print(__doc__)
        sys.exit(2)
    query = " ".join(args)
    hits = search(query)
    print(f"查询: {query!r}  命中 {len(hits)} 条")
    print("=" * 70)
    for h in hits:
        e = h["entry"]
        print(f"■ {e['heading']}  （{e['component_cn']} · {e['kb']}）")
        print(f"  score={h['score']} kind={h['kind']} id={e['id']} 章节={e['chapter']}")
        if e["concepts"]:
            print(f"  概念: {'、'.join(e['concepts'][:6])}")
        print(f"  正文: {e['text'][:220]}…")
        if e["scope_note"]:
            print(f"  适用: {e['scope_note'][:150]}")
        print("-" * 70)
