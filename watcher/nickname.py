#!/usr/bin/env python3
"""
昵称规范化工具 — 实体归一（多昵称 → 同一身份）
====================================================
读取 StandardUsers_{群号}.md 中的稳定称呼，
对任意 QQ 号返回应使用的稳定昵称。

用法:
  from nickname import normalize, load_standard_users
  nick = normalize("100000003", fallback="原始群名片")
  # → 稳定称呼（StandardUsers 中第一条）

群隔离（与 watcher 的 _group_or_global_file 同规则）：
  StandardUsers_{gid}.md 优先，缺失回退全局 StandardUsers.md

数据源优先级：名册 Members_{gid}.json 的 stable → StandardUsers → fallback。
名册由离线管线生成（合并平台实时名册 + 历史名片变体 + 人工字段）——
「一个人可能有多个名字、同一 QQ 号换名片仍是同一人」的实现本体。
"""

import os, re, json

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
BOTMEMORY_DIR = os.path.join(os.path.dirname(_MODULE_DIR), "BotMemory")
# 兼容旧名（部分脚本 import 该常量）；实际查找请用 _candidate_files()
STANDARD_USERS_FILE = os.path.join(BOTMEMORY_DIR, "StandardUsers.md")

# 缓存：{文件路径: (mtime, {qq: [称呼...]})}，避免每行消息都重读磁盘
_cache = {}


def _candidate_files(group_id=None):
    """按优先级返回候选文件：按群文件 → 全局文件（兼容两套命名）。"""
    names = []
    if group_id:
        names.append(f"StandardUsers_{group_id}.md")
    names.append("StandardUsers.md")
    return [os.path.join(BOTMEMORY_DIR, n) for n in names]


def _parse_file(path):
    """解析 StandardUsers 文件，返回 {qq: [稳定称呼列表]}"""
    result = {}
    if not os.path.exists(path):
        return result
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    blocks = re.split(r'\n## ', content)
    for block in blocks:
        if not block.strip():
            continue
        qq_m = re.search(r'(\d{6,})', block)
        if not qq_m:
            continue
        qq = qq_m.group(1)

        stable_m = re.search(r'稳定称呼:\s*(.+)', block)
        if stable_m:
            raw = stable_m.group(1).strip()
            names = [n.strip() for n in re.split(r'[/／、,]', raw) if n.strip()]
            if names:
                result[qq] = names
    return result


def load_standard_users(group_id=None):
    """解析 StandardUsers 文件，返回 {qq: [稳定称呼列表]}。
    group_id 给定时优先读按群文件；未命中再回退全局/其它按群文件。
    """
    merged = {}
    for path in _candidate_files(group_id):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        hit = _cache.get(path)
        if hit and hit[0] == mtime:
            data = hit[1]
        else:
            data = _parse_file(path)
            _cache[path] = (mtime, data)
        for qq, names in data.items():
            merged.setdefault(qq, names)

    # 兜底：若全局名不存在，扫描所有按群文件（多群部署下避免漏读）
    if not merged:
        try:
            import glob
            for path in glob.glob(os.path.join(BOTMEMORY_DIR, "StandardUsers_*.md")):
                try:
                    mtime = os.path.getmtime(path)
                except OSError:
                    continue
                hit = _cache.get(path)
                if hit and hit[0] == mtime:
                    data = hit[1]
                else:
                    data = _parse_file(path)
                    _cache[path] = (mtime, data)
                for qq, names in data.items():
                    merged.setdefault(qq, names)
        except Exception:
            pass
    return merged


_roster_cache = {}


def load_roster(group_id=None):
    """读名册 Members_{gid}.json（离线名册管线生成）。
    返回 {qq: {"stable": 稳定称呼, "aliases": [...], "identity": 身份}}；无则 {}。
    带 mtime 缓存：_clean_msg 会高频调用，避免反复读盘。
    """
    if not group_id:
        return {}
    path = os.path.join(BOTMEMORY_DIR, f"Members_{group_id}.json")
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return {}
    hit = _roster_cache.get(path)
    if hit and hit[0] == mtime:
        return hit[1]
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        members = data.get("members", {}) if isinstance(data, dict) else {}
    except Exception:
        members = {}
    _roster_cache[path] = (mtime, members)
    return members


def normalize(qq, fallback=None, group_id=None):
    """返回给定 QQ 的稳定昵称。

    优先级：名册 Members_{gid}.json 的 stable → StandardUsers 第一条 → fallback。
    """
    qq = str(qq)
    roster = load_roster(group_id)
    if qq in roster:
        stable = (roster[qq] or {}).get("stable")
        if stable:
            return stable

    users = load_standard_users(group_id)
    if qq in users and users[qq]:
        return users[qq][0]
    return fallback


def all_aliases(qq, group_id=None):
    """返回该 QQ 的所有已知别名（名册 aliases + StandardUsers 称呼），用于 @ 标记还原。"""
    qq = str(qq)
    out = []
    roster = load_roster(group_id)
    if qq in roster:
        out.extend((roster[qq] or {}).get("aliases") or [])
        stable = (roster[qq] or {}).get("stable")
        if stable:
            out.insert(0, stable)
    out.extend(load_standard_users(group_id).get(qq) or [])
    seen, uniq = set(), []
    for a in out:
        a = str(a).strip()
        if a and a not in seen:
            seen.add(a)
            uniq.append(a)
    return uniq


def normalize_all(grouped_data, group_id=None):
    """批量规范化：输入 {qq: [messages]}, 返回 {qq: 稳定称呼 或 None}"""
    users = load_standard_users(group_id)
    qq_to_stable = {}
    for qq in grouped_data:
        if qq in users and users[qq]:
            qq_to_stable[qq] = users[qq][0]
        else:
            qq_to_stable[qq] = None
    return qq_to_stable


if __name__ == "__main__":
    import sys
    gid = None
    args = sys.argv[1:]
    # 支持 --group <gid>
    if "--group" in args:
        i = args.index("--group")
        gid = args[i + 1]
        del args[i:i + 2]

    if args:
        qq = args[0]
        fallback = args[1] if len(args) >= 2 else None
        nick = normalize(qq, fallback, group_id=gid)
        if nick:
            print(nick)
        else:
            print(f"⚠️  {qq} 不在任何 StandardUsers/名册中")
            sys.exit(1)
    else:
        users = load_standard_users(gid)
        print(f"已加载 {len(users)} 个条目 (group_id={gid or '全局'}):")
        for qq, names in sorted(users.items()):
            print(f"  {qq}: {', '.join(names)}")
        roster = load_roster(gid)
        if roster:
            print(f"\n名册 Members_{gid}.json: {len(roster)} 人")
