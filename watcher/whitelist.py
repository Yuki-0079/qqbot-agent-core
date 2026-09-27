#!/usr/bin/env python3
"""
响应白名单（群 + 用户）共享模块
========================================
配置文件: bot_whitelist.json（默认在仓库根目录），支持热更新（60s 缓存）。

{
  "allowed_groups": ["100000001"],   # 群白名单：只在这些群响应 @；空数组 = 不响应任何群（fail-closed）
  "allowed_users":  []               # 用户白名单：空 = 白名单群内所有成员都可触发；非空 = 仅这些号可触发
}
（示例中的号码为演示占位值，非真实号码。）

安全默认（fail-closed）：配置文件缺失/解析失败 → 视为空白名单 → 任何群都不响应。
宁可 bot 哑火也不在非授权群回复，避免误响应风险。
"""

import json
import os
import time

WHITELIST_FILE = os.getenv(
    "BOT_WHITELIST_FILE",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bot_whitelist.json"),
)
REFRESH_SEC = int(os.getenv("BOT_WHITELIST_REFRESH", "60"))

_cache = {"ts": 0.0, "data": None}


def _load():
    """读取白名单配置（带 60s 缓存）。失败 → 空白名单（fail-closed）。"""
    now = time.time()
    if _cache["data"] is not None and now - _cache["ts"] < REFRESH_SEC:
        return _cache["data"]

    data = {"allowed_groups": [], "allowed_users": []}
    try:
        with open(WHITELIST_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        data["allowed_groups"] = [str(g) for g in raw.get("allowed_groups", [])]
        data["allowed_users"] = [str(u) for u in raw.get("allowed_users", [])]
    except FileNotFoundError:
        pass  # fail-closed
    except Exception:
        pass  # 文件损坏 → fail-closed

    _cache["ts"] = now
    _cache["data"] = data
    return data


def is_group_allowed(group_id):
    """群是否在白名单内。空白名单 = 不允许任何群（fail-closed）。"""
    groups = _load()["allowed_groups"]
    return bool(groups) and str(group_id) in groups


def is_user_allowed(user_id):
    """用户是否可触发。用户白名单为空 = 不限制（群白名单内的成员均可触发）。"""
    users = _load()["allowed_users"]
    return not users or str(user_id) in users


def allowed_groups():
    """当前群白名单列表（用于日志/巡检）。"""
    return _load()["allowed_groups"]


def allowed_users():
    """当前用户白名单列表（用于日志/巡检）。"""
    return _load()["allowed_users"]
