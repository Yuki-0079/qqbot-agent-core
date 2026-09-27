# -*- coding: utf-8 -*-
"""【示例】凭据外部注入 —— 真实平台适配器如何加载 token。

原则：凭据**只走外部注入**，任何脚本不得硬编码：
    1) 环境变量 NAPCAT_TOKEN（优先）
    2) 同目录 napcat_token.txt（一行，纯值，不带引号）

真实适配器（transport_napcat.py 之类）不随本仓库提供；接入时：
    from example_napcat_auth import load_napcat_token
    NAPCAT_TOKEN = load_napcat_token()

本文件不含任何真实凭据；请确保 token 文件与含凭据的文件不进版本库（见 .gitignore）。

路径解析基于本文件所在目录，故与调用方的 cwd 无关。
"""
import os

_TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "napcat_token.txt")


def load_napcat_token() -> str:
    """返回 NapCat token；两处来源都没有时返回空串（由调用方决定是否告警）。"""
    v = (os.getenv("NAPCAT_TOKEN") or "").strip()
    if v:
        return v
    try:
        with open(_TOKEN_FILE, "r", encoding="utf-8") as f:
            return f.readline().strip()
    except OSError:
        return ""


if __name__ == "__main__":
    _t = load_napcat_token()
    print(f"token file : {_TOKEN_FILE}")
    print(f"resolved   : {'OK' if _t else 'MISSING'} (len={len(_t)})")
