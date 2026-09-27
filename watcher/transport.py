# -*- coding: utf-8 -*-
"""Transport —— 与具体 IM 平台解耦的消息收发接口。
============================================================
Agent 核心（watcher）只依赖本文件的 Transport 接口；消息从哪来、发到哪去
由具体适配器决定：

  · MockTransport（本文件）    —— 本地演示/测试：出站写入日志 + 打印，不连任何平台
  · 真实平台适配器（不随仓库）—— 如 NapCat/OneBot 适配器：实现同一接口，
    通过平台 HTTP API 收发消息。凭据注入示范见 example_napcat_auth.py。

接口约定（与 watcher 的实际调用点对齐）：
  send_group_msg(group_id, text) -> dict      出站：发群文本
  send_group_image(group_id, path) -> dict    出站：发群图片
  send_private_msg(user_id, text) -> dict     出站：发私聊文本
  fetch_image(file_id, dest_dir) -> str|None  媒体：把消息里的图取到本地
  get_login_info() -> dict                    账号：当前登录身份信息

返回值约定（与 OneBot 风格一致）：
  成功 = {"status": "ok", "data": {...}}；
  失败 = {"status": "failed", "error": "..."}（watcher 按 status 判断成败）。
"""
from __future__ import annotations

import json
import os
import time

try:
    from abc import ABC
except ImportError:  # pragma: no cover
    ABC = object


class Transport(ABC):
    """消息收发抽象。实现类需覆盖下列方法。"""

    def send_group_msg(self, group_id, text) -> dict:
        raise NotImplementedError

    def send_group_image(self, group_id, image_path) -> dict:
        raise NotImplementedError

    def send_private_msg(self, user_id, text) -> dict:
        raise NotImplementedError

    def fetch_image(self, file_id, dest_dir):
        raise NotImplementedError

    def get_login_info(self) -> dict:
        raise NotImplementedError


class MockTransport(Transport):
    """本地演示实现：
      · 出站消息不真正发送 —— 写入 logs/mock_transport.log（JSONL）并打印到控制台
      · fetch_image 生成一张 1×1 占位图，保证离线演示的图片链路可走通
    """

    def __init__(self, log_path=None):
        self.log_path = log_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "logs", "mock_transport.log")

    def _record(self, kind, payload):
        rec = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "kind": kind, "payload": payload}
        try:
            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except OSError:
            pass
        preview = payload.get("text") or payload.get("path") or payload.get("file_id") or ""
        print(f"[MockTransport] {kind} -> {str(preview)[:80]}", flush=True)

    def _ok(self, **data):
        d = {"message_id": int(time.time() * 1000) % 1000000}
        d.update(data)
        return {"status": "ok", "data": d}

    def send_group_msg(self, group_id, text):
        self._record("send_group_msg", {"group_id": str(group_id), "text": text})
        return self._ok()

    def send_group_image(self, group_id, image_path):
        self._record("send_group_image", {"group_id": str(group_id), "path": image_path})
        return self._ok()

    def send_private_msg(self, user_id, text):
        self._record("send_private_msg", {"user_id": str(user_id), "text": text})
        return self._ok()

    def fetch_image(self, file_id, dest_dir):
        """生成 1×1 占位 PNG。

        真实适配器此处为：向平台请求图片（get_image）→ 本地缓存复制 / URL 下载兜底。
        """
        import base64
        png_1x1 = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
        base = os.path.basename(str(file_id).replace("\\", "/").strip()) or "placeholder.png"
        try:
            os.makedirs(dest_dir, exist_ok=True)
            dst = os.path.join(dest_dir, base)
            with open(dst, "wb") as f:
                f.write(png_1x1)
        except OSError:
            return None
        self._record("fetch_image", {"file_id": base, "saved": dst})
        return dst

    def get_login_info(self):
        return {"status": "ok", "data": {"user_id": "100000002", "nickname": "演示机器人"}}


def get_transport():
    """按 TRANSPORT 环境变量构造适配器（默认 mock）。

    真实适配器（如 NapCat/OneBot）不随仓库提供；如需接入：
      1) 实现一个 Transport 子类（参考 MockTransport 的接口形状）
      2) 在下面注册（或直接改自己的副本）
    """
    name = (os.getenv("TRANSPORT") or "mock").strip().lower()
    if name in ("", "mock"):
        return MockTransport()
    if name == "napcat":
        raise RuntimeError(
            "NapCat 适配器不随本仓库提供（平台 ToS 合规原因）。"
            "请自行实现 Transport 接口 —— 凭据注入示范见 example_napcat_auth.py。")
    raise RuntimeError(f"未知 TRANSPORT: {name}（本仓库可用: mock）")
