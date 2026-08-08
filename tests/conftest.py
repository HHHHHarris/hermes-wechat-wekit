"""Stub out the Hermes runtime so the adapter can be unit-tested standalone.

The adapter imports `gateway.platforms.base` and `gateway.config` from the
Hermes agent. Rather than requiring a full Hermes install in CI, we register
minimal stand-ins before the adapter is imported. They only need to satisfy the
import and the class hierarchy — no test here exercises Hermes behaviour.
"""

import sys
import types
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class _Platform(str, Enum):
    WECHAT_WEKIT = "wechat-wekit"


@dataclass
class _SendResult:
    success: bool
    message_id: str = ""
    error: str = ""


class _MessageType(str, Enum):
    # Mirrors gateway.platforms.base.MessageType — keep in step with Hermes.
    TEXT = "text"
    LOCATION = "location"
    PHOTO = "photo"
    VIDEO = "video"
    AUDIO = "audio"
    VOICE = "voice"
    DOCUMENT = "document"
    STICKER = "sticker"
    COMMAND = "command"


@dataclass
class _MessageEvent:
    text: str = ""
    message_type: "_MessageType" = _MessageType.TEXT
    source: object = None
    message_id: str = ""
    timestamp: object = None


@dataclass
class _MessageSource:
    chat_id: str = ""
    chat_name: str = ""
    chat_type: str = "dm"
    user_id: str = ""
    user_name: str = ""
    platform: object = None


class _BasePlatformAdapter:
    def __init__(self, config=None, platform=None, **kwargs):
        self.config = config
        self.platform = platform

    def build_source(self, **kwargs):
        return _MessageSource(**kwargs)

    async def handle_message(self, event):
        return None


def _install():
    gateway = types.ModuleType("gateway")
    platforms = types.ModuleType("gateway.platforms")
    base = types.ModuleType("gateway.platforms.base")
    config = types.ModuleType("gateway.config")

    base.BasePlatformAdapter = _BasePlatformAdapter
    base.SendResult = _SendResult
    base.MessageEvent = _MessageEvent
    base.MessageType = _MessageType
    base.MessageSource = _MessageSource
    config.Platform = _Platform

    gateway.platforms = platforms
    platforms.base = base
    sys.modules.setdefault("gateway", gateway)
    sys.modules.setdefault("gateway.platforms", platforms)
    sys.modules.setdefault("gateway.platforms.base", base)
    sys.modules.setdefault("gateway.config", config)


_install()
