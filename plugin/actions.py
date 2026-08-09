"""WeKit action helpers and the Hermes agent tools built on top of them.

The platform adapter in ``adapter.py`` is about the message *stream* — poll for
inbound, render it, send a reply.  This module is about everything else WeKit's
HTTP API can do that a conversation might call for: pulling chat history,
re-fetching an image WeChat never downloaded, sending a native voice bubble,
managing group members, posting to Moments, and reading contact labels.

Two layers live here, deliberately kept apart:

* :class:`WeKitActions` — a thin, Hermes-free async wrapper over the WeKit REST
  endpoints (``http://<phone>:3001/api/...``).  It imports nothing from the
  agent runtime, so it is unit-testable against a mocked ``httpx`` client and is
  reused by the adapter itself (e.g. to resolve a label into an allow-list).

* The tool layer — JSON schemas, handlers, and :func:`register_tools` — wires
  those actions into Hermes as agent-callable tools in the ``hermes-wechat-wekit``
  toolset.  Because this plugin's platform key is ``wechat-wekit``, Hermes
  derives that toolset name automatically and enables it for WeChat sessions;
  no config edit is needed (see ``_get_platform_tools`` in the gateway).

Safety
------
Sending a message (text, voice, video) is no riskier than the reply the agent
already sends, so those tools are always available.  Actions that change the
account's social graph or are visible to other people — accepting a friend,
adding/removing/inviting group members, posting to Moments — are gated behind
``WEKIT_ENABLE_WRITE_ACTIONS``.  With the gate off (the default) those tools
still appear, but refuse to act and say how to enable them, so the model cannot
silently reshape the account.  Automating a personal WeChat account violates its
terms; use a dedicated secondary account.
"""

from __future__ import annotations

import asyncio
import contextlib
import mimetypes
import os
import posixpath
from typing import Any

import httpx


def _load_file(path: str, default_mime: str) -> tuple[str, str, bytes]:
    """Read a local file into (basename, mime, bytes).

    Blocking — call via ``asyncio.to_thread`` so file I/O never runs on the
    event loop the inbound poll shares.
    """
    if not os.path.isfile(path):
        raise WeKitActionError(f"file not found: {path}")
    name = os.path.basename(path)
    mime = mimetypes.guess_type(name)[0] or default_mime
    with open(path, "rb") as fh:
        return name, mime, fh.read()


def _safe_unlink(path: str) -> None:
    with contextlib.suppress(OSError):
        os.remove(path)

# tool_result / tool_error live in the Hermes runtime.  Fall back to compatible
# local shims when imported outside it (tests, standalone use) so this module
# stays importable without the agent installed.
try:  # pragma: no cover - trivial import glue
    from tools.registry import tool_error, tool_result
except Exception:  # pragma: no cover
    import json as _json

    def tool_result(data: Any = None, **kwargs: Any) -> str:
        return _json.dumps(data if data is not None else kwargs, ensure_ascii=False)

    def tool_error(message: Any, **extra: Any) -> str:
        out = {"error": str(message)}
        out.update(extra)
        return _json.dumps(out, ensure_ascii=False)


# ── the REST wrapper ──────────────────────────────────────────────────────


class WeKitActionError(RuntimeError):
    """A WeKit endpoint returned an error or unreachable response."""


class WeKitActions:
    """Async wrapper over the WeKit ``/api`` surface.

    Pass an existing ``httpx.AsyncClient`` to reuse the adapter's connection;
    omit it and each call opens and closes its own short-lived client (fine for
    the one-shot tool handlers).
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        client: httpx.AsyncClient | None = None,
        *,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = (base_url or "").rstrip("/")
        self.token = (token or "").strip()
        self._client = client
        self._timeout = timeout

    # -- plumbing ----------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
        files: list | None = None,
        data: dict | None = None,
        expect: str = "json",
    ) -> Any:
        url = f"{self.base_url}/api/{path.lstrip('/')}"

        async def _do(client: httpx.AsyncClient) -> httpx.Response:
            return await client.request(
                method,
                url,
                headers=self._headers(),
                params=params,
                json=json_body,
                files=files,
                data=data,
            )

        try:
            if self._client is not None:
                resp = await _do(self._client)
            else:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    resp = await _do(client)
        except httpx.HTTPError as exc:
            raise WeKitActionError(f"{method} {path} failed: {exc}") from exc

        if resp.status_code >= 400:
            body = resp.text[:300]
            raise WeKitActionError(f"{method} {path} -> {resp.status_code}: {body}")

        if expect == "raw":
            return resp.content
        text = resp.text.strip()
        if not text:
            return None
        try:
            return resp.json()
        except ValueError:
            return text

    async def _upload(self, local_path: str) -> str:
        """Push a local file to the phone; return the path WeKit saved it under."""
        name, mime, payload = await asyncio.to_thread(
            _load_file, local_path, "application/octet-stream"
        )
        res = await self._request(
            "POST",
            "media/upload",
            files=[("file", (name, payload, mime))],
        )
        path = (res or {}).get("path") if isinstance(res, dict) else None
        if not path:
            raise WeKitActionError(f"media/upload returned no path: {res!r}")
        return path

    # -- feature 1: history (ChatLab) --------------------------------------

    async def pull_history(self, conv_id: str, count: int = 50) -> list[dict]:
        """Return up to ``count`` recent messages for a conversation, oldest first.

        WeKit's history endpoint is page-based and resolves group senders to
        names; it returns only ``{sender, content, type}`` per message (no
        timestamp or server id), so this pages back until it has enough and
        hands the slice back in reading order.
        """
        count = max(1, min(int(count), 1000))
        page_size = min(count, 100)
        collected: list[dict] = []
        page = 1
        while len(collected) < count:
            batch = await self._request(
                "GET",
                f"conversations/{conv_id}/history",
                params={"page-index": page, "page-size": page_size},
            )
            if not isinstance(batch, list) or not batch:
                break
            collected.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
        collected = collected[:count]
        collected.reverse()  # endpoint returns newest-first; make it chronological
        return collected

    # -- feature 2: re-fetch media from CDN --------------------------------

    async def cache_image(self, msg_svr_id: int | str) -> str:
        """Force WeChat to pull an image from the CDN into its own cache.

        Fixes the gap where a received image WeChat never auto-downloaded has no
        local bytes to fetch.  Returns the cached path; follow with a GET of
        ``messages/{id}/image`` to read it.
        """
        res = await self._request("POST", f"messages/{msg_svr_id}/image/cache")
        if isinstance(res, dict):
            return res.get("path") or ""
        return str(res or "")

    async def cache_file(self, msg_svr_id: int | str, talker: str | None = None) -> str:
        params = {"talker": talker} if talker else None
        res = await self._request(
            "POST", f"messages/{msg_svr_id}/file/cache", params=params
        )
        if isinstance(res, dict):
            return res.get("path") or ""
        return str(res or "")

    # -- feature 3: native voice bubble ------------------------------------

    async def audio_duration(self, phone_path: str) -> int:
        res = await self._request(
            "POST", "utils/audio/duration", json_body={"path": phone_path}
        )
        if isinstance(res, dict):
            for key in ("durationMs", "duration", "value"):
                if key in res:
                    return int(res[key])
        try:
            return int(res)
        except (TypeError, ValueError):
            return 0

    async def send_voice(
        self,
        conv_id: str,
        mp3_path: str,
        *,
        duration_ms: int | None = None,
    ) -> dict:
        """Send a local mp3 as a native WeChat voice bubble.

        Uploads the mp3, converts it to SILK on the phone (WeChat's voice
        format), reads its duration, and sends it down the voice path.
        """
        phone_mp3 = await self._upload(mp3_path)
        silk_path = posixpath.splitext(phone_mp3)[0] + ".silk"
        await self._request(
            "POST",
            "utils/audio/mp3-to-silk",
            json_body={"srcPath": phone_mp3, "destPath": silk_path},
        )
        dur = duration_ms if duration_ms is not None else await self.audio_duration(phone_mp3)
        await self._request(
            "POST",
            "messages/voice",
            json_body={"convId": conv_id, "path": silk_path, "durationMs": int(dur)},
        )
        return {"ok": True, "durationMs": int(dur)}

    # -- feature 4: groups + friends ---------------------------------------

    async def list_group_members(self, group_id: str) -> list[dict]:
        res = await self._request("GET", f"groups/{group_id}/members")
        return res if isinstance(res, list) else []

    async def group_member_op(
        self, group_id: str, op: str, member_wxids: list[str]
    ) -> dict:
        """op ∈ {add, delete, invite}; ``member_wxids`` is one or more wxIds."""
        if op not in ("add", "delete", "invite"):
            raise WeKitActionError(f"unknown group op: {op}")
        member_wxids = [w for w in member_wxids if w]
        if not member_wxids:
            raise WeKitActionError("no member wxIds given")
        body = (
            {"memberWxid": member_wxids[0]}
            if len(member_wxids) == 1
            else {"memberWxids": member_wxids}
        )
        await self._request("POST", f"groups/{group_id}/members/{op}", json_body=body)
        return {"ok": True, "op": op, "members": member_wxids}

    async def accept_friend(
        self, user_id: str, ticket: str, scene: int, privacy: int | None = None
    ) -> dict:
        body: dict[str, Any] = {"userId": user_id, "ticket": ticket, "scene": int(scene)}
        if privacy is not None:
            body["privacy"] = int(privacy)
        await self._request("POST", "contacts/verify", json_body=body)
        return {"ok": True, "userId": user_id}

    # -- feature 5: Moments + video ----------------------------------------

    async def post_moment_text(self, content: str) -> dict:
        await self._request("POST", "moments/text", json_body={"content": content})
        return {"ok": True}

    async def post_moment_pics(self, content: str, pic_paths: list[str]) -> dict:
        files = []
        for p in pic_paths:
            name, mime, payload = await asyncio.to_thread(_load_file, p, "image/jpeg")
            files.append(("file", (name, payload, mime)))
        if not files:
            raise WeKitActionError("no images given")
        await self._request(
            "POST", "moments/pics", data={"content": content}, files=files
        )
        return {"ok": True, "images": len(files)}

    async def send_video(self, conv_id: str, video_path: str) -> dict:
        name, mime, payload = await asyncio.to_thread(_load_file, video_path, "video/mp4")
        await self._request(
            "POST",
            "messages/video",
            data={"convId": conv_id},
            files=[("file", (name, payload, mime))],
        )
        return {"ok": True}

    # -- feature 6: contact labels -----------------------------------------

    async def list_labels(self) -> list[dict]:
        res = await self._request("GET", "labels")
        return res if isinstance(res, list) else []

    async def contacts_by_label(self, label_id_or_name: str) -> list[str]:
        res = await self._request("GET", f"labels/{label_id_or_name}/contacts")
        return [str(x) for x in res] if isinstance(res, list) else []

    async def set_contact_labels(self, wx_id: str, labels: list[str]) -> dict:
        """Replace a contact's label set.

        WeChat only accepts labels that already exist: inside WeKit, a name it
        cannot resolve to an id is skipped with a log line, and the endpoint
        still answers 200 — so an unknown name would look like it worked and
        silently do nothing. Names are therefore checked against the label list
        first, and an unknown one is an error rather than a no-op. WeKit exposes
        no way to create a label, so a new one has to be made in WeChat itself.
        """
        labels = list(labels)
        if labels:
            known = {str(lbl.get("labelName") or "") for lbl in await self.list_labels()}
            missing = [name for name in labels if name not in known]
            if missing:
                raise WeKitActionError(
                    f"no such WeChat label: {', '.join(missing)}. "
                    f"Existing labels: {', '.join(sorted(known)) or '(none)'}. "
                    "Labels can only be created in the WeChat app itself "
                    "(Me -> Contacts -> Tags); WeKit exposes no endpoint for it."
                )
        await self._request(
            "POST", f"contacts/{wx_id}/labels", json_body={"labels": labels}
        )
        return {"ok": True, "wxId": wx_id, "labels": labels}


# ── env / gating helpers ───────────────────────────────────────────────────


def _actions_from_env() -> WeKitActions | None:
    base = os.getenv("WEKIT_BASE_URL")
    token = os.getenv("WEKIT_TOKEN")
    if not base or not token:
        return None
    return WeKitActions(base, token)


def _check_wekit_available() -> bool:
    return bool(os.getenv("WEKIT_BASE_URL") and os.getenv("WEKIT_TOKEN"))


def _write_actions_enabled() -> bool:
    return str(os.getenv("WEKIT_ENABLE_WRITE_ACTIONS") or "").lower() in (
        "1",
        "true",
        "yes",
    )


_WRITE_GATE_MSG = (
    "This action changes the account or is visible to others and is disabled by "
    "default. Set WEKIT_ENABLE_WRITE_ACTIONS=1 in the environment to allow it."
)


async def _edge_tts_to_mp3(text: str, voice: str, out_path: str) -> None:
    """Render text to an mp3 with edge-tts (optional dependency)."""
    import edge_tts  # imported lazily so the module loads without it

    communicate = edge_tts.Communicate(text, voice)
    await communicate.save(out_path)


# ── tool schemas ──────────────────────────────────────────────────────────

_CONV = {"type": "string", "description": "WeChat conversation id: a wxid for a "
         "person, or a ...@chatroom id for a group."}

PULL_HISTORY_SCHEMA = {
    "name": "wechat_pull_history",
    "description": (
        "Read recent messages from a WeChat conversation (a person or a group). "
        "Returns messages oldest-first with the sender resolved to a name in "
        "groups. Read-only. Non-text messages appear as <type:...> placeholders."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "conv_id": _CONV,
            "count": {"type": "integer", "description": "How many recent messages "
                      "to fetch (default 50, max 1000)."},
        },
        "required": ["conv_id"],
    },
}

SEND_VOICE_SCHEMA = {
    "name": "wechat_send_voice",
    "description": (
        "Send a native WeChat voice bubble to a conversation. Provide `text` to "
        "synthesize speech (edge-tts) or `audio_path` to send an existing "
        "mp3/wav. Same risk as sending any message."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "conv_id": _CONV,
            "text": {"type": "string", "description": "Text to speak (synthesized to voice)."},
            "audio_path": {"type": "string", "description": "Local mp3/wav to send as voice."},
            "voice": {"type": "string", "description": "edge-tts voice (default Xiaoxiao)."},
        },
        "required": ["conv_id"],
    },
}

SEND_VIDEO_SCHEMA = {
    "name": "wechat_send_video",
    "description": "Send a local video file (mp4) as a native WeChat video message.",
    "parameters": {
        "type": "object",
        "properties": {
            "conv_id": _CONV,
            "video_path": {"type": "string", "description": "Path to a local video file."},
        },
        "required": ["conv_id", "video_path"],
    },
}

GROUP_MEMBERS_SCHEMA = {
    "name": "wechat_group_members",
    "description": (
        "List, add, remove, or invite members of a WeChat group. action=list is "
        "read-only; add/remove/invite change the group and are gated behind "
        "WEKIT_ENABLE_WRITE_ACTIONS."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "group_id": {"type": "string", "description": "Group id (...@chatroom)."},
            "action": {"type": "string", "enum": ["list", "add", "remove", "invite"]},
            "member_wxids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "wxIds to add/remove/invite (not needed for list).",
            },
        },
        "required": ["group_id", "action"],
    },
}

ACCEPT_FRIEND_SCHEMA = {
    "name": "wechat_accept_friend",
    "description": (
        "Accept a pending friend request. Needs the userId, ticket, and scene "
        "from the request (surfaced on the incoming add-request message). Gated "
        "behind WEKIT_ENABLE_WRITE_ACTIONS."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "ticket": {"type": "string"},
            "scene": {"type": "integer"},
            "privacy": {"type": "integer"},
        },
        "required": ["user_id", "ticket", "scene"],
    },
}

MOMENT_SCHEMA = {
    "name": "wechat_post_moment",
    "description": (
        "Post to the account's Moments (朋友圈) timeline — text, or text plus "
        "images. Public to the account's contacts. Gated behind "
        "WEKIT_ENABLE_WRITE_ACTIONS."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "content": {"type": "string", "description": "Text of the moment."},
            "image_paths": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional local image paths to attach.",
            },
        },
        "required": ["content"],
    },
}

LABELS_SCHEMA = {
    "name": "wechat_labels",
    "description": (
        "Manage WeChat contact labels (标签/groups). action=list lists labels; "
        "action=members lists the wxIds carrying a label (use this to drive the "
        "agent's allow-list); action=set replaces a contact's full label set "
        "(gated behind WEKIT_ENABLE_WRITE_ACTIONS). Labels must already exist — "
        "they can only be created in the WeChat app itself, so 'set' with an "
        "unknown name fails rather than creating it."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["list", "members", "set"]},
            "label": {"type": "string", "description": "Label id or name (for members)."},
            "wx_id": {"type": "string", "description": "Contact wxId (for set)."},
            "labels": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Full label set to assign the contact (for set).",
            },
        },
        "required": ["action"],
    },
}


# ── tool handlers ─────────────────────────────────────────────────────────


async def _handle_pull_history(args: dict, **_kw: Any) -> str:
    act = _actions_from_env()
    if act is None:
        return tool_error("WeKit not configured (WEKIT_BASE_URL / WEKIT_TOKEN).")
    conv_id = (args.get("conv_id") or "").strip()
    if not conv_id:
        return tool_error("conv_id is required.")
    try:
        msgs = await act.pull_history(conv_id, int(args.get("count") or 50))
    except Exception as exc:
        return tool_error(str(exc))
    return tool_result({"conv_id": conv_id, "count": len(msgs), "messages": msgs})


async def _handle_send_voice(args: dict, **_kw: Any) -> str:
    act = _actions_from_env()
    if act is None:
        return tool_error("WeKit not configured (WEKIT_BASE_URL / WEKIT_TOKEN).")
    conv_id = (args.get("conv_id") or "").strip()
    if not conv_id:
        return tool_error("conv_id is required.")
    audio_path = (args.get("audio_path") or "").strip()
    text = (args.get("text") or "").strip()
    tmp_created: str | None = None
    try:
        if not audio_path:
            if not text:
                return tool_error("Provide either text or audio_path.")
            voice = (args.get("voice") or "zh-CN-XiaoxiaoNeural").strip()
            tmp_created = f"/tmp/wekit-tts-{abs(hash((conv_id, text))) & 0xffffff}.mp3"
            try:
                await _edge_tts_to_mp3(text, voice, tmp_created)
            except ImportError:
                return tool_error(
                    "edge-tts is not installed; pass audio_path with a ready mp3 "
                    "instead, or `pip install edge-tts`."
                )
            audio_path = tmp_created
        res = await act.send_voice(conv_id, audio_path)
    except Exception as exc:
        return tool_error(str(exc))
    finally:
        if tmp_created:
            await asyncio.to_thread(_safe_unlink, tmp_created)
    return tool_result(res)


async def _handle_send_video(args: dict, **_kw: Any) -> str:
    act = _actions_from_env()
    if act is None:
        return tool_error("WeKit not configured (WEKIT_BASE_URL / WEKIT_TOKEN).")
    conv_id = (args.get("conv_id") or "").strip()
    video_path = (args.get("video_path") or "").strip()
    if not conv_id or not video_path:
        return tool_error("conv_id and video_path are required.")
    try:
        res = await act.send_video(conv_id, video_path)
    except Exception as exc:
        return tool_error(str(exc))
    return tool_result(res)


async def _handle_group_members(args: dict, **_kw: Any) -> str:
    act = _actions_from_env()
    if act is None:
        return tool_error("WeKit not configured (WEKIT_BASE_URL / WEKIT_TOKEN).")
    group_id = (args.get("group_id") or "").strip()
    action = (args.get("action") or "").strip()
    if not group_id or not action:
        return tool_error("group_id and action are required.")
    try:
        if action == "list":
            members = await act.list_group_members(group_id)
            return tool_result({"group_id": group_id, "count": len(members),
                                "members": members})
        if not _write_actions_enabled():
            return tool_error(_WRITE_GATE_MSG)
        op = {"add": "add", "remove": "delete", "invite": "invite"}.get(action)
        if not op:
            return tool_error(f"unknown action: {action}")
        member_wxids = args.get("member_wxids") or []
        res = await act.group_member_op(group_id, op, list(member_wxids))
    except Exception as exc:
        return tool_error(str(exc))
    return tool_result(res)


async def _handle_accept_friend(args: dict, **_kw: Any) -> str:
    act = _actions_from_env()
    if act is None:
        return tool_error("WeKit not configured (WEKIT_BASE_URL / WEKIT_TOKEN).")
    if not _write_actions_enabled():
        return tool_error(_WRITE_GATE_MSG)
    user_id = (args.get("user_id") or "").strip()
    ticket = (args.get("ticket") or "").strip()
    scene = args.get("scene")
    if not user_id or not ticket or scene is None:
        return tool_error("user_id, ticket, and scene are required.")
    try:
        res = await act.accept_friend(user_id, ticket, int(scene), args.get("privacy"))
    except Exception as exc:
        return tool_error(str(exc))
    return tool_result(res)


async def _handle_post_moment(args: dict, **_kw: Any) -> str:
    act = _actions_from_env()
    if act is None:
        return tool_error("WeKit not configured (WEKIT_BASE_URL / WEKIT_TOKEN).")
    if not _write_actions_enabled():
        return tool_error(_WRITE_GATE_MSG)
    content = args.get("content") or ""
    image_paths = args.get("image_paths") or []
    try:
        if image_paths:
            res = await act.post_moment_pics(content, list(image_paths))
        else:
            res = await act.post_moment_text(content)
    except Exception as exc:
        return tool_error(str(exc))
    return tool_result(res)


async def _handle_labels(args: dict, **_kw: Any) -> str:
    act = _actions_from_env()
    if act is None:
        return tool_error("WeKit not configured (WEKIT_BASE_URL / WEKIT_TOKEN).")
    action = (args.get("action") or "").strip()
    try:
        if action == "list":
            return tool_result({"labels": await act.list_labels()})
        if action == "members":
            label = (args.get("label") or "").strip()
            if not label:
                return tool_error("label is required for action=members.")
            wxids = await act.contacts_by_label(label)
            return tool_result({"label": label, "count": len(wxids), "wxids": wxids})
        if action == "set":
            if not _write_actions_enabled():
                return tool_error(_WRITE_GATE_MSG)
            wx_id = (args.get("wx_id") or "").strip()
            labels = args.get("labels")
            if not wx_id or labels is None:
                return tool_error("wx_id and labels are required for action=set.")
            res = await act.set_contact_labels(wx_id, list(labels))
            return tool_result(res)
        return tool_error(f"unknown action: {action}")
    except Exception as exc:
        return tool_error(str(exc))


# name, schema, handler, emoji
_TOOLS = (
    ("wechat_pull_history", PULL_HISTORY_SCHEMA, _handle_pull_history, "📜"),
    ("wechat_send_voice", SEND_VOICE_SCHEMA, _handle_send_voice, "🎙️"),
    ("wechat_send_video", SEND_VIDEO_SCHEMA, _handle_send_video, "🎬"),
    ("wechat_group_members", GROUP_MEMBERS_SCHEMA, _handle_group_members, "👥"),
    ("wechat_accept_friend", ACCEPT_FRIEND_SCHEMA, _handle_accept_friend, "🤝"),
    ("wechat_post_moment", MOMENT_SCHEMA, _handle_post_moment, "📸"),
    ("wechat_labels", LABELS_SCHEMA, _handle_labels, "🏷️"),
)

TOOLSET = "hermes-wechat-wekit"


def register_tools(ctx: Any) -> None:
    """Register the WeChat action tools. Called from the plugin's register()."""
    for name, schema, handler, emoji in _TOOLS:
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=schema,
            handler=handler,
            check_fn=_check_wekit_available,
            is_async=True,
            emoji=emoji,
        )
