"""Unit tests for the WeKit action layer (plugin/actions.py).

No phone and no Hermes runtime: WeKit's HTTP API is faked with
``httpx.MockTransport`` so the request shape each action sends — path, method,
body, query, multipart — is asserted directly, and the tool handlers are
exercised against a monkeypatched environment.
"""

import asyncio
import json
import os
import tempfile
from pathlib import Path

import httpx
import pytest

from plugin import actions as act

# ── a recording fake for the WeKit REST server ─────────────────────────────


class FakeWeKit:
    """Records every request and replies from a small routing table."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.routes: dict[tuple[str, str], object] = {}

    def route(self, method: str, path: str, payload):
        # path is the part after /api/ ; payload is a dict/list (json) or (status, dict)
        self.routes[(method.upper(), path)] = payload

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        key = (request.method, request.url.path)
        payload = self.routes.get(key)
        if payload is None:
            # Default OK with a success body.
            return httpx.Response(200, json={"success": True})
        status, body = payload if isinstance(payload, tuple) else (200, payload)
        if isinstance(body, (dict, list)):
            return httpx.Response(status, json=body)
        return httpx.Response(status, text=str(body))

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))

    def paths(self) -> list[str]:
        return [r.url.path for r in self.requests]

    def find(self, method: str, path_suffix: str) -> httpx.Request:
        for r in self.requests:
            if r.method == method.upper() and r.url.path.endswith(path_suffix):
                return r
        raise AssertionError(f"no {method} request ending in {path_suffix}")


def actions_with(fake: FakeWeKit) -> act.WeKitActions:
    return act.WeKitActions("http://phone:3001", "TESTTOKEN", fake.client())


# ── feature 1: history ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pull_history_paginates_and_reverses():
    fake = FakeWeKit()
    # newest-first pages; page 1 full (size 100 would be needed, so use count<=size)
    fake.route("GET", "/api/conversations/g@chatroom/history",
               [{"sender": "b", "content": "newest", "type": "text"},
                {"sender": "a", "content": "older", "type": "text"}])
    msgs = await actions_with(fake).pull_history("g@chatroom", count=2)
    # oldest first after reversal
    assert [m["content"] for m in msgs] == ["older", "newest"]
    # sends a Bearer token
    assert fake.requests[0].headers["Authorization"] == "Bearer TESTTOKEN"


@pytest.mark.asyncio
async def test_pull_history_stops_when_page_short():
    fake = FakeWeKit()
    # a single short page (< page_size) must not loop forever
    fake.route("GET", "/api/conversations/wxid_x/history",
               [{"sender": "a", "content": "hi", "type": "text"}])
    msgs = await actions_with(fake).pull_history("wxid_x", count=500)
    assert len(msgs) == 1
    # exactly one page fetched
    assert len([p for p in fake.paths() if p.endswith("/history")]) == 1


# ── feature 2: CDN cache ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cache_image_posts_and_returns_path():
    fake = FakeWeKit()
    fake.route("POST", "/api/messages/12345/image/cache", {"path": "/sdcard/x.jpg"})
    path = await actions_with(fake).cache_image(12345)
    assert path == "/sdcard/x.jpg"
    assert fake.find("POST", "/messages/12345/image/cache")


# ── feature 3: voice ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_voice_uploads_converts_and_sends(tmp_path):
    mp3 = tmp_path / "hi.mp3"
    mp3.write_bytes(b"ID3fakeaudio")
    fake = FakeWeKit()
    fake.route("POST", "/api/media/upload", {"path": "/sdcard/up/hi.mp3"})
    fake.route("POST", "/api/utils/audio/duration", {"durationMs": 1500})
    res = await actions_with(fake).send_voice("wxid_y", str(mp3))
    assert res == {"ok": True, "durationMs": 1500}
    # order: upload -> mp3-to-silk -> duration -> voice
    order = [p for p in fake.paths() if p.startswith("/api/")]
    assert "/api/media/upload" in order
    silk = fake.find("POST", "/utils/audio/mp3-to-silk")
    body = json.loads(silk.content)
    assert body["srcPath"] == "/sdcard/up/hi.mp3"
    assert body["destPath"] == "/sdcard/up/hi.silk"
    voice = fake.find("POST", "/messages/voice")
    vbody = json.loads(voice.content)
    assert vbody == {"convId": "wxid_y", "path": "/sdcard/up/hi.silk", "durationMs": 1500}


# ── feature 4: groups + friends ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_group_member_op_single_vs_many():
    fake = FakeWeKit()
    a = actions_with(fake)
    await a.group_member_op("g@chatroom", "delete", ["wxid_1"])
    body = json.loads(fake.find("POST", "/groups/g@chatroom/members/delete").content)
    assert body == {"memberWxid": "wxid_1"}

    fake2 = FakeWeKit()
    await actions_with(fake2).group_member_op("g@chatroom", "invite", ["a", "b"])
    body2 = json.loads(fake2.find("POST", "/groups/g@chatroom/members/invite").content)
    assert body2 == {"memberWxids": ["a", "b"]}


@pytest.mark.asyncio
async def test_group_member_op_rejects_bad_op_and_empty():
    a = actions_with(FakeWeKit())
    with pytest.raises(act.WeKitActionError):
        await a.group_member_op("g", "promote", ["x"])
    with pytest.raises(act.WeKitActionError):
        await a.group_member_op("g", "add", [])


@pytest.mark.asyncio
async def test_accept_friend_body():
    fake = FakeWeKit()
    await actions_with(fake).accept_friend("wxid_z", "TICKET", 30, privacy=1)
    body = json.loads(fake.find("POST", "/contacts/verify").content)
    assert body == {"userId": "wxid_z", "ticket": "TICKET", "scene": 30, "privacy": 1}


# ── feature 5: moments + video ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_post_moment_text():
    fake = FakeWeKit()
    await actions_with(fake).post_moment_text("hello world")
    body = json.loads(fake.find("POST", "/moments/text").content)
    assert body == {"content": "hello world"}


@pytest.mark.asyncio
async def test_post_moment_pics_multipart(tmp_path):
    img = tmp_path / "p.jpg"
    img.write_bytes(b"\xff\xd8\xff\xe0jpeg")
    fake = FakeWeKit()
    await actions_with(fake).post_moment_pics("caption", [str(img)])
    req = fake.find("POST", "/moments/pics")
    assert req.headers["content-type"].startswith("multipart/form-data")
    assert b"caption" in req.content


@pytest.mark.asyncio
async def test_send_video_multipart(tmp_path):
    vid = tmp_path / "v.mp4"
    vid.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    fake = FakeWeKit()
    await actions_with(fake).send_video("wxid_v", str(vid))
    req = fake.find("POST", "/messages/video")
    assert req.headers["content-type"].startswith("multipart/form-data")
    assert b"wxid_v" in req.content


# ── feature 6: labels ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_contacts_by_label_returns_strings():
    fake = FakeWeKit()
    fake.route("GET", "/api/labels/friends/contacts", ["wxid_a", "wxid_b"])
    wxids = await actions_with(fake).contacts_by_label("friends")
    assert wxids == ["wxid_a", "wxid_b"]


@pytest.mark.asyncio
async def test_contacts_by_label_escapes_the_name():
    # Label names are typed by a person: Chinese, spaces, sometimes a slash.
    # Unescaped, "work/home" would become two path segments.
    fake = FakeWeKit()
    await actions_with(fake).contacts_by_label("work/home")
    # The slash must survive as one path segment. httpx decodes .path, so the
    # raw form is what carries the proof.
    raw = fake.requests[0].url.raw_path.decode()
    assert "labels/work%2Fhome/contacts" in raw, raw

    fake2 = FakeWeKit()
    await actions_with(fake2).contacts_by_label("同事")
    raw2 = fake2.requests[0].url.raw_path.decode()
    assert "labels/%E5%90%8C%E4%BA%8B/contacts" in raw2, raw2


@pytest.mark.asyncio
async def test_set_contact_labels_body():
    fake = FakeWeKit()
    fake.route("GET", "/api/labels",
               [{"labelId": 1, "labelName": "vip"}, {"labelId": 2, "labelName": "work"}])
    await actions_with(fake).set_contact_labels("wxid_a", ["vip", "work"])
    body = json.loads(fake.find("POST", "/contacts/wxid_a/labels").content)
    assert body == {"labels": ["vip", "work"]}


@pytest.mark.asyncio
async def test_set_contact_labels_rejects_unknown_label():
    # WeChat silently skips a label it cannot resolve and still answers 200, so
    # an unknown name must be caught here rather than reported as success.
    fake = FakeWeKit()
    fake.route("GET", "/api/labels", [{"labelId": 1, "labelName": "vip"}])
    with pytest.raises(act.WeKitActionError) as exc:
        await actions_with(fake).set_contact_labels("wxid_a", ["nope"])
    assert "nope" in str(exc.value)
    # and it must not have sent the doomed write
    assert not any(r.url.path.endswith("/labels") and r.method == "POST"
                   for r in fake.requests)


@pytest.mark.asyncio
async def test_set_contact_labels_empty_clears_without_lookup():
    # Clearing a contact's labels is always valid — no label list to check.
    fake = FakeWeKit()
    await actions_with(fake).set_contact_labels("wxid_a", [])
    body = json.loads(fake.find("POST", "/contacts/wxid_a/labels").content)
    assert body == {"labels": []}
    assert not any(r.method == "GET" for r in fake.requests)


# ── error surface ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_200_saying_success_false_is_an_error():
    # WeKit answers some failures with HTTP 200 and {"success": false}. Reading
    # only the status is how a write that did nothing gets reported as done —
    # this project already shipped that bug once, for contact labels.
    fake = FakeWeKit()
    fake.route("POST", "/api/moments/text", {"success": False, "error": "nope"})
    with pytest.raises(act.WeKitActionError) as exc:
        await actions_with(fake).post_moment_text("hi")
    assert "nope" in str(exc.value)


@pytest.mark.asyncio
async def test_voice_stops_when_silk_conversion_refuses():
    # The proven case: mp3-to-silk answers 200 {"success": false} when ffmpeg is
    # missing, and the old code went on to send a voice message pointing at a
    # file that was never written.
    fake = FakeWeKit()
    fake.route("POST", "/api/media/upload", {"path": "/sdcard/up/a.mp3"})
    fake.route("POST", "/api/utils/audio/mp3-to-silk",
               {"success": False, "error": "ffmpeg missing"})
    fd, mp3 = tempfile.mkstemp(suffix=".mp3")
    os.close(fd)
    await asyncio.to_thread(Path(mp3).write_bytes, b"ID3x")
    try:
        with pytest.raises(act.WeKitActionError) as exc:
            await actions_with(fake).send_voice("wxid_y", mp3)
    finally:
        os.unlink(mp3)
    assert "ffmpeg" in str(exc.value)
    # and crucially it must NOT have gone on to send the message
    assert not any(r.url.path.endswith("/messages/voice") for r in fake.requests)


@pytest.mark.asyncio
async def test_pull_history_stops_if_the_server_repeats_a_page():
    # An endpoint that ignores page-index would otherwise pad the history with
    # repetition the model reads as real conversation.
    fake = FakeWeKit()
    page = [{"sender": f"s{i}", "content": f"m{i}", "type": "text"}
            for i in range(100)]
    fake.route("GET", "/api/conversations/c/history", page)
    msgs = await actions_with(fake).pull_history("c", count=500)
    assert len(msgs) == 100
    # two requests: the first page, then the repeat that ends the loop
    assert len([p for p in fake.paths() if p.endswith("/history")]) == 2


@pytest.mark.asyncio
async def test_http_error_becomes_action_error():
    fake = FakeWeKit()
    fake.route("GET", "/api/labels", (500, {"error": "boom"}))
    with pytest.raises(act.WeKitActionError):
        await actions_with(fake).list_labels()


# ── env + gating helpers ───────────────────────────────────────────────────


def test_actions_from_env(monkeypatch):
    monkeypatch.delenv("WEKIT_BASE_URL", raising=False)
    monkeypatch.delenv("WEKIT_TOKEN", raising=False)
    assert act._actions_from_env() is None
    monkeypatch.setenv("WEKIT_BASE_URL", "http://phone:3001")
    monkeypatch.setenv("WEKIT_TOKEN", "T")
    a = act._actions_from_env()
    assert isinstance(a, act.WeKitActions)
    assert a.base_url == "http://phone:3001"


def test_write_gate_default_off(monkeypatch):
    monkeypatch.delenv("WEKIT_ENABLE_WRITE_ACTIONS", raising=False)
    assert act._write_actions_enabled() is False
    monkeypatch.setenv("WEKIT_ENABLE_WRITE_ACTIONS", "1")
    assert act._write_actions_enabled() is True


# ── tool handlers ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_handle_post_moment_blocked_without_gate(monkeypatch):
    monkeypatch.setenv("WEKIT_BASE_URL", "http://phone:3001")
    monkeypatch.setenv("WEKIT_TOKEN", "T")
    monkeypatch.delenv("WEKIT_ENABLE_WRITE_ACTIONS", raising=False)
    out = json.loads(await act._handle_post_moment({"content": "hi"}))
    assert "error" in out
    assert "WEKIT_ENABLE_WRITE_ACTIONS" in out["error"]


@pytest.mark.asyncio
async def test_handle_pull_history_needs_conv_id(monkeypatch):
    monkeypatch.setenv("WEKIT_BASE_URL", "http://phone:3001")
    monkeypatch.setenv("WEKIT_TOKEN", "T")
    out = json.loads(await act._handle_pull_history({}))
    assert "error" in out


@pytest.mark.asyncio
async def test_handle_group_members_list_is_read_only(monkeypatch):
    # list works even with the write gate off; routed through a fake client
    monkeypatch.setenv("WEKIT_BASE_URL", "http://phone:3001")
    monkeypatch.setenv("WEKIT_TOKEN", "T")
    monkeypatch.delenv("WEKIT_ENABLE_WRITE_ACTIONS", raising=False)
    fake = FakeWeKit()
    fake.route("GET", "/api/groups/g@chatroom/members",
               [{"wxId": "a"}, {"wxId": "b"}])
    monkeypatch.setattr(act, "_actions_from_env", lambda: actions_with(fake))
    out = json.loads(await act._handle_group_members(
        {"group_id": "g@chatroom", "action": "list"}))
    assert out["count"] == 2


@pytest.mark.asyncio
async def test_handle_group_members_write_blocked(monkeypatch):
    monkeypatch.setenv("WEKIT_BASE_URL", "http://phone:3001")
    monkeypatch.setenv("WEKIT_TOKEN", "T")
    monkeypatch.delenv("WEKIT_ENABLE_WRITE_ACTIONS", raising=False)
    out = json.loads(await act._handle_group_members(
        {"group_id": "g@chatroom", "action": "remove", "member_wxids": ["x"]}))
    assert "error" in out and "WEKIT_ENABLE_WRITE_ACTIONS" in out["error"]


@pytest.mark.asyncio
async def test_send_voice_temp_files_are_unique_and_cleaned(monkeypatch, tmp_path):
    # Two voice messages sent at once must not write over each other's audio —
    # which a temp name derived from the text would do for identical text.
    monkeypatch.setenv("WEKIT_BASE_URL", "http://phone:3001")
    monkeypatch.setenv("WEKIT_TOKEN", "T")
    made: list[str] = []

    async def fake_tts(text, voice, out_path):
        made.append(out_path)
        await asyncio.to_thread(Path(out_path).write_bytes, b"ID3fake")

    sent: list[str] = []

    async def fake_send_voice(self, conv_id, mp3_path, **kw):
        sent.append(mp3_path)
        return {"ok": True, "durationMs": 1}

    monkeypatch.setattr(act, "_edge_tts_to_mp3", fake_tts)
    monkeypatch.setattr(act.WeKitActions, "send_voice", fake_send_voice)

    await asyncio.gather(*[
        act._handle_send_voice({"conv_id": "wxid_a", "text": "same words"})
        for _ in range(2)
    ])

    assert len(set(made)) == 2, f"temp files collided: {made}"
    assert sorted(sent) == sorted(made)
    # and neither is left behind
    left = await asyncio.to_thread(lambda: [p for p in made if Path(p).exists()])
    assert left == []


def test_register_tools_registers_all():
    registered = []

    class Ctx:
        def register_tool(self, **kw):
            registered.append(kw["name"])

    act.register_tools(Ctx())
    assert set(registered) == {
        "wechat_pull_history", "wechat_send_voice", "wechat_send_video",
        "wechat_group_members", "wechat_accept_friend", "wechat_post_moment",
        "wechat_labels",
    }
    # every tool lands in the auto-enabled platform toolset
    assert act.TOOLSET == "hermes-wechat-wekit"
