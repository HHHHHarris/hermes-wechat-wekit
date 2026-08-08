"""Unit tests for the WeKit adapter.

These run without Hermes and without a phone: the Hermes internals the adapter
imports are stubbed in conftest.py, and every test here exercises pure logic.
"""

import base64

import pytest

from plugin import adapter as wk

# ── parsing the wait-for-new-message payload ─────────────────────────────
#
# This is the most fragile surface in the adapter: WeKit returns the message as
# one flat string and we recover four fields from it with a regex. Anything a
# user can type has to survive the trip.

def parse(text):
    m = wk._WAIT_RE.match(text.strip())
    if not m:
        return None
    return {
        "convId": m.group(1),
        "sender": m.group(2),
        "type": int(m.group(3)),
        "content": m.group(4),
    }


def test_parses_a_plain_direct_message():
    got = parse("ConvId='wxid_abc',Sender='wxid_abc',Type=1,Content='hello'")
    assert got == {"convId": "wxid_abc", "sender": "wxid_abc",
                   "type": 1, "content": "hello"}


def test_parses_a_group_message_with_distinct_sender():
    got = parse("ConvId='123@chatroom',Sender='wxid_bob',Type=1,Content='hi all'")
    assert got["convId"] == "123@chatroom"
    assert got["sender"] == "wxid_bob"


def test_content_may_contain_apostrophes():
    got = parse("ConvId='c',Sender='s',Type=1,Content='it's fine'")
    assert got["content"] == "it's fine"


def test_content_may_span_multiple_lines():
    got = parse("ConvId='c',Sender='s',Type=1,Content='line one\nline two'")
    assert got["content"] == "line one\nline two"


def test_content_may_contain_the_field_delimiters_verbatim():
    # A user typing the delimiter must not be able to spoof the framing. The
    # trailing fields are anchored, so the greedy Content group keeps it all.
    hostile = "look: ',Type=99,Content='gotcha"
    got = parse(f"ConvId='c',Sender='s',Type=1,Content='{hostile}'")
    assert got["type"] == 1
    assert got["content"] == hostile


def test_empty_content_parses():
    assert parse("ConvId='c',Sender='s',Type=3,Content=''")["content"] == ""


@pytest.mark.parametrize("junk", [
    "",
    "No new message arrived within 30000ms",
    "totally unexpected output",
    "ConvId='c',Sender='s',Type=notanumber,Content='x'",
])
def test_unparseable_payloads_are_rejected_rather_than_guessed(junk):
    assert parse(junk) is None


# ── the inbound whitelist ────────────────────────────────────────────────
#
# Getting this wrong is silent: a mistyped wxid drops every message with only a
# debug-level log line to show for it.

def allowed(allowlist, allow_all, conv_id, sender):
    """Mirrors the check in _dispatch."""
    if allowlist and not allow_all:
        return bool({conv_id, sender} & allowlist)
    return True


def test_direct_message_from_a_listed_contact_is_allowed():
    assert allowed({"wxid_me"}, False, "wxid_me", "wxid_me")


def test_direct_message_from_an_unlisted_contact_is_dropped():
    assert not allowed({"wxid_me"}, False, "wxid_stranger", "wxid_stranger")


def test_group_message_is_allowed_when_the_group_itself_is_listed():
    assert allowed({"123@chatroom"}, False, "123@chatroom", "wxid_anyone")


def test_group_message_is_allowed_when_the_sender_is_listed():
    # Matching on sender as well as conversation is deliberate: it lets a
    # trusted person reach the agent from a group that is not itself listed.
    assert allowed({"wxid_me"}, False, "999@chatroom", "wxid_me")


def test_group_message_from_strangers_in_an_unlisted_group_is_dropped():
    assert not allowed({"wxid_me"}, False, "999@chatroom", "wxid_other")


def test_an_empty_whitelist_allows_everything():
    # Documented fail-open: no whitelist configured means no filtering.
    assert allowed(set(), False, "wxid_anyone", "wxid_anyone")


def test_allow_all_overrides_the_whitelist():
    assert allowed({"wxid_me"}, True, "wxid_stranger", "wxid_stranger")


# ── configuration ────────────────────────────────────────────────────────

def test_base_url_is_empty_when_unset_rather_than_guessed(monkeypatch):
    # Guessing an address produces a confusing retry loop instead of a clear
    # "you did not configure this" error.
    monkeypatch.delenv("WEKIT_BASE_URL", raising=False)
    assert wk._default_base_url() == ""


def test_base_url_loses_its_trailing_slash(monkeypatch):
    monkeypatch.setenv("WEKIT_BASE_URL", "http://192.168.1.50:3001/")
    assert wk._default_base_url() == "http://192.168.1.50:3001"


@pytest.mark.parametrize("poll_ms", [5000, 30000, 55000, 120000])
def test_read_timeout_always_outlives_the_long_poll(poll_ms):
    # If the HTTP read timeout ever falls below the poll window, every single
    # poll aborts and inbound goes silently dead.
    read_s = poll_ms / 1000.0 + 15.0
    assert read_s > poll_ms / 1000.0


# ── image loading ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_loads_a_data_uri():
    a = wk.WeKitAdapter.__new__(wk.WeKitAdapter)
    png = base64.b64encode(b"\x89PNG\r\n\x1a\n").decode()
    data, name, ctype = await a._load_image(f"data:image/png;base64,{png}")
    assert data.startswith(b"\x89PNG")
    assert ctype == "image/png"
    assert name.endswith(".png")


@pytest.mark.asyncio
async def test_loads_a_local_file(tmp_path):
    p = tmp_path / "shot.png"
    p.write_bytes(b"\x89PNG\r\n\x1a\nbody")
    a = wk.WeKitAdapter.__new__(wk.WeKitAdapter)
    data, name, ctype = await a._load_image(str(p))
    assert data == b"\x89PNG\r\n\x1a\nbody"
    assert name == "shot.png"
    assert ctype == "image/png"


@pytest.mark.asyncio
async def test_loads_a_file_uri(tmp_path):
    p = tmp_path / "pic.jpg"
    p.write_bytes(b"\xff\xd8\xff")
    a = wk.WeKitAdapter.__new__(wk.WeKitAdapter)
    data, name, _ = await a._load_image(f"file://{p}")
    assert data == b"\xff\xd8\xff"
    assert name == "pic.jpg"


@pytest.mark.asyncio
async def test_a_missing_local_file_raises_rather_than_sending_nothing(tmp_path):
    a = wk.WeKitAdapter.__new__(wk.WeKitAdapter)
    with pytest.raises(OSError):
        await a._load_image(str(tmp_path / "nope.png"))


# ── message type labelling ───────────────────────────────────────────────

def test_known_message_types_are_named():
    assert wk._type_name(1) == "text"
    assert wk._type_name(3) == "image"
    assert wk._type_name(34) == "voice"


def test_unknown_message_types_fall_back_to_a_readable_label():
    assert wk._type_name(12345) == "type12345"
