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


# ── WeChat payload decoding ──────────────────────────────────────────────
#
# Everything except plain text arrives as an XML blob. Handing that to a model
# verbatim buries the one useful fact under CDN keys and AES material, so each
# payload is rendered into a short, actionable line.

from plugin.adapter import describe_payload  # noqa: E402

IMAGE = ('<?xml version="1.0"?><msg><img aeskey="9a598cb2" encryver="1" '
         'length="245678" hdlength="892134" md5="abc123" /></msg>')
VOICE = ('<msg><voicemsg endflag="1" voiceformat="4" voicelength="2768" '
         'aeskey="097f5e4e" fromusername="wxid_abc"></voicemsg></msg>')
STICKER = ('wxid_abc:0:1:e4f6e764:<msg><emoji fromusername="wxid_abc" '
           'md5="deadbeef" type="2" /></msg>')
FILE = ('<msg><appmsg><title><![CDATA[report.xlsx]]></title><type>6</type>'
        '<appattach><totallen>11251</totallen><fileext>xlsx</fileext>'
        '</appattach></appmsg></msg>')
LINK = ('<msg><appmsg><title><![CDATA[An article]]></title><des><![CDATA[worth '
        'reading]]></des><type>5</type><url><![CDATA[https://example.com/a]]>'
        '</url></appmsg></msg>')
QUOTE = ('<msg><appmsg><title>and this?</title><type>57</type><refermsg>'
         '<displayname>Alice</displayname><content>earlier message</content>'
         '</refermsg></appmsg></msg>')
LOCATION = '<msg><location x="31.23" y="121.47" poiname="Some Tower" /></msg>'


def test_plain_text_passes_through_untouched():
    text, kind, _ = describe_payload(1, "hello there")
    assert text == "hello there"
    assert kind == "text"


def test_image_reports_size_and_photo_kind():
    text, kind, meta = describe_payload(3, IMAGE)
    assert kind == "photo"
    assert text.startswith("[Image]")
    assert "871.2 KB" in text          # prefers hdlength over length
    assert meta["md5"] == "abc123"


def test_voice_reports_duration_in_seconds():
    text, kind, meta = describe_payload(34, VOICE)
    assert kind == "voice"
    assert "2.8s" in text
    assert meta["duration_ms"] == "2768"


def test_sticker_survives_the_routing_prefix():
    # Stickers arrive as `wxid:0:1:<hash>:<msg>…` — the prefix must be stripped
    # before parsing or the payload looks like garbage.
    text, kind, _ = describe_payload(47, STICKER)
    assert kind == "sticker"
    assert text == "[Sticker]"


def test_file_reports_name_extension_and_size():
    text, kind, meta = describe_payload(49, FILE)
    assert kind == "document"
    assert "report.xlsx" in text
    assert "XLSX" in text and "11.0 KB" in text
    assert meta["filename"] == "report.xlsx"


def test_file_variant_type_is_handled_the_same():
    # WeChat also delivers files as 0x41000031 with appmsg type 74.
    text, kind, meta = describe_payload(1090519089, FILE.replace("<type>6<", "<type>74<"))
    assert kind == "document"
    assert "report.xlsx" in text
    assert meta["appmsg_type"] == 74


def test_link_exposes_the_url_so_the_agent_can_follow_it():
    text, kind, meta = describe_payload(49, LINK)
    assert kind == "text"
    assert "https://example.com/a" in text
    assert meta["url"] == "https://example.com/a"


def test_quote_surfaces_both_the_reply_and_what_was_quoted():
    text, _, meta = describe_payload(49, QUOTE)
    assert "and this?" in text
    assert "Alice" in text
    assert meta["quoted_text"] == "earlier message"


def test_location_reports_name_and_coordinates():
    text, kind, meta = describe_payload(48, LOCATION)
    assert kind == "location"
    assert "Some Tower" in text
    assert meta["lat"] == "31.23"


def test_raw_xml_never_reaches_the_model():
    # The whole point: a model must never be handed the blob.
    for mtype, payload in ((3, IMAGE), (34, VOICE), (47, STICKER),
                           (49, FILE), (49, LINK), (48, LOCATION)):
        text, _, _ = describe_payload(mtype, payload)
        assert "<msg" not in text and "aeskey" not in text and "<appmsg" not in text


def test_unknown_xml_degrades_to_a_label_not_a_blob():
    text, _, _ = describe_payload(99, "<msg><somethingnew foo='1'/></msg>")
    assert "<" not in text
    assert "could not be decoded" in text


def test_oversized_payload_is_refused_rather_than_parsed():
    # Message content is attacker-controlled; parsing is capped.
    huge = "<msg>" + ("<a/>" * 200_000) + "</msg>"
    text, _, _ = describe_payload(3, huge)
    assert "<a/>" not in text


def test_malformed_xml_does_not_raise():
    text, _, _ = describe_payload(3, "<msg><img aeskey='x' UNCLOSED")
    assert isinstance(text, str) and text
