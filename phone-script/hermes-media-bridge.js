/*
 * hermes-media-bridge.js — WeKit user script
 *
 * WHY THIS EXISTS
 * ---------------
 * WeKit can download a received attachment, but every one of its download
 * endpoints is keyed by `msgSvrId`, and no WeKit API ever hands one out:
 * `wait-for-new-message` returns ConvId/Sender/Type/Content and nothing else,
 * and `get-chat-history` returns only sender/content. So an agent on the other
 * end of the API can see that a file arrived but can never fetch it.
 *
 * This script closes that gap from inside WeChat, without patching WeKit.
 * It hooks the same WCDB insert WeKit itself hooks, reads `msgSvrId` straight
 * off the ContentValues, and asks WeKit's own local API to download the
 * attachment. The file lands in WeKit's download folder, where the agent can
 * collect it by name.
 *
 * INSTALL
 *   adb push hermes-media-bridge.js \
 *     /sdcard/Android/data/com.tencent.mm/WeKit/scripts_js/
 * then enable "JavaScript scripting" in WeKit and restart WeChat.
 *
 * THREADING
 * The insert hook runs on WeChat's database thread. Doing network I/O there
 * would stall the app — a large attachment would freeze WeChat outright — so
 * the hook only records an id and returns. A worker thread started from
 * onLoad() does the downloading.
 */

// EDIT THIS before pushing the script to the phone: it must equal the token set
// in WeKit's "API + MCP server" settings. It is a real credential — full read
// and send access to the WeChat account — so it stays a placeholder in the repo
// and never gets committed with a live value.
var TOKEN = "PUT-YOUR-WEKIT-TOKEN-HERE";
var API = "http://127.0.0.1:3001/api";       // WeKit listens inside this process
var QUEUE_PREFIX = "hermes_pending_";
var POLL_INTERVAL_S = 2;
// Attempts before an item is abandoned. Bounded so one permanently broken
// message cannot be retried forever, but more than one so a timeout or a
// momentary hiccup does not silently cost the user their attachment.
var MAX_TRIES = 3;

/**
 * One-line summary of an http response for the log.
 * WeKit builds the response object itself and an error path can leave `status`
 * or `ok` absent, so nothing here assumes a field exists — an earlier version
 * printed "status=undefined" and called a working download a failure.
 */
function describe(resp) {
    if (!resp) return "no response";
    var status = (typeof resp.status === "number") ? resp.status : "?";
    var out = "status=" + status + " ok=" + (resp.ok === true);
    if (resp.error) out += " err=" + resp.error;
    else if (resp.body) out += " body=" + String(resp.body).substring(0, 120);
    return out;
}

// WeChat message type -> the WeKit download endpoint that understands it.
// 1090519089 (0x41000031) is the variant WeChat uses for file transfers.
var ENDPOINT = {
    3: "image",
    34: "voice",
    47: "sticker",
    49: "file",
    1090519089: "file"
};

// The live SQLiteDatabase handle, captured from the insert hook. WeKit exposes
// no way to look a message up by anything other than msgSvrId, and no way to
// obtain a msgSvrId at all — but the hook hands us the database itself, so
// messages that arrived before this script was installed are still reachable.
var lastDb = null;

/**
 * Queue any received media still sitting in the message table.
 * Runs once the database handle is known, so a file sent before the script was
 * installed can still be collected.
 */
function backfill(limit) {
    if (!lastDb) return 0;
    var found = 0;
    var cur = null;
    try {
        cur = lastDb.rawQuery(
            "SELECT msgSvrId, type, talker FROM message " +
            "WHERE isSend = 0 AND msgSvrId > 0 AND type IN (3,34,47,49,1090519089) " +
            "ORDER BY createTime DESC LIMIT " + limit, null);
        while (cur.moveToNext()) {
            var id = cur.getString(0);   // string, not number — see the hook
            var type = cur.getInt(1);
            var talker = cur.getString(2) || "";
            if (!ENDPOINT[type] || !id || id === "0") continue;
            storage.set(QUEUE_PREFIX + id, type + "|" + talker);
            log.i("hermes-media-bridge: backfilled msgSvrId=" + id + " type=" + type);
            found++;
        }
    } catch (e) {
        log.e("hermes-media-bridge: backfill failed: " + e);
    } finally {
        if (cur) { try { cur.close(); } catch (e2) {} }
    }
    return found;
}

function onLoad() {
    log.i("hermes-media-bridge: starting");

    // Fail loudly rather than at the first download: an unedited token turns
    // every fetch into a 401 whose only trace is a warning line, which reads
    // like "media retrieval is flaky" instead of "the script was never set up".
    if (TOKEN === "PUT-YOUR-WEKIT-TOKEN-HERE") {
        log.e("hermes-media-bridge: TOKEN is still the placeholder — edit it to " +
              "match WeKit's API server token, then reload the script. " +
              "No media will be downloaded until you do.");
    }

    xposed.hookAfter(
        "com.tencent.wcdb.database.SQLiteDatabase",
        "insertWithOnConflict",
        function (thisObj, args, result) {
            // Keep this path as short as possible: it is on WeChat's DB thread.
            try {
                if (args[0] !== "message") return;
                lastDb = thisObj;      // the live SQLiteDatabase — see backfill()
                var values = args[2];

                // One line per message row, so it is possible to tell "the hook
                // never fired" apart from "the hook fired and skipped it".
                log.d("hermes-media-bridge: insert type=" + values.getAsInteger("type") +
                      " isSend=" + values.getAsInteger("isSend") +
                      " msgSvrId=" + values.getAsString("msgSvrId"));

                if (values.getAsInteger("isSend") == 1) return;   // our own message

                var type = values.getAsInteger("type");
                if (!type || !ENDPOINT[type]) return;

                // Read the id as a STRING. msgSvrId is a 64-bit value well past
                // 2^53, so letting it become a JS number silently rounds it —
                // the download then asks for a message id that does not exist.
                var id = values.getAsString("msgSvrId");
                if (!id || id === "0") return;

                var talker = values.getAsString("talker") || "";
                storage.set(QUEUE_PREFIX + id, type + "|" + talker);
                log.i("hermes-media-bridge: queued msgSvrId=" + id + " type=" + type);
            } catch (e) {
                log.e("hermes-media-bridge: hook failed: " + e);
            }
        }
    );

    // Downloader. Runs on its own thread and only ever touches globals, so it
    // does not depend on closing over anything from the hook.
    task.run(function () {
        log.i("hermes-media-bridge: downloader running");
        var didBackfill = false;
        while (true) {
            // The database handle only exists after the first insert, so the
            // one-shot backfill waits for it rather than running at load time.
            if (!didBackfill && lastDb) {
                didBackfill = true;
                log.i("hermes-media-bridge: backfill queued " + backfill(10) + " item(s)");
            }
            try {
                var keys = storage.keys();
                for (var i = 0; i < keys.length; i++) {
                    var key = keys[i];
                    if (key.indexOf(QUEUE_PREFIX) !== 0) continue;

                    var id = key.substring(QUEUE_PREFIX.length);
                    var parts = String(storage.get(key)).split("|");
                    var endpoint = ENDPOINT[parseInt(parts[0], 10)];
                    var talker = parts.length > 1 ? parts[1] : "";
                    var tries = parts.length > 2 ? parseInt(parts[2], 10) : 0;
                    if (isNaN(tries)) tries = 0;

                    if (!endpoint) { storage.remove(key); continue; }

                    // An entry is dropped only on a *definite* outcome, or once
                    // it has burned its attempts. An earlier version dropped it
                    // before even trying, which meant one transient failure lost
                    // the attachment for good.
                    tries++;
                    if (tries >= MAX_TRIES) {
                        storage.remove(key);
                    } else {
                        storage.set(key, parts[0] + "|" + talker + "|" + tries);
                    }

                    // For images and files, ask WeChat to pull the bytes from
                    // the CDN into its own cache first. A message WeChat never
                    // auto-downloaded (large image on mobile data, an old file)
                    // has no local bytes, so a bare GET would only ever return a
                    // thumbnail or fail — this closes that gap. The cache call is
                    // idempotent, so running it on already-cached media is cheap.
                    // Voice and stickers have no cache endpoint; they GET directly.
                    if (endpoint === "image" || endpoint === "file") {
                        // Isolated: the cache step is an *optimisation* — it
                        // helps when WeChat never downloaded the media — so if
                        // it throws (an older WeKit without this endpoint, a
                        // socket error) the download must still be attempted.
                        // Sharing the outer try/catch would have skipped the GET.
                        try {
                            var cacheUrl = API + "/messages/" + id + "/" + endpoint + "/cache" +
                                (endpoint === "file" && talker ?
                                    ("?talker=" + encodeURIComponent(talker)) : "");
                            var cresp = http.post(cacheUrl, null, null,
                                                  { Authorization: "Bearer " + TOKEN });
                            // Logged either way: on success this is the only
                            // evidence the CDN step ran at all, and inferring it
                            // from a missing warning is not evidence.
                            log.i("hermes-media-bridge: cache " + endpoint + " " + id +
                                  " -> " + describe(cresp));
                        } catch (ce) {
                            log.w("hermes-media-bridge: cache " + endpoint + " " + id +
                                  " threw (" + ce + ") — downloading anyway");
                        }
                    }

                    var url = API + "/messages/" + id + "/" + endpoint;
                    var resp = http.get(url, talker ? { talker: talker } : {},
                                        { Authorization: "Bearer " + TOKEN });
                    if (resp && resp.ok === true) {
                        storage.remove(key);          // definite success
                        log.i("hermes-media-bridge: downloaded " + id + " -> " + resp.body);
                    } else {
                        // NOT necessarily a failure, which is why the retry is
                        // worth having. This endpoint answers with the saved
                        // *path*, not the bytes, so the 10s read timeout baked
                        // into WeKit's HTTP client is spent on WeChat pulling
                        // from the CDN — a large attachment blows through it
                        // while the download itself carries on. The next attempt
                        // then finds the file already cached and returns almost
                        // at once. The agent locates media by scanning the
                        // download folder anyway, so a timeout here does not
                        // mean the user lost the file.
                        log.w("hermes-media-bridge: download " + endpoint + " " + id +
                              " inconclusive (attempt " + tries + "/" + MAX_TRIES + ") -> " +
                              describe(resp) + " — the file may still have landed; " +
                              "large media exceeds WeKit's 10s HTTP read timeout");
                    }
                }
            } catch (e) {
                log.e("hermes-media-bridge: downloader error: " + e);
            }
            datetime.sleepS(POLL_INTERVAL_S);
        }
    });
}
