import csv
import json
import math
import time
import tempfile
from pathlib import Path
import requests
import subprocess as _sp

try:
    import browser_cookie3
    BROWSER_COOKIE3_AVAILABLE = True
except ImportError:
    BROWSER_COOKIE3_AVAILABLE = False

try:
    import whisper
    WHISPER_AVAILABLE = True
except ImportError:
    WHISPER_AVAILABLE = False

_whisper_model = None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tiktok.com/",
}

LANG_PRIORITY = [
    "eng-GB", "eng-US", "fra-FR", "deu-DE", "spa-ES",
    "ita-IT",
]


def get_whisper_model(model_size="base"):
    global _whisper_model
    if _whisper_model is None:
        if not WHISPER_AVAILABLE:
            raise RuntimeError("openai-whisper not installed: pip install openai-whisper")
        print(f"[whisper] Loading model '{model_size}'...")
        _whisper_model = whisper.load_model(model_size)
    return _whisper_model


def extract_default_scope_json(html):
    # Brace-counting parser
    marker = '"__DEFAULT_SCOPE__":'
    idx = html.find(marker)
    if idx == -1:
        return None

    brace_start = html.find('{', idx + len(marker))
    if brace_start == -1:
        return None

    depth, in_str, escape, pos = 0, False, False, brace_start
    while pos < len(html):
        ch = html[pos]
        if escape:
            escape = False
        elif ch == '\\' and in_str:
            escape = True
        elif ch == '"':
            in_str = not in_str
        elif not in_str:
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    break
        pos += 1

    try:
        return json.loads(html[brace_start:pos + 1])
    except json.JSONDecodeError:
        return None


def fetch_page_data(video_url, session):
    try:
        r = session.get(video_url, headers=HEADERS, timeout=20)
    except requests.exceptions.RequestException as e:
        return False, f"Request error: {type(e).__name__}: {e}"

    if not r.ok:
        return False, f"HTTP {r.status_code}"

    data = extract_default_scope_json(r.text)
    if not data:
        return False, "DEFAULT_SCOPE not found (bot-check or page structure changed)"

    return True, data


def try_subtitles_from_data(data, video_id, out_dir, session):
    try:
        subtitle_infos = data["webapp.video-detail"]["itemInfo"]["itemStruct"]["video"]["subtitleInfos"]
    except KeyError:
        return False, "No subtitleInfos key in JSON", "subtitleInfos"

    if not subtitle_infos:
        return False, "subtitleInfos is empty", "subtitleInfos"

    best = sorted(
        subtitle_infos,
        key=lambda x: LANG_PRIORITY.index(x.get("LanguageCodeName"))
        if x.get("LanguageCodeName") in LANG_PRIORITY else math.inf,
    )[0]

    fmt    = best.get("Format", "unknown")
    lang   = best.get("LanguageCodeName", "unknown")
    source = best.get("Source", "unknown")
    suffix = "vtt" if fmt == "webvtt" else ("json" if fmt == "creator_caption" else "bin")

    vid_dir = out_dir / str(video_id)
    vid_dir.mkdir(parents=True, exist_ok=True)
    filename = vid_dir / f"{video_id}_{fmt}_{lang}_{source}.{suffix}"

    try:
        rr = session.get(best["Url"], headers=HEADERS, timeout=20)
    except requests.exceptions.RequestException as e:
        return False, f"Subtitle download error: {e}", "subtitleInfos"

    if not rr.ok:
        return False, f"Subtitle HTTP {rr.status_code}", "subtitleInfos"

    filename.write_bytes(rr.content)
    return True, str(filename), "subtitleInfos"

def has_audio_stream(path):
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", "a", str(path)
    ]
    try:
        r = _sp.run(cmd, capture_output=True, text=True, timeout=10)
        return len(json.loads(r.stdout).get("streams", [])) > 0
    except Exception:
        return False
def extract_video_urls_from_data(data):
    try:
        video_struct = data["webapp.video-detail"]["itemInfo"]["itemStruct"]["video"]
    except KeyError:
        return []

    candidates = []
    for br in sorted(video_struct.get("bitrateInfo") or [], key=lambda b: b.get("Bitrate", 999999)):
        for url in (br.get("PlayAddr") or {}).get("UrlList") or []:
            if url not in candidates:
                candidates.append(url)

    for key in ("playAddr", "downloadAddr"):
        val = video_struct.get(key)
        if val and val not in candidates:
            candidates.append(val)

    return candidates


def download_video_from_url(candidates, tmp_dir, session, max_retries=3):
    last_error = "No candidate URLs"
    for url in candidates:
        for attempt in range(1, max_retries + 1):
            try:
                r = session.get(url, headers=HEADERS, timeout=60, stream=True)
                if not r.ok:
                    last_error = f"HTTP {r.status_code}"
                    break

                out_path = Path(tmp_dir) / "video.mp4"
                with out_path.open("wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 16):
                        if chunk:
                            fh.write(chunk)

                if out_path.stat().st_size < 1024:
                    last_error = "File too small"
                    out_path.unlink(missing_ok=True)
                    break

                return True, str(out_path)

            except (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                last_error = f"{type(e).__name__}: {e}"
                if attempt < max_retries:
                    time.sleep(2 * attempt)

    return False, f"All URLs failed: {last_error}"


def transcribe_with_whisper(audio_path, video_id, out_dir, model_size="base"):
    if not has_audio_stream(audio_path):
        return False, "No audio stream in video"

    result = get_whisper_model(model_size).transcribe(audio_path, task="transcribe", verbose=False)

    vid_dir  = out_dir / str(video_id)
    vid_dir.mkdir(parents=True, exist_ok=True)
    filename = vid_dir / f"{video_id}_whisper_{result.get('language', 'xx')}.vtt"

    with filename.open("w", encoding="utf-8") as fh:
        fh.write("WEBVTT\n\n")
        for seg in result.get("segments", []):
            h, m = divmod(int(seg["start"]), 3600)
            m, s = divmod(m, 60)
            start = f"{h:02d}:{m:02d}:{s:06.3f}"
            h, m = divmod(int(seg["end"]), 3600)
            m, s = divmod(m, 60)
            end = f"{h:02d}:{m:02d}:{s:06.3f}"
            fh.write(f"{start} --> {end}\n{seg['text'].strip()}\n\n")

    return True, str(filename)


def download_transcript(video_url, video_id, out_dir, session, whisper_model_size="base", use_whisper_fallback=True):
    ok, data = fetch_page_data(video_url, session)
    if not ok:
        return False, data, "fetch_failed"

    ok, info, method = try_subtitles_from_data(data, video_id, out_dir, session)
    if ok:
        return ok, info, method

    if not use_whisper_fallback:
        return False, info, "no_captions"

    if not WHISPER_AVAILABLE:
        return False, "openai-whisper not installed (pip install openai-whisper)", "whisper"

    video_urls = extract_video_urls_from_data(data)
    if not video_urls:
        return False, "Could not find video URL in page JSON", "whisper"

    with tempfile.TemporaryDirectory() as tmp_dir:
        dl_ok, dl_info = download_video_from_url(video_urls, tmp_dir, session)
        if not dl_ok:
            return False, f"Video download failed: {dl_info}", "whisper"

        w_ok, w_info = transcribe_with_whisper(dl_info, video_id, out_dir, model_size=whisper_model_size)
        if w_ok:
            return True, w_info, "whisper"
        return False, f"Whisper failed: {w_info}", "whisper"


def build_session(cookies_browser="chrome"):
    session = requests.Session()
    if BROWSER_COOKIE3_AVAILABLE:
        try:
            jar = browser_cookie3.chrome(domain_name=".tiktok.com")
            session.cookies.update(jar)
            print("[cookies] Loaded TikTok cookies from Chrome.")
        except Exception as e:
            print(f"[cookies] Could not load Chrome cookies ({e}). Proceeding without.")
    else:
        print("[cookies] browser_cookie3 not installed — running unauthenticated.")
    return session


def main():
    DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "TikTok"
    csv_path = DATA_DIR / "query_result_2026-01-17T19_26_44.636561Z.csv"
    out_dir  = DATA_DIR / "tiktok_transcripts_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    WHISPER_MODEL      = "base"
    USE_WHISPER        = True
    COOKIES_BROWSER    = "chrome"
    SLEEP_BETWEEN_REQS = 5.0
    PAUSE_EVERY        = 50
    PAUSE_DURATION     = 60

    failed_path = DATA_DIR / "transcript_failed_ids.txt"

    session = build_session(cookies_browser=COOKIES_BROWSER)

    already_done = set(p.parent.name for p in out_dir.rglob("*.vtt"))
    print(f"Already collected: {len(already_done)} transcripts — skipping these.")

    # load previously failed ids
    if failed_path.exists():
        failed_ids = set(failed_path.read_text().splitlines())
    else:
        failed_ids = set()
    print(f"Previously failed: {len(failed_ids)} videos — skipping these.")

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    ok_count = sub_count = whis_count = fail_count = skipped = attempted = 0
    new_failed = []

    for i, row in enumerate(rows, start=1):
        video_id = row.get("VideoId")
        username = row.get("AuthorName")
        if not video_id or not username:
            continue

        if video_id in already_done or video_id in failed_ids:
            skipped += 1
            continue

        attempted += 1
        if attempted % PAUSE_EVERY == 0:
            print(f"Pausing {PAUSE_DURATION}s to avoid rate limiting...")
            time.sleep(PAUSE_DURATION)

        url = f"https://www.tiktok.com/@{username}/video/{video_id}"
        ok, info, method = download_transcript(
            url, video_id, out_dir, session,
            whisper_model_size=WHISPER_MODEL,
            use_whisper_fallback=USE_WHISPER,
        )

        print(f"[{i}/{len(rows)}] {video_id} -> {'OK' if ok else 'NO'} [{method}] | {info}")

        if ok:
            ok_count += 1
            already_done.add(video_id)
            if method == "whisper":
                whis_count += 1
            else:
                sub_count += 1
        if not ok:
            fail_count += 1
            new_failed.append(video_id)
            with failed_path.open("a") as f:
                f.write(video_id + "\n")

        time.sleep(SLEEP_BETWEEN_REQS)

    # write any remaining failures
    if new_failed:
        remainder = new_failed[-(len(new_failed) % 100) or len(new_failed):]
        with failed_path.open("a") as f:
            f.write("\n".join(remainder) + "\n")

    print(
        f"\nDone. {ok_count}/{len(rows)} transcripts saved to: {out_dir}\n"
        f"  via subtitleInfos : {sub_count}\n"
        f"  via Whisper       : {whis_count}\n"
        f"  failed            : {fail_count}\n"
        f"  skipped (done)    : {skipped}"
    )


if __name__ == "__main__":
    main()
