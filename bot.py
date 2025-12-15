#!/usr/bin/env python3
"""
Final bot.py — Bilibili -> YouTube uploader (robust, safe, copy-paste ready)

- Downloads best available video (bestvideo+bestaudio/best) and merges to mp4.
- Retries downloads up to DOWNLOAD_RETRIES times, then skips.
- Attempts up to MAX_VIDEOS successful uploads, stops if SKIP_LIMIT skipped videos encountered.
- Translates titles via free translators (googletrans / deep-translator / unidecode) with caching.
- Uses token.json (uploaded to repo) or env YOUTUBE_TOKEN_JSON to authenticate to YouTube.
- Uploads with EMPTY description (no source link).
- Sets thumbnail.jpg (if available) after a successful upload.
- Improved resumable uploader with progress prints, larger chunk size and backoff retries.
"""

import os
import sys
import re
import json
import time
import random
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import requests

# Google API
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# ---------- Config (from env) ----------
BILIBILI_CHANNEL_URL = os.getenv("BILIBILI_CHANNEL_URL", "https://space.bilibili.com/87877349/video")
MAX_VIDEOS = int(os.getenv("BILIBILI_MAX_VIDEOS", "1"))
BILIBILI_COOKIES_ENV = os.getenv("BILIBILI_COOKIES")  # optional cookies content
BOT_USER_AGENT = os.getenv("BOT_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36")

# YouTube / token handling
YOUTUBE_TOKEN_ENV = os.getenv("YOUTUBE_TOKEN_JSON")  # full token.json content, optional
YOUTUBE_CLIENT_SECRETS_PATH = os.getenv("YOUTUBE_CLIENT_SECRET_PATH", "client_secret.json")
YOUTUBE_PRIVACY_STATUS = os.getenv("YOUTUBE_PRIVACY_STATUS", "public")  # public / unlisted / private
YOUTUBE_CATEGORY_ID = os.getenv("YOUTUBE_CATEGORY_ID", "22")
YOUTUBE_DESCRIPTION = ""  # always empty by design per user's choice

# Retry/skip settings
DOWNLOAD_RETRIES = int(os.getenv("DOWNLOAD_RETRIES", "3"))
SKIP_LIMIT = int(os.getenv("SKIP_LIMIT", "5"))

# Repo paths
REPO_PATH = Path.cwd()
DOWNLOADED_IDS_PATH = REPO_PATH / "downloaded_ids.json"
TRANSLATIONS_PATH = REPO_PATH / "translations.json"
FALLBACK_TITLE_PATH = REPO_PATH / "fallback_title.txt"
TOKEN_PATH = REPO_PATH / "token.json"

# Media extensions that count
MEDIA_EXTS = (".mp4", ".mkv", ".m4a", ".webm", ".flv", ".ts", ".mov", ".avi", ".mp3", ".aac")

# YouTube scopes (token must already include these)
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# ---------- Utilities ----------
def sanitize_filename_keep_unicode(s: str, max_length=140) -> str:
    if not s:
        s = "video"
    s = s.strip()
    s = re.sub(r"[\x00-\x1f\x7f<>:\"/\\|?*]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s[:max_length]
    s = s.replace(" ", "_")
    if not s:
        s = "video"
    return s

def sanitize_title_for_youtube(s: str, max_len=100) -> str:
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    s = re.sub(r"\s+", " ", s)
    s = s[:max_len]
    return s

def load_json_set(path: Path):
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def save_json_obj(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def save_downloaded_ids_and_commit(path: Path, ids_set, github_token: Optional[str] = None):
    save_json_obj(path, sorted(list(ids_set)))
    print(f"Saved {len(ids_set)} IDs to {path}")
    gh = github_token or os.getenv("GITHUB_TOKEN")
    if not gh:
        return
    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", str(path)], check=True)
        subprocess.run(["git", "commit", "-m", "Update downloaded_ids.json [skip ci]"], check=False)
        repo = os.getenv("GITHUB_REPOSITORY")
        branch = os.getenv("GITHUB_REF", "refs/heads/main").split("/")[-1]
        remote = f"https://x-access-token:{gh}@github.com/{repo}.git"
        subprocess.run(["git", "push", remote, f"HEAD:{branch}"], check=False)
        print("Pushed downloaded_ids.json to repo")
    except Exception as e:
        print("Failed to commit/push downloaded_ids.json:", e)

def save_translations_and_commit(path: Path, translations: dict, github_token: Optional[str] = None):
    try:
        save_json_obj(path, translations)
        print(f"Saved translations to {path}")
    except Exception as e:
        print("Failed to save translations:", e)
    gh = github_token or os.getenv("GITHUB_TOKEN")
    if not gh:
        return
    try:
        subprocess.run(["git", "add", str(path)], check=True)
        subprocess.run(["git", "commit", "-m", "Update translations.json [skip ci]"], check=False)
        repo = os.getenv("GITHUB_REPOSITORY")
        branch = os.getenv("GITHUB_REF", "refs/heads/main").split("/")[-1]
        remote = f"https://x-access-token:{gh}@github.com/{repo}.git"
        subprocess.run(["git", "push", remote, f"HEAD:{branch}"], check=False)
        print("Pushed translations.json to repo")
    except Exception as e:
        print("Failed to commit/push translations.json:", e)

def find_downloaded_file_by_prefix(prefix: str):
    files = sorted(os.listdir("."), key=lambda x: os.path.getmtime(x), reverse=True)
    for fname in files:
        if fname.startswith(prefix) and fname.lower().endswith(MEDIA_EXTS):
            return os.path.abspath(fname)
    return None

def remove_partial_files(prefix: str):
    for f in os.listdir("."):
        if f.startswith(prefix):
            try:
                if any(f.lower().endswith(ext) for ext in MEDIA_EXTS) or f.endswith(".part") or f.endswith(".tmp"):
                    os.remove(f)
                    print("Removed partial file:", f)
            except Exception:
                pass

# ---------- Translators ----------
def load_translations(path: Path):
    if path.exists():
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    return {}

def try_googletrans(text: str):
    try:
        from googletrans import Translator
        translator = Translator()
        res = translator.translate(text, dest="en")
        return res.text
    except Exception:
        return None

def try_deep_google(text: str):
    try:
        from deep_translator import GoogleTranslator
        return GoogleTranslator(source='auto', target='en').translate(text)
    except Exception:
        return None

def try_deep_libre(text: str):
    try:
        from deep_translator import LibreTranslator
        return LibreTranslator(source='auto', target='en').translate(text)
    except Exception:
        return None

def try_mymemory(text: str):
    try:
        from deep_translator import MyMemoryTranslator
        return MyMemoryTranslator(source='auto', target='en').translate(text)
    except Exception:
        return None

def try_unidecode(text: str):
    try:
        from unidecode import unidecode
        return unidecode(text)
    except Exception:
        return None

def translate_title_for_vid(vid: str, original_title: str, translations_cache: dict):
    if not original_title:
        return original_title or vid
    if vid in translations_cache and translations_cache[vid]:
        return translations_cache[vid]
    attempts = [
        ("googletrans", try_googletrans),
        ("deep_google", try_deep_google),
        ("libre", try_deep_libre),
        ("mymemory", try_mymemory),
        ("unidecode", try_unidecode),
    ]
    last_success = None
    for name, fn in attempts:
        try:
            translated = fn(original_title)
            if translated and translated.strip():
                last_success = translated.strip()
                print(f"Translated using {name}: {last_success}")
                break
        except Exception as e:
            print(f"Translator {name} error (ignored): {e}")
        time.sleep(random.uniform(0.4, 0.9))
    if last_success:
        translations_cache[vid] = last_success
        return last_success
    try:
        if FALLBACK_TITLE_PATH.exists():
            fallback = FALLBACK_TITLE_PATH.read_text(encoding="utf-8").strip()
            if fallback:
                translations_cache[vid] = fallback
                print("Using fallback title from file for vid", vid)
                return fallback
    except Exception:
        pass
    final = original_title or vid
    translations_cache[vid] = final
    return final

# ---------- Playlist helpers ----------
def fetch_flat_playlist_entries(channel_url, cookies_path=None, max_retries=4, initial_delay=4):
    cmd_base = [
        "yt-dlp",
        "--flat-playlist",
        "-j",
        "--no-warnings",
        "--no-progress",
        "--user-agent", BOT_USER_AGENT,
    ]
    cmd = cmd_base + (["--cookies", str(cookies_path)] if cookies_path else []) + [channel_url]
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            print(f"flat-playlist attempt {attempt} ...")
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, timeout=120)
            stderr = (proc.stderr or "").strip()
            if stderr and ("Request is rejected by server" in stderr or "Request is blocked" in stderr or "352" in stderr or "412" in stderr):
                print(f"flat-playlist blocked (attempt {attempt}): {stderr.splitlines()[:2]}")
                time.sleep(delay)
                delay *= 2
                continue
            lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
            entries = []
            for ln in lines:
                try:
                    obj = json.loads(ln)
                    entries.append(obj)
                except Exception:
                    continue
            return entries
        except subprocess.CalledProcessError as e:
            print(f"flat-playlist failed (attempt {attempt}): {e}; stderr: {(e.stderr or '')[:200]}")
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            print(f"flat-playlist unexpected error: {e}")
            time.sleep(delay)
            delay *= 2
    return []

def fetch_single_item_metadata(channel_url, item_index, cookies_path=None, max_retries=3, initial_delay=3):
    cmd_base = [
        "yt-dlp",
        "-j",
        "--no-warnings",
        "--no-progress",
        "--user-agent", BOT_USER_AGENT,
        "--playlist-items", str(item_index),
    ]
    cmd = cmd_base + (["--cookies", str(cookies_path)] if cookies_path else []) + [channel_url]
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, timeout=60)
            stderr = (proc.stderr or "").strip()
            if stderr and ("Request is rejected by server" in stderr or "Request is blocked" in stderr or "352" in stderr or "412" in stderr):
                print(f"single-item blocked index={item_index} (attempt {attempt}).")
                time.sleep(delay)
                delay *= 2
                continue
            out = proc.stdout.strip()
            if not out:
                return None
            data = json.loads(out)
            return data
        except subprocess.CalledProcessError as e:
            print(f"single-item failed index={item_index} attempt={attempt}: {e}; stderr: {(e.stderr or '')[:200]}")
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            print(f"single-item unexpected error index={item_index}: {e}")
            time.sleep(delay)
            delay *= 2
    return None

# ---------- YouTube helpers ----------
def ensure_token_file():
    if TOKEN_PATH.exists():
        print("token.json found in repo root.")
        return True
    token_env = YOUTUBE_TOKEN_ENV or os.getenv("YOUTUBE_TOKEN_JSON")
    if token_env:
        try:
            TOKEN_PATH.write_text(token_env, encoding="utf-8")
            print("Wrote token.json from YOUTUBE_TOKEN_JSON env.")
            return True
        except Exception as e:
            print("Failed to write token.json from env:", e)
            return False
    print("No token.json found and no YOUTUBE_TOKEN_JSON env present.")
    return False

def get_youtube_service():
    if not ensure_token_file():
        raise SystemExit("token.json missing (add token.json to repo root or set YOUTUBE_TOKEN_JSON secret).")
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    service = build("youtube", "v3", credentials=creds, cache_discovery=False)
    return service

def youtube_upload_video(service, file_path, title, description, privacy="public", category_id="22"):
    """
    Uploads video via resumable upload. Returns the uploaded YouTube videoId or raises.
    Improved: larger chunk, progress prints, exponential backoff on transient errors.
    """
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": str(category_id),
        },
        "status": {
            "privacyStatus": privacy,
        }
    }

    CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB
    media = MediaFileUpload(file_path, chunksize=CHUNK_SIZE, resumable=True, mimetype="video/mp4")
    request = service.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    retry = 0
    MAX_RETRIES = 12
    while response is None:
        try:
            print("Initiating resumable upload to YouTube...")
            status, response = request.next_chunk()
            if status:
                try:
                    prog = getattr(status, "progress", lambda: None)()
                    if prog is None:
                        prog = getattr(status, "resumable_progress", None)
                    if prog is not None:
                        try:
                            percent = int(prog * 100)
                            print(f"Upload progress: {percent}%")
                        except Exception:
                            print("Upload progressing...")
                    else:
                        print("Upload progressing...")
                except Exception:
                    print("Upload progressing...")
            if response:
                if "id" in response:
                    print("Upload completed, video ID:", response["id"])
                    return response["id"]
                else:
                    raise Exception("Upload finished but no video id returned: " + str(response))
        except HttpError as e:
            retry += 1
            status_code = None
            try:
                status_code = e.resp.status
            except Exception:
                pass
            print(f"HttpError during upload (attempt {retry}):", e)
            if status_code and 400 <= status_code < 500 and status_code not in (429, 408):
                raise
            if retry > MAX_RETRIES:
                raise
            sleep_seconds = min(600, (2 ** retry) + random.uniform(0, 3))
            print(f"Sleeping {sleep_seconds:.1f}s before retrying upload...")
            time.sleep(sleep_seconds)
        except Exception as e:
            retry += 1
            print(f"Upload error (attempt {retry}): {e}")
            if retry > MAX_RETRIES:
                raise
            sleep_seconds = min(600, (2 ** retry) + random.uniform(0, 3))
            print(f"Sleeping {sleep_seconds:.1f}s before retrying upload...")
            time.sleep(sleep_seconds)

def youtube_set_thumbnail(service, video_id: str, thumbnail_path: str):
    try:
        media = MediaFileUpload(thumbnail_path, mimetype="image/jpeg")
        request = service.thumbnails().set(videoId=video_id, media_body=media)
        resp = request.execute()
        print("Thumbnail set response:", resp)
    except Exception as e:
        print("Thumbnail set failed:", e)

# ---------- Main ----------
def main():
    cookies_path = None
    if BILIBILI_COOKIES_ENV:
        cookies_path = REPO_PATH / "cookies.txt"
        cookies_path.write_text(BILIBILI_COOKIES_ENV, encoding="utf-8")
        print("Wrote cookies to", cookies_path)

    if os.getenv("GITHUB_TOKEN"):
        try:
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
            subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], check=True)
        except Exception:
            pass

    has_ffmpeg = shutil.which("ffmpeg") is not None
    if not has_ffmpeg:
        print("Warning: ffmpeg not found. Merging may fail or produce audio-only files.")

    try:
        yt_service = get_youtube_service()
    except Exception as e:
        print("YouTube service initialization failed:", e)
        raise SystemExit("YouTube auth missing or invalid. Ensure token.json exists or YOUTUBE_TOKEN_JSON secret is set.")

    downloaded_ids = load_json_set(DOWNLOADED_IDS_PATH)
    translations_cache = load_translations(TRANSLATIONS_PATH)
    print(f"Loaded {len(downloaded_ids)} downloaded IDs and {len(translations_cache)} translations")

    print("Attempting flat-playlist fetch (low load)...")
    entries = fetch_flat_playlist_entries(BILIBILI_CHANNEL_URL, cookies_path=cookies_path) or []

    candidates = []
    for entry in entries:
        vid = entry.get("id") or entry.get("url") or entry.get("webpage_url")
        if not vid:
            continue
        if vid in downloaded_ids:
            print(f"Skipping already-downloaded (flat): {vid}")
            continue
        candidates.append({"id": vid, "webpage_url": f"https://www.bilibili.com/video/{vid}"})

    if not candidates:
        print("Flat-playlist returned nothing; using per-item fallback (limited checks).")
        max_checks = int(os.getenv("BILIBILI_MAX_CHECKS", "200"))
        idx = 1
        while len(candidates) < max_checks:
            data = fetch_single_item_metadata(BILIBILI_CHANNEL_URL, idx, cookies_path=cookies_path)
            idx += 1
            if not data:
                if idx > max_checks:
                    break
                continue
            vid = data.get("id")
            if not vid or vid in downloaded_ids:
                continue
            candidates.append({"id": vid, "webpage_url": data.get("webpage_url") or f"https://www.bilibili.com/video/{vid}"})
            if len(candidates) >= max_checks:
                break

    if not candidates:
        print("No candidate videos found. Exiting.")
        return

    print(f"Found {len(candidates)} candidate videos. Will attempt downloads until {MAX_VIDEOS} successes or {SKIP_LIMIT} skips.")

    successes = 0
    skips = 0
    idx = 0
    github_token = os.getenv("GITHUB_TOKEN")

    while successes < MAX_VIDEOS and idx < len(candidates) and skips < SKIP_LIMIT:
        cand = candidates[idx]
        idx += 1
        vid = cand["id"]
        webpage = cand["webpage_url"]
        print(f"Processing candidate {vid} ({idx}/{len(candidates)})")

        meta_json = None
        try:
            meta_cmd = ["yt-dlp", "-j", "--no-warnings", "--no-progress", "--user-agent", BOT_USER_AGENT]
            if cookies_path:
                meta_cmd += ["--cookies", str(cookies_path)]
            meta_cmd += [webpage]
            meta_proc = subprocess.run(meta_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, timeout=60)
            meta_json = json.loads(meta_proc.stdout.strip())
        except Exception as e:
            print("Failed to fetch metadata for", vid, ":", e)

        orig_title = (meta_json.get("title") if meta_json else "") or ""
        final_title = translate_title_for_vid(vid, orig_title, translations_cache)
        save_translations_and_commit(TRANSLATIONS_PATH, translations_cache, github_token=github_token)

        safe_name = sanitize_filename_keep_unicode(final_title or vid)
        print(f"Title -> '{orig_title}' -> Translated -> '{final_title}' -> Filename -> '{safe_name}'")

        thumb_local = None
        thumb_url = meta_json.get("thumbnail") if meta_json else None
        if thumb_url:
            try:
                r = requests.get(thumb_url, timeout=20)
                if r.status_code == 200:
                    thumb_local = "thumbnail.jpg"
                    with open(thumb_local, "wb") as fh:
                        fh.write(r.content)
                    print("Saved thumbnail as:", thumb_local)
            except Exception as e:
                print("Thumbnail download failed:", e)
                thumb_local = None

        download_ok = False
        attempt = 0
        downloaded_file = None
        while attempt < DOWNLOAD_RETRIES and not download_ok:
            attempt += 1
            print(f"Download attempt {attempt}/{DOWNLOAD_RETRIES} for {vid}")
            out_template = f"{safe_name}.%(ext)s"
            dl_cmd = [
                "yt-dlp",
                "-f", "bestvideo+bestaudio/best",
                "--merge-output-format", "mp4",
                "-o", out_template,
                "--no-warnings",
                "--no-progress",
                "--user-agent", BOT_USER_AGENT,
                "--no-playlist",
            ]
            if cookies_path:
                dl_cmd += ["--cookies", str(cookies_path)]
            dl_cmd += [webpage]

            dl_proc = subprocess.run(dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stderr_snip = (dl_proc.stderr or "").strip()[:2000]
            if stderr_snip:
                print("yt-dlp stderr (short):", stderr_snip)

            downloaded_file = find_downloaded_file_by_prefix(safe_name)
            if downloaded_file and os.path.getsize(downloaded_file) > 100:
                print("Downloaded file located:", downloaded_file)
                download_ok = True
                break
            else:
                remove_partial_files(safe_name)
                if attempt < DOWNLOAD_RETRIES:
                    sleep_for = random.uniform(5.0, 12.0)
                    print(f"Download failed for {vid} on attempt {attempt}. Waiting {sleep_for:.1f}s before retry.")
                    time.sleep(sleep_for)
                else:
                    print(f"Download failed for {vid} after {DOWNLOAD_RETRIES} attempts; will skip this video.")

        if not download_ok:
            skips += 1
            print(f"Skipping video {vid}. Skips so far this run: {skips}/{SKIP_LIMIT}")
            try:
                if thumb_local and os.path.exists(thumb_local):
                    os.remove(thumb_local)
            except Exception:
                pass
            continue

        video_id = None
        try:
            yt_title = sanitize_title_for_youtube(final_title) or vid
            print("Uploading to YouTube with title:", yt_title)
            video_id = youtube_upload_video(
                yt_service,
                downloaded_file,
                yt_title,
                "",  # empty description
                privacy=YOUTUBE_PRIVACY_STATUS,
                category_id=YOUTUBE_CATEGORY_ID
            )
        except Exception as e:
            print("Video upload failed:", e)
            try:
                os.remove(downloaded_file)
            except Exception:
                pass
            skips += 1
            print(f"Skipping video {vid} due to upload failure. Skips so far: {skips}/{SKIP_LIMIT}")
            if skips >= SKIP_LIMIT:
                print("Reached skip limit after upload failure; stopping further processing.")
                break
            continue

        if video_id:
            try:
                if thumb_local and os.path.exists(thumb_local):
                    try:
                        youtube_set_thumbnail(yt_service, video_id, thumb_local)
                    except Exception as e:
                        print("Thumbnail set failed:", e)
                    try:
                        os.remove(thumb_local)
                    except Exception:
                        pass
            except Exception as e:
                print("Thumbnail handling unexpected error:", e)

            downloaded_ids.add(vid)
            save_downloaded_ids_and_commit(DOWNLOADED_IDS_PATH, downloaded_ids, github_token=github_token)
            save_translations_and_commit(TRANSLATIONS_PATH, translations_cache, github_token=github_token)

            try:
                os.remove(downloaded_file)
            except Exception:
                pass

            successes += 1
            print(f"Successfully processed {vid} -> YouTube ID {video_id}. Total successes this run: {successes}/{MAX_VIDEOS}")
        else:
            print(f"Upload did not return a video_id for {vid}; skipping finalization.")
            try:
                if downloaded_file and os.path.exists(downloaded_file):
                    os.remove(downloaded_file)
            except Exception:
                pass
            skips += 1
            if skips >= SKIP_LIMIT:
                print("Reached skip limit; stopping.")
                break

        time.sleep(random.uniform(1.0, 2.0))

    print(f"Run finished: {successes} successful uploads, {skips} skipped videos.")

    try:
        if BILIBILI_COOKIES_ENV:
            p = REPO_PATH / "cookies.txt"
            if p.exists():
                p.unlink()
                print("Removed cookies.txt for security.")
    except Exception:
        pass

if __name__ == "__main__":
    main()
