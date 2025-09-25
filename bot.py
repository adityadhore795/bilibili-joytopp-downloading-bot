#!/usr/bin/env python3
"""
Bilibili -> Dropbox bot (cookie-aware) with robust retry policy.

Rules implemented:
- Try to download each video in best quality (bv*+ba/b) and merge to mp4.
- If a download fails, retry the same video up to DOWNLOAD_RETRIES times.
- If still failing, do NOT save that video's ID; increment SKIP counter and move to the next video.
- If SKIP counter reaches SKIP_LIMIT in a single run, stop processing further videos.
- Continue until MAX_VIDEOS successful downloads or stop due to SKIP_LIMIT or no more entries.
- All other features retained: cookies support, thumbnail as thumbnail.jpg, translation chain + caching,
  chunked Dropbox uploads, downloaded_ids.json & translations.json persistence and commit, cleanup.
"""

import os
import subprocess
import json
import dropbox
import requests
import re
import time
import random
from pathlib import Path
import shutil
import sys

# ---------- Config ----------
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
BILIBILI_CHANNEL_URL = os.getenv("BILIBILI_CHANNEL_URL", "https://space.bilibili.com/87877349/video")
MAX_VIDEOS = int(os.getenv("BILIBILI_MAX_VIDEOS", "1"))
DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER", "/joytopp")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
BILIBILI_COOKIES_ENV = os.getenv("BILIBILI_COOKIES")  # cookies.txt content
BOT_USER_AGENT = os.getenv("BOT_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36")

# Retry & skip policy (can be overridden via env)
DOWNLOAD_RETRIES = int(os.getenv("DOWNLOAD_RETRIES", "3"))  # total attempts per video
SKIP_LIMIT = int(os.getenv("SKIP_LIMIT", "5"))            # how many skipped videos allowed before stopping this run

REPO_PATH = Path.cwd()
DOWNLOADED_IDS_PATH = REPO_PATH / "downloaded_ids.json"
TRANSLATIONS_PATH = REPO_PATH / "translations.json"
FALLBACK_TITLE_PATH = REPO_PATH / "fallback_title.txt"

MEDIA_EXTS = (".mp4", ".mkv", ".m4a", ".webm", ".flv", ".ts", ".mov", ".avi", ".mp3", ".aac")

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

def get_dropbox_access_token(app_key, app_secret, refresh_token):
    resp = requests.post(
        "https://api.dropboxapi.com/oauth2/token",
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        auth=(app_key, app_secret),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def clear_dropbox_folder(dbx, folder_path):
    try:
        res = dbx.files_list_folder(folder_path)
        for entry in res.entries:
            try:
                dbx.files_delete_v2(entry.path_lower)
                print(f"Deleted {entry.name} from Dropbox")
            except Exception as e:
                print("Failed to delete", entry, e)
        while res.has_more:
            res = dbx.files_list_folder_continue(res.cursor)
            for entry in res.entries:
                try:
                    dbx.files_delete_v2(entry.path_lower)
                    print(f"Deleted {entry.name} from Dropbox")
                except Exception as e:
                    print("Failed to delete", entry, e)
    except dropbox.exceptions.ApiError:
        try:
            dbx.files_create_folder_v2(folder_path)
            print(f"Created Dropbox folder {folder_path}")
        except Exception:
            pass

def load_json_set(path: Path):
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def save_json_obj(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def save_downloaded_ids_and_commit(path: Path, ids_set):
    save_json_obj(path, sorted(list(ids_set)))
    print(f"Saved {len(ids_set)} IDs to {path}")
    if not GITHUB_TOKEN:
        print("No GITHUB_TOKEN — skipping commit for downloaded_ids.json")
        return
    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", str(path)], check=True)
        subprocess.run(["git", "commit", "-m", "Update downloaded_ids.json [skip ci]"], check=False)
        repo = os.getenv("GITHUB_REPOSITORY")
        branch = os.getenv("GITHUB_REF", "refs/heads/main").split("/")[-1]
        remote = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{repo}.git"
        subprocess.run(["git", "push", remote, f"HEAD:{branch}"], check=False)
        print("Pushed downloaded_ids.json to repo")
    except Exception as e:
        print("Failed to commit/push downloaded_ids.json:", e)

def save_translations_and_commit(path: Path, translations: dict):
    try:
        save_json_obj(path, translations)
        print(f"Saved translations to {path}")
    except Exception as e:
        print("Failed to save translations:", e)
    if not GITHUB_TOKEN:
        return
    try:
        subprocess.run(["git", "add", str(path)], check=True)
        subprocess.run(["git", "commit", "-m", "Update translations.json [skip ci]"], check=False)
        repo = os.getenv("GITHUB_REPOSITORY")
        branch = os.getenv("GITHUB_REF", "refs/heads/main").split("/")[-1]
        remote = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{repo}.git"
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
    # remove files that start with prefix and are media ext or temp
    for f in os.listdir("."):
        if f.startswith(prefix):
            try:
                if any(f.lower().endswith(ext) for ext in MEDIA_EXTS) or f.endswith(".part") or f.endswith(".tmp"):
                    os.remove(f)
                    print("Removed partial file:", f)
            except Exception:
                pass

def chunked_upload(dbx, local_path, dropbox_path, chunk_size=50 * 1024 * 1024):
    file_size = os.path.getsize(local_path)
    with open(local_path, "rb") as f:
        if file_size <= 150 * 1024 * 1024:
            dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        else:
            print(f"Large file {local_path} -> using chunked upload ({file_size/1024/1024:.1f} MB)")
            upload_session_start_result = dbx.files_upload_session_start(f.read(chunk_size))
            cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id, offset=f.tell())
            commit = dropbox.files.CommitInfo(path=dropbox_path, mode=dropbox.files.WriteMode.overwrite)
            while f.tell() < file_size:
                if (file_size - f.tell()) <= chunk_size:
                    dbx.files_upload_session_finish(f.read(chunk_size), cursor, commit)
                else:
                    dbx.files_upload_session_append_v2(f.read(chunk_size), cursor)
                    cursor.offset = f.tell()
    print(f"Uploaded to Dropbox: {dropbox_path}")

# ---------- Translators (unchanged chain) ----------
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
    # fallback to file if present
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

# ---------- Playlist helpers (cookie-aware) ----------
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

# ---------- Main ----------
def main():
    cookies_path = None
    if BILIBILI_COOKIES_ENV:
        cookies_path = REPO_PATH / "cookies.txt"
        cookies_path.write_text(BILIBILI_COOKIES_ENV, encoding="utf-8")
        print("Wrote cookies to", cookies_path)

    # ensure git identity early so commits don't error
    if GITHUB_TOKEN:
        try:
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
            subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], check=True)
        except Exception:
            pass

    # detect ffmpeg
    has_ffmpeg = shutil.which("ffmpeg") is not None
    if not has_ffmpeg:
        print("Warning: ffmpeg not found. Merging may fail or produce audio-only files.")

    try:
        if not (DROPBOX_APP_KEY and DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN):
            raise SystemExit("Please set Dropbox secrets.")

        access_token = get_dropbox_access_token(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN)
        dbx = dropbox.Dropbox(access_token)

        print("Cleaning remote Dropbox folder:", DROPBOX_FOLDER)
        clear_dropbox_folder(dbx, DROPBOX_FOLDER)

        downloaded_ids = load_json_set(DOWNLOADED_IDS_PATH)
        translations_cache = load_translations(TRANSLATIONS_PATH)
        print(f"Loaded {len(downloaded_ids)} downloaded IDs and {len(translations_cache)} translations")

        print("Attempting flat-playlist fetch (low load)...")
        entries = fetch_flat_playlist_entries(BILIBILI_CHANNEL_URL, cookies_path=cookies_path) or []

        # Build a list of entries (if flat-playlist returns minimal info, we will fetch metadata later)
        candidates = []
        for entry in entries:
            vid = entry.get("id") or entry.get("url") or entry.get("webpage_url")
            if not vid:
                continue
            if vid in downloaded_ids:
                print(f"Skipping already-downloaded (flat): {vid}")
                continue
            candidates.append({"id": vid, "webpage_url": f"https://www.bilibili.com/video/{vid}"})
            # we intentionally do not stop at MAX_VIDEOS here; we'll process sequentially and count successes

        # fallback per-item if flat returned nothing
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
        while successes < MAX_VIDEOS and idx < len(candidates) and skips < SKIP_LIMIT:
            cand = candidates[idx]
            idx += 1
            vid = cand["id"]
            webpage = cand["webpage_url"]
            print(f"Processing candidate {vid} ({idx}/{len(candidates)})")

            # Fetch full metadata for title & thumbnail
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
            # Persist translations early
            save_translations_and_commit(TRANSLATIONS_PATH, translations_cache)

            safe_name = sanitize_filename_keep_unicode(final_title or vid)
            print(f"Title -> '{orig_title}' -> Translated -> '{final_title}' -> Filename -> '{safe_name}'")

            # Prepare thumbnail
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

            # Download with retries
            download_ok = False
            attempt = 0
            while attempt < DOWNLOAD_RETRIES and not download_ok:
                attempt += 1
                print(f"Download attempt {attempt}/{DOWNLOAD_RETRIES} for {vid}")
                out_template = f"{safe_name}.%(ext)s"
                dl_cmd = [
                    "yt-dlp",
                    "-f", "bv*+ba/b",
                    "--merge-output-format", "mp4",
                    "-o", out_template,
                    "--no-warnings",
                    "--no-progress",
                    "--user-agent", BOT_USER_AGENT,
                ]
                if cookies_path:
                    dl_cmd += ["--cookies", str(cookies_path)]
                dl_cmd += [webpage]

                dl_proc = subprocess.run(dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                stderr_snip = (dl_proc.stderr or "").strip()[:1000]
                if stderr_snip:
                    print("yt-dlp stderr (short):", stderr_snip)

                downloaded_file = find_downloaded_file_by_prefix(safe_name)
                if downloaded_file and os.path.getsize(downloaded_file) > 100:  # >100 bytes sanity
                    print("Downloaded file located:", downloaded_file)
                    download_ok = True
                    break
                else:
                    # cleanup partials if any
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
                # remove thumbnail local if created
                try:
                    if thumb_local and os.path.exists(thumb_local):
                        os.remove(thumb_local)
                except Exception:
                    pass
                # do NOT add vid to downloaded_ids
                # continue to next candidate
                continue

            # Upload thumbnail
            if thumb_local and os.path.exists(thumb_local):
                try:
                    chunked_upload(dbx, thumb_local, f"{DROPBOX_FOLDER}/thumbnail.jpg")
                except Exception as e:
                    print("Thumbnail upload failed:", e)
                try:
                    os.remove(thumb_local)
                except Exception:
                    pass

            # Upload video
            try:
                chunked_upload(dbx, downloaded_file, f"{DROPBOX_FOLDER}/{os.path.basename(downloaded_file)}")
            except Exception as e:
                print("Video upload failed:", e)
                # do not mark as downloaded if upload failed; cleanup and skip
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

            # Success: record and commit
            downloaded_ids.add(vid)
            save_downloaded_ids_and_commit(DOWNLOADED_IDS_PATH, downloaded_ids)
            # translations already saved earlier, but ensure persisted
            save_translations_and_commit(TRANSLATIONS_PATH, translations_cache)

            # cleanup local file
            try:
                os.remove(downloaded_file)
            except Exception:
                pass

            successes += 1
            print(f"Successfully processed {vid}. Total successes this run: {successes}/{MAX_VIDEOS}")

            # polite pause between successful videos
            time.sleep(random.uniform(1.0, 2.0))

        print(f"Run finished: {successes} successful downloads, {skips} skipped videos.")

    finally:
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
