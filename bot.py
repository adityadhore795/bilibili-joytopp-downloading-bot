#!/usr/bin/env python3
"""
Bilibili -> Dropbox bot with cookie support (Option A).

- Reads BILIBILI_COOKIES env secret (Netscape cookies.txt content) and writes ./cookies.txt at runtime.
- Uses --cookies cookies.txt for yt-dlp calls to avoid Bilibili rate-limits.
- Removes cookies.txt after run.
- Keeps previous features: thumbnail.jpg, chunked upload, downloaded_ids.json persistence, translation fallback, cleanup.
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

# ---------- Config from env ----------
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
BILIBILI_CHANNEL_URL = os.getenv("BILIBILI_CHANNEL_URL", "https://space.bilibili.com/87877349/video")
MAX_VIDEOS = int(os.getenv("BILIBILI_MAX_VIDEOS", "1"))
DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER", "/joytopp")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")  # optional
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")     # provided by Actions
REPO_PATH = Path.cwd()
DOWNLOADED_IDS_PATH = REPO_PATH / "downloaded_ids.json"
BILIBILI_COOKIES_ENV = os.getenv("BILIBILI_COOKIES")  # the secret content

MEDIA_EXTS = (".mp4", ".mkv", ".m4a", ".webm", ".flv", ".ts", ".mov", ".avi", ".mp3", ".aac")

USER_AGENT = os.getenv("BOT_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36")

# ---------- Utilities ----------
def sanitize_filename_keep_unicode(s: str, max_length=120) -> str:
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

def load_downloaded_ids(path: Path):
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def save_downloaded_ids_and_commit(path: Path, ids_set):
    path.write_text(json.dumps(sorted(list(ids_set)), indent=2), encoding="utf-8")
    print(f"Saved {len(ids_set)} IDs to {path}")

    if not GITHUB_TOKEN:
        print("No GITHUB_TOKEN available — skipping commit of downloaded_ids.json")
        return

    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", str(path)], check=True)
        subprocess.run(["git", "commit", "-m", "Update downloaded_ids.json [skip ci]"], check=False)
        repo = os.getenv("GITHUB_REPOSITORY")
        ref = os.getenv("GITHUB_REF", "refs/heads/main")
        branch = ref.split("/")[-1]
        remote = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{repo}.git"
        subprocess.run(["git", "push", remote, f"HEAD:{branch}"], check=False)
        print("Pushed downloaded_ids.json to repo")
    except Exception as e:
        print("Failed to commit/push downloaded_ids.json:", e)

def translate_to_english_free_or_api(text: str) -> str:
    if not text:
        return text
    if GOOGLE_API_KEY:
        try:
            url = "https://translation.googleapis.com/language/translate/v2"
            resp = requests.post(url, data={"q": text, "target": "en", "format": "text", "key": GOOGLE_API_KEY}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return data["data"]["translations"][0]["translatedText"]
        except Exception as e:
            print("Official translate failed:", e)
    try:
        from googletrans import Translator
        translator = Translator()
        result = translator.translate(text, dest="en")
        return result.text
    except Exception as e:
        print("Free googletrans translate failed or not available:", e)
    return text

def find_downloaded_file_by_prefix(prefix: str):
    files = sorted(os.listdir("."), key=lambda x: os.path.getmtime(x), reverse=True)
    for fname in files:
        if fname.startswith(prefix) and fname.lower().endswith(MEDIA_EXTS):
            return os.path.abspath(fname)
    return None

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

# ---------- Playlist fetch helpers (cookie-aware) ----------
def fetch_flat_playlist_entries(channel_url, cookies_path=None, max_retries=5, initial_delay=4):
    cmd_base = [
        "yt-dlp",
        "--flat-playlist",
        "-j",
        "--no-warnings",
        "--no-progress",
        "--user-agent", USER_AGENT,
    ]
    if cookies_path:
        cmd = cmd_base + ["--cookies", str(cookies_path), channel_url]
    else:
        cmd = cmd_base + [channel_url]

    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            print(f"flat-playlist attempt {attempt}...")
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, timeout=120)
            stderr = (proc.stderr or "").strip()
            if stderr:
                if "Request is rejected by server" in stderr or "Request is blocked" in stderr or "352" in stderr or "412" in stderr:
                    print(f"flat-playlist server rejected (attempt {attempt}): {stderr.splitlines()[:3]}")
                    time.sleep(delay)
                    delay *= 2
                    continue
                else:
                    print("flat-playlist stderr (info):", stderr[:400])
            lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
            entries = []
            for ln in lines:
                try:
                    obj = json.loads(ln)
                    entries.append(obj)
                except Exception:
                    continue
            if entries:
                return entries
            else:
                print("flat-playlist returned no entries.")
                return []
        except subprocess.CalledProcessError as e:
            print(f"flat-playlist command failed (attempt {attempt}): {e}; stderr: {(e.stderr or '')[:200]}")
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            print(f"flat-playlist unexpected error (attempt {attempt}): {e}")
            time.sleep(delay)
            delay *= 2
    print("flat-playlist failed after retries.")
    return []

def fetch_single_item_metadata(channel_url, item_index, cookies_path=None, max_retries=3, initial_delay=3):
    cmd_base = [
        "yt-dlp",
        "-j",
        "--no-warnings",
        "--no-progress",
        "--user-agent", USER_AGENT,
        "--playlist-items", str(item_index),
    ]
    cmd = cmd_base + (["--cookies", str(cookies_path)] if cookies_path else []) + [channel_url]

    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, timeout=60)
            stderr = (proc.stderr or "").strip()
            if stderr and ("Request is rejected by server" in stderr or "Request is blocked" in stderr or "352" in stderr or "412" in stderr):
                print(f"single-item server rejected for index {item_index} (attempt {attempt}).")
                time.sleep(delay)
                delay *= 2
                continue
            out = proc.stdout.strip()
            if not out:
                return None
            data = json.loads(out)
            return data
        except subprocess.CalledProcessError as e:
            print(f"single-item fetch failed index={item_index} attempt={attempt}: {e}; stderr: {(e.stderr or '')[:200]}")
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            print(f"single-item unexpected error index={item_index} attempt={attempt}: {e}")
            time.sleep(delay)
            delay *= 2
    return None

# ---------- Main ----------
def main():
    # Write cookies file if secret present
    cookies_path = None
    if BILIBILI_COOKIES_ENV:
        cookies_path = REPO_PATH / "cookies.txt"
        cookies_path.write_text(BILIBILI_COOKIES_ENV, encoding="utf-8")
        print("Wrote cookies to", cookies_path)

    try:
        if not (DROPBOX_APP_KEY and DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN):
            raise SystemExit("Please set Dropbox secrets.")

        access_token = get_dropbox_access_token(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN)
        dbx = dropbox.Dropbox(access_token)

        # Clear folder
        print("Cleaning remote Dropbox folder:", DROPBOX_FOLDER)
        clear_dropbox_folder(dbx, DROPBOX_FOLDER)

        # Load downloaded IDs
        downloaded_ids = load_downloaded_ids(DOWNLOADED_IDS_PATH)
        print(f"Loaded {len(downloaded_ids)} previously downloaded IDs")

        # 1) flat-playlist (low load)
        print("Attempting flat-playlist fetch (low load)...")
        entries = fetch_flat_playlist_entries(BILIBILI_CHANNEL_URL, cookies_path=cookies_path)

        new_videos = []
        if entries:
            for entry in entries:
                vid = entry.get("id") or entry.get("url")
                title = entry.get("title") or ""
                if not vid:
                    continue
                if vid in downloaded_ids:
                    print(f"Skipping already-downloaded (flat): {vid} / {title}")
                    continue
                new_videos.append({"id": vid, "title": title, "webpage_url": f"https://www.bilibili.com/video/{vid}"})
                if len(new_videos) >= MAX_VIDEOS:
                    break

        # 2) fallback per-item checks (rate-limited), if needed
        if not new_videos:
            print("Flat-playlist gave nothing or was blocked; using per-item fallback (limited checks).")
            max_checks = int(os.getenv("BILIBILI_MAX_CHECKS", "200"))
            idx = 1
            while len(new_videos) < MAX_VIDEOS and idx <= max_checks:
                data = fetch_single_item_metadata(BILIBILI_CHANNEL_URL, idx, cookies_path=cookies_path)
                if not data:
                    idx += 1
                    # jitter
                    time.sleep(random.uniform(0.6, 1.2))
                    continue
                vid = data.get("id")
                title = data.get("title") or ""
                if not vid:
                    idx += 1
                    continue
                if vid in downloaded_ids:
                    print(f"Skipping already-downloaded (fallback): {vid} / {title}")
                    idx += 1
                    continue
                new_videos.append({"id": vid, "title": title, "webpage_url": data.get("webpage_url") or f"https://www.bilibili.com/video/{vid}"})
                idx += 1
                time.sleep(random.uniform(0.8, 1.6))

        if not new_videos:
            print("No new videos found after safe checks. Exiting.")
            return

        print(f"Will process {len(new_videos)} new video(s).")

        # Process videos
        for v in new_videos:
            vid = v["id"]
            orig_title = v.get("title") or ""
            translated = translate_to_english_free_or_api(orig_title)
            final_title = translated if translated else orig_title
            safe_name = sanitize_filename_keep_unicode(final_title or vid)

            print(f"Processing {vid} — '{final_title}' -> filename '{safe_name}'")

            # try to fetch full metadata (to get thumbnail), cookie-aware
            thumb_local = None
            thumb_url = None
            try:
                meta_cmd = ["yt-dlp", "-j", "--no-warnings", "--no-progress", "--user-agent", USER_AGENT]
                if cookies_path:
                    meta_cmd += ["--cookies", str(cookies_path)]
                meta_cmd += [v["webpage_url"]]
                meta_proc = subprocess.run(meta_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, timeout=60)
                meta_json = json.loads(meta_proc.stdout.strip())
                thumb_url = meta_json.get("thumbnail")
            except Exception:
                thumb_url = None

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

            # Download video (cookie-aware)
            out_template = f"{safe_name}.%(ext)s"
            dl_cmd = [
                "yt-dlp",
                "-f", "bestvideo+bestaudio/best",
                "--merge-output-format", "mp4",
                "-o", out_template,
                "--no-warnings",
                "--no-progress",
                "--user-agent", USER_AGENT,
            ]
            if cookies_path:
                dl_cmd += ["--cookies", str(cookies_path)]
            dl_cmd += [v["webpage_url"]]

            print("Running yt-dlp to download...")
            dl_proc = subprocess.run(dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if dl_proc.stderr:
                print("yt-dlp stderr (short):", dl_proc.stderr.strip()[:800])

            downloaded_file = find_downloaded_file_by_prefix(safe_name)
            if not downloaded_file:
                time.sleep(2)
                downloaded_file = find_downloaded_file_by_prefix(safe_name)
            if not downloaded_file:
                print(f"ERROR: downloaded file not found for {safe_name}. Skipping.")
                continue

            print("Downloaded file located:", downloaded_file)

            # upload thumbnail
            if thumb_local and os.path.exists(thumb_local):
                chunked_upload(dbx, thumb_local, f"{DROPBOX_FOLDER}/thumbnail.jpg")
                try:
                    os.remove(thumb_local)
                except Exception:
                    pass

            # upload video
            chunked_upload(dbx, downloaded_file, f"{DROPBOX_FOLDER}/{os.path.basename(downloaded_file)}")

            # record ID and commit
            downloaded_ids.add(vid)
            save_downloaded_ids_and_commit(DOWNLOADED_IDS_PATH, downloaded_ids)

            # cleanup local file
            try:
                os.remove(downloaded_file)
            except Exception:
                pass

            # be polite between videos
            time.sleep(random.uniform(1.0, 2.0))

        print("All done.")

    finally:
        # Remove cookies file if we created it (security)
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
