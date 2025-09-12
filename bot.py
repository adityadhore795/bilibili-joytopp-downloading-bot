#!/usr/bin/env python3
"""
Final bot.py — Bilibili -> Dropbox

Changes made:
- Preserve full original title (including non-ASCII).
- Attempt free translation via googletrans (if available); if translation fails, use original title.
- Thumbnails saved as "thumbnail.jpg".
- All other behavior (chunked uploads, cleanup, downloaded_ids persistence) unchanged.
"""

import os
import subprocess
import json
import dropbox
import requests
import re
import time
from pathlib import Path

# ---------- Config from env (unchanged) ----------
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
BILIBILI_CHANNEL_URL = os.getenv("BILIBILI_CHANNEL_URL", "https://space.bilibili.com/87877349/video")
MAX_VIDEOS = int(os.getenv("BILIBILI_MAX_VIDEOS", "1"))
DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER", "/joytopp")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")  # optional (you said you don't have it)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")     # used to push downloaded_ids.json back
REPO_PATH = Path.cwd()
DOWNLOADED_IDS_PATH = REPO_PATH / "downloaded_ids.json"

# Known media extensions
MEDIA_EXTS = (".mp4", ".mkv", ".m4a", ".webm", ".flv", ".ts", ".mov", ".avi", ".mp3", ".aac")

# ---------- Utilities ----------
def sanitize_filename_keep_unicode(s: str, max_length=120) -> str:
    """
    Keep unicode characters, but remove filesystem-problematic chars and control chars.
    Replace runs of whitespace with single underscore. Truncate to max_length.
    """
    if not s:
        s = "video"
    s = s.strip()
    # Remove control chars and reserved path chars
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
        # folder doesn't exist — try to create it (ignore errors)
        try:
            dbx.files_create_folder_v2(folder_path)
            print(f"Created Dropbox folder {folder_path}")
        except Exception:
            print(f"Could not create Dropbox folder {folder_path} (may be fine).")

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
        # Git config & commit
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
    """
    Try Google Cloud Translate if GOOGLE_API_KEY is set.
    Otherwise try free googletrans library (if installed).
    If all fail, return the original text unchanged.
    """
    if not text:
        return text

    # 1) Try official API if key present
    if GOOGLE_API_KEY:
        try:
            url = "https://translation.googleapis.com/language/translate/v2"
            resp = requests.post(url, data={"q": text, "target": "en", "format": "text", "key": GOOGLE_API_KEY}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return data["data"]["translations"][0]["translatedText"]
            else:
                print("Translate API error:", resp.status_code, resp.text)
        except Exception as e:
            print("Official translate failed:", e)

    # 2) Try free googletrans package if available
    try:
        from googletrans import Translator
        translator = Translator()
        result = translator.translate(text, dest="en")
        return result.text
    except Exception as e:
        # googletrans may fail or be blocked; just fallback
        print("Free googletrans translate failed or not available:", e)

    # fallback: return original
    return text

def find_downloaded_file_by_prefix(prefix: str):
    # return first file in cwd that starts with prefix and has a media extension
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

# ---------- Main ----------
def main():
    if not (DROPBOX_APP_KEY and DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN):
        raise SystemExit("Please set DROPBOX_APP_KEY, DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN as secrets.")

    # Dropbox auth
    access_token = get_dropbox_access_token(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN)
    dbx = dropbox.Dropbox(access_token)

    # Clear remote folder
    print("Cleaning remote Dropbox folder:", DROPBOX_FOLDER)
    clear_dropbox_folder(dbx, DROPBOX_FOLDER)

    # Load downloaded IDs
    downloaded_ids = load_downloaded_ids(DOWNLOADED_IDS_PATH)
    print(f"Loaded {len(downloaded_ids)} previously downloaded IDs")

    # Fetch playlist metadata (JSON per entry)
    meta_cmd = [
        "yt-dlp",
        "-j",  # JSON per entry
        "--skip-download",
        "--playlist-end", str(MAX_VIDEOS),
        BILIBILI_CHANNEL_URL,
    ]
    print("Fetching metadata from Bilibili...")
    proc = subprocess.run(meta_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.stderr:
        print("yt-dlp stderr (info/warnings):")
        print(proc.stderr.strip()[:2000])

    entries = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            entries.append(obj)
        except Exception:
            continue

    if not entries:
        print("No playlist entries found. Exiting.")
        return

    # Build list of videos to process (skip downloaded)
    to_process = []
    for entry in entries:
        vid = entry.get("id") or entry.get("webpage_url") or entry.get("url")
        if not vid:
            print("Skipping entry without id/url")
            continue
        if vid in downloaded_ids:
            print(f"Skipping already-downloaded video: {vid} / {entry.get('title')}")
            continue
        to_process.append((vid, entry))

    if not to_process:
        print("No new videos to download.")
        return

    # Process each new video
    for vid, entry in to_process:
        try:
            orig_title = entry.get("title") or ""
            # Always use full original title; try to translate if possible
            translated = translate_to_english_free_or_api(orig_title)
            # If translation returns something different and non-empty, prefer it; otherwise keep original
            final_title = translated if translated and translated != orig_title else orig_title

            safe_name = sanitize_filename_keep_unicode(final_title or orig_title or vid)
            print(f"Downloading video {vid} as '{safe_name}'")

            # Download thumbnail (always save as thumbnail.jpg)
            thumb_url = entry.get("thumbnail")
            thumb_local = None
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

            # Download video (best + merge to mp4)
            out_template = f"{safe_name}.%(ext)s"
            dl_cmd = [
                "yt-dlp",
                "-f", "bestvideo+bestaudio/best",
                "--merge-output-format", "mp4",
                "-o", out_template,
                entry.get("webpage_url") or entry.get("url"),
            ]
            dl_proc = subprocess.run(dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if dl_proc.stderr:
                print("yt-dlp stderr (info/warnings):")
                print(dl_proc.stderr.strip()[:2000])

            # Find downloaded file
            downloaded_file = find_downloaded_file_by_prefix(safe_name)
            if not downloaded_file:
                time.sleep(2)
                downloaded_file = find_downloaded_file_by_prefix(safe_name)
            if not downloaded_file:
                raise RuntimeError(f"Downloaded file for '{safe_name}' not found.")

            print("Downloaded file located:", downloaded_file)

            # Upload thumbnail (if any) with fixed name thumbnail.jpg
            if thumb_local and os.path.exists(thumb_local):
                dest_thumb = f"{DROPBOX_FOLDER}/thumbnail.jpg"
                chunked_upload(dbx, thumb_local, dest_thumb)
                try:
                    os.remove(thumb_local)
                except Exception:
                    pass

            # Upload video
            dest_video = f"{DROPBOX_FOLDER}/{os.path.basename(downloaded_file)}"
            chunked_upload(dbx, downloaded_file, dest_video)

            # mark id as downloaded
            downloaded_ids.add(vid)
            print(f"Finished processing {vid} -> uploaded to {dest_video}")

            # cleanup local video
            try:
                os.remove(downloaded_file)
            except Exception as e:
                print("Failed to remove local file:", e)

        except Exception as e:
            print("Error processing video", vid, e)
            # continue with next entry

    # Save & push downloaded IDs back to repo
    save_downloaded_ids_and_commit(DOWNLOADED_IDS_PATH, downloaded_ids)
    print("All done.")

if __name__ == "__main__":
    main()
