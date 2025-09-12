#!/usr/bin/env python3
"""
Full-featured Bilibili -> Dropbox bot.

Features:
- Uses Dropbox refresh token to get short-lived access tokens.
- Deletes all files in DROPBOX_FOLDER at start.
- Reads playlist metadata from Bilibili (yt-dlp), skips already-downloaded IDs.
- Uses Google Translate API (if GOOGLE_API_KEY provided) to translate title to English.
- Downloads video (bestvideo+bestaudio / best) and merges to mp4 (requires ffmpeg).
- Downloads thumbnail and uploads both video + thumbnail to Dropbox (chunked upload for large files).
- Cleans up local files.
- Persists downloaded IDs in downloaded_ids.json in the repo and commits/pushes changes (uses GITHUB_TOKEN).
"""

import os
import subprocess
import json
import dropbox
import requests
import unicodedata
import re
import time
from pathlib import Path

# ---------- Config from env ----------
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
BILIBILI_CHANNEL_URL = os.getenv("BILIBILI_CHANNEL_URL", "https://space.bilibili.com/87877349/video")
MAX_VIDEOS = int(os.getenv("BILIBILI_MAX_VIDEOS", "1"))
DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER", "/joytopp")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")  # optional
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")     # provided by Actions; used to push downloaded_ids.json back
REPO_PATH = Path.cwd()
DOWNLOADED_IDS_PATH = REPO_PATH / "downloaded_ids.json"

# Media extensions for detection
MEDIA_EXTS = (".mp4", ".mkv", ".m4a", ".webm", ".flv", ".ts", ".mov", ".avi", ".mp3", ".aac")

# ---------- Utilities ----------
def sanitize_filename(s: str, max_length=120) -> str:
    if not s:
        s = "video"
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")  # drop non-ascii
    s = re.sub(r'[\\/:*?"<>|]+', "", s)
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
    # recursively delete all entries in the folder (simple approach)
    try:
        has_more = True
        cursor = None
        # list the folder contents and delete them
        res = dbx.files_list_folder(folder_path)
        for entry in res.entries:
            try:
                dbx.files_delete_v2(entry.path_lower)
                print(f"Deleted {entry.name} from Dropbox")
            except Exception as e:
                print("Failed to delete", entry, e)
        # handle pagination
        while res.has_more:
            res = dbx.files_list_folder_continue(res.cursor)
            for entry in res.entries:
                try:
                    dbx.files_delete_v2(entry.path_lower)
                    print(f"Deleted {entry.name} from Dropbox")
                except Exception as e:
                    print("Failed to delete", entry, e)
    except dropbox.exceptions.ApiError as e:
        # folder likely doesn't exist; try create
        try:
            dbx.files_create_folder_v2(folder_path)
            print(f"Created Dropbox folder {folder_path}")
        except Exception:
            # ignore if can't create
            print(f"Dropbox folder {folder_path} not present and couldn't be created (may be fine).")

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

    # Try to commit & push back to repo so the list persists
    if not GITHUB_TOKEN:
        print("No GITHUB_TOKEN available — skipping commit of downloaded_ids.json")
        return

    try:
        # configure git user
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", str(path)], check=True)
        subprocess.run(["git", "commit", "-m", "Update downloaded_ids.json [skip ci]"], check=False)  # commit may no-op
        repo = os.getenv("GITHUB_REPOSITORY")  # e.g. owner/repo
        ref = os.getenv("GITHUB_REF", "refs/heads/main")
        branch = ref.split("/")[-1]
        remote = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{repo}.git"
        subprocess.run(["git", "push", remote, f"HEAD:{branch}"], check=False)
        print("Pushed downloaded_ids.json to repo")
    except Exception as e:
        print("Failed to commit/push downloaded_ids.json:", e)

def translate_to_english(text: str, api_key: str) -> str:
    if not api_key or not text:
        return text
    try:
        url = "https://translation.googleapis.com/language/translate/v2"
        resp = requests.post(url, data={"q": text, "target": "en", "format": "text", "key": api_key}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data["data"]["translations"][0]["translatedText"]
        else:
            print("Translate API error:", resp.status_code, resp.text)
            return text
    except Exception as e:
        print("Translate failed:", e)
        return text

def find_downloaded_file_by_prefix(prefix: str):
    # return first file in cwd that starts with prefix and has a media extension
    for fname in sorted(os.listdir("."), key=lambda x: os.path.getmtime(x), reverse=True):
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

# ---------- Main flow ----------
def main():
    if not (DROPBOX_APP_KEY and DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN):
        raise SystemExit("Please set DROPBOX_APP_KEY, DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN as secrets.")

    # 1) Dropbox auth
    access_token = get_dropbox_access_token(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN)
    dbx = dropbox.Dropbox(access_token)

    # 2) Clear Dropbox folder
    print("Cleaning remote Dropbox folder:", DROPBOX_FOLDER)
    clear_dropbox_folder(dbx, DROPBOX_FOLDER)

    # 3) Load downloaded IDs
    downloaded_ids = load_downloaded_ids(DOWNLOADED_IDS_PATH)
    print(f"Loaded {len(downloaded_ids)} previously downloaded IDs")

    # 4) Get playlist metadata (yt-dlp JSON entries)
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
            # ignore non-json lines
            continue

    if not entries:
        print("No playlist entries found. Exiting.")
        return

    # 5) Prepare list of videos to download (skip already-downloaded)
    to_process = []
    for entry in entries:
        vid = entry.get("id") or entry.get("webpage_url") or entry.get("url")
        if not vid:
            print("Skipping entry with no id/url:", entry.get("title"))
            continue
        if vid in downloaded_ids:
            print(f"Skipping already-downloaded video: {vid} / {entry.get('title')}")
            continue
        to_process.append((vid, entry))

    if not to_process:
        print("No new videos to download.")
        return

    # 6) Process videos (one by one)
    for vid, entry in to_process:
        try:
            orig_title = entry.get("title") or ""
            # Try to use explicit english title fields if present
            english_candidates = [
                entry.get("title_en"),
                entry.get("english_title"),
                entry.get("alt_title"),
            ]
            english_title = next((t for t in english_candidates if t), None)

            if not english_title:
                # fallback: use Google Translate if API key provided
                if GOOGLE_API_KEY and orig_title:
                    print("Translating title to English via Google API...")
                    try:
                        english_title = translate_to_english(orig_title, GOOGLE_API_KEY)
                        print("Translated title:", english_title)
                    except Exception as e:
                        print("Translate failed, using original title:", e)
                        english_title = orig_title
                else:
                    english_title = orig_title

            safe_name = sanitize_filename(english_title or orig_title or vid)
            print(f"Downloading video {vid} as '{safe_name}'")

            # Download thumbnail (if present)
            thumb_url = entry.get("thumbnail")
            thumb_local = None
            if thumb_url:
                try:
                    r = requests.get(thumb_url, timeout=20)
                    if r.status_code == 200:
                        thumb_local = f"{safe_name}.jpg"
                        with open(thumb_local, "wb") as fh:
                            fh.write(r.content)
                        print("Saved thumbnail:", thumb_local)
                except Exception as e:
                    print("Thumbnail download failed:", e)
                    thumb_local = None

            # Download video (force best + merge to mp4). Output template uses sanitized name so we can find it easily.
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
                # fallback: wait a short moment and check again
                time.sleep(2)
                downloaded_file = find_downloaded_file_by_prefix(safe_name)
            if not downloaded_file:
                raise RuntimeError(f"Downloaded file for '{safe_name}' not found on disk.")

            print("Downloaded file located:", downloaded_file)

            # Upload thumbnail first (if exists)
            if thumb_local and os.path.exists(thumb_local):
                dest_thumb = f"{DROPBOX_FOLDER}/{os.path.basename(thumb_local)}"
                chunked_upload(dbx, thumb_local, dest_thumb)
                # remove thumbnail local after upload
                try:
                    os.remove(thumb_local)
                except Exception:
                    pass

            # Upload video (supports chunking)
            dest_video = f"{DROPBOX_FOLDER}/{os.path.basename(downloaded_file)}"
            chunked_upload(dbx, downloaded_file, dest_video)

            # success → mark id as downloaded
            downloaded_ids.add(vid)
            print(f"Finished processing {vid} -> uploaded to {dest_video}")

            # cleanup local video file
            try:
                os.remove(downloaded_file)
            except Exception as e:
                print("Failed to remove local file:", e)

        except Exception as e:
            print("Error processing video", vid, e)
            # continue to next video

    # 7) Save & commit downloaded IDs back to repo
    save_downloaded_ids_and_commit(DOWNLOADED_IDS_PATH, downloaded_ids)
    print("All done.")

if __name__ == "__main__":
    main()
