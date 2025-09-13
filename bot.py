#!/usr/bin/env python3
"""
Final bot.py — Bilibili -> Dropbox
"""

import os
import subprocess
import json
import dropbox
import requests
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
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")     # used to push downloaded_ids.json
REPO_PATH = Path.cwd()
DOWNLOADED_IDS_PATH = REPO_PATH / "downloaded_ids.json"

MEDIA_EXTS = (".mp4", ".mkv", ".m4a", ".webm", ".flv", ".ts", ".mov", ".avi", ".mp3", ".aac")

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
        print("Free googletrans translate failed:", e)
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

# ---------- Main ----------
def main():
    if not (DROPBOX_APP_KEY and DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN):
        raise SystemExit("Please set Dropbox secrets.")

    # Dropbox auth
    access_token = get_dropbox_access_token(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN)
    dbx = dropbox.Dropbox(access_token)

    # Clear remote folder
    print("Cleaning remote Dropbox folder:", DROPBOX_FOLDER)
    clear_dropbox_folder(dbx, DROPBOX_FOLDER)

    # Load IDs
    downloaded_ids = load_downloaded_ids(DOWNLOADED_IDS_PATH)
    print(f"Loaded {len(downloaded_ids)} previously downloaded IDs")

    # --- Fetch videos one by one until new found ---
    print("Fetching metadata from Bilibili...")
    new_videos = []
    video_index = 1
    while len(new_videos) < MAX_VIDEOS:
        yt_cmd = ["yt-dlp", "-j", "--playlist-items", str(video_index), BILIBILI_CHANNEL_URL]
        try:
            proc = subprocess.run(yt_cmd, stdout=subprocess.PIPE, text=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Failed to fetch video {video_index}, skipping. Error: {e}")
            video_index += 1
            continue

        if not proc.stdout.strip():
            print("No more videos available in channel.")
            break

        try:
            data = json.loads(proc.stdout.strip())
        except json.JSONDecodeError:
            print(f"Could not parse metadata for video {video_index}, skipping.")
            video_index += 1
            continue

        vid_id = data.get("id")
        title = data.get("title") or "untitled"

        if vid_id in downloaded_ids:
            print(f"Skipping already-downloaded video: {vid_id} / {title}")
            video_index += 1
            continue

        print(f"Found new video: {vid_id} / {title}")
        new_videos.append(data)
        video_index += 1

    if not new_videos:
        print("No new videos to download.")
        return

    # --- Process each new video ---
    for entry in new_videos:
        vid = entry.get("id")
        orig_title = entry.get("title") or ""
        translated = translate_to_english_free_or_api(orig_title)
        final_title = translated if translated else orig_title
        safe_name = sanitize_filename_keep_unicode(final_title or vid)

        print(f"Downloading video {vid} as '{safe_name}'")

        # Download thumbnail
        thumb_local = None
        thumb_url = entry.get("thumbnail")
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

        # Download video
        out_template = f"{safe_name}.%(ext)s"
        dl_cmd = ["yt-dlp", "-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4", "-o", out_template, entry.get("webpage_url") or entry.get("url")]
        dl_proc = subprocess.run(dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if dl_proc.stderr:
            print("yt-dlp stderr:", dl_proc.stderr.strip()[:1000])

        downloaded_file = find_downloaded_file_by_prefix(safe_name)
        if not downloaded_file:
            time.sleep(2)
            downloaded_file = find_downloaded_file_by_prefix(safe_name)
        if not downloaded_file:
            print(f"Could not find downloaded file for {safe_name}")
            continue

        print("Downloaded file located:", downloaded_file)

        # Upload to Dropbox
        if thumb_local and os.path.exists(thumb_local):
            chunked_upload(dbx, thumb_local, f"{DROPBOX_FOLDER}/thumbnail.jpg")
            os.remove(thumb_local)
        chunked_upload(dbx, downloaded_file, f"{DROPBOX_FOLDER}/{os.path.basename(downloaded_file)}")

        # Save ID
        downloaded_ids.add(vid)
        save_downloaded_ids_and_commit(DOWNLOADED_IDS_PATH, downloaded_ids)

        try:
            os.remove(downloaded_file)
        except Exception:
            pass

    print("All done.")

if __name__ == "__main__":
    main()
