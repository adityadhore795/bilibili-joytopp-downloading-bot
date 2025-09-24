import os
import json
import subprocess
import dropbox
import time
import re
from pathlib import Path

# --- Config from env ---
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
DROPBOX_FOLDER = "/joytopp"
CHANNEL_URL = os.getenv("BILIBILI_CHANNEL_URL")
MAX_VIDEOS = int(os.getenv("BILIBILI_MAX_VIDEOS", "1"))
DOWNLOADED_IDS_PATH = Path("downloaded_ids.json")
COOKIES_PATH = Path("cookies.txt")

# --- Dropbox client ---
dbx = dropbox.Dropbox(DROPBOX_TOKEN)

# --- Helpers ---
def sanitize_filename(name: str) -> str:
    """Make filename safe for filesystem"""
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def load_downloaded_ids(path: Path):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_downloaded_ids(path: Path, ids: set):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(ids), f, ensure_ascii=False, indent=2)

def upload_to_dropbox(local_path: str, dropbox_path: str):
    file_size = os.path.getsize(local_path)
    with open(local_path, "rb") as f:
        if file_size <= 150 * 1024 * 1024:  # <= 150 MB
            dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
        else:
            CHUNK_SIZE = 4 * 1024 * 1024
            upload_session_start_result = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
            cursor = dropbox.files.UploadSessionCursor(
                session_id=upload_session_start_result.session_id, offset=f.tell()
            )
            commit = dropbox.files.CommitInfo(path=dropbox_path, mode=dropbox.files.WriteMode("overwrite"))

            while f.tell() < file_size:
                if (file_size - f.tell()) <= CHUNK_SIZE:
                    dbx.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                else:
                    dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE), cursor)
                    cursor.offset = f.tell()

def find_downloaded_file_by_prefix(prefix: str):
    for file in os.listdir("."):
        if file.startswith(prefix):
            return os.path.abspath(file)
    return None

# --- Main ---
print("Fetching metadata from Bilibili...")
downloaded = load_downloaded_ids(DOWNLOADED_IDS_PATH)
new_videos = []

# Try flat playlist first
print("Attempting flat-playlist fetch (low load)...")
flat_cmd = ["yt-dlp", "-j", "--cookies", str(COOKIES_PATH), "--flat-playlist", CHANNEL_URL]
try:
    flat_proc = subprocess.run(flat_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    lines = flat_proc.stdout.strip().splitlines()
    for line in lines:
        data = json.loads(line)
        vid_id = data.get("id")
        if vid_id not in downloaded:
            new_videos.append(data)
        if len(new_videos) >= MAX_VIDEOS:
            break
except subprocess.CalledProcessError as e:
    print("Flat-playlist failed, stderr:", e.stderr[:400])

if not new_videos:
    print("No new videos to download.")
    exit(0)

print(f"Will process {len(new_videos)} new video(s).")

for vid in new_videos:
    vid_id = vid.get("id")
    video_url = f"https://www.bilibili.com/video/{vid_id}"
    raw_title = vid.get("title") or vid_id
    safe_name = sanitize_filename(raw_title)

    print(f"Processing {vid_id} — '{raw_title}' -> filename '{safe_name}'")

    # --- Thumbnail ---
    thumb_url = vid.get("thumbnail")
    if thumb_url:
        try:
            subprocess.run(["curl", "-L", thumb_url, "-o", "thumbnail.jpg"], check=True)
            print("Saved thumbnail as: thumbnail.jpg")
            upload_to_dropbox("thumbnail.jpg", f"{DROPBOX_FOLDER}/thumbnail.jpg")
        except Exception as e:
            print("Thumbnail download failed:", e)

    # --- Video Download ---
    dl_cmd = [
        "yt-dlp",
        "--cookies", str(COOKIES_PATH),
        "-f", "bv*+ba/b",   # best video + best audio, fallback best
        "-o", f"{safe_name}.%(ext)s",
        video_url,
    ]
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

    # --- Upload video ---
    dropbox_dest = f"{DROPBOX_FOLDER}/{os.path.basename(downloaded_file)}"
    upload_to_dropbox(downloaded_file, dropbox_dest)
    print(f"Uploaded to Dropbox: {dropbox_dest}")

    # --- Mark as done ---
    downloaded.add(vid_id)
    save_downloaded_ids(DOWNLOADED_IDS_PATH, downloaded)

print("All done.")
