import os
import subprocess
import json
import dropbox
import requests

APP_KEY = os.getenv("DROPBOX_APP_KEY")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
BILIBILI_CHANNEL_URL = "https://space.bilibili.com/87877349/video"

# --- Config ---
channel_url = "https://space.bilibili.com/87877349/video"
max_videos = int(os.getenv("BILIBILI_MAX_VIDEOS", "1"))
dropbox_folder = "/joytopp"


# --- 1. Get short-lived access token from refresh token ---
resp = requests.post(
    "https://api.dropboxapi.com/oauth2/token",
    data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN},
    auth=(APP_KEY, APP_SECRET),
)
resp.raise_for_status()
ACCESS_TOKEN = resp.json()["access_token"]

dbx = dropbox.Dropbox(ACCESS_TOKEN)

# --- 2. Delete all files in /joytopp ---
print("Cleaning /joytopp folder in Dropbox...")
try:
    result = dbx.files_list_folder(dropbox_folder)
    for entry in result.entries:
        dbx.files_delete_v2(entry.path_lower)
        print(f"Deleted {entry.name}")
except dropbox.exceptions.ApiError:
    # folder might not exist, create it
    dbx.files_create_folder_v2(dropbox_folder)

# --- 3. Download latest video(s) from Bilibili ---
print("Downloading videos from Bilibili...")

download_cmd = [
    "yt-dlp",
    "--no-warnings",
    "--no-progress",
    "-o", "%(title)s.%(ext)s",
    "--max-downloads", str(max_videos),
    "--playlist-end", str(max_videos),
    "--print", "after_move:filepath",
    "https://space.bilibili.com/87877349/video",
]

# Run yt-dlp and capture stdout/stderr (do NOT merge stderr into stdout)
proc = subprocess.run(download_cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

stdout = proc.stdout.strip()
stderr = proc.stderr.strip()
if stderr:
    # show but do not treat as filename
    print("yt-dlp stderr (warnings/info):")
    print(stderr)

# Parse candidate lines from stdout and keep only real files
candidates = [line.strip() for line in stdout.splitlines() if line.strip()]

# known media extensions we care about
MEDIA_EXTS = (".mp4", ".mkv", ".m4a", ".webm", ".flv", ".ts", ".mov", ".avi", ".mp3", ".aac")

filenames = []
cwd = os.getcwd()
for line in candidates:
    # 1) if the line is an absolute path and exists
    if os.path.isabs(line) and os.path.exists(line):
        filenames.append(line)
        continue
    # 2) if it looks like a media filename, try relative path in cwd
    if any(line.endswith(ext) for ext in MEDIA_EXTS):
        possible = os.path.join(cwd, line) if not os.path.isabs(line) else line
        if os.path.exists(possible):
            filenames.append(possible)
            continue

# 3) fallback: if nothing found, look for recent media files in working dir
if not filenames:
    all_media = [f for f in os.listdir(cwd) if f.lower().endswith(MEDIA_EXTS)]
    # sort by modification time (most recent first)
    all_media.sort(key=lambda p: os.path.getmtime(os.path.join(cwd, p)), reverse=True)
    filenames = [os.path.join(cwd, f) for f in all_media[:max_videos]]

if not filenames:
    raise RuntimeError(
        "No downloaded files found. yt-dlp stdout:\n"
        + stdout[:2000]
        + "\n\nyt-dlp stderr:\n"
        + stderr[:2000]
    )

print("Downloaded files:", filenames)

# Now download
subprocess.run(
    [
        "yt-dlp",
        "-o", "%(title)s.%(ext)s",
        "--max-downloads", str(max_videos),
        "--playlist-end", str(max_videos),
        "https://space.bilibili.com/87877349/video"
    ],
    check=False,   # don’t crash on yt-dlp exit 101
)

# --- 4. Upload to Dropbox ---
print("Uploading to Dropbox...")
for fname in filenames:
    path = f"{dropbox_folder}/{os.path.basename(fname)}"
    with open(os.path.abspath(fname), "rb") as f:
        dbx.files_upload(f.read(), path, mode=dropbox.files.WriteMode.overwrite)
    print(f"Uploaded {fname} → {path}")
