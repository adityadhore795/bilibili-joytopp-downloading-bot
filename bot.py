import os
import subprocess
import json
import dropbox
import requests

# --- Config ---
channel_url = os.getenv("BILIBILI_CHANNEL_URL", "https://space.bilibili.com/87877349")
max_videos = int(os.getenv("BILIBILI_MAX_VIDEOS", "1"))
dropbox_folder = "/joytopp"

APP_KEY = os.getenv("DROPBOX_APP_KEY")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")

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
    "--get-filename",
    "-o", "%(title)s.%(ext)s",
    "--max-downloads", str(max_videos),
    "--dateafter", "0",  # no date limit, we just want latest
    "--playlist-end", str(max_videos),
    channel_url,
]
# Get filenames first
filenames = subprocess.check_output(download_cmd, text=True).splitlines()

# Now download
subprocess.run(
    [
        "yt-dlp",
        "-o", "%(title)s.%(ext)s",
        "--max-downloads", str(max_videos),
        "--playlist-end", str(max_videos),
        channel_url,
    ],
    check=True,
)

# --- 4. Upload to Dropbox ---
print("Uploading to Dropbox...")
for fname in filenames:
    path = f"{dropbox_folder}/{os.path.basename(fname)}"
    with open(fname, "rb") as f:
        dbx.files_upload(f.read(), path, mode=dropbox.files.WriteMode.overwrite)
    print(f"Uploaded {fname} → {path}")
