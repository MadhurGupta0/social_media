"""
Topic → YouTube search → Reap clipping pipeline
Flow:
  1. Search YouTube for topic, pick top result
  2. Pass YouTube URL directly to Reap
  3. Poll until done
  4. Print title, duration, virality score, download link, captions
"""

import os
import time
import requests
import yt_dlp

# ── Config ────────────────────────────────────────────────────────────────────
REAP_API_KEY = os.environ.get("REAP_API_KEY")
print(f"Using Reap API Key: {REAP_API_KEY}")
BASE_URL = "https://public.reap.video/api/v1/automation"
HEADERS = {"Authorization": f"Bearer {REAP_API_KEY}", "Content-Type": "application/json"}

CLIPPING_OPTIONS = {
    "genre": "talking",
    "exportResolution": 1080,
    "exportOrientation": "portrait",
    "reframeClips": True,
    "captionsPreset": "system_beasty",
    "enableEmojis": True,
    "enableHighlights": True,
    "language": "en",
    "minClipDuration": 30,
    "maxClipDuration": 60,
    "watermark": False,        # requires a paid Reap plan — remove this line if unsupported
}

POLL_INTERVAL = 15
# ─────────────────────────────────────────────────────────────────────────────


# ── Step 1: Search YouTube, return top result URL ─────────────────────────────
def search_youtube(topic: str) -> str:
    opts = {"quiet": True, "skip_download": True, "extract_flat": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        result = ydl.extract_info(f"ytsearch1:{topic}", download=False)

    entry = result["entries"][0]
    url = f"https://www.youtube.com/watch?v={entry['id']}"
    print(f"Found: {entry.get('title')}  ({url})")
    return url


# ── Step 2: Send YouTube URL to Reap for clipping ────────────────────────────
def create_clips_project(youtube_url: str) -> dict:
    payload = {"sourceUrl": youtube_url, **CLIPPING_OPTIONS}
    resp = requests.post(f"{BASE_URL}/create-clips", headers=HEADERS, json=payload)
    if not resp.ok:
        print(f"Reap API error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return resp.json()


# ── Step 3: Poll until complete ───────────────────────────────────────────────
def wait_for_completion(project_id: str) -> None:
    print(f"\nPolling project {project_id} every {POLL_INTERVAL}s ...")
    while True:
        resp = requests.get(
            f"{BASE_URL}/get-project-status",
            headers=HEADERS,
            params={"projectId": project_id},
        )
        resp.raise_for_status()
        status = resp.json().get("status", "unknown")
        print(f"  Status: {status}")

        if status == "completed":
            break
        if status == "failed":
            raise RuntimeError(f"Reap project {project_id} failed.")

        time.sleep(POLL_INTERVAL)


# ── Step 4: Retrieve clips ────────────────────────────────────────────────────
def get_clips(project_id: str) -> list[dict]:
    resp = requests.get(
        f"{BASE_URL}/get-project-clips",
        headers=HEADERS,
        params={"projectId": project_id},
    )
    resp.raise_for_status()
    return resp.json().get("clips", [])


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run(topic: str):
    # 1. Search YouTube
    print(f"\nSearching YouTube for: '{topic}'")
    youtube_url = search_youtube(topic)

    # 2. Send to Reap
    print(f"\nSending to Reap for clipping ...")
    project = create_clips_project(youtube_url)
    project_id = project["id"]
    print(f"Project ID: {project_id}  |  Status: {project['status']}")

    # 3. Wait
    wait_for_completion(project_id)

    # 4. Print results
    clips = get_clips(project_id)
    print(f"\nDone! {len(clips)} clip(s) ready:\n")
    for i, clip in enumerate(clips, 1):
        print(f"  [{i}] Title         : {clip.get('title')}")
        print(f"       Duration      : {clip.get('duration')}s")
        print(f"       Virality Score: {clip.get('viralityScore')}")
        print(f"       Download URL  : {clip.get('clipUrl')}")
        captions = clip.get("captions") or clip.get("transcript") or clip.get("subtitles")
        if captions:
            print(f"       Captions      : {captions}")
        print()


if __name__ == "__main__":
    run(topic="Understanding Depression")
