import os
import time
import requests


GRAPH_API_BASE = "https://graph.instagram.com/v21.0"


def upload_video_to_instagram(
    video_path: str,
    caption: str,
    ig_user_id: str,
    access_token: str,
    video_url: str = None,
) -> str:
    """
    Upload a video as an Instagram Reel.

    Instagram Graph API requires a publicly accessible video URL.
    Either pass `video_url` directly, or host the file yourself and pass the URL.

    Returns the published media ID.
    """
    if not video_url:
        raise ValueError(
            "Instagram API requires a public video URL. "
            "Host your video (e.g. on S3, Cloudinary, or ngrok) and pass it as `video_url`."
        )

    print(f"[Instagram] Creating media container for: {os.path.basename(video_path)}")

    # Step 1 — Create media container
    container_resp = requests.post(
        f"{GRAPH_API_BASE}/{ig_user_id}/media",
        data={
            "media_type": "REELS",
            "video_url": video_url,
            "caption": caption,
            "access_token": access_token,
        },
    )
    container_resp.raise_for_status()
    creation_id = container_resp.json()["id"]
    print(f"[Instagram] Container created: {creation_id}")

    # Step 2 — Wait for container to finish processing
    _wait_for_container(creation_id, access_token)

    # Step 3 — Publish
    publish_resp = requests.post(
        f"{GRAPH_API_BASE}/{ig_user_id}/media_publish",
        data={
            "creation_id": creation_id,
            "access_token": access_token,
        },
    )
    publish_resp.raise_for_status()
    media_id = publish_resp.json()["id"]
    print(f"[Instagram] Published! Media ID: {media_id}")
    return media_id


def _wait_for_container(creation_id: str, access_token: str, timeout: int = 300):
    """Poll until the media container status is FINISHED."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{GRAPH_API_BASE}/{creation_id}",
            params={
                "fields": "status_code,status",
                "access_token": access_token,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status_code", "")
        print(f"[Instagram] Container status: {status}")

        if status == "FINISHED":
            return
        if status == "ERROR":
            raise RuntimeError(f"Instagram container processing failed: {data}")

        time.sleep(10)

    raise TimeoutError("Instagram container processing timed out.")


def upload_clips_to_instagram(
    clip_paths: list[str],
    captions: list[str] | str,
    ig_user_id: str,
    access_token: str,
    get_public_url,  # callable: (local_path) -> public_url
) -> list[str]:
    """
    Upload multiple clips to Instagram.

    Args:
        clip_paths:     List of local video file paths.
        captions:       Either a list of captions (one per clip) or a single string reused for all.
        ig_user_id:     Your Instagram Business/Creator account ID.
        access_token:   Long-lived Instagram Graph API access token.
        get_public_url: A callable that takes a local path and returns a public HTTPS URL.
                        Implement this however your hosting works (S3, Cloudinary, ngrok, etc.)

    Returns:
        List of published media IDs.
    """
    if isinstance(captions, str):
        captions = [captions] * len(clip_paths)

    media_ids = []
    for i, (path, caption) in enumerate(zip(clip_paths, captions)):
        print(f"\n[Instagram] Uploading clip {i + 1}/{len(clip_paths)}: {path}")
        public_url = get_public_url(path)
        media_id = upload_video_to_instagram(
            video_path=path,
            caption=caption,
            ig_user_id=ig_user_id,
            access_token=access_token,
            video_url=public_url,
        )
        media_ids.append(media_id)

    return media_ids


# ─── Supabase Storage hosting ────────────────────────────────────────────────

def supabase_uploader(local_path: str) -> str:
    """
    Upload a video to a Supabase Storage public bucket and return its public URL.
    Requires: pip install supabase
    Set env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_BUCKET
    """
    from supabase import create_client

    url = os.environ["SUPABASE_URL"]          # e.g. https://xxxx.supabase.co
    key = os.environ["SUPABASE_SERVICE_KEY"]  # Service role key (not anon key)
    bucket = os.environ.get("SUPABASE_BUCKET", "mello_audio")

    client = create_client(url, key)

    filename = os.path.basename(local_path)
    dest_path = f"clips/{filename}"

    print(f"[Supabase] Uploading {filename} to bucket '{bucket}' ...")
    with open(local_path, "rb") as f:
        client.storage.from_(bucket).upload(
            path=dest_path,
            file=f,
            file_options={"content-type": "video/mp4", "upsert": "true"},
        )

    public_url = client.storage.from_(bucket).get_public_url(dest_path)
    print(f"[Supabase] Public URL: {public_url}")
    return public_url


# ─── Free hosting — no account or keys needed ────────────────────────────────

def free_host_uploader(local_path: str) -> str:
    """
    Upload a video to 0x0.st (free, no account needed) and return its public URL.
    Files are kept for at least 30 days. No size limit for videos under ~512 MB.
    """
    print(f"[Host] Uploading {os.path.basename(local_path)} to 0x0.st ...")
    with open(local_path, "rb") as f:
        resp = requests.post("https://0x0.st", files={"file": f})
    resp.raise_for_status()
    url = resp.text.strip()
    print(f"[Host] Public URL: {url}")
    return url


def transfersh_uploader(local_path: str) -> str:
    """
    Fallback: upload to transfer.sh (free, no account needed).
    Files expire after 14 days.
    """
    filename = os.path.basename(local_path)
    print(f"[Host] Uploading {filename} to transfer.sh ...")
    with open(local_path, "rb") as f:
        resp = requests.put(f"https://transfer.sh/{filename}", data=f)
    resp.raise_for_status()
    url = resp.text.strip()
    print(f"[Host] Public URL: {url}")
    return url


# ─── Quick test ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Fill these in or set as environment variables
    IG_USER_ID = os.environ.get("IG_USER_ID", "YOUR_IG_USER_ID")
    ACCESS_TOKEN = os.environ.get("IG_ACCESS_TOKEN", "YOUR_LONG_LIVED_TOKEN")

    # Clips produced by cpt_code.py pipeline
    clips = ["clip_1.mp4", "clip_2.mp4"]

    ids = upload_clips_to_instagram(
        clip_paths=clips,
        captions="#mentalhealth #shorts",
        ig_user_id=IG_USER_ID,
        access_token=ACCESS_TOKEN,
        get_public_url=supabase_uploader,  # or swap with free_host_uploader
    )

    print("\nAll done! Published media IDs:", ids)
