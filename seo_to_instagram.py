"""
Full pipeline: SEO Trends → Reap clipping → Instagram upload

Flow:
  1. seotreand.get_seo_topics()              → topic briefs with target queries
  2. reap_pipeline (search → clip → poll)    → clips with public download URLs
  3. instagram_upload.upload_video_to_instagram() → publish highest-virality clip as a Reel

Deduplication: the Supabase `social_media` table is checked before sending a YouTube
URL to Reap — if it was already processed in a previous run it is skipped.
After a successful upload the row (youtube_link, download_link, topic) is inserted.
"""

import os
from supabase import create_client, Client
from seotreand import get_seo_topics
from reap_pipeline import search_youtube, create_clips_project, wait_for_completion, get_clips
from instagram_upload import upload_video_to_instagram

IG_USER_ID    = os.environ["IG_USER_ID"]
ACCESS_TOKEN  = os.environ["IG_ACCESS_TOKEN"]
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

TABLE = "social media"   # matches the Supabase table name exactly
MAX_TOPICS = 3

db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Supabase helpers ──────────────────────────────────────────────────────────

def already_processed(youtube_url: str) -> bool:
    """Return True if this YouTube URL exists in the social_media table."""
    result = (
        db.table(TABLE)
        .select("id")
        .eq("youtube_link", youtube_url)
        .limit(1)
        .execute()
    )
    return len(result.data) > 0


def save_record(youtube_url: str, download_url: str, topic_title: str) -> None:
    """Insert a completed pipeline record into the social_media table."""
    db.table(TABLE).insert({
        "youtube_link":  youtube_url,
        "download_link": download_url,
        "topic":         topic_title,
    }).execute()
    print(f"[Supabase] Row saved — topic: {topic_title}")


# ── Caption builder ───────────────────────────────────────────────────────────

def build_caption(topic: dict) -> str:
    title      = topic.get("title", "")
    focus_kw   = topic.get("focus_keyword", "")
    secondary  = topic.get("secondary_keywords", [])
    hashtags   = " ".join(f"#{kw.replace(' ', '')}" for kw in [focus_kw] + secondary if kw)
    return f"{title}\n\n{hashtags}\n\n#mentalhealth #shorts"


# ── Per-topic pipeline ────────────────────────────────────────────────────────

def process_topic(topic: dict) -> str | None:
    """
    1. Search YouTube for the topic query.
    2. Skip if the URL is already in Supabase (deduplication).
    3. Send to Reap, poll, pick highest-virality clip.
    4. Upload clip URL directly to Instagram.
    5. Save record to Supabase.
    Returns the Instagram media ID, or None if skipped/failed.
    """
    query = topic.get("target_query") or topic.get("title")
    print(f"\n{'='*60}")
    print(f"Topic : {topic.get('title')}")
    print(f"Query : {query}")

    # Step 1 — find YouTube video
    youtube_url = search_youtube(query)

    # Step 2 — deduplication check
    if already_processed(youtube_url):
        print(f"[Supabase] Already processed {youtube_url} — skipping.")
        return None

    # Step 3 — send to Reap
    print("Submitting to Reap...")
    project    = create_clips_project(youtube_url)
    project_id = project["id"]
    print(f"Reap project ID: {project_id}")
    wait_for_completion(project_id)

    clips = get_clips(project_id)
    if not clips:
        print("No clips returned — skipping.")
        return None

    best      = max(clips, key=lambda c: c.get("viralityScore", 0))
    clip_url  = best.get("clipUrl")
    if not clip_url:
        print("Top clip has no URL — skipping.")
        return None

    print(f"Best clip : {best.get('title')}")
    print(f"Virality  : {best.get('viralityScore')}")
    print(f"URL       : {clip_url}")

    # Step 4 — upload to Instagram (Reap URL is already public HTTPS)
    media_id = upload_video_to_instagram(
        video_path=best.get("title", "clip") + ".mp4",
        caption=build_caption(topic),
        ig_user_id=IG_USER_ID,
        access_token=ACCESS_TOKEN,
        video_url=clip_url,
    )
    print(f"Instagram media ID: {media_id}")

    # Step 5 — persist to Supabase
    save_record(
        youtube_url=youtube_url,
        download_url=clip_url,
        topic_title=topic.get("title", query),
    )

    return media_id


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("Fetching SEO topics from Google Trends...")
    data   = get_seo_topics()
    topics = data.get("topics", [])[:MAX_TOPICS]
    print(f"Got {len(topics)} topic(s) to process.\n")

    media_ids = []
    for topic in topics:
        try:
            mid = process_topic(topic)
            if mid:
                media_ids.append(mid)
        except Exception as exc:
            print(f"[ERROR] Topic '{topic.get('title')}' failed: {exc}")

    print(f"\n{'='*60}")
    print(f"Pipeline complete. Published {len(media_ids)} reel(s).")
    for mid in media_ids:
        print(f"  - {mid}")


if __name__ == "__main__":
    main()
