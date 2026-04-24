"""
Full pipeline: SEO Trends → Reap clipping → Instagram upload

Flow:
  1. seotreand.get_seo_topics()              → topic briefs with target queries
  2. reap_pipeline (search → clip → poll)    → clips with public download URLs
  3. instagram_upload.upload_video_to_instagram() → publish highest-virality clip as a Reel

Checkpoint system: pipeline_state.json records each completed stage per topic.
On restart the pipeline resumes from the last successful stage — no work is repeated.

Stages per topic (in order):
  youtube_searched → reap_submitted → reap_completed → instagram_uploaded → db_saved
"""

import os
import json
import boto3
from datetime import datetime, timezone
from supabase import create_client, Client
from seotreand import get_seo_topics
from reap_pipeline import search_youtube, create_clips_project, wait_for_completion, get_clips
from instagram_upload import upload_video_to_instagram

bedrock_client = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

MODEL_ID = "meta.llama3-8b-instruct-v1:0"


def improve_caption(raw_caption: str) -> str:
    prompt = (
        "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n"
        "You write viral Instagram Reel captions for a mental health page.\n"
        "Rewrite the caption below to be more engaging, emotionally resonant, and hook the viewer in the first line.\n"
        "Keep it under 200 characters. Include the original hashtags at the end.\n\n"
        f"Caption: {raw_caption}\n"
        "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"
    )

    response = bedrock_client.invoke_model(
        modelId=MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({"prompt": prompt, "max_gen_len": 256, "temperature": 0.7}),
    )

    result = json.loads(response["body"].read())
    improved = result.get("generation", "").strip()
    print(f"[Bedrock] Improved caption: {improved}")
    return improved or raw_caption

IG_USER_ID    = os.environ["IG_USER_ID"]
ACCESS_TOKEN  = os.environ["IG_ACCESS_TOKEN"]
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

TABLE          = "social media"
MAX_TOPICS     = 3
STATE_FILE     = "pipeline_state.json"
FALLBACK_FILE  = "failed_records.jsonl"

db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def checkpoint(state: dict, topic_key: str, stage: str, **data) -> None:
    """Mark a stage as done and persist any associated data."""
    if topic_key not in state:
        state[topic_key] = {}
    state[topic_key][stage] = {"done": True, "ts": datetime.now(timezone.utc).isoformat(), **data}
    save_state(state)
    print(f"[Checkpoint] {topic_key} → {stage}")


def stage_done(state: dict, topic_key: str, stage: str) -> bool:
    return state.get(topic_key, {}).get(stage, {}).get("done", False)


def stage_data(state: dict, topic_key: str, stage: str) -> dict:
    return state.get(topic_key, {}).get(stage, {})


# ── Supabase helpers ──────────────────────────────────────────────────────────

def already_processed(youtube_url: str) -> bool:
    try:
        result = (
            db.table(TABLE)
            .select("id")
            .eq("youtube_link", youtube_url)
            .limit(1)
            .execute()
        )
        return len(result.data) > 0
    except Exception:
        return False


def _save_local(youtube_url: str, download_url: str, topic_title: str) -> None:
    record = {
        "created_at":    datetime.now(timezone.utc).isoformat(),
        "youtube_link":  youtube_url,
        "download_link": download_url,
        "topic":         topic_title,
    }
    with open(FALLBACK_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    print(f"[Local] Record saved to {FALLBACK_FILE} — topic: {topic_title}")


def save_record(youtube_url: str, download_url: str, topic_title: str) -> None:
    try:
        db.table(TABLE).insert({
            "youtube_link":  youtube_url,
            "download_link": download_url,
            "topic":         topic_title,
        }).execute()
        print(f"[Supabase] Row saved — topic: {topic_title}")
    except Exception as exc:
        print(f"[Supabase] Insert failed ({exc}) — falling back to local file.")
        _save_local(youtube_url, download_url, topic_title)


# ── Caption builder ───────────────────────────────────────────────────────────

def build_caption(topic: dict) -> str:
    title     = topic.get("title", "")
    focus_kw  = topic.get("focus_keyword", "")
    secondary = topic.get("secondary_keywords", [])
    hashtags  = " ".join(f"#{kw.replace(' ', '')}" for kw in [focus_kw] + secondary if kw)
    return f"{title}\n\n{hashtags}\n\n#mentalhealth #shorts"


# ── Per-topic pipeline ────────────────────────────────────────────────────────

def process_topic(topic: dict, state: dict) -> str | None:
    """
    Resume-aware pipeline for a single topic.
    Each stage checks the checkpoint before running and saves on success.
    Returns the Instagram media ID, or None if skipped/failed.
    """
    topic_key = topic.get("title", topic.get("target_query", "unknown"))
    query     = topic.get("target_query") or topic.get("title")

    print(f"\n{'='*60}")
    print(f"Topic : {topic_key}")
    print(f"Query : {query}")

    # ── Stage 1: YouTube search ───────────────────────────────────────────────
    if stage_done(state, topic_key, "youtube_searched"):
        youtube_url = stage_data(state, topic_key, "youtube_searched")["youtube_url"]
        print(f"[Resume] youtube_searched → {youtube_url}")
    else:
        youtube_url = search_youtube(query)
        if already_processed(youtube_url):
            print(f"[Skip] {youtube_url} already in Supabase.")
            checkpoint(state, topic_key, "youtube_searched", youtube_url=youtube_url)
            checkpoint(state, topic_key, "db_saved")         # mark whole topic done
            return None
        checkpoint(state, topic_key, "youtube_searched", youtube_url=youtube_url)

    # ── Stage 2: Submit to Reap ───────────────────────────────────────────────
    if stage_done(state, topic_key, "reap_submitted"):
        project_id = stage_data(state, topic_key, "reap_submitted")["project_id"]
        print(f"[Resume] reap_submitted → project {project_id}")
    else:
        print("Submitting to Reap...")
        project    = create_clips_project(youtube_url)
        project_id = project["id"]
        print(f"Reap project ID: {project_id}")
        checkpoint(state, topic_key, "reap_submitted", project_id=project_id)

    # ── Stage 3: Wait for Reap to finish ─────────────────────────────────────
    if stage_done(state, topic_key, "reap_completed"):
        clip_url = stage_data(state, topic_key, "reap_completed")["clip_url"]
        print(f"[Resume] reap_completed → {clip_url}")
    else:
        wait_for_completion(project_id)
        clips = get_clips(project_id)
        if not clips:
            print("No clips returned — skipping.")
            return None
        best     = max(clips, key=lambda c: c.get("viralityScore", 0))
        clip_url = best.get("clipUrl")
        if not clip_url:
            print("Top clip has no URL — skipping.")
            return None
        print(f"Best clip : {best.get('title')}  |  Virality: {best.get('viralityScore')}")
        print(f"URL       : {clip_url}")
        checkpoint(state, topic_key, "reap_completed",
                   clip_url=clip_url,
                   clip_title=best.get("title"),
                   virality=best.get("viralityScore"))

    # ── Stage 4: Upload to Instagram ─────────────────────────────────────────
    if stage_done(state, topic_key, "instagram_uploaded"):
        media_id = stage_data(state, topic_key, "instagram_uploaded")["media_id"]
        print(f"[Resume] instagram_uploaded → media {media_id}")
    else:
        clip_title = stage_data(state, topic_key, "reap_completed").get("clip_title", "clip")
        caption = improve_caption(build_caption(topic))
        media_id = upload_video_to_instagram(
            video_path=clip_title + ".mp4",
            caption=caption,
            ig_user_id=IG_USER_ID,
            access_token=ACCESS_TOKEN,
            video_url=clip_url,
        )
        print(f"Instagram media ID: {media_id}")
        checkpoint(state, topic_key, "instagram_uploaded", media_id=media_id)

    # ── Stage 5: Save to Supabase ─────────────────────────────────────────────
    if stage_done(state, topic_key, "db_saved"):
        print("[Resume] db_saved — already recorded.")
    else:
        save_record(
            youtube_url=youtube_url,
            download_url=clip_url,
            topic_title=topic_key,
        )
        checkpoint(state, topic_key, "db_saved")

    return media_id


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    state = load_state()
    if state:
        print(f"[Checkpoint] Resuming from {STATE_FILE} ({len(state)} topic(s) have prior progress).")

    print("Fetching SEO topics from Google Trends...")
    data   = get_seo_topics()
    topics = data.get("topics", [])[:MAX_TOPICS]
    print(f"Got {len(topics)} topic(s) to process.\n")

    media_ids = []
    for topic in topics:
        topic_key = topic.get("title", topic.get("target_query", "unknown"))
        if stage_done(state, topic_key, "db_saved"):
            print(f"[Skip] '{topic_key}' fully completed in a previous run.")
            continue
        try:
            mid = process_topic(topic, state)
            if mid:
                media_ids.append(mid)
        except Exception as exc:
            print(f"[ERROR] Topic '{topic_key}' failed at current stage: {exc}")
            print("        Progress saved — re-run to resume from this point.")

    print(f"\n{'='*60}")
    print(f"Pipeline complete. Published {len(media_ids)} reel(s).")
    for mid in media_ids:
        print(f"  - {mid}")

    # Clean up state file once everything in this batch is done
    all_done = all(
        stage_done(state, t.get("title", t.get("target_query", "")), "db_saved")
        for t in topics
    )
    if all_done and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print(f"[Checkpoint] All topics complete — {STATE_FILE} cleared.")


if __name__ == "__main__":
    main()
