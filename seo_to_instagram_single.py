"""
Full pipeline: SEO Trends → Reap clipping → Instagram upload (single file)

Flow:
  1. get_seo_topics()                          → topic briefs with target queries
  2. search_youtube / create_clips_project      → clips with public download URLs
  3. upload_video_to_instagram()                → publish highest-virality clip as a Reel

Checkpoint system: pipeline_state.json records each completed stage per topic.
On restart the pipeline resumes from the last successful stage — no work is repeated.

Stages per topic (in order):
  youtube_searched → reap_submitted → reap_completed → instagram_uploaded → db_saved
"""

import os
import json
import re
import time
import random
import subprocess
import tempfile
import urllib.request
from datetime import datetime, timezone

import urllib3
import boto3
import requests
import yt_dlp
from pytrends.request import TrendReq
from dotenv import load_dotenv
from supabase import create_client, Client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Env ───────────────────────────────────────────────────────────────────────

# load_dotenv only for local development; Lambda supplies env vars directly
if os.path.exists(".env"):
    load_dotenv()

IG_USER_ID   = os.environ["IG_USER_ID"]
ACCESS_TOKEN = os.environ["IG_ACCESS_TOKEN"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

TABLE               = "social media"
MAX_TOPICS          = 1
STATE_FILE          = "/tmp/pipeline_state.json"
FALLBACK_FILE       = "/tmp/failed_records.jsonl"
POST_COOLDOWN_HOURS = 10

db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Bedrock ───────────────────────────────────────────────────────────────────

bedrock_client = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
)

MODEL_ID = "meta.llama3-8b-instruct-v1:0"

# ── Reap config ───────────────────────────────────────────────────────────────

REAP_API_KEY = os.environ.get(
    "REAP_API_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhdXRvbWF0aW9uLWFwaXw2OWU4NjZhNGY3N2YzNzY5NDRjMmU1YWUiLCJleHAiOjE4MjA5MDIzMDguMDIyODAxOX0.D6kQBcyPGHyuZRn3lSIWQU6VjeHip5xkKSCi32grJRo",
)
print(f"Using Reap API Key: {REAP_API_KEY}")
REAP_BASE_URL = "https://public.reap.video/api/v1/automation"
REAP_HEADERS  = {"Authorization": f"Bearer {REAP_API_KEY}", "Content-Type": "application/json"}

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
    "watermark": False,
}

POLL_INTERVAL = 15

GRAPH_API_BASE = "https://graph.instagram.com/v21.0"
FB_GRAPH_BASE  = "https://graph.facebook.com/v21.0"
FB_PAGE_ID           = os.environ.get("FB_PAGE_ID", "1111020322091625")
FB_PAGE_ACCESS_TOKEN = os.environ.get("FB_PAGE_ACCESS_TOKEN", "")

BGM_PATH   = os.environ.get("BGM_PATH", os.path.join(os.path.dirname(__file__), "background_music.mp3"))
BGM_VOLUME = float(os.environ.get("BGM_VOLUME", "0.40"))

# ═════════════════════════════════════════════════════════════════════════════
# SEO TRENDS
# ═════════════════════════════════════════════════════════════════════════════

def _extract_queries(related_queries, top_n=10):
    seen, queries = set(), []
    for keyword in related_queries:
        for kind in ("top", "rising"):
            df = related_queries[keyword].get(kind)
            if df is not None and not df.empty:
                for q in df["query"].head(top_n).tolist():
                    if q.lower() not in seen:
                        seen.add(q.lower())
                        queries.append(q)
    return queries


def _clean_json(s: str) -> str:
    s = re.sub(r',\s*([}\]])', r'\1', s)
    s = re.sub(r"'([^']*)'", r'"\1"', s)
    s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', s)
    return s


def _parse_json_response(text: str, retries: int = 2) -> dict:
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON object found in model response:\n{text}")
    raw = match.group()
    for attempt in range(retries + 1):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            if attempt == retries:
                raise ValueError(f"Could not parse JSON after {retries} cleanup attempts.\nRaw:\n{raw}")
            raw = _clean_json(raw)


def get_seo_topics(keywords=None) -> dict:
    if keywords is None:
        keywords = [
            "attachment style", "anxious attachment", "avoidant attachment", "secure attachment",
            "relationship anxiety", "toxic relationship", "codependency", "emotional unavailability",
            "fearful avoidant", "relationship trauma",
        ]

    random.shuffle(keywords)
    keywords = keywords[:5]

    queries_text = None
    try:
        pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=0.5)
        pytrends.build_payload(keywords, timeframe='today 3-m', geo='US')
        related_queries = pytrends.related_queries()
        all_queries = _extract_queries(related_queries)
        if all_queries:
            queries_text = "\n".join(f"- {q}" for q in all_queries)
            print(f"[Trends] Fetched {len(all_queries)} related queries from Google Trends.")
    except Exception as exc:
        print(f"[Trends] Google Trends unavailable ({exc}) — falling back to seed keywords.")

    if queries_text is None:
        queries_text = "\n".join(f"- {kw}" for kw in keywords)
        print("[Trends] Using seed keywords directly.")

    prompt = f"""You are a relationships and attachment style content strategist.

Based on the following relationship and attachment style topics, generate 5 topic briefs for Instagram Reels.

Topics:
{queries_text}

Return ONLY a valid JSON object — no markdown, no code fences, no explanation.

The format must be exactly:
{{
  "topics": [
    {{
      "title": "...",
      "target_query": "...",
      "focus_keyword": "...",
      "secondary_keywords": ["...", "...", "...", "...", "..."]
    }}
  ]
}}

Rules:
- title: under 60 characters, attention-grabbing, focus keyword near the start
- target_query: exact query from the trending list above
- focus_keyword: the main keyword to target
- secondary_keywords: 4-5 related hashtag-friendly terms
"""

    response = bedrock_client.converse(
        modelId=MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 2048, "temperature": 0.7, "topP": 0.9},
        additionalModelRequestFields={},
        performanceConfig={"latency": "standard"},
    )

    text = response["output"]["message"]["content"][0]["text"]
    return _parse_json_response(text)


# ═════════════════════════════════════════════════════════════════════════════
# REAP PIPELINE
# ═════════════════════════════════════════════════════════════════════════════

_MUSIC_TITLE_KEYWORDS = {"official music video", "official video", "lyrics", "audio", "music video", "full video"}

def _is_music_result(info: dict) -> bool:
    categories = [c.lower() for c in (info.get("categories") or [])]
    if "music" in categories:
        return True
    title_lower = (info.get("title") or "").lower()
    return any(kw in title_lower for kw in _MUSIC_TITLE_KEYWORDS)


def _enrich_query(topic: str) -> str:
    """Append talk/podcast context so generic terms don't hit music results."""
    filler_words = {"attachment", "relationships", "codependency", "trauma", "anxiety", "avoidant"}
    if topic.lower().strip() in filler_words:
        return f"{topic} talk podcast mental health"
    return topic


def search_youtube(topic: str, max_candidates: int = 15) -> list[str]:
    query = _enrich_query(topic)
    search_opts = {"quiet": True, "skip_download": True, "extract_flat": True}
    with yt_dlp.YoutubeDL(search_opts) as ydl:
        result = ydl.extract_info(f"ytsearch{max_candidates}:{query}", download=False)

    entries = result.get("entries", [])
    if not entries:
        raise RuntimeError(f"No YouTube results for: {query}")

    candidates = []
    meta_opts = {"quiet": True, "skip_download": True}
    with yt_dlp.YoutubeDL(meta_opts) as ydl:
        for entry in entries:
            video_id = entry.get("id")
            if not video_id:
                continue
            url = f"https://www.youtube.com/watch?v={video_id}"
            try:
                info = ydl.extract_info(url, download=False)
            except Exception:
                continue

            if _is_music_result(info):
                print(f"[Skip] Music content, skipping: {info.get('title')}  ({url})")
                continue

            duration_s = info.get("duration") or 0
            if duration_s > 1800:
                print(f"[Skip] Video too long ({duration_s // 60}m), skipping: {info.get('title')}  ({url})")
                continue

            print(f"Found candidate: {info.get('title')}  ({url})")
            candidates.append(url)

    if not candidates:
        raise RuntimeError(f"No suitable (non-music) YouTube video found for: {topic}")
    return candidates


def create_clips_project(youtube_url: str) -> dict:
    payload = {"sourceUrl": youtube_url, **CLIPPING_OPTIONS}
    resp = requests.post(f"{REAP_BASE_URL}/create-clips", headers=REAP_HEADERS, json=payload, verify=False)
    if not resp.ok:
        print(f"Reap API error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return resp.json()


def wait_for_completion(project_id: str, max_wait: int = 600) -> None:
    """Poll until Reap completes. max_wait keeps total within Lambda's 15-min limit."""
    print(f"\nPolling project {project_id} every {POLL_INTERVAL}s (max {max_wait}s) ...")
    deadline = time.time() + max_wait
    while True:
        if time.time() > deadline:
            raise TimeoutError(
                f"Reap project {project_id} did not complete within {max_wait}s. "
                "Re-invoke Lambda to resume from checkpoint."
            )
        resp = requests.get(
            f"{REAP_BASE_URL}/get-project-status",
            headers=REAP_HEADERS,
            params={"projectId": project_id},
            verify=False,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "unknown")
        print(f"  Status: {status}")

        if status == "completed":
            break
        if status == "failed":
            raise RuntimeError(f"Reap project {project_id} failed.")

        time.sleep(POLL_INTERVAL)


def get_clips(project_id: str) -> list[dict]:
    resp = requests.get(
        f"{REAP_BASE_URL}/get-project-clips",
        headers=REAP_HEADERS,
        params={"projectId": project_id},
        verify=False,
    )
    resp.raise_for_status()
    return resp.json().get("clips", [])


# ═════════════════════════════════════════════════════════════════════════════
# INSTAGRAM UPLOAD
# ═════════════════════════════════════════════════════════════════════════════

def _wait_for_container(creation_id: str, access_token: str, timeout: int = 300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{GRAPH_API_BASE}/{creation_id}",
            params={"fields": "status_code,status", "access_token": access_token},
        )
        resp.raise_for_status()
        data   = resp.json()
        status = data.get("status_code", "")
        print(f"[Instagram] Container status: {status}")

        if status == "FINISHED":
            return
        if status == "ERROR":
            raise RuntimeError(f"Instagram container processing failed: {data}")

        time.sleep(10)

    raise TimeoutError("Instagram container processing timed out.")


def upload_video_to_instagram(
    video_path: str,
    caption: str,
    ig_user_id: str,
    access_token: str,
    video_url: str = None,
) -> str:
    if not video_url:
        raise ValueError(
            "Instagram API requires a public video URL. "
            "Host your video (e.g. on S3, Cloudinary, or ngrok) and pass it as `video_url`."
        )

    print(f"[Instagram] Creating media container for: {os.path.basename(video_path)}")

    container_resp = requests.post(
        f"{GRAPH_API_BASE}/{ig_user_id}/media",
        data={
            "media_type":  "REELS",
            "video_url":   video_url,
            "caption":     caption,
            "access_token": access_token,
        },
    )
    container_resp.raise_for_status()
    creation_id = container_resp.json()["id"]
    print(f"[Instagram] Container created: {creation_id}")

    _wait_for_container(creation_id, access_token)

    publish_resp = requests.post(
        f"{GRAPH_API_BASE}/{ig_user_id}/media_publish",
        data={"creation_id": creation_id, "access_token": access_token},
    )
    publish_resp.raise_for_status()
    media_id = publish_resp.json()["id"]
    print(f"[Instagram] Published! Media ID: {media_id}")
    return media_id


def _adapt_caption_for_facebook(caption: str) -> str:
    return (
        caption
        .replace("Follow @self.mind.app", "Follow SelfMind on Facebook: facebook.com/selfmindforyou")
        .replace("Follow @self.mind.app", "Follow SelfMind on Facebook: facebook.com/selfmindforyou")
        .replace("+ Visit Our Site: selfmind.app", "Find us at facebook.com/selfmindforyou")
    )


def upload_video_to_facebook(video_url: str, caption: str) -> str:
    if not FB_PAGE_ACCESS_TOKEN:
        print("[Facebook] FB_PAGE_ACCESS_TOKEN not set — skipping Facebook upload.")
        return None

    fb_caption = _adapt_caption_for_facebook(caption)
    print(f"[Facebook] Uploading reel to page {FB_PAGE_ID} ...")
    resp = requests.post(
        f"{FB_GRAPH_BASE}/{FB_PAGE_ID}/videos",
        data={
            "file_url":     video_url,
            "description":  fb_caption,
            "published":    "true",
            "access_token": FB_PAGE_ACCESS_TOKEN,
        },
    )
    if not resp.ok:
        print(f"[Facebook] Upload failed {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    video_id = resp.json().get("id")
    print(f"[Facebook] Posted! Video ID: {video_id}")
    return video_id


def supabase_uploader(local_path: str) -> str:
    bucket   = os.environ.get("SUPABASE_BUCKET", "mello_audio")
    filename = os.path.basename(local_path)
    dest_path = f"clips/{filename}"

    print(f"[Supabase] Uploading {filename} to bucket '{bucket}' ...")
    file_size = os.path.getsize(local_path)
    upload_url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{dest_path}"
    with open(local_path, "rb") as f:
        resp = requests.post(
            upload_url,
            headers={
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "video/mp4",
                "Content-Length": str(file_size),
                "x-upsert": "true",
            },
            data=f,
            timeout=300,
        )
    if not resp.ok:
        raise RuntimeError(f"Supabase upload failed {resp.status_code}: {resp.text}")

    public_url = db.storage.from_(bucket).get_public_url(dest_path)
    print(f"[Supabase] Public URL: {public_url}")
    return public_url


# ═════════════════════════════════════════════════════════════════════════════
# BGM MIXING
# ═════════════════════════════════════════════════════════════════════════════

def mix_bgm(
    clip_url: str,
    output_path: str,
    music_path: str = BGM_PATH,
    volume: float = BGM_VOLUME,
) -> str:
    if not os.path.exists(music_path):
        raise FileNotFoundError(
            f"Background music file not found: {music_path}\n"
            "Place an mp3/wav file there or set the BGM_PATH env var."
        )

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
        tmp_video = tmp.name

    try:
        print(f"[BGM] Downloading clip from {clip_url} ...")
        urllib.request.urlretrieve(clip_url, tmp_video)

        print(f"[BGM] Mixing background music at volume {volume} ...")
        cmd = [
            "ffmpeg", "-y",
            "-i", tmp_video,
            "-stream_loop", "-1", "-i", music_path,
            "-filter_complex",
            f"[1:a]volume={volume}[bg];[0:a][bg]amix=inputs=2:duration=first:dropout_transition=2[aout]",
            "-map", "0:v",
            "-map", "[aout]",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "28",
            "-vf", "scale=1080:-2",
            "-c:a", "aac",
            "-b:a", "128k",
            "-shortest",
            output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg failed:\n{result.stderr}")

        print(f"[BGM] Mixed video saved to {output_path}")
        return output_path
    finally:
        if os.path.exists(tmp_video):
            os.remove(tmp_video)


# ═════════════════════════════════════════════════════════════════════════════
# CAPTION / BEDROCK
# ═════════════════════════════════════════════════════════════════════════════

def improve_caption(raw_caption: str) -> str:
    prompt = (
        "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n"
        "You write viral Instagram Reel captions for a mental health and self-growth page called @self.mind.app.\n\n"
        "Style guide — follow this EXACTLY:\n"
        "- Open with a short, punchy emotional hook (1 line)\n"
        "- Use short single-sentence lines separated by blank lines to create rhythm\n"
        "- Build through contrast or a turning point (e.g. 'So I made a decision—')\n"
        "- End with what the brand stands for in 1-2 lines\n"
        "- Close CTA: 'Follow @self.mind.app for [benefit]\\n+ Visit our website: selfmind.app'\n"
        "- Then 5-8 relevant hashtags on the last line\n"
        "- Do NOT use bullet points, emojis, or asterisks\n"
        "- Total length: 150–300 words\n\n"
        "Example style:\n"
        "For a long time, I lived for approval.\n\n"
        "For expectations.\nFor comfort zones that weren't mine.\n\n"
        "I was told what I could do.\nWhat I shouldn't try.\n\n"
        "So I made a decision—\n\nI'll live for myself.\n\n"
        "Not to prove anything.\nNot to impress anyone.\n\n"
        "self.mind.app stands for owning your story.\n\n"
        "Follow @self.mind.app for real stories of resilience\n"
        "#mentalhealth #selfgrowth #mindset\n\n"
        "---\n\n"
        f"Now rewrite this caption in that style:\n{raw_caption}\n\n"
        "Output ONLY the caption text itself. No preamble, no labels, no phrases like "
        "'\"Here is the rewritten caption:\"' or '\"Here you go:\"'. Start directly with the first line of the caption.\n"
        "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"
    )

    response = bedrock_client.invoke_model(
        modelId=MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({"prompt": prompt, "max_gen_len": 512, "temperature": 0.75}),
    )

    result   = json.loads(response["body"].read())
    improved = result.get("generation", "").strip()
    # Strip common preamble lines the model sometimes emits
    preamble = re.compile(r'^(here (is|are|\'s)|sure[,!]?|okay[,!]?|of course[,!]?)[^\n]*\n', re.IGNORECASE)
    improved = preamble.sub("", improved).strip()
    print(f"[Bedrock] Improved caption: {improved}")
    return improved or raw_caption


def build_caption(topic: dict) -> str:
    title     = topic.get("title", "")
    focus_kw  = topic.get("focus_keyword", "")
    secondary = topic.get("secondary_keywords", [])
    hashtags  = " ".join(f"#{kw.replace(' ', '')}" for kw in [focus_kw] + secondary if kw)
    return (
        f"{title}\n\n"
        f"Follow @self.mind.app for daily mental health reminders\n"
        f"+ Download the app: selfmind.app\n\n"
        f"{hashtags} #mentalhealth #selfgrowth #mindset"
    )


# ═════════════════════════════════════════════════════════════════════════════
# CHECKPOINT / SUPABASE HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def last_post_within_cooldown() -> bool:
    try:
        result = (
            db.table(TABLE)
            .select("created_at")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if not result.data:
            return False
        last_ts = datetime.fromisoformat(result.data[0]["created_at"])
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        hours_since = (datetime.now(timezone.utc) - last_ts).total_seconds() / 3600
        print(f"[Cooldown] Last post was {hours_since:.1f}h ago (limit: {POST_COOLDOWN_HOURS}h).")
        return hours_since < POST_COOLDOWN_HOURS
    except Exception as exc:
        print(f"[Cooldown] Could not check last post time: {exc} — proceeding anyway.")
        return False


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def checkpoint(state: dict, topic_key: str, stage: str, **data) -> None:
    if topic_key not in state:
        state[topic_key] = {}
    state[topic_key][stage] = {"done": True, "ts": datetime.now(timezone.utc).isoformat(), **data}
    save_state(state)
    print(f"[Checkpoint] {topic_key} → {stage}")


def stage_done(state: dict, topic_key: str, stage: str) -> bool:
    return state.get(topic_key, {}).get(stage, {}).get("done", False)


def stage_data(state: dict, topic_key: str, stage: str) -> dict:
    return state.get(topic_key, {}).get(stage, {})


def already_processed(youtube_url: str, clip_url: str = None) -> bool:
    try:
        result = (
            db.table(TABLE)
            .select("id")
            .eq("youtube_link", youtube_url)
            .limit(1)
            .execute()
        )
        if len(result.data) > 0:
            return True
        if clip_url:
            result = (
                db.table(TABLE)
                .select("id")
                .eq("download_link", clip_url)
                .limit(1)
                .execute()
            )
            return len(result.data) > 0
        return False
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


# ═════════════════════════════════════════════════════════════════════════════
# PER-TOPIC PIPELINE
# ═════════════════════════════════════════════════════════════════════════════

def process_topic(topic: dict, state: dict) -> str | None:
    topic_key = topic.get("title", topic.get("target_query", "unknown"))
    query     = topic.get("target_query") or topic.get("title")
    media_id  = None

    print(f"\n{'='*60}")
    print(f"Topic : {topic_key}")
    print(f"Query : {query}")

    # Stage 1: YouTube search
    if stage_done(state, topic_key, "youtube_searched"):
        youtube_url = stage_data(state, topic_key, "youtube_searched")["youtube_url"]
        print(f"[Resume] youtube_searched → {youtube_url}")
    else:
        candidates = search_youtube(query)
        youtube_url = None
        for candidate in candidates:
            if already_processed(candidate):
                print(f"[Skip] {candidate} already in Supabase — trying next candidate.")
                continue
            youtube_url = candidate
            break
        if not youtube_url:
            print(f"[Skip] All {len(candidates)} candidates already in Supabase — skipping topic.")
            checkpoint(state, topic_key, "youtube_searched", youtube_url=candidates[0])
            checkpoint(state, topic_key, "db_saved")
            return None
        checkpoint(state, topic_key, "youtube_searched", youtube_url=youtube_url)

    # Stage 2: Submit to Reap
    if stage_done(state, topic_key, "reap_submitted"):
        project_id = stage_data(state, topic_key, "reap_submitted")["project_id"]
        print(f"[Resume] reap_submitted → project {project_id}")
    else:
        print("Submitting to Reap...")
        project    = create_clips_project(youtube_url)
        project_id = project["id"]
        print(f"Reap project ID: {project_id}")
        checkpoint(state, topic_key, "reap_submitted", project_id=project_id)

    # Stage 3: Wait for Reap
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

    # Stage 4: Upload to Instagram
    if stage_done(state, topic_key, "instagram_uploaded"):
        media_id = stage_data(state, topic_key, "instagram_uploaded")["media_id"]
        print(f"[Resume] instagram_uploaded → media {media_id}")
    else:
        if already_processed(youtube_url, clip_url):
            print("[Skip] Clip already posted — skipping.")
            return None
        caption = improve_caption(build_caption(topic))

        mixed_path = os.path.join("/tmp", f"bgm_{int(time.time())}.mp4")
        mix_bgm(clip_url, mixed_path)

        try:
            final_url = supabase_uploader(mixed_path)
            media_id = upload_video_to_instagram(
                video_path=mixed_path,
                caption=caption,
                ig_user_id=IG_USER_ID,
                access_token=ACCESS_TOKEN,
                video_url=final_url,
            )
            print(f"Instagram media ID: {media_id}")
            upload_video_to_facebook(final_url, caption)
            checkpoint(state, topic_key, "instagram_uploaded", media_id=media_id)
        finally:
            if os.path.exists(mixed_path):
                os.remove(mixed_path)
                print(f"[Cleanup] Deleted local video: {mixed_path}")

    # Stage 5: Save to Supabase
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


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def main():
    if last_post_within_cooldown():
        print(f"[Cooldown] Last post was less than {POST_COOLDOWN_HOURS}h ago — exiting.")
        return

    state = load_state()

    # Find any topic keys that aren't fully done yet
    incomplete_keys = [
        k for k, v in state.items()
        if k != "__topics__" and not v.get("db_saved", {}).get("done", False)
    ]
    saved_topics = state.get("__topics__", [])

    if incomplete_keys and saved_topics:
        # Resume with the exact same topics from the previous run
        topics = [
            t for t in saved_topics
            if t.get("title", t.get("target_query", "")) in incomplete_keys
        ]
        print(f"[Checkpoint] Resuming {len(topics)} incomplete topic(s) from {STATE_FILE}.")
    elif incomplete_keys and not saved_topics:
        # Old state format: reconstruct minimal topic dicts from saved keys
        topics = [
            {"title": k, "target_query": k, "focus_keyword": "", "secondary_keywords": []}
            for k in incomplete_keys[:MAX_TOPICS]
        ]
        print(f"[Checkpoint] Resuming {len(topics)} incomplete topic(s) (old state format).")
    else:
        print("Fetching SEO topics from Google Trends...")
        data   = get_seo_topics()
        topics = data.get("topics", [])[:MAX_TOPICS]
        state["__topics__"] = topics
        save_state(state)
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
                break  # one reel per day
        except Exception as exc:
            print(f"[ERROR] Topic '{topic_key}' failed at current stage: {exc}")
            print("        Progress saved — re-run to resume from this point.")

    print(f"\n{'='*60}")
    print(f"Pipeline complete. Published {len(media_ids)} reel(s).")
    for mid in media_ids:
        print(f"  - {mid}")

    all_done = all(
        stage_done(state, t.get("title", t.get("target_query", "")), "db_saved")
        for t in topics
    )
    if all_done and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print(f"[Checkpoint] All topics complete — {STATE_FILE} cleared.")


def lambda_handler(_event, _context):
    main()
    return {"statusCode": 200, "body": "Pipeline complete"}


if __name__ == "__main__":
    main()
