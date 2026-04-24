import sys
import json
import os
import re
import random
import boto3
from pytrends.request import TrendReq
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

bedrock_client = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

MODEL_ID = "meta.llama3-8b-instruct-v1:0"


def _extract_queries(related_queries, top_n=10):
    """Return a flat list of unique query strings from top + rising results."""
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


def get_seo_topics(keywords=None) -> dict:
    """
    Fetch trending queries for *keywords* from Google Trends, then use
    Azure OpenAI to generate 10 fully SEO-optimised blog briefs.
    Returns the parsed JSON dict: {"topics": [...]}
    """
    if keywords is None:
        keywords = ["anxiety", "mental health", "overthinking", "depression", "mindfulness",
                    "self care", "therapy", "stress management", "emotional intelligence", "childhood trauma"]

    # Shuffle so each run targets different keywords, then cap at 5 (Google Trends limit)
    random.shuffle(keywords)
    keywords = keywords[:5]

    # ── Google Trends ─────────────────────────────────────────────────────────
    pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=0.5)
    pytrends.build_payload(keywords, timeframe='today 3-m', geo='US')
    related_queries = pytrends.related_queries()

    all_queries = _extract_queries(related_queries)
    queries_text = "\n".join(f"- {q}" for q in all_queries)

    # ── Azure OpenAI ──────────────────────────────────────────────────────────
    prompt = f"""You are an expert SEO content strategist for a mental health blog.

Based on the following trending search queries from Google Trends, generate 5 fully SEO-optimised blog topic briefs.

Trending queries:
{queries_text}

Return ONLY a valid JSON object — no markdown, no code fences, no explanation.

The format must be exactly:
{{
  "topics": [
    {{
      "title": "...",
      "target_query": "...",
      "focus_keyword": "...",
      "secondary_keywords": ["...", "...", "...", "...", "..."],
      "url_slug": "...",
      "meta_description": "...",
      "search_intent": "...",
      "suggested_word_count": "...",
      "content_outline": {{
        "introduction": "...",
        "sections": [
          {{
            "h2": "...",
            "h3s": ["...", "..."]
          }}
        ],
        "conclusion": "..."
      }},
      "featured_snippet_target": "...",
      "internal_linking_opportunities": ["...", "...", "..."]
    }}
  ]
}}

Rules:
- title: under 60 characters, focus keyword near the start
- target_query: exact query from the trending list above
- secondary_keywords: 4-5 LSI/related terms
- url_slug: lowercase, hyphenated, no stop words
- meta_description: 150-160 characters, includes focus keyword, ends with a CTA
- search_intent: one of Informational / Navigational / Transactional / Commercial
- suggested_word_count: realistic range based on what ranks (e.g. "1200-1500 words")
- content_outline sections: 4-6 H2s, each with 1-2 H3s where relevant
- featured_snippet_target: a specific question this post should answer to win a snippet
- internal_linking_opportunities: 2-3 related blog topic areas to link to
"""

    response = bedrock_client.converse(
        modelId=MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 2048, "temperature": 0.7, "topP": 0.9},
        additionalModelRequestFields={},
        performanceConfig={"latency": "standard"},
    )

    text = response["output"]["message"]["content"][0]["text"]

    # Strip markdown code fences if Llama wraps the JSON
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON found in model response:\n{text}")
    return json.loads(match.group())


if __name__ == "__main__":
    print("Fetching SEO topics...\n")
    data = get_seo_topics()
    print(json.dumps(data, indent=2, ensure_ascii=False))
