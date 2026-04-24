FROM python:3.13-slim-bookworm

WORKDIR /app

# Install Python deps
COPY requriments.txt .
RUN pip install --no-cache-dir -r requriments.txt \
    && pip install --no-cache-dir supabase python-dotenv requests pytrends boto3

# Copy source code
COPY seo_to_instagram.py .
COPY seotreand.py .
COPY reap_pipeline.py .
COPY instagram_upload.py .

# Copy entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# All secrets come from ECS task definition env vars — no .env file baked in
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/entrypoint.sh"]
