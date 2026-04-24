FROM python:3.13-slim-bookworm

WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requriments.txt .
RUN pip install --no-cache-dir -r requriments.txt \
    && pip install --no-cache-dir supabase python-dotenv requests pytrends

# Copy source code
COPY . .

# Write the cron job: run full SEO→Instagram pipeline at 09:00 UTC every day
RUN echo "30 7 * * * root cd /app && python seo_to_instagram.py >> /var/log/pipeline.log 2>&1" \
    > /etc/cron.d/seo-pipeline \
    && chmod 0644 /etc/cron.d/seo-pipeline \
    && crontab /etc/cron.d/seo-pipeline

# Keep the container alive running cron in the foreground
CMD ["cron", "-f"]
