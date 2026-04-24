FROM python:3.11-slim

WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requriments.txt .
RUN pip install --no-cache-dir -r requriments.txt \
    && pip install --no-cache-dir supabase python-dotenv requests

# Copy source code and any video assets
COPY . .

# Write the cron job: run at 09:00 UTC every day, log to /var/log/upload.log
RUN echo "0 9 * * * root cd /app && python run_upload.py >> /var/log/upload.log 2>&1" \
    > /etc/cron.d/instagram-upload \
    && chmod 0644 /etc/cron.d/instagram-upload \
    && crontab /etc/cron.d/instagram-upload

# Keep the container alive running cron in the foreground
CMD ["cron", "-f"]
