#!/bin/sh
# Runs the pipeline once a day at 12:00 PM IST (06:30 UTC).
# Sleeps until the next scheduled time on each iteration.

SCHEDULE_HOUR=6
SCHEDULE_MIN=30

while true; do
    now_sec=$(date -u +%s)
    target=$(date -u -d "today ${SCHEDULE_HOUR}:${SCHEDULE_MIN}:00" +%s 2>/dev/null \
             || date -u -j -f "%H:%M:%S" "${SCHEDULE_HOUR}:${SCHEDULE_MIN}:00" +%s)

    # If we've already passed today's run time, schedule for tomorrow
    if [ "$now_sec" -ge "$target" ]; then
        target=$((target + 86400))
    fi

    sleep_sec=$((target - now_sec))
    echo "[Scheduler] Next run in ${sleep_sec}s (12:00 PM IST / 06:30 UTC)"
    sleep "$sleep_sec"

    echo "[Scheduler] Starting pipeline at $(date -u)"
    python seo_to_instagram.py
    echo "[Scheduler] Pipeline finished at $(date -u)"
done
