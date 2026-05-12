FROM public.ecr.aws/lambda/python:3.13

# Install ffmpeg (static build — no package manager needed)
RUN dnf install -y tar xz && \
    curl -L https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz \
        -o /tmp/ffmpeg.tar.xz && \
    tar -xf /tmp/ffmpeg.tar.xz -C /tmp && \
    mv /tmp/ffmpeg-*-amd64-static/ffmpeg /usr/local/bin/ffmpeg && \
    rm -rf /tmp/ffmpeg* && \
    dnf clean all

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source + bundled BGM asset
COPY seo_to_instagram_single.py ${LAMBDA_TASK_ROOT}/
COPY background_music.mp3 ${LAMBDA_TASK_ROOT}/

ENV PYTHONUNBUFFERED=1

CMD ["seo_to_instagram_single.lambda_handler"]
