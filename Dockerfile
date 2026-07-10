FROM apache/airflow:2.9.3-python3.11

ARG EMBEDDING_MODEL_NAME=sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2

USER root

ENV PYTHONPATH=/opt/project/src
ENV PLAYWRIGHT_BROWSERS_PATH=/home/airflow/.cache/ms-playwright
ENV HF_HOME=/home/airflow/.cache/huggingface
ENV EMBEDDING_MODEL_NAME=${EMBEDDING_MODEL_NAME}

COPY requirements.txt /tmp/requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libcairo2 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libgbm1 \
        libglib2.0-0 \
        libnspr4 \
        libnss3 \
        libpango-1.0-0 \
        libx11-6 \
        libxcb1 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxkbcommon0 \
        libxrandr2 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/airflow/.cache/ms-playwright /home/airflow/.cache/huggingface \
    && chown -R airflow:0 /home/airflow/.cache

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && python -m playwright install chromium \
    && python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('${EMBEDDING_MODEL_NAME}')"

ENV HF_HUB_OFFLINE=1
ENV TRANSFORMERS_OFFLINE=1

WORKDIR /opt/project
COPY --chown=airflow:0 . /opt/project

USER airflow
