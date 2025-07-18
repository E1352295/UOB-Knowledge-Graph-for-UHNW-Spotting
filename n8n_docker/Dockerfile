FROM n8nio/n8n:latest
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

RUN apk add --no-cache \
    python3 py3-pip build-base libffi-dev openssl-dev musl-dev \
    libgomp g++ bash git curl poppler-utils poppler-dev openssh-server && \
    RUN python3 -m venv /opt/venv && \
    chown -R node:node /opt/venv && \       # 让 node 拥有写权
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip wheel setuptools && \
    /opt/venv/bin/pip install --no-cache-dir \
    /opt/venv/bin/pip install --no-cache-dir python-dateutil pypdf PyPDF2 tqdm pandas pydantic \
        pdfplumber python-dotenv google-generativeai langchain py2neo neo4j rapidfuzz networkx\
        spacy pdfminer.six && \
    /opt/venv/bin/python -m spacy download en_core_web_sm
    apk del build-base g++   # ⬅ 删除编译器瘦身

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/home/node/.local/bin:/opt/venv/bin:$PATH" \
    PIP_BREAK_SYSTEM_PACKAGES=1 \
    PIP_NO_CACHE_DIR=1 \
    NODE_OPTIONS="--max_old_space_size=18432" \
    OPENBLAS_NUM_THREADS=8 \
    OMP_NUM_THREADS=8

RUN mkdir -p /data/scripts && chown -R node:node /data /home/node
VOLUME ["/home/node/.n8n","/data"]

USER node
