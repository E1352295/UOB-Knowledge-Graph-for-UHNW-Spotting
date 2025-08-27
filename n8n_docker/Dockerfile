# Base image: keep the official n8n image to inherit its entrypoint and layout
FROM n8nio/n8n:latest

# Use root for package installation
USER root

# System packages:
# - python3 + pip for the venv
# - build tooling only in a temporary virtual package ".build-deps" (removed later)
# - poppler tools for PDF handling (as your file includes poppler-utils/poppler-dev)
# - bash/git/curl/ssh client are often handy within n8n custom nodes
RUN apk add --no-cache \
      python3 py3-pip libffi-dev openssl-dev musl-dev libgomp g++ \
      bash git curl poppler-utils poppler-dev openssh-client \
    && apk add --no-cache --virtual .build-deps build-base

# Create venv and upgrade base Python tooling
RUN python3 -m venv /opt/venv \
 && chown -R node:node /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip wheel setuptools

# Install your Python libraries into the venv
# (kept the exact set you listed, plus kept ordering tidy)
RUN /opt/venv/bin/pip install --no-cache-dir \
      python-dateutil pypdf PyPDF2 tqdm pandas pydantic \
      pdfplumber python-dotenv google-generativeai langchain \
      py2neo neo4j rapidfuzz networkx spacy pdfminer.six \
 && /opt/venv/bin/python -m spacy download en_core_web_sm \
 # freeze exact env into requirements files (both top-level and inside venv)
 && /opt/venv/bin/pip freeze --all > /opt/venv/requirements.txt \
 && cp /opt/venv/requirements.txt /requirements.txt \
 # remove build toolchain to slim the image
 && apk del .build-deps

# Runtime environment
ENV VIRTUAL_ENV=/opt/venv \
    PATH="/home/node/.local/bin:/opt/venv/bin:${PATH}" \
    PIP_BREAK_SYSTEM_PACKAGES=1 \
    PIP_NO_CACHE_DIR=1 \
    NODE_OPTIONS="--max_old_space_size=18432" \
    OPENBLAS_NUM_THREADS=8 \
    OMP_NUM_THREADS=8

# Prepare bind-mount friendly locations (match n8n conventions)
RUN mkdir -p /data/scripts \
 && chown -R node:node /data /home/node

# Persist common n8n volumes
VOLUME ["/home/node/.n8n", "/data"]

# Drop privileges for runtime (as in your original)
USER node
