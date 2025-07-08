FROM n8nio/n8n:latest
USER root

RUN apk add --no-cache python3 py3-pip build-base libffi-dev openssl-dev \
    musl-dev libgomp g++ && \
    python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir python-dateutil pypdf PyPDF2 tqdm pandas \
        pdfplumber python-dotenv google-generativeai langchain py2neo neo4j \
        spacy pdfminer.six && \
    /opt/venv/bin/python -m spacy download en_core_web_sm

ENV NODE_OPTIONS="--max_old_space_size=18432" \
    OPENBLAS_NUM_THREADS=8 \
    OMP_NUM_THREADS=8

# Expose venv binaries first in PATH
ENV PATH="/opt/venv/bin:$PATH"
USER node
