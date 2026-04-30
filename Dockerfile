FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libjpeg-dev \
    libpng-dev \
    libfreetype6-dev \
    fonts-nanum \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# rembg U2Net 모델 빌드 타임 사전 다운로드 (첫 실행 콜드스타트 방지)
RUN python -c "from rembg import new_session; new_session('u2net')" || echo "[WARN] rembg model pre-download skipped"

COPY . .

RUN mkdir -p uploads

EXPOSE 8000

CMD ["sh", "-c", "uvicorn server:app --host 0.0.0.0 --port ${PORT:-8000}"]
