FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1         PYTHONUNBUFFERED=1         SAR_DATA_DIR=/data         SAR_HOST=0.0.0.0         SAR_PORT=8000

WORKDIR /app

# System deps (optional: for pandas/openpyxl performance)
RUN apt-get update && apt-get install -y --no-install-recommends         build-essential         && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md LICENSE requirements.txt /app/
COPY src /app/src

RUN pip install --no-cache-dir -U pip &&         pip install --no-cache-dir .

# Data volume for registries
VOLUME ["/data"]

EXPOSE 8000

CMD ["uvicorn", "sar.app:app", "--host", "0.0.0.0", "--port", "8000"]
