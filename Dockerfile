FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY etl ./etl
COPY ml ./ml
COPY extra ./extra
COPY sql ./sql
COPY tests ./tests
COPY pytest.ini .

CMD ["python", "-m", "etl.pipeline", "all"]
