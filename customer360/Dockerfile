FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/airflow

WORKDIR /opt/airflow

COPY requirements.txt ./requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ libpq-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . /opt/airflow

CMD ["bash", "-lc", "python -m src.main validate"]
