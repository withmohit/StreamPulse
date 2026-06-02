FROM python:3.12-slim

WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry==2.4.1 && \
    poetry config virtualenvs.create false

# Dependency layer (cached unless pyproject.toml / poetry.lock change)
COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-root --no-interaction

# Source code
COPY . .
