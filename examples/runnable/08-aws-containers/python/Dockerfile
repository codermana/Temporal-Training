# Python Worker image — uv provisions deps from pyproject.toml.
FROM python:3.12-slim
# Copy the uv binary from its official image (no pip bootstrap needed).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml .
RUN uv sync --no-dev
COPY . .
# The Worker dials *out* to the Frontend; no inbound port to EXPOSE.
ENTRYPOINT ["uv", "run", "--no-sync", "worker.py"]
