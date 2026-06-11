FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 TZ=Asia/Taipei PATH="/app/.venv/bin:$PATH"
COPY --from=builder /app/.venv /app/.venv
EXPOSE 8000
CMD ["uvicorn", "quanquant.web.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
