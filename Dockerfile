ARG python_version

FROM python:${python_version}-alpine

RUN apk add gcc musl-dev libffi-dev openssl-dev git

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Install dependencies first to leverage Docker cache
COPY pyproject.toml uv.lock README.rst /app/
RUN uv sync --locked --no-install-project --no-dev

# Install the project separately for optimal layer caching
COPY dysql /app/dysql
RUN uv sync --locked --no-dev