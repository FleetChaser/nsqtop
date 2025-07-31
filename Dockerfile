FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Install terminfo files
RUN apt-get update && apt-get install -y ncurses-term && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
COPY README.md .
COPY nsq_top.py .

# Install dependencies using uv
RUN uv sync --python $(which python)

# Add the virtual environment to PATH so we can use python directly
ENV PATH="/app/.venv/bin:$PATH"

# Your command to run the script
CMD ["python", "nsq_top.py"]