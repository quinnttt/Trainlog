FROM python:3.13

WORKDIR /code

# Fonts the Discord trip cards fall back to (src/trip_card.py). Montserrat has
# 969 codepoints — Latin, Cyrillic, Greek — so without these a Japanese, Thai
# or Hebrew station name renders as .notdef boxes. droid-fallback is 7 MB and
# covers CJK; noto-core is 41 MB and covers most everything else.
RUN apt-get update \
    && apt-get install -y --no-install-recommends fonts-droid-fallback fonts-noto-core \
    && rm -rf /var/lib/apt/lists/*


COPY --from=ghcr.io/astral-sh/uv:0.13.10 /uv /uvx /usr/local/bin/

# Define uv venv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies 
COPY pyproject.toml uv.lock /code/
RUN uv sync --frozen --no-dev --no-install-project

COPY . /code

RUN git config --global --add safe.directory /code

EXPOSE 5000

# Command to run the application using Gunicorn, serving on port 5000
ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:5000", "-t", "600", "app:app", "--capture-output"]
