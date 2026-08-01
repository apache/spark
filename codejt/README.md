# CodeJT

CodeJT is a new application project created by Combs Contracting LLC and owned by Jonathan Combs.
This workspace contains a minimal prototype with Apache License 2.0 licensing.

CodeJT is owned and maintained by Jonathan Combs of Combs Contracting LLC.

## Structure

- `app.py` — minimal Python web app using the standard library
- `LICENSE` — Apache License 2.0
- `.gitignore` — basic Python ignore rules
- `Dockerfile` — build a containerized CodeJT API service
- `docker-compose.yml` — run CodeJT locally with Docker Compose

## Getting started

1. Install Python dependencies:

```bash
python3 -m pip install -r codejt/requirements.txt
```

2. Run locally:

```bash
cd codejt
uvicorn api:app --host 0.0.0.0 --port 8080
```

3. Open `http://localhost:8080/docs` in your browser to use the FastAPI documentation.

4. Build with Docker:

```bash
docker build -t codejt ./codejt
```

5. Run with Docker Compose:

```bash
docker compose -f codejt/docker-compose.yml up --build
```

## API Authentication

The CodeJT API uses an API key for production requests.
Set the key using `CODEJT_API_KEY` before starting the service.

Example request header:

```
X-API-Key: your-secret-key
```

If using Docker Compose, place the key in `codejt/.env`.

## Persistence

CodeJT uses SQLite persistence for source metadata and stored code assets in `codejt/codejt.db`.
