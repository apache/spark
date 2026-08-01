FROM python:3.12-slim
LABEL maintainer="Jonathan Combs <jonathan@combscontracting.com>"
LABEL org.opencontainers.image.title="CodeJT"
LABEL org.opencontainers.image.description="CodeJT API service by Combs Contracting LLC"
LABEL org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY . /app
EXPOSE 8080
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8080"]
