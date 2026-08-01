import os
from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from uuid import uuid4
from typing import List
from models import SourcePayload, SourceResponse
from db import init_db, create_source as db_create_source, get_source as db_get_source, list_sources as db_list_sources, update_source as db_update_source, delete_source as db_delete_source
from datetime import datetime

# OpenAPI / docs metadata
tags_metadata = [
    {"name": "sources", "description": "Operations to create, retrieve, update and delete CodeJT sources."},
    {"name": "health", "description": "Service health and metadata endpoints."},
]

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


def get_api_key(api_key_header: str | None = Security(api_key_header)):
    expected_key = os.getenv("CODEJT_API_KEY", "codejt_default_key")
    if api_key_header == expected_key:
        return api_key_header
    raise HTTPException(status_code=401, detail="Unauthorized")


app = FastAPI(
    title="CodeJT API",
    description="CodeJT production API for managing CodeJT-owned source metadata and code assets.",
    version="0.1.0",
    terms_of_service="https://www.combscontracting.com/terms",
    contact={"name": "Jonathan Combs", "url": "https://combscontracting.com", "email": "jonathan@combscontracting.com"},
    license_info={"name": "Apache 2.0", "url": "https://www.apache.org/licenses/LICENSE-2.0.html"},
    openapi_tags=tags_metadata,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    init_db()


@app.get("/", tags=["health"], summary="Service health and OpenAPI")
def root():
    return {
        "status": "ok",
        "message": "CodeJT API is running",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


@app.post("/sources", response_model=SourceResponse, tags=["sources"], summary="Create a new source")
def create_source(payload: SourcePayload, api_key: str = Depends(get_api_key)):
    source_id = str(uuid4())
    now = datetime.utcnow()
    return db_create_source(payload, source_id, now, now)


@app.get("/sources/{source_id}", response_model=SourceResponse, tags=["sources"], summary="Get a source by id")
def get_source(source_id: str, api_key: str = Depends(get_api_key)):
    source = db_get_source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


@app.get("/sources", response_model=List[SourceResponse], tags=["sources"], summary="List sources")
def list_sources(api_key: str = Depends(get_api_key)):
    return db_list_sources()


@app.put("/sources/{source_id}", response_model=SourceResponse, tags=["sources"], summary="Update an existing source")
def update_source(source_id: str, payload: SourcePayload, api_key: str = Depends(get_api_key)):
    now = datetime.utcnow()
    source = db_update_source(source_id, payload, now)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


@app.delete("/sources/{source_id}", tags=["sources"], summary="Delete a source by id")
def delete_source(source_id: str, api_key: str = Depends(get_api_key)):
    deleted = db_delete_source(source_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"detail": "Source deleted"}
