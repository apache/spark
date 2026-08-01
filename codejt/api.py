import os
from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from uuid import uuid4
from typing import List
from models import SourcePayload, SourceResponse
from db import init_db, create_source as db_create_source, get_source as db_get_source, list_sources as db_list_sources, update_source as db_update_source, delete_source as db_delete_source
from datetime import datetime

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


@app.post("/sources", response_model=SourceResponse)
def create_source(payload: SourcePayload, api_key: str = Depends(get_api_key)):
    source_id = str(uuid4())
    now = datetime.utcnow()
    return db_create_source(payload, source_id, now, now)


@app.get("/sources/{source_id}", response_model=SourceResponse)
def get_source(source_id: str, api_key: str = Depends(get_api_key)):
    source = db_get_source(source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


@app.get("/sources", response_model=List[SourceResponse])
def list_sources(api_key: str = Depends(get_api_key)):
    return db_list_sources()


@app.put("/sources/{source_id}", response_model=SourceResponse)
def update_source(source_id: str, payload: SourcePayload, api_key: str = Depends(get_api_key)):
    now = datetime.utcnow()
    source = db_update_source(source_id, payload, now)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


@app.delete("/sources/{source_id}")
def delete_source(source_id: str, api_key: str = Depends(get_api_key)):
    deleted = db_delete_source(source_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"detail": "Source deleted"}
