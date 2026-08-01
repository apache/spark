from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Optional


class SourceMeta(BaseModel):
    owner: str = Field(default="CodeJT")
    tags: List[str] = Field(default_factory=list)
    category: Optional[str] = None
    version: Optional[str] = "1.0"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SourcePayload(BaseModel):
    title: str
    content: str
    metadata: SourceMeta = Field(default_factory=SourceMeta)


class SourceResponse(SourcePayload):
    id: str
    created_at: datetime
    updated_at: datetime
