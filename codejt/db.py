import sqlite3
from pathlib import Path
from typing import List, Optional
from datetime import datetime
from models import SourcePayload, SourceResponse

DB_PATH = Path(__file__).resolve().parent / "codejt.db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    owner TEXT NOT NULL,
    tags TEXT NOT NULL,
    category TEXT,
    version TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute(CREATE_TABLE_SQL)
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.commit()


def row_to_source(row: sqlite3.Row) -> SourceResponse:
    return SourceResponse(
        id=row["id"],
        title=row["title"],
        content=row["content"],
        metadata={
            "owner": row["owner"],
            "tags": row["tags"].split(",") if row["tags"] else [],
            "category": row["category"],
            "version": row["version"],
            "created_at": datetime.fromisoformat(row["created_at"]),
            "updated_at": datetime.fromisoformat(row["updated_at"]),
        },
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


def create_source(payload: SourcePayload, source_id: str, created_at: datetime, updated_at: datetime) -> SourceResponse:
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO sources (id, title, content, owner, tags, category, version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                source_id,
                payload.title,
                payload.content,
                payload.metadata.owner,
                ",".join(payload.metadata.tags),
                payload.metadata.category,
                payload.metadata.version,
                created_at.isoformat(),
                updated_at.isoformat(),
            ),
        )
        conn.commit()
    return SourceResponse(
        id=source_id,
        title=payload.title,
        content=payload.content,
        metadata=payload.metadata,
        created_at=created_at,
        updated_at=updated_at,
    )


def get_source(source_id: str) -> Optional[SourceResponse]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        return row_to_source(row) if row else None


def list_sources() -> List[SourceResponse]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM sources ORDER BY created_at DESC").fetchall()
        return [row_to_source(row) for row in rows]


def update_source(source_id: str, payload: SourcePayload, updated_at: datetime) -> Optional[SourceResponse]:
    with get_connection() as conn:
        source = get_source(source_id)
        if source is None:
            return None
        conn.execute(
            "UPDATE sources SET title = ?, content = ?, owner = ?, tags = ?, category = ?, version = ?, updated_at = ? WHERE id = ?",
            (
                payload.title,
                payload.content,
                payload.metadata.owner,
                ",".join(payload.metadata.tags),
                payload.metadata.category,
                payload.metadata.version,
                updated_at.isoformat(),
                source_id,
            ),
        )
        conn.commit()
        return get_source(source_id)


def delete_source(source_id: str) -> bool:
    with get_connection() as conn:
        result = conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
        conn.commit()
        return result.rowcount > 0
