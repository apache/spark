#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from re import sub


DOC_EXTENSIONS = {".html", ".markdown", ".md", ".rst", ".txt"}
SKIP_DIRS = {
    ".jekyll-cache",
    ".local_ruby_bundle",
    "_generated",
    "_site",
    "api",
}


@dataclass(frozen=True)
class Document:
    path: Path
    text: str


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    source_path: str
    chunk_index: int
    text: str
    heading: str | None


def iter_doc_paths(docs_dir: Path) -> list[Path]:
    if not docs_dir.exists():
        raise FileNotFoundError(f"Documentation directory does not exist: {docs_dir}")

    paths: list[Path] = []
    for path in docs_dir.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.relative_to(docs_dir).parts[:-1]):
            continue
        if path.suffix.lower() in DOC_EXTENSIONS:
            paths.append(path)
    return sorted(paths)


def load_documents(docs_dir: Path) -> list[Document]:
    documents: list[Document] = []
    for path in iter_doc_paths(docs_dir):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if path.suffix.lower() == ".html":
            text = _html_to_text(text)
        else:
            text = _plain_doc_to_text(text)
        if text.strip():
            documents.append(Document(path=path, text=text))
    return documents


def split_documents(
    documents: list[Document],
    *,
    root_dir: Path,
    chunk_size: int = 1600,
    overlap: int = 200,
) -> list[Chunk]:
    chunks: list[Chunk] = []
    for document in documents:
        source_path = _display_path(document.path, root_dir)
        text = _normalize_whitespace(document.text)
        heading = _first_heading(document.text)
        for chunk_index, chunk_text in enumerate(_split_text(text, chunk_size, overlap)):
            chunks.append(
                Chunk(
                    chunk_id=f"{source_path}:{chunk_index}",
                    source_path=source_path,
                    chunk_index=chunk_index,
                    text=chunk_text,
                    heading=heading,
                )
            )
    return chunks


def _display_path(path: Path, root_dir: Path) -> str:
    try:
        return path.relative_to(root_dir).as_posix()
    except ValueError:
        return path.as_posix()


def _html_to_text(text: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError as exc:
        raise RuntimeError(
            "beautifulsoup4 is required for HTML docs. "
            "Install dependencies with: pip install -r ai_docs_chat/requirements.txt"
        ) from exc

    soup = BeautifulSoup(text, "html.parser")
    for element in soup(["script", "style", "noscript"]):
        element.decompose()
    return soup.get_text("\n")


def _plain_doc_to_text(text: str) -> str:
    return text


def _normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = sub(r"[ \t]+", " ", text)
    text = sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _first_heading(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            heading = stripped.lstrip("#").strip()
            if heading:
                return heading
        if stripped and len(stripped) <= 120 and not stripped.endswith("."):
            return stripped
    return None


def _split_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    if chunk_size <= overlap:
        raise ValueError("chunk_size must be greater than overlap")

    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        if end < len(text):
            boundary = text.rfind("\n\n", start, end)
            if boundary <= start:
                boundary = text.rfind(". ", start, end)
            if boundary > start:
                end = boundary + 1

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= len(text):
            break
        start = max(end - overlap, start + 1)
    return chunks
