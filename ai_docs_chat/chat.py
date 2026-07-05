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

import argparse
from pathlib import Path
from typing import Any

from ai_docs_chat.config import load_config


SYSTEM_PROMPT = """You answer questions about Apache Spark documentation.

Use only the provided documentation context.
If the context does not contain enough information, say that the provided
documentation excerpts do not contain enough information.
Cite source file paths for important claims."""


def main() -> None:
    args = _parse_args()
    config = load_config()

    index_dir = Path(args.index_dir or config.index_dir)
    if not index_dir.exists():
        raise RuntimeError(
            f"Index directory does not exist: {index_dir}. "
            "Run: python -m ai_docs_chat.index_docs"
        )

    embed_model = args.embed_model or config.embed_model
    top_k = args.top_k or config.top_k
    ollama_model = args.model or config.ollama_model

    chromadb = _load_chromadb()
    client = chromadb.PersistentClient(path=str(index_dir))
    collection = client.get_collection(config.collection_name)

    print(f"Loading embedding model: {embed_model}")
    embedder = _load_embedder(embed_model)

    if args.question:
        _answer_question(
            args.question,
            collection=collection,
            embedder=embedder,
            top_k=top_k,
            ollama_base_url=config.ollama_base_url,
            ollama_model=ollama_model,
            show_context=args.show_context,
        )
        return

    print("Ask questions about Spark docs. Type 'exit' or 'quit' to stop.")
    while True:
        question = input("spark-docs> ").strip()
        if question.lower() in {"exit", "quit"}:
            break
        if not question:
            continue
        _answer_question(
            question,
            collection=collection,
            embedder=embedder,
            top_k=top_k,
            ollama_base_url=config.ollama_base_url,
            ollama_model=ollama_model,
            show_context=args.show_context,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chat with local Spark docs through Ollama.")
    parser.add_argument("question", nargs="?", help="Ask one question and exit.")
    parser.add_argument("--index-dir", help="Persistent Chroma index directory.")
    parser.add_argument("--embed-model", help="SentenceTransformer embedding model.")
    parser.add_argument("--model", help="Ollama chat model.")
    parser.add_argument("--top-k", type=int, help="Number of chunks to retrieve.")
    parser.add_argument("--show-context", action="store_true", help="Print retrieved context.")
    return parser.parse_args()


def _answer_question(
    question: str,
    *,
    collection: Any,
    embedder: Any,
    top_k: int,
    ollama_base_url: str,
    ollama_model: str,
    show_context: bool,
) -> None:
    question_embedding = embedder.encode(
        [question],
        normalize_embeddings=True,
    )[0].tolist()
    result = collection.query(
        query_embeddings=[question_embedding],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )

    documents = result["documents"][0]
    metadatas = result["metadatas"][0]
    context = _format_context(documents, metadatas)
    if show_context:
        print()
        print("Retrieved context:")
        print(context)

    prompt = f"{SYSTEM_PROMPT}\n\nContext:\n{context}\n\nQuestion:\n{question}"

    answer = _ask_ollama(ollama_base_url, ollama_model, prompt)
    print()
    print(answer.strip())
    print()
    print("Sources:")
    for source in _unique_sources(metadatas):
        print(f"- {source}")
    print()


def _format_context(documents: list[str], metadatas: list[dict[str, Any]]) -> str:
    blocks = []
    for document, metadata in zip(documents, metadatas):
        source_path = metadata.get("source_path", "unknown")
        heading = metadata.get("heading") or ""
        label = f"[source: {source_path}]"
        if heading:
            label = f"{label} {heading}"
        blocks.append(f"{label}\n{document}")
    return "\n\n---\n\n".join(blocks)


def _unique_sources(metadatas: list[dict[str, Any]]) -> list[str]:
    seen = set()
    sources: list[str] = []
    for metadata in metadatas:
        source = metadata.get("source_path")
        if source and source not in seen:
            seen.add(source)
            sources.append(source)
    return sources


def _ask_ollama(base_url: str, model: str, prompt: str) -> str:
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError(
            "requests is required. "
            "Install dependencies with: pip install -r ai_docs_chat/requirements.txt"
        ) from exc

    response = requests.post(
        f"{base_url}/api/chat",
        json={
            "model": model,
            "stream": False,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=180,
    )
    response.raise_for_status()
    payload = response.json()
    try:
        return payload["message"]["content"]
    except KeyError as exc:
        raise RuntimeError(f"Unexpected Ollama response: {payload}") from exc


def _load_embedder(model_name: str):
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            "sentence-transformers is required. "
            "Install dependencies with: pip install -r ai_docs_chat/requirements.txt"
        ) from exc
    return SentenceTransformer(model_name)


def _load_chromadb():
    try:
        import chromadb
    except ImportError as exc:
        raise RuntimeError(
            "chromadb is required. "
            "Install dependencies with: pip install -r ai_docs_chat/requirements.txt"
        ) from exc
    return chromadb


if __name__ == "__main__":
    main()
