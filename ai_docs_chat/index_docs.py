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
import shutil
from pathlib import Path

from ai_docs_chat.config import load_config
from ai_docs_chat.docs_loader import load_documents, split_documents


def main() -> None:
    args = _parse_args()
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0")
    if args.chroma_batch_size <= 0:
        raise ValueError("--chroma-batch-size must be greater than 0")

    config = load_config()

    docs_dir = Path(args.docs_dir or config.docs_dir)
    index_dir = Path(args.index_dir or config.index_dir)
    embed_model = args.embed_model or config.embed_model

    if index_dir.exists():
        shutil.rmtree(index_dir)
    index_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading docs from {docs_dir}")
    documents = load_documents(docs_dir)
    chunks = split_documents(documents, root_dir=Path.cwd())
    print(f"Loaded {len(documents)} files and created {len(chunks)} chunks")

    if not chunks:
        raise RuntimeError(f"No documentation chunks found in {docs_dir}")

    print(f"Loading embedding model: {embed_model}")
    embedder = _load_embedder(embed_model)
    embeddings = embedder.encode(
        [chunk.text for chunk in chunks],
        batch_size=args.batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,
    ).tolist()

    chromadb = _load_chromadb()
    client = chromadb.PersistentClient(path=str(index_dir))
    collection = client.get_or_create_collection(config.collection_name)

    metadatas = [
        {
            "source_path": chunk.source_path,
            "chunk_index": chunk.chunk_index,
            "heading": chunk.heading or "",
        }
        for chunk in chunks
    ]

    for start in range(0, len(chunks), args.chroma_batch_size):
        end = start + args.chroma_batch_size
        collection.add(
            ids=[chunk.chunk_id for chunk in chunks[start:end]],
            documents=[chunk.text for chunk in chunks[start:end]],
            embeddings=embeddings[start:end],
            metadatas=metadatas[start:end],
        )

    print(f"Indexed {len(chunks)} chunks into {index_dir}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a local Chroma index for Spark docs.")
    parser.add_argument("--docs-dir", help="Documentation directory to index.")
    parser.add_argument("--index-dir", help="Persistent Chroma index directory.")
    parser.add_argument("--embed-model", help="SentenceTransformer embedding model.")
    parser.add_argument("--batch-size", type=int, default=32, help="Embedding batch size.")
    parser.add_argument(
        "--chroma-batch-size",
        type=int,
        default=1000,
        help="Number of chunks to write to Chroma per add call.",
    )
    return parser.parse_args()


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
