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

import os
from dataclasses import dataclass
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc


@dataclass(frozen=True)
class Config:
    docs_dir: Path
    index_dir: Path
    embed_model: str
    ollama_base_url: str
    ollama_model: str
    top_k: int
    collection_name: str = "spark_docs"


def load_config() -> Config:
    return Config(
        docs_dir=Path(os.environ.get("SPARK_DOCS_CHAT_DOCS_DIR", "docs")),
        index_dir=Path(
            os.environ.get("SPARK_DOCS_CHAT_INDEX_DIR", ".spark-docs-index/chroma")
        ),
        embed_model=os.environ.get("SPARK_DOCS_CHAT_EMBED_MODEL", "BAAI/bge-small-en-v1.5"),
        ollama_base_url=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/"),
        ollama_model=os.environ.get("OLLAMA_MODEL", "gemma4:e2b"),
        top_k=_int_env("SPARK_DOCS_CHAT_TOP_K", 6),
    )
