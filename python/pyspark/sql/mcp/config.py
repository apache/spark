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

import os
from dataclasses import dataclass, field
from typing import Optional


DEFAULT_MAX_ROWS = 1000
DEFAULT_PAGE_SIZE = 100
DEFAULT_QUERY_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class ServerConfig:
    """Runtime configuration for the Spark MCP server.

    Use :meth:`from_env` to populate from CLI args + environment variables.
    """

    connect_url: str
    read_only: bool = True
    max_rows: int = DEFAULT_MAX_ROWS
    default_page_size: int = DEFAULT_PAGE_SIZE
    query_timeout_seconds: int = DEFAULT_QUERY_TIMEOUT_SECONDS
    transport: str = "stdio"
    user_id: Optional[str] = None
    extra_session_configs: dict = field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        *,
        connect_url: Optional[str] = None,
        read_only: Optional[bool] = None,
        transport: Optional[str] = None,
        max_rows: Optional[int] = None,
        query_timeout_seconds: Optional[int] = None,
        user_id: Optional[str] = None,
    ) -> "ServerConfig":
        url = connect_url or os.environ.get("SPARK_REMOTE")
        if not url:
            raise ValueError(
                "Spark Connect URL must be provided via --connect-url or SPARK_REMOTE"
            )
        return cls(
            connect_url=url,
            read_only=_default(read_only, _env_bool("SPARK_MCP_READ_ONLY", True)),
            max_rows=_default(
                max_rows, _env_int("SPARK_MCP_MAX_ROWS", DEFAULT_MAX_ROWS)
            ),
            query_timeout_seconds=_default(
                query_timeout_seconds,
                _env_int("SPARK_MCP_QUERY_TIMEOUT_SECONDS", DEFAULT_QUERY_TIMEOUT_SECONDS),
            ),
            transport=transport or os.environ.get("SPARK_MCP_TRANSPORT", "stdio"),
            user_id=user_id or os.environ.get("SPARK_MCP_USER_ID"),
        )


def _default(value, fallback):
    return fallback if value is None else value


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer; got {raw!r}") from exc
