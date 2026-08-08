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

from typing import Any, Dict

from pyspark.sql.mcp.session import SessionHolder
from pyspark.sql.mcp.tools.registry import ToolSpec

# Configs that are safe to surface to an LLM. Anything else is filtered out
# to avoid leaking tokens or workspace-scoped identifiers.
_SAFE_CONFIG_PREFIXES = (
    "spark.app.",
    "spark.sql.",
    "spark.connect.",
)
_REDACT_SUBSTRINGS = ("token", "secret", "password", "credential")


async def _handle(_args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    config = holder.config

    version = spark.version
    current_catalog = spark.catalog.currentCatalog()
    current_database = spark.catalog.currentDatabase()
    safe_configs = _collect_safe_configs(spark)

    return {
        "spark_version": version,
        "connect_url": config.connect_url,
        "read_only": config.read_only,
        "current_catalog": current_catalog,
        "current_database": current_database,
        "configs": safe_configs,
    }


def _collect_safe_configs(spark: Any) -> Dict[str, str]:
    try:
        items = spark.conf.getAll()
    except Exception:
        return {}
    out: Dict[str, str] = {}
    for key, value in items.items():
        if not key.startswith(_SAFE_CONFIG_PREFIXES):
            continue
        if any(needle in key.lower() for needle in _REDACT_SUBSTRINGS):
            continue
        out[key] = value
    return out


def get_session_info_spec() -> ToolSpec:
    return ToolSpec(
        name="get_session_info",
        description=(
            "Return Spark version, connection URL, current catalog/database, "
            "and a filtered subset of session configs. Use this first to "
            "orient before issuing other tool calls."
        ),
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        handler=_handle,
    )
