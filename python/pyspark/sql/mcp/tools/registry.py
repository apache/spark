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

"""Tool registry.

A tool is a (name, description, input schema, handler) tuple. Handlers receive
the parsed arguments dict and a :class:`SessionHolder`, and return any
JSON-serialisable Python value. The server adapter is responsible for turning
that into MCP ``TextContent`` envelopes.

Keeping handlers free of MCP types means we can unit-test them without the
SDK installed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List

from pyspark.sql.mcp.session import SessionHolder

ToolHandler = Callable[[Dict[str, Any], SessionHolder], Awaitable[Any]]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: ToolHandler


def all_tools() -> List[ToolSpec]:
    """Return the full tool surface exposed by the server."""
    # Imports are local to avoid forcing tool-module imports when only the
    # registry shape is needed (e.g. in lightweight tests).
    from pyspark.sql.mcp.tools import catalog, query, session

    return [
        session.get_session_info_spec(),
        catalog.list_catalogs_spec(),
        catalog.list_databases_spec(),
        catalog.list_tables_spec(),
        catalog.describe_table_spec(),
        query.list_functions_spec(),
        query.execute_sql_spec(),
        query.preview_table_spec(),
        query.explain_query_spec(),
        query.analyze_query_spec(),
    ]
