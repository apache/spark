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

"""SQL execution and plan tools."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar

from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.sql.mcp.safety import assert_read_only
from pyspark.sql.mcp.session import SessionHolder
from pyspark.sql.mcp.tools.catalog import _to_dict
from pyspark.sql.mcp.tools.registry import ToolSpec


T = TypeVar("T")


async def _with_timeout(holder: SessionHolder, func: Callable[[], T], *, label: str) -> T:
    """Run a blocking Spark Connect call in a worker thread under the configured timeout."""
    timeout = holder.config.query_timeout_seconds
    coro: Awaitable[T] = asyncio.to_thread(func)
    if timeout and timeout > 0:
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise PySparkRuntimeError(
                errorClass="MCP_QUERY_TIMEOUT",
                messageParameters={"label": label, "timeout": str(timeout)},
            ) from exc
    return await coro


# ---------------------------------------------------------------------------
# list_functions
# ---------------------------------------------------------------------------


async def _handle_list_functions(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    database = args.get("database")
    pattern = args.get("pattern")

    kwargs: Dict[str, Any] = {}
    if database is not None:
        kwargs["dbName"] = database
    if pattern is not None:
        kwargs["pattern"] = pattern
    funcs = list(spark.catalog.listFunctions(**kwargs))

    limit = int(args.get("limit", holder.config.default_page_size))
    offset = int(args.get("offset", 0))
    total = len(funcs)
    page = funcs[offset : offset + limit]
    return {
        "items": [_to_dict(f) for f in page],
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "total": total,
        "truncated": offset + limit < total,
    }


def list_functions_spec() -> ToolSpec:
    return ToolSpec(
        name="list_functions",
        description=(
            "List user-defined and built-in functions visible in a database. "
            "Useful when authoring SQL — surfaces both UDFs and the built-in "
            "function library."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "database": {"type": "string"},
                "pattern": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            "additionalProperties": False,
        },
        handler=_handle_list_functions,
    )


# ---------------------------------------------------------------------------
# Result rendering helpers
# ---------------------------------------------------------------------------


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    return dict(row)


def _render_markdown(rows: List[Dict[str, Any]], schema_fields: List[str]) -> str:
    if not schema_fields:
        return "_(no columns)_"
    header = "| " + " | ".join(schema_fields) + " |"
    sep = "| " + " | ".join("---" for _ in schema_fields) + " |"
    body = []
    for row in rows:
        body.append("| " + " | ".join(_md_cell(row.get(c)) for c in schema_fields) + " |")
    return "\n".join([header, sep, *body])


def _md_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text


def _schema_fields(schema: Any) -> List[str]:
    try:
        return [field.name for field in schema.fields]
    except AttributeError:
        return []


# ---------------------------------------------------------------------------
# execute_sql
# ---------------------------------------------------------------------------


async def _handle_execute_sql(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    query: str = args["query"]
    sql_args: Optional[Any] = args.get("args")
    fmt: str = args.get("format", "json")
    limit_arg = args.get("limit", holder.config.default_page_size)
    offset = int(args.get("offset", 0))

    if holder.config.read_only:
        assert_read_only(query)

    capped_limit = min(int(limit_arg), int(holder.config.max_rows))
    df = _sql(spark, query, sql_args)
    schema_fields = _schema_fields(df.schema)

    page_df = df.offset(offset).limit(capped_limit + 1) if offset else df.limit(capped_limit + 1)
    collected = await _with_timeout(holder, page_df.collect, label="execute_sql")
    rows = [_row_to_dict(r) for r in collected]
    truncated = len(rows) > capped_limit
    rows = rows[:capped_limit]

    payload: Dict[str, Any] = {
        "row_count": len(rows),
        "truncated": truncated,
        "limit": capped_limit,
        "offset": offset,
        "schema": [
            {"name": field.name, "type": str(field.dataType), "nullable": field.nullable}
            for field in df.schema.fields
        ],
    }
    if fmt == "json":
        payload["rows"] = rows
    elif fmt == "markdown":
        payload["markdown"] = _render_markdown(rows, schema_fields)
    elif fmt == "arrow_base64":
        import base64
        import io

        buf = io.BytesIO()
        page_df_arrow = df.offset(offset).limit(capped_limit) if offset else df.limit(capped_limit)
        try:
            import pyarrow as pa
        except ImportError as exc:  # pragma: no cover
            raise PySparkRuntimeError(
                errorClass="MCP_PYARROW_REQUIRED",
                messageParameters={"format": "arrow_base64"},
            ) from exc
        table = await _with_timeout(holder, page_df_arrow.toArrow, label="execute_sql")
        with pa.ipc.new_stream(buf, table.schema) as writer:
            writer.write_table(table)
        payload["arrow_base64"] = base64.b64encode(buf.getvalue()).decode("ascii")
    else:
        raise PySparkValueError(
            errorClass="MCP_UNSUPPORTED_FORMAT",
            messageParameters={
                "format": str(fmt),
                "allowed": "json, markdown, arrow_base64",
            },
        )
    return payload


def _sql(spark: Any, query: str, sql_args: Optional[Any]) -> Any:
    if sql_args is None:
        return spark.sql(query)
    return spark.sql(query, args=sql_args)


def execute_sql_spec() -> ToolSpec:
    return ToolSpec(
        name="execute_sql",
        description=(
            "Run a SQL query and return rows. Read-only by default — only "
            "SELECT/WITH/SHOW/DESCRIBE/EXPLAIN/USE/SET are accepted unless "
            "the server is started with --no-read-only. Results are paginated "
            "and capped by the server's max_rows setting."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL statement."},
                "args": {
                    "description": (
                        "Bind parameters. Object for named (':name'), array for positional ('?')."
                    ),
                    "oneOf": [{"type": "object"}, {"type": "array"}],
                },
                "limit": {"type": "integer", "minimum": 1, "default": 100},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
                "format": {
                    "type": "string",
                    "enum": ["json", "markdown", "arrow_base64"],
                    "default": "json",
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        handler=_handle_execute_sql,
    )


# ---------------------------------------------------------------------------
# preview_table
# ---------------------------------------------------------------------------


async def _handle_preview_table(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    name = args["name"]
    limit = int(args.get("limit", 20))
    return await _handle_execute_sql(
        {"query": f"SELECT * FROM {name}", "limit": limit, "format": args.get("format", "json")},
        holder,
    )


def preview_table_spec() -> ToolSpec:
    return ToolSpec(
        name="preview_table",
        description=(
            "Convenience wrapper for SELECT * FROM <name> LIMIT <limit>. "
            "Honors the server's max_rows cap and read-only mode."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "default": 20},
                "format": {"type": "string", "enum": ["json", "markdown"], "default": "json"},
            },
            "required": ["name"],
            "additionalProperties": False,
        },
        handler=_handle_preview_table,
    )


# ---------------------------------------------------------------------------
# explain_query
# ---------------------------------------------------------------------------


_EXPLAIN_MODES = ("simple", "extended", "formatted", "cost", "codegen")


async def _handle_explain_query(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    query = args["query"]
    mode = args.get("mode", "formatted")
    if mode not in _EXPLAIN_MODES:
        raise PySparkValueError(
            errorClass="MCP_INVALID_EXPLAIN_MODE",
            messageParameters={
                "allowed": ", ".join(_EXPLAIN_MODES),
                "mode": str(mode),
            },
        )
    if holder.config.read_only:
        assert_read_only(query)

    df = _sql(spark, query, args.get("args"))
    plan = await _with_timeout(holder, lambda: df._explain_string(mode=mode), label="explain_query")
    return {"mode": mode, "plan": plan}


def explain_query_spec() -> ToolSpec:
    return ToolSpec(
        name="explain_query",
        description=(
            "Return the query plan for a SQL statement without executing it. "
            "Modes: simple, extended, formatted (default), cost, codegen."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "mode": {"type": "string", "enum": list(_EXPLAIN_MODES), "default": "formatted"},
                "args": {"oneOf": [{"type": "object"}, {"type": "array"}]},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        handler=_handle_explain_query,
    )


# ---------------------------------------------------------------------------
# analyze_query
# ---------------------------------------------------------------------------


async def _handle_analyze_query(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    query = args["query"]
    if holder.config.read_only:
        assert_read_only(query)

    df = _sql(spark, query, args.get("args"))
    extended = await _with_timeout(
        holder, lambda: df._explain_string(mode="extended"), label="analyze_query"
    )
    formatted = await _with_timeout(
        holder, lambda: df._explain_string(mode="formatted"), label="analyze_query"
    )
    schema = [
        {"name": field.name, "type": str(field.dataType), "nullable": field.nullable}
        for field in df.schema.fields
    ]
    return {
        "schema": schema,
        "extended_plan": extended,
        "formatted_plan": formatted,
    }


def analyze_query_spec() -> ToolSpec:
    return ToolSpec(
        name="analyze_query",
        description=(
            "Higher-level wrapper around explain: returns output schema plus "
            "both extended and formatted plans, so an LLM can reason about a "
            "query without executing it."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "args": {"oneOf": [{"type": "object"}, {"type": "array"}]},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        handler=_handle_analyze_query,
    )
