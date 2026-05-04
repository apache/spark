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

"""Catalog browsing tools."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List

from pyspark.sql.mcp.session import SessionHolder
from pyspark.sql.mcp.tools.registry import ToolSpec


# ---------------------------------------------------------------------------
# Result shaping
# ---------------------------------------------------------------------------


def _to_dict(obj: Any) -> Dict[str, Any]:
    """Best-effort conversion of catalog dataclasses (CatalogMetadata, Database,
    Table, Column, Function) into plain dicts."""
    if is_dataclass(obj):
        return asdict(obj)
    if hasattr(obj, "_asdict"):  # NamedTuple
        return dict(obj._asdict())
    if isinstance(obj, dict):
        return obj
    # Fallback: scrape public attributes.
    return {
        k: getattr(obj, k)
        for k in dir(obj)
        if not k.startswith("_") and not callable(getattr(obj, k))
    }


def _paginate(items: List[Any], limit: int, offset: int) -> Dict[str, Any]:
    total = len(items)
    end = offset + limit if limit is not None else total
    page = items[offset:end]
    return {
        "items": [_to_dict(x) for x in page],
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "total": total,
        "truncated": end < total,
    }


# ---------------------------------------------------------------------------
# list_catalogs
# ---------------------------------------------------------------------------


async def _handle_list_catalogs(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    catalogs = list(spark.catalog.listCatalogs())
    limit = int(args.get("limit", holder.config.default_page_size))
    offset = int(args.get("offset", 0))
    return _paginate(catalogs, limit, offset)


def list_catalogs_spec() -> ToolSpec:
    return ToolSpec(
        name="list_catalogs",
        description="List catalogs available to the current Spark session.",
        input_schema={
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            "additionalProperties": False,
        },
        handler=_handle_list_catalogs,
    )


# ---------------------------------------------------------------------------
# list_databases
# ---------------------------------------------------------------------------


async def _handle_list_databases(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    catalog = args.get("catalog")
    pattern = args.get("pattern")

    if catalog:
        spark.catalog.setCurrentCatalog(catalog)
    if pattern is not None:
        databases = list(spark.catalog.listDatabases(pattern))
    else:
        databases = list(spark.catalog.listDatabases())

    limit = int(args.get("limit", holder.config.default_page_size))
    offset = int(args.get("offset", 0))
    return _paginate(databases, limit, offset)


def list_databases_spec() -> ToolSpec:
    return ToolSpec(
        name="list_databases",
        description=(
            "List databases (a.k.a. schemas) in the current or named catalog. "
            "Optional name pattern uses Spark's LIKE semantics."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "catalog": {"type": "string"},
                "pattern": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            "additionalProperties": False,
        },
        handler=_handle_list_databases,
    )


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------


async def _handle_list_tables(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    database = args.get("database")
    pattern = args.get("pattern")

    kwargs: Dict[str, Any] = {}
    if database is not None:
        kwargs["dbName"] = database
    if pattern is not None:
        kwargs["pattern"] = pattern
    tables = list(spark.catalog.listTables(**kwargs))

    limit = int(args.get("limit", holder.config.default_page_size))
    offset = int(args.get("offset", 0))
    return _paginate(tables, limit, offset)


def list_tables_spec() -> ToolSpec:
    return ToolSpec(
        name="list_tables",
        description=(
            "List tables and views in a database. Defaults to the current "
            "database if 'database' is omitted. Returns each table's name, "
            "type (TABLE/VIEW/TEMPORARY), description, and database."
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
        handler=_handle_list_tables,
    )


# ---------------------------------------------------------------------------
# describe_table
# ---------------------------------------------------------------------------


async def _handle_describe_table(args: Dict[str, Any], holder: SessionHolder) -> Dict[str, Any]:
    spark = holder.get()
    name = args["name"]

    table = spark.catalog.getTable(name)
    columns = list(spark.catalog.listColumns(name))
    return {
        "table": _to_dict(table),
        "columns": [_to_dict(c) for c in columns],
    }


def describe_table_spec() -> ToolSpec:
    return ToolSpec(
        name="describe_table",
        description=(
            "Return a table's metadata: name, database, type, description, "
            "and the full column list with types, nullability, and partition "
            "flags. Accepts a fully-qualified or unqualified name."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Table name. May be qualified (catalog.db.table) or unqualified.",
                }
            },
            "required": ["name"],
            "additionalProperties": False,
        },
        handler=_handle_describe_table,
    )
