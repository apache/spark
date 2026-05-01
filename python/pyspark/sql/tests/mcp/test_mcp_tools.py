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

"""Unit tests for the Spark MCP server tool layer.

These tests exercise the tool handlers against an in-memory fake of the
Spark Connect session so they run without a live Spark cluster, without
the MCP SDK installed, and without grpcio. End-to-end tests with a real
Spark Connect server live elsewhere and run as part of the Connect test
matrix.
"""

import asyncio
import unittest
from collections import namedtuple
from typing import Any, Dict, List

from pyspark.sql.mcp.config import ServerConfig
from pyspark.sql.mcp.safety import ReadOnlyViolation
from pyspark.sql.mcp.tools.registry import all_tools


_StructField = namedtuple("StructField", "name dataType nullable")


class _Schema:
    def __init__(self, fields: List[_StructField]):
        self.fields = fields


class _Row(dict):
    def asDict(self, recursive: bool = False) -> Dict[str, Any]:
        return dict(self)


class _FakeDataFrame:
    def __init__(self, rows: List[Dict[str, Any]], schema: _Schema, _offset: int = 0):
        self._rows = rows
        self.schema = schema
        self._offset = _offset

    def offset(self, n: int) -> "_FakeDataFrame":
        return _FakeDataFrame(self._rows, self.schema, _offset=self._offset + n)

    def limit(self, n: int) -> "_FakeDataFrame":
        end = self._offset + n
        return _FakeDataFrame(self._rows[self._offset:end], self.schema)

    def collect(self) -> List[_Row]:
        return [_Row(r) for r in self._rows]

    def _explain_string(self, mode: str = "simple") -> str:
        return f"== {mode} plan ==\nLogicalRDD"


class _FakeCatalog:
    _CatalogMeta = namedtuple("CatalogMetadata", "name description")
    _Database = namedtuple("Database", "name catalog description locationUri")
    _Table = namedtuple(
        "Table", "name catalog namespace description tableType isTemporary"
    )
    _Column = namedtuple(
        "Column", "name description dataType nullable isPartition isBucket"
    )
    _Function = namedtuple(
        "Function", "name catalog namespace description className isTemporary"
    )

    def __init__(self):
        self._catalog = "spark_catalog"
        self._database = "default"

    def currentCatalog(self) -> str:
        return self._catalog

    def currentDatabase(self) -> str:
        return self._database

    def setCurrentCatalog(self, name: str) -> None:
        self._catalog = name

    def listCatalogs(self):
        return [
            self._CatalogMeta("spark_catalog", "in-memory"),
            self._CatalogMeta("hive_metastore", None),
        ]

    def listDatabases(self, pattern=None):
        return [self._Database("default", "spark_catalog", None, "/tmp")]

    def listTables(self, dbName=None, pattern=None):
        return [
            self._Table(
                "orders", "spark_catalog", ["default"], "orders", "MANAGED", False
            )
        ]

    def getTable(self, name):
        return self._Table(
            "orders", "spark_catalog", ["default"], "orders", "MANAGED", False
        )

    def listColumns(self, name, dbName=None):
        return [
            self._Column("id", None, "bigint", False, False, False),
            self._Column("amount", None, "decimal(10,2)", True, False, False),
        ]

    def listFunctions(self, dbName=None, pattern=None):
        return [
            self._Function(
                "coalesce", "spark_catalog", [], "", "BuiltIn", True
            ),
            self._Function(
                "explode", "spark_catalog", [], "", "BuiltIn", True
            ),
        ]


class _FakeConf:
    def getAll(self) -> Dict[str, str]:
        return {
            "spark.app.name": "demo",
            "spark.sql.shuffle.partitions": "200",
            "spark.connect.session.token": "REDACT_ME",
        }


class _FakeSpark:
    version = "4.0.0-test"

    def __init__(self, df: _FakeDataFrame):
        self._df = df
        self.catalog = _FakeCatalog()
        self.conf = _FakeConf()

    def sql(self, query, args=None):
        # Echo the same fake DataFrame for every query — the tools we exercise
        # care about row/schema shape, not query semantics.
        return self._df


class _FakeHolder:
    def __init__(self, **overrides: Any):
        self.config = ServerConfig(connect_url="sc://fake", **overrides)
        rows = [{"id": i, "amount": i * 10} for i in range(5)]
        schema = _Schema(
            [_StructField("id", "long", False), _StructField("amount", "long", True)]
        )
        self._spark = _FakeSpark(_FakeDataFrame(rows, schema))

    def get(self) -> _FakeSpark:
        return self._spark


def _run(coro):
    return asyncio.run(coro)


def _spec(name: str):
    for spec in all_tools():
        if spec.name == name:
            return spec
    raise KeyError(name)


class MCPToolsTest(unittest.TestCase):
    def test_registry_exposes_ten_tools(self):
        names = [s.name for s in all_tools()]
        self.assertEqual(len(names), 10)
        self.assertIn("get_session_info", names)
        self.assertIn("execute_sql", names)
        self.assertIn("explain_query", names)

    def test_get_session_info_redacts_token(self):
        out = _run(_spec("get_session_info").handler({}, _FakeHolder()))
        self.assertEqual(out["spark_version"], "4.0.0-test")
        self.assertEqual(out["read_only"], True)
        self.assertIn("spark.app.name", out["configs"])
        self.assertNotIn("spark.connect.session.token", out["configs"])

    def test_list_catalogs_paginates(self):
        out = _run(_spec("list_catalogs").handler({"limit": 1}, _FakeHolder()))
        self.assertEqual(out["returned"], 1)
        self.assertTrue(out["truncated"])
        self.assertEqual(out["total"], 2)

    def test_list_tables_returns_namespace(self):
        out = _run(_spec("list_tables").handler({}, _FakeHolder()))
        self.assertEqual(out["items"][0]["name"], "orders")
        self.assertEqual(out["items"][0]["namespace"], ["default"])

    def test_describe_table_includes_columns(self):
        out = _run(_spec("describe_table").handler({"name": "orders"}, _FakeHolder()))
        self.assertEqual(out["table"]["name"], "orders")
        col_names = [c["name"] for c in out["columns"]]
        self.assertEqual(col_names, ["id", "amount"])

    def test_execute_sql_json_truncates(self):
        out = _run(
            _spec("execute_sql").handler(
                {"query": "SELECT * FROM t", "limit": 3}, _FakeHolder()
            )
        )
        self.assertEqual(out["row_count"], 3)
        self.assertTrue(out["truncated"])
        self.assertEqual([c["name"] for c in out["schema"]], ["id", "amount"])

    def test_execute_sql_markdown(self):
        out = _run(
            _spec("execute_sql").handler(
                {"query": "SELECT * FROM t", "limit": 2, "format": "markdown"},
                _FakeHolder(),
            )
        )
        self.assertIn("| id | amount |", out["markdown"])
        self.assertNotIn("rows", out)

    def test_execute_sql_read_only_blocks_ddl(self):
        with self.assertRaises(ReadOnlyViolation):
            _run(_spec("execute_sql").handler({"query": "DROP TABLE t"}, _FakeHolder()))

    def test_execute_sql_read_only_off_allows_ddl(self):
        out = _run(
            _spec("execute_sql").handler(
                {"query": "DROP TABLE t"}, _FakeHolder(read_only=False)
            )
        )
        # Fake session returns the canned DataFrame; the test only asserts no
        # exception escaped past the safety filter.
        self.assertIn("row_count", out)

    def test_execute_sql_max_rows_caps_limit(self):
        out = _run(
            _spec("execute_sql").handler(
                {"query": "SELECT * FROM t", "limit": 100},
                _FakeHolder(max_rows=2),
            )
        )
        self.assertEqual(out["limit"], 2)
        self.assertEqual(out["row_count"], 2)
        self.assertTrue(out["truncated"])

    def test_explain_query_modes(self):
        out = _run(
            _spec("explain_query").handler(
                {"query": "SELECT 1", "mode": "extended"}, _FakeHolder()
            )
        )
        self.assertEqual(out["mode"], "extended")
        self.assertIn("extended plan", out["plan"])

    def test_explain_query_rejects_unknown_mode(self):
        with self.assertRaises(ValueError):
            _run(
                _spec("explain_query").handler(
                    {"query": "SELECT 1", "mode": "bogus"}, _FakeHolder()
                )
            )

    def test_analyze_query_bundles_schema_and_plans(self):
        out = _run(_spec("analyze_query").handler({"query": "SELECT 1"}, _FakeHolder()))
        self.assertIn("schema", out)
        self.assertIn("extended_plan", out)
        self.assertIn("formatted_plan", out)

    def test_preview_table_delegates_to_execute_sql(self):
        out = _run(
            _spec("preview_table").handler(
                {"name": "orders", "limit": 1}, _FakeHolder()
            )
        )
        self.assertEqual(out["row_count"], 1)


if __name__ == "__main__":
    unittest.main()
