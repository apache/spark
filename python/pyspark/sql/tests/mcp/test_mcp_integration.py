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

"""End-to-end integration test for the Spark MCP server.

This test boots a local Spark Connect session (via the in-process server
provided by ``SparkSession.builder.remote("local[*]")``) and exercises the
MCP tool handlers against it. The unit tests in ``test_mcp_tools`` cover the
same tools against a fake session; this file confirms that the handlers
actually agree with what the real Connect client returns.

The test is skipped automatically when:
  * Spark Connect dependencies (grpcio, pandas, pyarrow, ...) are missing.
  * Spark JARs are not built (the ``SparkSession`` cannot be created).

Run with::

    PYTHONPATH=python python -m unittest \
        pyspark.sql.tests.mcp.test_mcp_integration
"""

import asyncio
import unittest

from pyspark.testing.utils import (
    connect_requirement_message,
    should_test_connect,
)


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class MCPToolsConnectIntegrationTest(unittest.TestCase):
    """Run MCP tool handlers against a real Spark Connect session."""

    spark = None
    holder = None

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.appName(cls.__name__)
            # Disable the Spark UI: in some local builds Jetty's classpath
            # is mismatched and binding the UI throws. We don't need it for
            # an in-process test.
            .config("spark.ui.enabled", "false")
            # Force loopback for both driver and executor traffic so the
            # in-process Connect REPL artifact server is reachable from
            # local executors.
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .remote("local[2]")
            .getOrCreate()
        )

        # Seed a small dataset so the catalog/preview tools have something
        # to chew on.
        cls.spark.sql("CREATE TABLE IF NOT EXISTS mcp_test_orders (id BIGINT, amount DECIMAL(10,2)) USING parquet")
        cls.spark.sql("INSERT INTO mcp_test_orders VALUES (1, 10.50), (2, 20.00), (3, 30.75)")

        from pyspark.sql.mcp.config import ServerConfig
        from pyspark.sql.mcp.session import SessionHolder

        class _PreboundHolder(SessionHolder):
            """A SessionHolder that wraps an already-built SparkSession.

            The default holder calls ``SparkSession.builder.remote(...)``;
            for the integration test we want to reuse the session created
            in ``setUpClass`` so all assertions run against the same Spark
            instance.
            """

            def __init__(self, config, session):
                super().__init__(config)
                self._session = session

        cls.holder = _PreboundHolder(
            ServerConfig(connect_url="local[2]"),
            cls.spark,
        )

    @classmethod
    def tearDownClass(cls):
        try:
            if cls.spark is not None:
                cls.spark.sql("DROP TABLE IF EXISTS mcp_test_orders")
                cls.spark.stop()
        finally:
            cls.spark = None
            cls.holder = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @classmethod
    def _spec(cls, name):
        from pyspark.sql.mcp.tools.registry import all_tools

        for spec in all_tools():
            if spec.name == name:
                return spec
        raise KeyError(name)

    @classmethod
    def _call(cls, name, args=None):
        spec = cls._spec(name)
        return asyncio.run(spec.handler(args or {}, cls.holder))

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_get_session_info_reports_real_version(self):
        out = self._call("get_session_info")
        self.assertTrue(out["spark_version"], "spark_version should not be empty")
        # Sanity-check semver-ish prefix.
        self.assertRegex(out["spark_version"], r"^\d+\.\d+")
        self.assertEqual(out["read_only"], True)
        self.assertIn("current_catalog", out)
        self.assertIn("current_database", out)

    def test_list_catalogs_includes_default(self):
        out = self._call("list_catalogs")
        names = [item.get("name") for item in out["items"]]
        self.assertIn("spark_catalog", names)

    def test_list_databases_includes_default(self):
        out = self._call("list_databases")
        names = [item.get("name") for item in out["items"]]
        self.assertIn("default", names)

    def test_list_tables_finds_seeded_table(self):
        out = self._call("list_tables")
        names = [item.get("name") for item in out["items"]]
        self.assertIn("mcp_test_orders", names)

    def test_describe_table_returns_columns(self):
        out = self._call("describe_table", {"name": "mcp_test_orders"})
        self.assertEqual(out["table"]["name"], "mcp_test_orders")
        col_names = [c["name"] for c in out["columns"]]
        self.assertEqual(col_names, ["id", "amount"])

    def test_execute_sql_returns_rows(self):
        out = self._call(
            "execute_sql",
            {"query": "SELECT id, amount FROM mcp_test_orders ORDER BY id"},
        )
        self.assertEqual(out["row_count"], 3)
        self.assertFalse(out["truncated"])
        ids = [row["id"] for row in out["rows"]]
        self.assertEqual(ids, [1, 2, 3])
        # Schema metadata round-trips through Connect.
        names = [field["name"] for field in out["schema"]]
        self.assertEqual(names, ["id", "amount"])

    def test_execute_sql_truncates_at_limit(self):
        out = self._call(
            "execute_sql",
            {
                "query": "SELECT id FROM mcp_test_orders ORDER BY id",
                "limit": 2,
            },
        )
        self.assertEqual(out["row_count"], 2)
        self.assertTrue(out["truncated"])

    def test_execute_sql_blocks_ddl_in_read_only(self):
        from pyspark.errors import PySparkValueError

        with self.assertRaises(PySparkValueError) as ctx:
            self._call("execute_sql", {"query": "DROP TABLE mcp_test_orders"})
        self.assertEqual(ctx.exception.getCondition(), "MCP_READ_ONLY_VIOLATION")

    def test_explain_query_returns_plan_text(self):
        out = self._call(
            "explain_query",
            {"query": "SELECT COUNT(*) FROM mcp_test_orders", "mode": "formatted"},
        )
        self.assertEqual(out["mode"], "formatted")
        self.assertIn("Aggregate", out["plan"])

    def test_analyze_query_includes_schema_and_plans(self):
        out = self._call("analyze_query", {"query": "SELECT id FROM mcp_test_orders"})
        self.assertEqual([f["name"] for f in out["schema"]], ["id"])
        self.assertIn("extended_plan", out)
        self.assertIn("formatted_plan", out)

    def test_preview_table_returns_capped_rows(self):
        out = self._call("preview_table", {"name": "mcp_test_orders", "limit": 2})
        self.assertEqual(out["row_count"], 2)


if __name__ == "__main__":
    unittest.main()
