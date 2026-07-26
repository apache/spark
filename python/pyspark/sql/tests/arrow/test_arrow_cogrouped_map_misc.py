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
import time
import unittest
import logging

from pyspark.sql import Row
from pyspark.sql import functions as sf
from pyspark.sql.tests.arrow.test_arrow_cogrouped_map import CogroupedMapInArrowTestsFuncMixin
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import assertDataFrameEqual, have_pyarrow, pyarrow_requirement_message
from pyspark.util import is_remote_only

if have_pyarrow:
    import pyarrow as pa


@unittest.skipIf(
    not have_pyarrow,
    pyarrow_requirement_message,
)
class CogroupedMapInArrowMiscTestsMixin(CogroupedMapInArrowTestsFuncMixin):
    def test_apply_in_arrow_large_var_types(self):
        # SPARK-56929: when useLargeVarTypes=true, the expected schema computed by
        # worker.py for result validation must also use large_string/large_binary,
        # otherwise verify_arrow_result raises a spurious RESULT_COLUMN_TYPES_MISMATCH.
        left = self.spark.createDataFrame(
            [(0, "foo", b"foo"), (1, None, None)], "id long, s string, b binary"
        )
        right = self.spark.createDataFrame(
            [(0, "bar", b"bar"), (1, "baz", b"baz")], "id long, s string, b binary"
        )
        schema = "s string, b binary"

        def func(left_tbl, right_tbl):
            assert pa.types.is_large_string(left_tbl.schema.field("s").type)
            assert pa.types.is_large_binary(left_tbl.schema.field("b").type)
            return left_tbl.select(["s", "b"])

        expected = left.select("s", "b")
        for assign_cols_by_name in [True, False]:
            with self.subTest(assign_cols_by_name=assign_cols_by_name):
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.useLargeVarTypes": True,
                        "spark.sql.legacy.execution.pandas.groupedMap."
                        "assignColumnsByName": assign_cols_by_name,
                    }
                ):
                    cogrouped = left.groupBy("id").cogroup(right.groupBy("id"))
                    actual = cogrouped.applyInArrow(func, schema)
                    assertDataFrameEqual(actual, expected)

    def test_with_local_data(self):
        df1 = self.spark.createDataFrame(
            [(1, 1.0, "a"), (2, 2.0, "b"), (1, 3.0, "c"), (2, 4.0, "d")], ("id", "v1", "v2")
        )
        df2 = self.spark.createDataFrame([(1, "x"), (2, "y"), (1, "z")], ("id", "v3"))

        def summarize(left, right):
            return pa.Table.from_pydict(
                {
                    "left_rows": [left.num_rows],
                    "left_columns": [left.num_columns],
                    "right_rows": [right.num_rows],
                    "right_columns": [right.num_columns],
                }
            )

        df = (
            df1.groupby("id")
            .cogroup(df2.groupby("id"))
            .applyInArrow(
                summarize,
                schema="left_rows long, left_columns long, right_rows long, right_columns long",
            )
        )

        self.assertEqual(
            df._show_string(),
            "+---------+------------+----------+-------------+\n"
            "|left_rows|left_columns|right_rows|right_columns|\n"
            "+---------+------------+----------+-------------+\n"
            "|        2|           3|         2|            2|\n"
            "|        2|           3|         1|            2|\n"
            "+---------+------------+----------+-------------+\n",
        )

    def test_self_join(self):
        df = self.spark.createDataFrame([(1, 1)], ("k", "v"))

        def arrow_func(key, left, right):
            return pa.Table.from_pydict({"x": [2], "y": [2]})

        df2 = df.groupby("k").cogroup(df.groupby("k")).applyInArrow(arrow_func, "x long, y long")

        self.assertEqual(df2.join(df2).count(), 1)

    def test_arrow_batch_slicing(self):
        m, n = 100000, 10000

        df1 = self.spark.range(m).select((sf.col("id") % 2).alias("key"), sf.col("id").alias("v"))
        cols = {f"col_{i}": sf.col("v") + i for i in range(10)}
        df1 = df1.withColumns(cols)

        df2 = self.spark.range(n).select((sf.col("id") % 4).alias("key"), sf.col("id").alias("v"))
        cols = {f"col_{i}": sf.col("v") + i for i in range(20)}
        df2 = df2.withColumns(cols)

        def summarize(key, left, right):
            assert len(left) == m / 2 or len(left) == 0, len(left)
            assert len(right) == n / 4, len(right)
            return pa.Table.from_pydict(
                {
                    "key": [key[0].as_py()],
                    "left_rows": [left.num_rows],
                    "left_columns": [left.num_columns],
                    "right_rows": [right.num_rows],
                    "right_columns": [right.num_columns],
                }
            )

        schema = "key long, left_rows long, left_columns long, right_rows long, right_columns long"

        expected = [
            Row(key=0, left_rows=m / 2, left_columns=12, right_rows=n / 4, right_columns=22),
            Row(key=1, left_rows=m / 2, left_columns=12, right_rows=n / 4, right_columns=22),
            Row(key=2, left_rows=0, left_columns=12, right_rows=n / 4, right_columns=22),
            Row(key=3, left_rows=0, left_columns=12, right_rows=n / 4, right_columns=22),
        ]

        for maxRecords, maxBytes in [(1000, 2**31 - 1), (0, 4096), (1000, 4096)]:
            with self.subTest(maxRecords=maxRecords, maxBytes=maxBytes):
                with self.sql_conf(
                    {
                        "spark.sql.execution.arrow.maxRecordsPerBatch": maxRecords,
                        "spark.sql.execution.arrow.maxBytesPerBatch": maxBytes,
                    }
                ):
                    result = (
                        df1.groupby("key")
                        .cogroup(df2.groupby("key"))
                        .applyInArrow(summarize, schema=schema)
                        .sort("key")
                        .collect()
                    )

                    self.assertEqual(expected, result)

    def test_negative_and_zero_batch_size(self):
        for batch_size in [0, -1]:
            with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": batch_size}):
                self.do_test_apply_in_arrow(self.cogrouped)

    @unittest.skipIf(is_remote_only(), "Requires JVM access")
    def test_cogroup_apply_in_arrow_with_logging(self):
        import pyarrow as pa

        def func_with_logging(left, right):
            assert isinstance(left, pa.Table)
            assert isinstance(right, pa.Table)
            logger = logging.getLogger("test_arrow_cogrouped_map")
            logger.warning(
                "arrow cogrouped map: "
                + f"{dict(v1=left['v1'].to_pylist(), v2=right['v2'].to_pylist())}"
            )
            return left.join(right, keys="id", join_type="inner")

        left_df = self.spark.createDataFrame([(1, 10), (2, 20), (1, 30)], ["id", "v1"])
        right_df = self.spark.createDataFrame([(1, 100), (2, 200), (1, 300)], ["id", "v2"])

        grouped_left = left_df.groupBy("id")
        grouped_right = right_df.groupBy("id")
        cogrouped_df = grouped_left.cogroup(grouped_right)

        with self.sql_conf({"spark.sql.pyspark.worker.logging.enabled": "true"}):
            assertDataFrameEqual(
                cogrouped_df.applyInArrow(func_with_logging, "id long, v1 long, v2 long"),
                [Row(id=1, v1=v1, v2=v2) for v1 in [10, 30] for v2 in [100, 300]]
                + [Row(id=2, v1=20, v2=200)],
            )

            logs = self.spark.tvf.python_worker_logs()

            assertDataFrameEqual(
                logs.select("level", "msg", "context", "logger"),
                [
                    Row(
                        level="WARNING",
                        msg=f"arrow cogrouped map: {dict(v1=v1, v2=v2)}",
                        context={"func_name": func_with_logging.__name__},
                        logger="test_arrow_cogrouped_map",
                    )
                    for v1, v2 in [([10, 30], [100, 300]), ([20], [200])]
                ],
            )


class CogroupedMapInArrowMiscTests(CogroupedMapInArrowMiscTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.sc.environment["TZ"] = tz
        cls.spark.conf.set("spark.sql.session.timeZone", tz)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        ReusedSQLTestCase.tearDownClass()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
