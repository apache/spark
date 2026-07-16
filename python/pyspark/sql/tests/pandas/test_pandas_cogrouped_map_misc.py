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

import unittest
import logging

from pyspark.sql import functions as sf
from pyspark.sql.types import Row
from pyspark.sql.window import Window
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    assertDataFrameEqual,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import is_remote_only

if have_pandas:
    import pandas as pd

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class CogroupedApplyInPandasMiscTestsMixin:
    def test_with_window_function(self):
        # SPARK-42168: a window function with same partition keys but differing key order
        ids = 2
        days = 100
        vals = 10000
        parts = 10

        id_df = self.spark.range(ids)
        day_df = self.spark.range(days).withColumnRenamed("id", "day")
        vals_df = self.spark.range(vals).withColumnRenamed("id", "value")
        df = id_df.join(day_df).join(vals_df)

        left_df = df.withColumnRenamed("value", "left").repartition(parts).cache()
        # SPARK-42132: this bug requires us to alias all columns from df here
        right_df = (
            df.select(
                sf.col("id").alias("id"), sf.col("day").alias("day"), sf.col("value").alias("right")
            )
            .repartition(parts)
            .cache()
        )

        # note the column order is different to the groupBy("id", "day") column order below
        window = Window.partitionBy("day", "id")

        left_grouped_df = left_df.groupBy("id", "day")
        right_grouped_df = right_df.withColumn(
            "day_sum", sf.sum(sf.col("day")).over(window)
        ).groupBy("id", "day")

        def cogroup(left: "pd.DataFrame", right: "pd.DataFrame") -> "pd.DataFrame":
            return pd.DataFrame(
                [
                    {
                        "id": (
                            left["id"][0]
                            if not left.empty
                            else (right["id"][0] if not right.empty else None)
                        ),
                        "day": (
                            left["day"][0]
                            if not left.empty
                            else (right["day"][0] if not right.empty else None)
                        ),
                        "lefts": len(left.index),
                        "rights": len(right.index),
                    }
                ]
            )

        df = left_grouped_df.cogroup(right_grouped_df).applyInPandas(
            cogroup, schema="id long, day long, lefts integer, rights integer"
        )

        actual = df.orderBy("id", "day").take(days)
        self.assertEqual(actual, [Row(0, day, vals, vals) for day in range(days)])

    def test_with_local_data(self):
        df1 = self.spark.createDataFrame(
            [(1, 1.0, "a"), (2, 2.0, "b"), (1, 3.0, "c"), (2, 4.0, "d")], ("id", "v1", "v2")
        )
        df2 = self.spark.createDataFrame([(1, "x"), (2, "y"), (1, "z")], ("id", "v3"))

        def summarize(left, right):
            return pd.DataFrame(
                {
                    "left_rows": [len(left)],
                    "left_columns": [len(left.columns)],
                    "right_rows": [len(right)],
                    "right_columns": [len(right.columns)],
                }
            )

        df = (
            df1.groupby("id")
            .cogroup(df2.groupby("id"))
            .applyInPandas(
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
            return pd.DataFrame(
                {
                    "key": [key[0]],
                    "left_rows": [len(left)],
                    "left_columns": [len(left.columns)],
                    "right_rows": [len(right)],
                    "right_columns": [len(right.columns)],
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
                        .applyInPandas(summarize, schema=schema)
                        .sort("key")
                        .collect()
                    )

                    self.assertEqual(expected, result)

    @unittest.skipIf(is_remote_only(), "Requires JVM access")
    def test_cogroup_apply_in_pandas_with_logging(self):
        import pandas as pd

        def func_with_logging(left_pdf, right_pdf):
            assert isinstance(left_pdf, pd.DataFrame)
            assert isinstance(right_pdf, pd.DataFrame)
            logger = logging.getLogger("test_pandas_cogrouped_map")
            logger.warning(
                f"pandas cogrouped map: {dict(v1=list(left_pdf['v1']), v2=list(right_pdf['v2']))}"
            )
            return pd.merge(left_pdf, right_pdf, on=["id"])

        left_df = self.spark.createDataFrame([(1, 10), (2, 20), (1, 30)], ["id", "v1"])
        right_df = self.spark.createDataFrame([(1, 100), (2, 200), (1, 300)], ["id", "v2"])

        grouped_left = left_df.groupBy("id")
        grouped_right = right_df.groupBy("id")
        cogrouped_df = grouped_left.cogroup(grouped_right)

        with self.sql_conf({"spark.sql.pyspark.worker.logging.enabled": "true"}):
            assertDataFrameEqual(
                cogrouped_df.applyInPandas(func_with_logging, "id long, v1 long, v2 long"),
                [Row(id=1, v1=v1, v2=v2) for v1 in [10, 30] for v2 in [100, 300]]
                + [Row(id=2, v1=20, v2=200)],
            )

            logs = self.spark.tvf.python_worker_logs()

            assertDataFrameEqual(
                logs.select("level", "msg", "context", "logger"),
                [
                    Row(
                        level="WARNING",
                        msg=f"pandas cogrouped map: {dict(v1=v1, v2=v2)}",
                        context={"func_name": func_with_logging.__name__},
                        logger="test_pandas_cogrouped_map",
                    )
                    for v1, v2 in [([10, 30], [100, 300]), ([20], [200])]
                ],
            )


class CogroupedApplyInPandasMiscTests(CogroupedApplyInPandasMiscTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
