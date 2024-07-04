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

import time
import unittest

from pyspark.sql import Row
from pyspark.sql.functions import col, lit, count, sum, mean
from pyspark.errors import (
    PySparkAssertionError,
    PySparkTypeError,
    PySparkValueError,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase


class DataFrameObservationTestsMixin:
    def test_observe(self):
        # SPARK-36263: tests the DataFrame.observe(Observation, *Column) method
        from pyspark.sql import Observation

        df = self.spark.createDataFrame(
            [
                (1, 1.0, "one"),
                (2, 2.0, "two"),
                (3, 3.0, "three"),
            ],
            ["id", "val", "label"],
        )

        unnamed_observation = Observation()
        named_observation = Observation("metric")

        with self.assertRaises(PySparkAssertionError) as pe:
            unnamed_observation.get()

        self.check_error(
            exception=pe.exception,
            error_class="NO_OBSERVE_BEFORE_GET",
            message_parameters={},
        )

        observed = (
            df.orderBy("id")
            .observe(
                named_observation,
                count(lit(1)).alias("cnt"),
                sum(col("id")).alias("sum"),
                mean(col("val")).alias("mean"),
            )
            .observe(unnamed_observation, count(lit(1)).alias("rows"))
        )

        # test that observe works transparently
        actual = observed.collect()
        self.assertEqual(
            [
                {"id": 1, "val": 1.0, "label": "one"},
                {"id": 2, "val": 2.0, "label": "two"},
                {"id": 3, "val": 3.0, "label": "three"},
            ],
            [row.asDict() for row in actual],
        )

        # test that we retrieve the metrics
        self.assertEqual(named_observation.get, dict(cnt=3, sum=6, mean=2.0))
        self.assertEqual(unnamed_observation.get, dict(rows=3))

        with self.assertRaises(PySparkAssertionError) as pe:
            df.observe(named_observation, count(lit(1)).alias("count"))

        self.check_error(
            exception=pe.exception,
            error_class="REUSE_OBSERVATION",
            message_parameters={},
        )

        # observation requires name (if given) to be non empty string
        with self.assertRaisesRegex(TypeError, "`name` should be a str, got int"):
            Observation(123)
        with self.assertRaisesRegex(ValueError, "`name` must be a non-empty string, got ''."):
            Observation("")

        # dataframe.observe requires at least one expr
        with self.assertRaises(PySparkValueError) as pe:
            df.observe(Observation())

        self.check_error(
            exception=pe.exception,
            error_class="CANNOT_BE_EMPTY",
            message_parameters={"item": "exprs"},
        )

        # dataframe.observe requires non-None Columns
        for args in [(None,), ("id",), (lit(1), None), (lit(1), "id")]:
            with self.subTest(args=args):
                with self.assertRaises(PySparkTypeError) as pe:
                    df.observe(Observation(), *args)

                self.check_error(
                    exception=pe.exception,
                    error_class="NOT_LIST_OF_COLUMN",
                    message_parameters={"arg_name": "exprs"},
                )

    def test_observe_str(self):
        # SPARK-38760: tests the DataFrame.observe(str, *Column) method
        from pyspark.sql.streaming import StreamingQueryListener

        observed_metrics = None

        class TestListener(StreamingQueryListener):
            def onQueryStarted(self, event):
                pass

            def onQueryProgress(self, event):
                nonlocal observed_metrics
                observed_metrics = event.progress.observedMetrics

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        self.spark.streams.addListener(TestListener())

        df = self.spark.readStream.format("rate").option("rowsPerSecond", 10).load()
        df = df.observe("metric", count(lit(1)).alias("cnt"), sum(col("value")).alias("sum"))
        q = df.writeStream.format("noop").queryName("test").start()
        self.assertTrue(q.isActive)
        time.sleep(10)
        q.stop()

        self.assertTrue(isinstance(observed_metrics, dict))
        self.assertTrue("metric" in observed_metrics)
        row = observed_metrics["metric"]
        self.assertTrue(isinstance(row, Row))
        self.assertTrue(hasattr(row, "cnt"))
        self.assertTrue(hasattr(row, "sum"))
        self.assertGreaterEqual(row.cnt, 0)
        self.assertGreaterEqual(row.sum, 0)

    def test_observe_with_same_name_on_different_dataframe(self):
        # SPARK-45656: named observations with the same name on different datasets
        from pyspark.sql import Observation

        observation1 = Observation("named")
        df1 = self.spark.range(50)
        observed_df1 = df1.observe(observation1, count(lit(1)).alias("cnt"))

        observation2 = Observation("named")
        df2 = self.spark.range(100)
        observed_df2 = df2.observe(observation2, count(lit(1)).alias("cnt"))

        observed_df1.collect()
        observed_df2.collect()

        self.assertEqual(observation1.get, dict(cnt=50))
        self.assertEqual(observation2.get, dict(cnt=100))

    def test_observe_on_commands(self):
        from pyspark.sql import Observation

        df = self.spark.range(50)

        test_table = "test_table"

        # DataFrameWriter
        with self.table(test_table):
            for command, action in [
                ("collect", lambda df: df.collect()),
                ("show", lambda df: df.show(50)),
                ("save", lambda df: df.write.format("noop").mode("overwrite").save()),
                ("create", lambda df: df.writeTo(test_table).using("parquet").create()),
            ]:
                with self.subTest(command=command):
                    observation = Observation()
                    observed_df = df.observe(observation, count(lit(1)).alias("cnt"))
                    action(observed_df)
                    self.assertEqual(observation.get, dict(cnt=50))


class DataFrameObservationTests(
    DataFrameObservationTestsMixin,
    ReusedSQLTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_observation import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
