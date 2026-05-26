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

from pyspark.sql import Row, Observation, functions as F
from pyspark.sql.types import StructType, LongType
from pyspark.errors import (
    AnalysisException,
    PySparkAssertionError,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import assertDataFrameEqual, eventually


class DataFrameObservationTestsMixin:
    def test_observe(self):
        # SPARK-36263: tests the DataFrame.observe(Observation, *Column) method
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
            errorClass="NO_OBSERVE_BEFORE_GET",
            messageParameters={},
        )

        observed = (
            df.orderBy("id")
            .observe(
                named_observation,
                F.count(F.lit(1)).alias("cnt"),
                F.sum(F.col("id")).alias("sum"),
                F.mean(F.col("val")).alias("mean"),
            )
            .observe(unnamed_observation, F.count(F.lit(1)).alias("rows"))
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
            df.observe(named_observation, F.count(F.lit(1)).alias("count"))

        self.check_error(
            exception=pe.exception,
            errorClass="REUSE_OBSERVATION",
            messageParameters={},
        )

        new_observation = Observation("metric")
        with self.assertRaises(AnalysisException) as pe:
            observed.observe(new_observation, 2 * F.count(F.lit(1)).alias("cnt")).collect()

        self.check_error(
            exception=pe.exception,
            errorClass="DUPLICATED_METRICS_NAME",
            messageParameters={"metricName": "metric"},
        )

        # observation requires name (if given) to be non empty string
        with self.assertRaisesRegex(PySparkTypeError, "`name` should be str, got int"):
            Observation(123)
        with self.assertRaisesRegex(ValueError, "`name` must be a non-empty string, got ''."):
            Observation("")

        # dataframe.observe requires at least one expr
        with self.assertRaises(PySparkValueError) as pe:
            df.observe(Observation())

        self.check_error(
            exception=pe.exception,
            errorClass="CANNOT_BE_EMPTY",
            messageParameters={"item": "exprs"},
        )

        # dataframe.observe requires non-None Columns
        for args in [(None,), ("id",), (F.lit(1), None), (F.lit(1), "id")]:
            with self.subTest(args=args):
                with self.assertRaises(PySparkTypeError) as pe:
                    df.observe(Observation(), *args)

                self.check_error(
                    exception=pe.exception,
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "expected_type": "list[Column]",
                        "arg_name": "exprs",
                        "arg_type": "tuple",
                    },
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
        df = df.observe(
            "metric", F.count(F.lit(1)).alias("cnt"), F.sum(F.col("value")).alias("sum")
        )
        q = df.writeStream.format("noop").queryName("test").start()
        self.assertTrue(q.isActive)

        @eventually(timeout=10, catch_assertions=True)
        def check_observed_metrics():
            self.assertTrue(isinstance(observed_metrics, dict))
            self.assertTrue("metric" in observed_metrics)
            row = observed_metrics["metric"]
            self.assertIsInstance(row.cnt, int)
            self.assertIsInstance(row.sum, int)
            self.assertGreaterEqual(row.cnt, 0)
            self.assertGreaterEqual(row.sum, 0)
            return True

        check_observed_metrics()

        q.stop()

    def test_observe_with_same_name_on_different_dataframe(self):
        # SPARK-45656: named observations with the same name on different datasets
        observation1 = Observation("named")
        df1 = self.spark.range(50)
        observed_df1 = df1.observe(observation1, F.count(F.lit(1)).alias("cnt"))

        observation2 = Observation("named")
        df2 = self.spark.range(100)
        observed_df2 = df2.observe(observation2, F.count(F.lit(1)).alias("cnt"))

        observed_df1.collect()
        observed_df2.collect()

        self.assertEqual(observation1.get, dict(cnt=50))
        self.assertEqual(observation2.get, dict(cnt=100))

    def test_observe_on_commands(self):
        df = self.spark.range(50)
        schema = StructType().add("id", LongType(), nullable=False)

        test_table = "test_table"

        # DataFrameWriter
        for cache_enabled in [False, True]:
            with (
                self.subTest(cache_enabled=cache_enabled),
                self.sql_conf({"spark.connect.session.planCache.enabled": cache_enabled}),
            ):
                for command, action in [
                    ("collect", lambda df: df.collect()),
                    ("show", lambda df: df.show(50)),
                    ("save", lambda df: df.write.format("noop").mode("overwrite").save()),
                    ("create", lambda df: df.writeTo(test_table).using("parquet").create()),
                ]:
                    for select_star in [True, False]:
                        with (
                            self.subTest(command=command, select_star=select_star),
                            self.table(test_table),
                        ):
                            observation = Observation()
                            observed_df = df.observe(observation, F.count(F.lit(1)).alias("cnt"))
                            if select_star:
                                observed_df = observed_df.select("*")
                            self.assertEqual(observed_df.schema, schema)
                            action(observed_df)
                            self.assertEqual(observation.get, dict(cnt=50))

    def test_observe_with_struct_type(self):
        observation = Observation("struct")

        df = self.spark.range(10).observe(
            observation,
            F.struct(F.count(F.lit(1)).alias("rows"), F.max("id").alias("maxid")).alias("struct"),
        )

        assertDataFrameEqual(df, [Row(id=id) for id in range(10)])

        self.assertEqual(observation.get, {"struct": Row(rows=10, maxid=9)})

    def test_observe_with_array_type(self):
        observation = Observation("array")

        df = self.spark.range(10).observe(
            observation,
            F.array(F.count(F.lit(1))).alias("array"),
        )

        assertDataFrameEqual(df, [Row(id=id) for id in range(10)])

        self.assertEqual(observation.get, {"array": [10]})

    def test_observe_with_map_type(self):
        observation = Observation("map")

        df = self.spark.range(10).observe(
            observation,
            F.create_map(F.lit("count"), F.count(F.lit(1))).alias("map"),
        )

        assertDataFrameEqual(df, [Row(id=id) for id in range(10)])

        self.assertEqual(observation.get, {"map": {"count": 10}})

    def test_observation_errors_propagated_to_client(self):
        observation = Observation("test_observation")
        observed_df = self.spark.range(10).observe(
            observation,
            F.sum("id").alias("sum_id"),
            F.raise_error(F.lit("test error")).alias("raise_error"),
        )
        actual = observed_df.collect()
        self.assertEqual(
            [row.asDict() for row in actual],
            [{"id": i} for i in range(10)],
        )

        with self.assertRaises(PySparkException) as cm:
            _ = observation.get

        self.assertIn("test error", str(cm.exception))

    def test_observe_self_join(self):
        # SPARK-56322: self-joining an observed DataFrame
        obs = Observation("my_observation")
        df = (
            self.spark.range(100)
            .selectExpr("id", "CASE WHEN id < 10 THEN 'A' ELSE 'B' END AS group_key")
            .observe(obs, F.count(F.lit(1)).alias("row_count"))
        )

        df1 = df.where("id < 20")
        df2 = df.where("id % 2 == 0")

        joined = df1.alias("a").join(df2.alias("b"), on=["id"], how="inner")
        result = joined.collect()

        # The join should produce rows where id < 20 AND id is even
        expected_ids = sorted([i for i in range(20) if i % 2 == 0])
        actual_ids = sorted([row.id for row in result])
        self.assertEqual(actual_ids, expected_ids)

        # The observation should have been collected
        self.assertEqual(obs.get, {"row_count": 100})

        # Check the error conditions
        with self.assertRaises(PySparkAssertionError) as pe:
            joined.observe(obs, F.count(F.lit(1)).alias("row_count")).collect()

        self.check_error(
            exception=pe.exception,
            errorClass="REUSE_OBSERVATION",
            messageParameters={},
        )

        obs2 = Observation("my_observation")
        with self.assertRaises(AnalysisException) as pe:
            joined.observe(obs2, 2 * F.count(F.lit(1)).alias("row_count")).collect()

        self.check_error(
            exception=pe.exception,
            errorClass="DUPLICATED_METRICS_NAME",
            messageParameters={"metricName": "my_observation"},
        )

    def test_observe_lateral_join(self):
        # SPARK-56322: lateral self-joining an observed DataFrame
        obs = Observation("lateral_join_observation")
        df = self.spark.range(50).observe(obs, F.count(F.lit(1)).alias("row_count"))

        joined = (
            df.alias("left")
            .lateralJoin(
                df.alias("right"), on=F.expr("right.id between left.id - 1 and left.id + 1")
            )
            .selectExpr("left.id as left_id", "right.id as right_id")
        )
        result = joined.collect()

        # Joins on row 0 should produce rows 0 and 1
        bounded_matches = sorted([r.right_id for r in result if r.left_id == 0])
        self.assertEqual(bounded_matches, [0, 1])

        # Joins on row 25 should produce rows 24, 25, and 26
        unbounded_matches = sorted([r.right_id for r in result if r.left_id == 25])
        self.assertEqual(unbounded_matches, [24, 25, 26])

        # The observation should have been collected
        self.assertEqual(obs.get, {"row_count": 50})

        # Check the error conditions
        with self.assertRaises(PySparkAssertionError) as reused:
            joined.observe(obs, F.count(F.lit(1)).alias("row_count")).collect()

        self.check_error(
            exception=reused.exception,
            errorClass="REUSE_OBSERVATION",
            messageParameters={},
        )

        obs2 = Observation("lateral_join_observation")
        with self.assertRaises(AnalysisException) as pe:
            joined.observe(obs2, F.count(2 * F.lit(1)).alias("row_count")).collect()

        self.check_error(
            exception=pe.exception,
            errorClass="DUPLICATED_METRICS_NAME",
            messageParameters={"metricName": "lateral_join_observation"},
        )

    def test_observe_self_join_union(self):
        # SPARK-56322: union of observed DataFrames with same observation
        obs = Observation("union_obs")
        df = self.spark.range(50).observe(obs, F.count(F.lit(1)).alias("cnt"))

        df1 = df.where("id < 25")
        df2 = df.where("id >= 25")

        unioned = df1.union(df2)
        result = unioned.collect()

        actual_ids = sorted([row.id for row in result])
        self.assertEqual(actual_ids, list(range(50)))
        self.assertEqual(obs.get, {"cnt": 50})


class DataFrameObservationTests(
    DataFrameObservationTestsMixin,
    ReusedSQLTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
