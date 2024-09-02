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
import inspect
import os
import time
import unittest

from pyspark.errors import PythonException
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pyarrow,
    pyarrow_requirement_message,
)

if have_pyarrow:
    import pyarrow as pa
    import pyarrow.compute as pc


def function_variations(func):
    """Wraps a applyInArrow function returning a Table to return an iter of batches"""
    yield func
    num_args = len(inspect.getfullargspec(func).args)
    if num_args == 2:
        def iter_func(left, right):
            yield from func(left, right).to_batches()

        yield iter_func
    else:
        def iter_keys_func(keys, left, right):
            yield from func(keys, left, right).to_batches()

        yield iter_keys_func


@unittest.skipIf(
    not have_pyarrow,
    pyarrow_requirement_message,  # type: ignore[arg-type]
)
class CogroupedMapInArrowTestsMixin:
    @property
    def left(self):
        return self.spark.range(0, 10, 2, 3).withColumn("v", col("id") * 10)

    @property
    def right(self):
        return self.spark.range(0, 10, 3, 3).withColumn("v", col("id") * 10)

    @property
    def cogrouped(self):
        grouped_left_df = self.left.groupBy((col("id") / 4).cast("int"))
        grouped_right_df = self.right.groupBy((col("id") / 4).cast("int"))
        return grouped_left_df.cogroup(grouped_right_df)

    @staticmethod
    def apply_in_arrow_func(left, right):
        return pa.Table.from_batches(
            CogroupedMapInArrowTests.apply_in_arrow_iterator_func(left, right)
        )

    @staticmethod
    def apply_in_arrow_iterator_func(left, right):
        assert isinstance(left, pa.Table)
        assert isinstance(right, pa.Table)
        assert left.schema.names == ["id", "v"]
        assert right.schema.names == ["id", "v"]

        left_ids = left.to_pydict()["id"]
        right_ids = right.to_pydict()["id"]
        result = {
            "metric": ["min", "max", "len", "sum"],
            "left": [min(left_ids), max(left_ids), len(left_ids), sum(left_ids)],
            "right": [min(right_ids), max(right_ids), len(right_ids), sum(right_ids)],
        }
        yield pa.RecordBatch.from_pydict(result)

    @staticmethod
    def apply_in_arrow_with_key_func(key_column):
        def func(key, left, right):
            assert isinstance(key, tuple)
            assert all(isinstance(scalar, pa.Scalar) for scalar in key)
            if key_column:
                assert all(
                    (pc.divide(k, pa.scalar(4)).cast(pa.int32()),) == key
                    for table in [left, right]
                    for k in table.column(key_column)
                )
            return CogroupedMapInArrowTestsMixin.apply_in_arrow_func(left, right)

        return func

    @staticmethod
    def apply_in_pandas_with_key_func(key_column):
        def func(key, left, right):
            return CogroupedMapInArrowTestsMixin.apply_in_arrow_with_key_func(key_column)(
                tuple(pa.scalar(k) for k in key),
                pa.Table.from_pandas(left),
                pa.Table.from_pandas(right),
            ).to_pandas()

        return func

    def do_test_apply_in_arrow(self, cogrouped_df, key_column="id"):
        schema = "metric string, left long, right long"

        # compare with result of applyInPandas
        expected = cogrouped_df.applyInPandas(
            CogroupedMapInArrowTestsMixin.apply_in_pandas_with_key_func(key_column), schema
        )

        # apply in arrow without key
        actual = cogrouped_df.applyInArrow(
            CogroupedMapInArrowTestsMixin.apply_in_arrow_func, schema
        ).collect()
        self.assertEqual(actual, expected.collect())

        # apply in arrow with key
        actual2 = cogrouped_df.applyInArrow(
            CogroupedMapInArrowTestsMixin.apply_in_arrow_with_key_func(key_column), schema
        ).collect()
        self.assertEqual(actual2, expected.collect())

        # apply in arrow returning iterator of batches
        actual3 = cogrouped_df.applyInArrow(
            CogroupedMapInArrowTestsMixin.apply_in_arrow_iterator_func, schema
        ).collect()
        self.assertEqual(actual3, expected.collect())

    def test_apply_in_arrow(self):
        self.do_test_apply_in_arrow(self.cogrouped)

    def test_apply_in_arrow_empty_groupby(self):
        grouped_left_df = self.left.groupBy()
        grouped_right_df = self.right.groupBy()
        cogrouped_df = grouped_left_df.cogroup(grouped_right_df)
        self.do_test_apply_in_arrow(cogrouped_df, key_column=None)

    def test_apply_in_arrow_not_returning_arrow_table(self):
        def func(key, left, right):
            return key
        
        def iter_func(key, left, right):
            yield key

        with self.quiet():
            with self.assertRaisesRegex(
                PythonException,
                "Return type of the user-defined function should be pyarrow.Table, but is tuple",
            ):
                self.cogrouped.applyInArrow(func, schema="id long").collect()

            with self.assertRaisesRegex(
                PythonException,
                "Return type of the user-defined function should be pyarrow.RecordBatch, but is tuple",
            ):
                self.cogrouped.applyInArrow(iter_func, schema="id long").collect()

    def test_apply_in_arrow_returning_wrong_types(self):
        for schema, expected in [
            ("id integer, v long", "column 'id' \\(expected int32, actual int64\\)"),
            (
                "id integer, v integer",
                "column 'id' \\(expected int32, actual int64\\), "
                "column 'v' \\(expected int32, actual int64\\)",
            ),
            ("id long, v integer", "column 'v' \\(expected int32, actual int64\\)"),
            ("id long, v string", "column 'v' \\(expected string, actual int64\\)"),
        ]:
            with self.subTest(schema=schema):
                with self.quiet():
                    for func_variation in function_variations(lambda left, right: left):
                        with self.assertRaisesRegex(
                            PythonException,
                            f"Columns do not match in their data type: {expected}",
                        ):
                            self.cogrouped.applyInArrow(
                                func_variation, schema=schema
                            ).collect()

    def test_apply_in_arrow_returning_wrong_types_positional_assignment(self):
        for schema, expected in [
            ("a integer, b long", "column 'a' \\(expected int32, actual int64\\)"),
            (
                "a integer, b integer",
                "column 'a' \\(expected int32, actual int64\\), "
                "column 'b' \\(expected int32, actual int64\\)",
            ),
            ("a long, b int", "column 'b' \\(expected int32, actual int64\\)"),
            ("a long, b string", "column 'b' \\(expected string, actual int64\\)"),
        ]:
            with self.subTest(schema=schema):
                with self.sql_conf(
                    {"spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName": False}
                ):
                    with self.quiet():
                        with self.assertRaisesRegex(
                            PythonException,
                            f"Columns do not match in their data type: {expected}",
                        ):
                            self.cogrouped.applyInArrow(
                                lambda left, right: left, schema=schema
                            ).collect()

    def test_apply_in_arrow_returning_wrong_column_names(self):
        def stats(key, left, right):
            # returning three columns
            return pa.Table.from_pydict(
                {
                    "id": [key[0].as_py()],
                    "v": [pc.mean(left.column("v")).as_py()],
                    "v2": [pc.stddev(right.column("v")).as_py()],
                }
            )

        with self.quiet():
            with self.assertRaisesRegex(
                PythonException,
                "Column names of the returned pyarrow.Table do not match specified schema. "
                "Missing: m. Unexpected: v, v2.\n",
            ):
                # stats returns three columns while here we set schema with two columns
                self.cogrouped.applyInArrow(stats, schema="id long, m double").collect()

    def test_apply_in_arrow_returning_empty_dataframe(self):
        def odd_means(key, left, right):
            if key[0].as_py() == 0:
                return pa.table([])
            else:
                return pa.Table.from_pydict(
                    {
                        "id": [key[0].as_py()],
                        "m": [pc.mean(left.column("v")).as_py()],
                        "n": [pc.mean(right.column("v")).as_py()],
                    }
                )

        schema = "id long, m double, n double"
        actual = self.cogrouped.applyInArrow(odd_means, schema=schema).sort("id").collect()
        expected = [Row(id=1, m=50.0, n=60.0), Row(id=2, m=80.0, n=90.0)]
        self.assertEqual(expected, actual)

    def test_apply_in_arrow_returning_empty_dataframe_and_wrong_column_names(self):
        def odd_means(key, left, _):
            if key[0].as_py() % 2 == 0:
                return pa.table([[]], names=["id"])
            else:
                return pa.Table.from_pydict(
                    {"id": [key[0].as_py()], "m": [pc.mean(left.column("v")).as_py()]}
                )

        with self.quiet():
            with self.assertRaisesRegex(
                PythonException,
                "Column names of the returned pyarrow.Table do not match specified schema. "
                "Missing: m.\n",
            ):
                # stats returns one column for even keys while here we set schema with two columns
                self.cogrouped.applyInArrow(odd_means, schema="id long, m double").collect()

    def test_apply_in_arrow_column_order(self):
        df = self.left
        expected = df.select(df.id, (df.v * 3).alias("u"), df.v).collect()

        # Function returns a table with required column names but different order
        def change_col_order(left, _):
            return left.append_column("u", pc.multiply(left.column("v"), 3))

        # The result should assign columns by name from the table
        result = (
            self.cogrouped.applyInArrow(change_col_order, "id long, u long, v long")
            .sort("id", "v")
            .select("id", "u", "v")
            .collect()
        )
        self.assertEqual(expected, result)

    def test_positional_assignment_conf(self):
        with self.sql_conf(
            {"spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName": False}
        ):

            def foo(left, right):
                return pa.Table.from_pydict({"x": ["hi"], "y": [1]})

            result = self.cogrouped.applyInArrow(foo, "a string, b long").select("a", "b").collect()
            for r in result:
                self.assertEqual(r.a, "hi")
                self.assertEqual(r.b, 1)

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

class CogroupedMapInArrowTests(CogroupedMapInArrowTestsMixin, ReusedSQLTestCase):
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
    from pyspark.sql.tests.test_arrow_cogrouped_map import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
