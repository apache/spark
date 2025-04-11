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
import shutil
import tempfile
import time
import unittest
from typing import cast

from pyspark.sql import Row
from pyspark.sql.functions import col, encode, lit
from pyspark.errors import PythonException
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import eventually

if have_pandas:
    import pandas as pd


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class MapInPandasTestsMixin:
    spark: SparkSession

    @staticmethod
    def identity_dataframes_iter(*columns: str):
        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert pdf.columns.tolist() == list(columns)
                yield pdf

        return func

    @staticmethod
    def identity_dataframes_wo_column_names_iter(*columns: str):
        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert pdf.columns.tolist() == list(columns)
                yield pdf.rename(columns=list(pdf.columns).index)

        return func

    @staticmethod
    def dataframes_and_empty_dataframe_iter(*columns: str):
        def func(iterator):
            for pdf in iterator:
                yield pdf
            # after yielding all elements, also yield an empty dataframe with given columns
            yield pd.DataFrame([], columns=list(columns))

        return func

    def test_map_in_pandas(self):
        # test returning iterator of DataFrames
        df = self.spark.range(10, numPartitions=3)
        actual = df.mapInPandas(self.identity_dataframes_iter("id"), "id long").collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

        # test returning list of DataFrames
        df = self.spark.range(10, numPartitions=3)
        actual = df.mapInPandas(lambda it: [pdf for pdf in it], "id long").collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_multiple_columns(self):
        data = [(1, "foo"), (2, None), (3, "bar"), (4, "bar")]
        df = self.spark.createDataFrame(data, "a int, b string")

        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert [d.name for d in list(pdf.dtypes)] == ["int32", "object"]
                yield pdf

        actual = df.mapInPandas(func, df.schema).collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_large_variable_types(self):
        with self.sql_conf({"spark.sql.execution.arrow.useLargeVarTypes": True}):

            def func(iterator):
                for pdf in iterator:
                    assert isinstance(pdf, pd.DataFrame)
                    yield pdf

            df = (
                self.spark.range(10, numPartitions=3)
                .select(col("id").cast("string").alias("str"))
                .withColumn("bin", encode(col("str"), "utf-8"))
            )
            actual = df.mapInPandas(func, "str string, bin binary").collect()
            expected = df.collect()
            self.assertEqual(actual, expected)

    def test_no_column_names(self):
        data = [(1, "foo"), (2, None), (3, "bar"), (4, "bar")]
        df = self.spark.createDataFrame(data, "a int, b string")

        def func(iterator):
            for pdf in iterator:
                yield pdf.rename(columns=list(pdf.columns).index)

        actual = df.mapInPandas(func, df.schema).collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_not_null(self):
        def func(iterator):
            for _ in iterator:
                yield pd.DataFrame({"a": [1, 2]})

        schema = "a long not null"
        df = self.spark.range(1).mapInPandas(func, schema)
        self.assertEqual(df.schema, StructType.fromDDL(schema))
        self.assertEqual(df.collect(), [Row(1), Row(2)])

    def test_violate_not_null(self):
        def func(iterator):
            for _ in iterator:
                yield pd.DataFrame({"a": [1, None]})

        schema = "a long not null"
        df = self.spark.range(1).mapInPandas(func, schema)
        self.assertEqual(df.schema, StructType.fromDDL(schema))
        with self.assertRaisesRegex(Exception, "is null"):
            df.collect()

    def test_different_output_length(self):
        def func(iterator):
            for _ in iterator:
                yield pd.DataFrame({"a": list(range(100))})

        df = self.spark.range(10)
        actual = df.repartition(1).mapInPandas(func, "a long").collect()
        self.assertEqual(set((r.a for r in actual)), set(range(100)))

    def test_other_than_dataframe_iter(self):
        with self.quiet():
            self.check_other_than_dataframe_iter()

    def check_other_than_dataframe_iter(self):
        def no_iter(_):
            return 1

        def bad_iter_elem(_):
            return iter([1])

        with self.assertRaisesRegex(
            PythonException,
            "Return type of the user-defined function should be iterator of pandas.DataFrame, "
            "but is int",
        ):
            (self.spark.range(10, numPartitions=3).mapInPandas(no_iter, "a int").count())

        with self.assertRaisesRegex(
            PythonException,
            "Return type of the user-defined function should be iterator of pandas.DataFrame, "
            "but is iterator of int",
        ):
            (self.spark.range(10, numPartitions=3).mapInPandas(bad_iter_elem, "a int").count())

    def test_dataframes_with_other_column_names(self):
        with self.quiet():
            self.check_dataframes_with_other_column_names()

    def check_dataframes_with_other_column_names(self):
        def dataframes_with_other_column_names(iterator):
            for pdf in iterator:
                yield pdf.rename(columns={"id": "iid"})

        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF\\] "
            "Column names of the returned pandas.DataFrame do not match "
            "specified schema. Missing: id. Unexpected: iid.\n",
        ):
            (
                self.spark.range(10, numPartitions=3)
                .withColumn("value", lit(0))
                .mapInPandas(dataframes_with_other_column_names, "id int, value int")
                .collect()
            )

    def test_dataframes_with_duplicate_column_names(self):
        with self.quiet():
            self.check_dataframes_with_duplicate_column_names()

    def check_dataframes_with_duplicate_column_names(self):
        def dataframes_with_other_column_names(iterator):
            for pdf in iterator:
                yield pdf.rename(columns={"id2": "id"})

        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF\\] "
            "Column names of the returned pandas.DataFrame do not match "
            "specified schema. Missing: id2.\n",
        ):
            (
                self.spark.range(10, numPartitions=3)
                .withColumn("id2", lit(0))
                .withColumn("value", lit(1))
                .mapInPandas(dataframes_with_other_column_names, "id int, id2 long, value int")
                .collect()
            )

    def test_dataframes_with_less_columns(self):
        with self.quiet():
            self.check_dataframes_with_less_columns()

    def check_dataframes_with_less_columns(self):
        df = self.spark.range(10, numPartitions=3).withColumn("value", lit(0))

        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF\\] "
            "Column names of the returned pandas.DataFrame do not match "
            "specified schema. Missing: id2.\n",
        ):
            f = self.identity_dataframes_iter("id", "value")
            (df.mapInPandas(f, "id int, id2 long, value int").collect())

        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[RESULT_LENGTH_MISMATCH_FOR_PANDAS_UDF\\] "
            "Number of columns of the returned pandas.DataFrame doesn't match "
            "specified schema. Expected: 3 Actual: 2\n",
        ):
            f = self.identity_dataframes_wo_column_names_iter("id", "value")
            (df.mapInPandas(f, "id int, id2 long, value int").collect())

    def test_dataframes_with_more_columns(self):
        df = self.spark.range(10, numPartitions=3).select(
            "id", col("id").alias("value"), col("id").alias("extra")
        )
        expected = df.select("id", "value").collect()

        f = self.identity_dataframes_iter("id", "value", "extra")
        actual = df.repartition(1).mapInPandas(f, "id long, value long").collect()
        self.assertEqual(actual, expected)

        f = self.identity_dataframes_wo_column_names_iter("id", "value", "extra")
        actual = df.repartition(1).mapInPandas(f, "id long, value long").collect()
        self.assertEqual(actual, expected)

    def test_dataframes_with_incompatible_types(self):
        with self.quiet():
            self.check_dataframes_with_incompatible_types()

    def check_dataframes_with_incompatible_types(self):
        def func(iterator):
            for pdf in iterator:
                yield pdf.assign(id=pdf["id"].apply(str))

        for safely in [True, False]:
            with self.subTest(convertToArrowArraySafely=safely), self.sql_conf(
                {"spark.sql.execution.pandas.convertToArrowArraySafely": safely}
            ):
                # sometimes we see ValueErrors
                with self.subTest(convert="string to double"):
                    expected = (
                        r"ValueError: Exception thrown when converting pandas.Series "
                        r"\(object\) with name 'id' to Arrow Array \(double\)."
                    )
                    if safely:
                        expected = expected + (
                            " It can be caused by overflows or other "
                            "unsafe conversions warned by Arrow. Arrow safe type check "
                            "can be disabled by using SQL config "
                            "`spark.sql.execution.pandas.convertToArrowArraySafely`."
                        )
                    with self.assertRaisesRegex(PythonException, expected + "\n"):
                        (
                            self.spark.range(10, numPartitions=3)
                            .mapInPandas(func, "id double")
                            .collect()
                        )

                # sometimes we see TypeErrors
                with self.subTest(convert="double to string"):
                    with self.assertRaisesRegex(
                        PythonException,
                        r"TypeError: Exception thrown when converting pandas.Series "
                        r"\(float64\) with name 'id' to Arrow Array \(string\).\n",
                    ):
                        (
                            self.spark.range(10, numPartitions=3)
                            .select(col("id").cast("double"))
                            .mapInPandas(self.identity_dataframes_iter("id"), "id string")
                            .collect()
                        )

    def test_empty_iterator(self):
        def empty_iter(_):
            return iter([])

        mapped = self.spark.range(10, numPartitions=3).mapInPandas(empty_iter, "a int, b string")
        self.assertEqual(mapped.count(), 0)

    def test_empty_dataframes(self):
        def empty_dataframes(_):
            return iter([pd.DataFrame({"a": []})])

        mapped = self.spark.range(10, numPartitions=3).mapInPandas(empty_dataframes, "a int")
        self.assertEqual(mapped.count(), 0)

    def test_empty_dataframes_without_columns(self):
        mapped = self.spark.range(10, numPartitions=3).mapInPandas(
            self.dataframes_and_empty_dataframe_iter(), "id int"
        )
        self.assertEqual(mapped.count(), 10)

    def test_empty_dataframes_with_less_columns(self):
        with self.quiet():
            self.check_empty_dataframes_with_less_columns()

    def check_empty_dataframes_with_less_columns(self):
        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF\\] "
            "Column names of the returned pandas.DataFrame do not match "
            "specified schema. Missing: value.\n",
        ):
            f = self.dataframes_and_empty_dataframe_iter("id")
            (
                self.spark.range(10, numPartitions=3)
                .withColumn("value", lit(0))
                .mapInPandas(f, "id int, value int")
                .collect()
            )

    def test_empty_dataframes_with_more_columns(self):
        mapped = self.spark.range(10, numPartitions=3).mapInPandas(
            self.dataframes_and_empty_dataframe_iter("id", "extra"), "id int"
        )
        self.assertEqual(mapped.count(), 10)

    def test_empty_dataframes_with_other_columns(self):
        with self.quiet():
            self.check_empty_dataframes_with_other_columns()

    def check_empty_dataframes_with_other_columns(self):
        def empty_dataframes_with_other_columns(iterator):
            for _ in iterator:
                yield pd.DataFrame({"iid": [], "value": []})

        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[RESULT_COLUMNS_MISMATCH_FOR_PANDAS_UDF\\] "
            "Column names of the returned pandas.DataFrame do not match "
            "specified schema. Missing: id. Unexpected: iid.\n",
        ):
            (
                self.spark.range(10, numPartitions=3)
                .withColumn("value", lit(0))
                .mapInPandas(empty_dataframes_with_other_columns, "id int, value int")
                .collect()
            )

    def test_chain_map_partitions_in_pandas(self):
        def func(iterator):
            for pdf in iterator:
                assert isinstance(pdf, pd.DataFrame)
                assert pdf.columns == ["id"]
                yield pdf

        df = self.spark.range(10, numPartitions=3)
        actual = df.mapInPandas(func, "id long").mapInPandas(func, "id long").collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_self_join(self):
        # SPARK-34319: self-join with MapInPandas
        df1 = self.spark.range(10, numPartitions=3)
        df2 = df1.mapInPandas(lambda iter: iter, "id long")
        actual = df2.join(df2).collect()
        expected = df1.join(df1).collect()
        self.assertEqual(sorted(actual), sorted(expected))

    # SPARK-33277
    @eventually(timeout=180, catch_assertions=True)
    def test_map_in_pandas_with_column_vector(self):
        path = tempfile.mkdtemp()
        shutil.rmtree(path)

        try:
            self.spark.range(0, 200000, 1, 1).write.parquet(path)

            def func(iterator):
                for pdf in iterator:
                    yield pd.DataFrame({"id": [0] * len(pdf)})

            for offheap in ["true", "false"]:
                with self.sql_conf({"spark.sql.columnVector.offheap.enabled": offheap}):
                    self.assertEqual(
                        self.spark.read.parquet(path).mapInPandas(func, "id long").head(), Row(0)
                    )
        finally:
            shutil.rmtree(path)

    def test_map_in_pandas_with_barrier_mode(self):
        df = self.spark.range(10)

        def func1(iterator):
            from pyspark import TaskContext, BarrierTaskContext

            tc = TaskContext.get()
            assert tc is not None
            assert not isinstance(tc, BarrierTaskContext)
            for batch in iterator:
                yield batch

        df.mapInPandas(func1, "id long", False).collect()

        def func2(iterator):
            from pyspark import TaskContext, BarrierTaskContext

            tc = TaskContext.get()
            assert tc is not None
            assert isinstance(tc, BarrierTaskContext)
            for batch in iterator:
                yield batch

        df.mapInPandas(func2, "id long", True).collect()


class MapInPandasTests(ReusedSQLTestCase, MapInPandasTestsMixin):
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
    from pyspark.sql.tests.pandas.test_pandas_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
