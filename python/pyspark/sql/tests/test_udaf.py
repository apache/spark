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

import datetime
import unittest
from collections import Counter
from decimal import Decimal as D

from pyspark.sql import Row
from pyspark.sql.functions import udaf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.udaf import Aggregator, UserDefinedAggregateFunction
from pyspark.errors import PySparkTypeError, PySparkNotImplementedError
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import assertDataFrameEqual


class UDAFTestsMixin:
    # ============ Core Functionality Tests ============

    def test_udaf_basic_aggregations(self):
        """Test basic aggregation types: sum, average, max."""

        class SumAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + (value or 0)

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        class AvgAgg(Aggregator):
            @staticmethod
            def zero():
                return (0.0, 0)

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return (buffer[0] + value, buffer[1] + 1)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return (buffer1[0] + buffer2[0], buffer1[1] + buffer2[1])

            @staticmethod
            def finish(reduction):
                return reduction[0] / reduction[1] if reduction[1] else None

        class MaxAgg(Aggregator):
            @staticmethod
            def zero():
                return None

            @staticmethod
            def reduce(buffer, value):
                if value is None:
                    return buffer
                return max(buffer, value) if buffer is not None else value

            @staticmethod
            def merge(buffer1, buffer2):
                if buffer1 is None:
                    return buffer2
                if buffer2 is None:
                    return buffer1
                return max(buffer1, buffer2)

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame([(1.0,), (2.0,), (3.0,), (4.0,), (5.0,)], ["value"])

        # Sum
        sum_udaf = udaf(SumAgg(), "double", "MySum")
        result = df.agg(sum_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MySum(value)": 15.0})])

        # Average (uses tuple buffer)
        avg_udaf = udaf(AvgAgg(), "double", "MyAvg")
        result = df.agg(avg_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MyAvg(value)": 3.0})])

        # Max
        max_udaf = udaf(MaxAgg(), "double", "MyMax")
        result = df.agg(max_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MyMax(value)": 5.0})])

    def test_udaf_with_groupby(self):
        """Test UDAF with groupBy - multiple groups, column order independent."""

        class SumAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + (value or 0)

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        # Test with key column BEFORE value column (previously failed)
        df = self.spark.createDataFrame(
            [("a", 1), ("a", 2), ("a", 3), ("b", 10), ("b", 20), ("c", 100)],
            ["key", "value"],
        )
        sum_udaf = udaf(SumAgg(), "bigint", "MySum")
        result = df.groupBy("key").agg(sum_udaf(df.value))
        expected = [
            Row(key="a", **{"MySum(value)": 6}),
            Row(key="b", **{"MySum(value)": 30}),
            Row(key="c", **{"MySum(value)": 100}),
        ]
        assertDataFrameEqual(result, expected, checkRowOrder=False)

    def test_udaf_edge_cases(self):
        """Test edge cases: nulls, empty dataframe."""

        class SumAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + (value or 0)

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        sum_udaf = udaf(SumAgg(), "bigint", "MySum")

        # Nulls
        df = self.spark.createDataFrame([(1,), (None,), (3,), (None,), (5,)], ["value"])
        result = df.agg(sum_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MySum(value)": 9})])

        # Empty DataFrame
        empty_df = self.spark.createDataFrame([], "value: int")
        result = empty_df.agg(sum_udaf(empty_df.value))
        assertDataFrameEqual(result, [Row(**{"MySum(value)": 0})])

    def test_udaf_large_dataset(self):
        """Test UDAF with large dataset to verify multi-partition aggregation."""

        class AvgAgg(Aggregator):
            @staticmethod
            def zero():
                return (0.0, 0)

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return (buffer[0] + value, buffer[1] + 1)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return (buffer1[0] + buffer2[0], buffer1[1] + buffer2[1])

            @staticmethod
            def finish(reduction):
                return reduction[0] / reduction[1] if reduction[1] else None

        data = [(float(i),) for i in range(1, 20001)]
        df = self.spark.createDataFrame(data, ["value"]).repartition(8)

        avg_udaf = udaf(AvgAgg(), "double", "MyAvg")
        result = df.agg(avg_udaf(df.value))
        expected_avg = 20001.0 / 2.0
        assertDataFrameEqual(result, [Row(**{"MyAvg(value)": expected_avg})])

    # ============ Input Type Tests (Consolidated) ============

    def test_udaf_various_input_types(self):
        """Test UDAF with various Spark input types."""

        class CountAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + (1 if value is not None else 0)

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        count_udaf = udaf(CountAgg(), "bigint", "Count")

        # String input
        df_str = self.spark.createDataFrame([("a",), ("b",), ("c",)], ["value"])
        result = df_str.agg(count_udaf(df_str.value))
        assertDataFrameEqual(result, [Row(**{"Count(value)": 3})])

        # Boolean input
        df_bool = self.spark.createDataFrame([(True,), (False,), (True,), (None,)], ["flag"])
        result = df_bool.agg(count_udaf(df_bool.flag))
        assertDataFrameEqual(result, [Row(**{"Count(flag)": 3})])

        # Date input
        df_date = self.spark.createDataFrame(
            [(datetime.date(2024, 1, 1),), (datetime.date(2024, 1, 10),), (None,)],
            ["dt"],
        )
        result = df_date.agg(count_udaf(df_date.dt))
        assertDataFrameEqual(result, [Row(**{"Count(dt)": 2})])

        # Timestamp input
        df_ts = self.spark.createDataFrame(
            [(datetime.datetime(2024, 1, 1, 10, 0),), (datetime.datetime(2024, 1, 1, 11, 0),)],
            ["ts"],
        )
        result = df_ts.agg(count_udaf(df_ts.ts))
        assertDataFrameEqual(result, [Row(**{"Count(ts)": 2})])

        # Decimal input
        schema = StructType([StructField("amount", DecimalType(10, 2), True)])
        df_dec = self.spark.createDataFrame([(D("10.50"),), (D("20.25"),)], schema)
        result = df_dec.agg(count_udaf(df_dec.amount))
        assertDataFrameEqual(result, [Row(**{"Count(amount)": 2})])

    def test_udaf_complex_types(self):
        """Test UDAF with complex types: array, map, struct, binary."""

        # Array input - sum all elements
        class ArraySumAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + sum(value) if value else buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        schema = StructType([StructField("nums", ArrayType(IntegerType()), True)])
        df_arr = self.spark.createDataFrame([([1, 2, 3],), ([4, 5],)], schema)
        arr_udaf = udaf(ArraySumAgg(), "bigint", "ArraySum")
        result = df_arr.agg(arr_udaf(df_arr.nums))
        assertDataFrameEqual(result, [Row(**{"ArraySum(nums)": 15})])

        # Map input - count entries
        class MapCountAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + len(value) if value else buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        schema = StructType([StructField("kv", MapType(StringType(), IntegerType()), True)])
        df_map = self.spark.createDataFrame([({"a": 1, "b": 2},), ({"c": 3},)], schema)
        map_udaf = udaf(MapCountAgg(), "bigint", "MapCount")
        result = df_map.agg(map_udaf(df_map.kv))
        assertDataFrameEqual(result, [Row(**{"MapCount(kv)": 3})])

        # Struct input - average age
        class StructAvgAgg(Aggregator):
            @staticmethod
            def zero():
                return (0, 0)

            @staticmethod
            def reduce(buffer, value):
                if value and value["age"]:
                    return (buffer[0] + value["age"], buffer[1] + 1)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return (buffer1[0] + buffer2[0], buffer1[1] + buffer2[1])

            @staticmethod
            def finish(reduction):
                return reduction[0] / reduction[1] if reduction[1] else None

        schema = StructType(
            [
                StructField(
                    "person",
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("age", IntegerType(), True),
                        ]
                    ),
                    True,
                )
            ]
        )
        df_struct = self.spark.createDataFrame(
            [({"name": "Alice", "age": 30},), ({"name": "Bob", "age": 40},)], schema
        )
        struct_udaf = udaf(StructAvgAgg(), "double", "AvgAge")
        result = df_struct.agg(struct_udaf(df_struct.person))
        assertDataFrameEqual(result, [Row(**{"AvgAge(person)": 35.0})])

        # Binary input - total length
        class BinaryLenAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + len(value) if value else buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        schema = StructType([StructField("data", BinaryType(), True)])
        df_bin = self.spark.createDataFrame([(bytearray(b"abc"),), (bytearray(b"de"),)], schema)
        bin_udaf = udaf(BinaryLenAgg(), "bigint", "BinLen")
        result = df_bin.agg(bin_udaf(df_bin.data))
        assertDataFrameEqual(result, [Row(**{"BinLen(data)": 5})])

    # ============ Complex Aggregation Tests ============

    def test_udaf_statistical_and_special(self):
        """Test statistical aggregations and special return types."""

        # Standard deviation
        class StdDevAgg(Aggregator):
            @staticmethod
            def zero():
                return (0, 0.0, 0.0)

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return (buffer[0] + 1, buffer[1] + value, buffer[2] + value * value)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return (
                    buffer1[0] + buffer2[0],
                    buffer1[1] + buffer2[1],
                    buffer1[2] + buffer2[2],
                )

            @staticmethod
            def finish(reduction):
                count, total, sq_total = reduction
                if count < 2:
                    return None
                mean = total / count
                variance = (sq_total / count) - (mean * mean)
                return variance**0.5

        df = self.spark.createDataFrame(
            [(2.0,), (4.0,), (4.0,), (4.0,), (5.0,), (5.0,), (7.0,), (9.0,)], ["value"]
        )
        stddev_udaf = udaf(StdDevAgg(), "double", "StdDev")
        result = df.agg(stddev_udaf(df.value))
        collected = result.collect()[0][0]
        self.assertAlmostEqual(collected, 2.0, places=5)

        # Mode (uses Counter buffer)
        class ModeAgg(Aggregator):
            @staticmethod
            def zero():
                return Counter()

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    buffer = Counter(buffer)
                    buffer[value] += 1
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return Counter(buffer1) + Counter(buffer2)

            @staticmethod
            def finish(reduction):
                return reduction.most_common(1)[0][0] if reduction else None

        df_int = self.spark.createDataFrame([(1,), (2,), (2,), (3,), (3,), (3,)], ["value"])
        mode_udaf = udaf(ModeAgg(), "bigint", "Mode")
        result = df_int.agg(mode_udaf(df_int.value))
        assertDataFrameEqual(result, [Row(**{"Mode(value)": 3})])

        # Return Array type (Top-3)
        class Top3Agg(Aggregator):
            @staticmethod
            def zero():
                return []

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    buffer = list(buffer)
                    buffer.append(value)
                    buffer.sort(reverse=True)
                    return buffer[:3]
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                combined = list(buffer1) + list(buffer2)
                combined.sort(reverse=True)
                return combined[:3]

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame([(5,), (1,), (9,), (3,), (7,), (2,)], ["value"])
        top3_udaf = udaf(Top3Agg(), ArrayType(LongType()), "Top3")
        result = df.agg(top3_udaf(df.value))
        collected = result.collect()[0][0]
        self.assertEqual(sorted(collected, reverse=True), [9, 7, 5])

    # ============ Validation Tests ============

    def test_udaf_creation_and_interface(self):
        """Test UDAF creation, interface validation, and column attributes."""
        # Interface
        self.assertTrue(hasattr(Aggregator, "zero"))
        self.assertTrue(hasattr(Aggregator, "reduce"))
        self.assertTrue(hasattr(Aggregator, "merge"))
        self.assertTrue(hasattr(Aggregator, "finish"))

        # Creation
        class SumAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + (value or 0)

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        sum_udaf = udaf(SumAgg(), "bigint", "MySum")
        self.assertIsInstance(sum_udaf, UserDefinedAggregateFunction)
        self.assertEqual(sum_udaf._name, "MySum")

        # Column attributes
        df = self.spark.createDataFrame([(1,)], ["value"])
        col = sum_udaf(df.value)
        self.assertTrue(hasattr(col, "_udaf_func"))
        self.assertTrue(hasattr(col, "_udaf_col"))

    def test_udaf_invalid_inputs(self):
        """Test error handling for invalid inputs."""

        # Invalid aggregator (missing required methods)
        class MissingMethods:
            @staticmethod
            def zero():
                return 0

            # Missing reduce, merge, finish

        with self.assertRaises(PySparkTypeError):
            udaf(MissingMethods(), "bigint")

        # Non-static method raises error
        class NonStaticZero(Aggregator):
            def zero(self):  # Missing @staticmethod
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1

            @staticmethod
            def finish(reduction):
                return reduction

        with self.assertRaises(PySparkTypeError) as ctx:
            udaf(NonStaticZero(), "bigint")
        self.assertIn("NOT_CALLABLE", str(ctx.exception))

    def test_udaf_unsupported_operations(self):
        """Test unsupported operations raise appropriate errors."""

        class SumAgg(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + (value or 0)

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        sum_udaf = udaf(SumAgg(), "bigint", "MySum")

        # Multiple UDAFs not supported
        with self.assertRaises(PySparkNotImplementedError):
            df.agg(sum_udaf(df.a), sum_udaf(df.b))

        # Mixed UDAF with other agg not supported
        from pyspark.sql.functions import min as spark_min

        with self.assertRaises(PySparkNotImplementedError):
            df.agg(sum_udaf(df.a), spark_min(df.b))


class UDAFTests(UDAFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.testing.utils import have_pandas, have_pyarrow

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
