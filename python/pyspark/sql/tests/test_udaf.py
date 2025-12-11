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

from pyspark.sql import Row
from pyspark.sql.functions import udaf
from pyspark.sql.udaf import Aggregator, UserDefinedAggregateFunction
from pyspark.errors import PySparkTypeError, PySparkNotImplementedError
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import assertDataFrameEqual


class UDAFTestsMixin:
    def test_udaf_basic_sum(self):
        """Test basic sum UDAF with df.agg()."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["value"])
        sum_udaf = udaf(MySum(), "bigint", "MySum")

        result = df.agg(sum_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MySum(value)": 15})])

    def test_udaf_with_groupby(self):
        """Test UDAF with groupBy().agg()."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame(
            [(1, "a"), (2, "a"), (3, "b"), (4, "b"), (5, "b")], ["value", "group"]
        )
        sum_udaf = udaf(MySum(), "bigint", "MySum")

        # Test using groupBy().agg()
        result = df.groupBy("group").agg(sum_udaf(df.value))
        assertDataFrameEqual(
            result.sort("group"),
            [
                Row(group="a", **{"MySum(value)": 3}),
                Row(group="b", **{"MySum(value)": 12}),
            ],
        )

    def test_udaf_average(self):
        """Test average UDAF with df.agg()."""

        class MyAverage(Aggregator):
            @staticmethod
            def zero():
                return (0.0, 0)  # (sum, count)

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
                if reduction[1] == 0:
                    return None
                return reduction[0] / reduction[1]

        df = self.spark.createDataFrame([(10,), (20,), (30,)], ["value"])
        avg_udaf = udaf(MyAverage(), "double", "MyAverage")

        result = df.agg(avg_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MyAverage(value)": 20.0})], checkRowOrder=False)

    def test_udaf_max(self):
        """Test max UDAF with df.agg()."""

        class MyMax(Aggregator):
            @staticmethod
            def zero():
                return None

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    if buffer is None:
                        return value
                    return max(buffer, value)
                return buffer

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

        df = self.spark.createDataFrame([(5,), (10,), (3,), (8,)], ["value"])
        max_udaf = udaf(MyMax(), "bigint", "MyMax")

        result = df.agg(max_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MyMax(value)": 10})])

    def test_udaf_with_nulls(self):
        """Test UDAF handling null values with df.agg()."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame([(1,), (None,), (3,), (None,), (5,)], ["value"])
        sum_udaf = udaf(MySum(), "bigint", "MySum")

        result = df.agg(sum_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MySum(value)": 9})])  # 1+3+5 = 9 (nulls ignored)

    def test_udaf_empty_dataframe(self):
        """Test UDAF with empty DataFrame using df.agg()."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        from pyspark.sql.types import StructType, StructField, LongType

        schema = StructType([StructField("value", LongType(), True)])
        df = self.spark.createDataFrame([], schema)
        sum_udaf = udaf(MySum(), "bigint", "MySum")

        result = df.agg(sum_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MySum(value)": 0})])

    def test_udaf_aggregator_interface(self):
        """Test that Aggregator interface is properly defined."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        agg = MySum()

        # Test zero
        zero_val = agg.zero()
        self.assertEqual(zero_val, 0)

        # Test reduce
        buffer = agg.reduce(agg.zero(), 5)
        self.assertEqual(buffer, 5)
        buffer = agg.reduce(buffer, 10)
        self.assertEqual(buffer, 15)

        # Test merge
        merged = agg.merge(5, 10)
        self.assertEqual(merged, 15)

        # Test finish
        result = agg.finish(merged)
        self.assertEqual(result, 15)

    def test_udaf_creation(self):
        """Test UDAF creation with udaf() function."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        sum_udaf = udaf(MySum(), "bigint")
        self.assertIsInstance(sum_udaf, UserDefinedAggregateFunction)
        self.assertEqual(sum_udaf._name, "MySum")

        # Test with custom name
        custom_udaf = udaf(MySum(), "bigint", "CustomSum")
        self.assertEqual(custom_udaf._name, "CustomSum")

    def test_udaf_invalid_aggregator(self):
        """Test that invalid aggregator raises error."""
        with self.assertRaises(PySparkTypeError):
            udaf("not an aggregator", "bigint")  # type: ignore

    def test_udaf_invalid_return_type(self):
        """Test that invalid return type raises error."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        with self.assertRaises(PySparkTypeError):
            udaf(MySum(), 123)  # type: ignore  # Not a DataType or string

    def test_udaf_non_static_method_raises_error(self):
        """Test that non-static methods in Aggregator raise error."""

        # Test with non-static zero method
        class BadAggregatorZero(Aggregator):
            def zero(self):  # Missing @staticmethod
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

        with self.assertRaises(PySparkTypeError) as cm:
            udaf(BadAggregatorZero(), "bigint")
        self.assertIn("zero", str(cm.exception))

        # Test with non-static reduce method
        class BadAggregatorReduce(Aggregator):
            @staticmethod
            def zero():
                return 0

            def reduce(self, buffer, value):  # Missing @staticmethod
                return buffer + (value or 0)

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        with self.assertRaises(PySparkTypeError) as cm:
            udaf(BadAggregatorReduce(), "bigint")
        self.assertIn("reduce", str(cm.exception))

        # Test with non-static merge method
        class BadAggregatorMerge(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + (value or 0)

            def merge(self, buffer1, buffer2):  # Missing @staticmethod
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        with self.assertRaises(PySparkTypeError) as cm:
            udaf(BadAggregatorMerge(), "bigint")
        self.assertIn("merge", str(cm.exception))

        # Test with non-static finish method
        class BadAggregatorFinish(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                return buffer + (value or 0)

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            def finish(self, reduction):  # Missing @staticmethod
                return reduction

        with self.assertRaises(PySparkTypeError) as cm:
            udaf(BadAggregatorFinish(), "bigint")
        self.assertIn("finish", str(cm.exception))

    def test_udaf_multi_column_not_supported(self):
        """Test that multi-column UDAF is not yet supported."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        sum_udaf = udaf(MySum(), "bigint")

        with self.assertRaises(PySparkNotImplementedError):
            df.agg(sum_udaf(df.a), sum_udaf(df.b))

    def test_udaf_large_dataset(self):
        """Test UDAF with large dataset (20000 rows) distributed across partitions."""

        class MySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        # Create a large dataset with 20000 rows
        # Use repartition to ensure data is distributed across multiple partitions
        data = [(i,) for i in range(1, 20001)]  # 1 to 20000
        df = self.spark.createDataFrame(data, ["value"])

        # Repartition to ensure data is distributed across partitions
        # This ensures each partition has data
        num_partitions = max(4, self.spark.sparkContext.defaultParallelism)
        df = df.repartition(num_partitions)

        # Verify data is distributed
        actual_partitions = df.rdd.getNumPartitions()
        self.assertGreater(
            actual_partitions, 1, "Data should be distributed across multiple partitions"
        )

        # Calculate expected sum: 1 + 2 + ... + 20000 = 20000 * 20001 / 2 = 200010000
        expected_sum = 20000 * 20001 // 2

        sum_udaf = udaf(MySum(), "bigint", "MySum")
        result = df.agg(sum_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"MySum(value)": expected_sum})])

    def test_udaf_large_dataset_average(self):
        """Test average UDAF with large dataset using df.agg()."""

        class MyAverage(Aggregator):
            @staticmethod
            def zero():
                return (0.0, 0)  # (sum, count)

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
                if reduction[1] == 0:
                    return None
                return reduction[0] / reduction[1]

        # Create a large dataset with 20000 rows
        data = [(float(i),) for i in range(1, 20001)]  # 1.0 to 20000.0
        df = self.spark.createDataFrame(data, ["value"])

        # Repartition to ensure data is distributed
        num_partitions = max(4, self.spark.sparkContext.defaultParallelism)
        df = df.repartition(num_partitions)

        # Expected average: (1 + 2 + ... + 20000) / 20000 = 20001 / 2 = 10000.5
        expected_avg = 20001.0 / 2.0

        avg_udaf = udaf(MyAverage(), "double", "MyAverage")
        result = df.agg(avg_udaf(df.value))
        assertDataFrameEqual(
            result, [Row(**{"MyAverage(value)": expected_avg})], checkRowOrder=False
        )

    def test_udaf_column_attributes(self):
        """Test that UDAF Column has correct attributes."""

        class MySum(Aggregator):
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

        df = self.spark.createDataFrame([(1,), (2,)], ["value"])
        sum_udaf = udaf(MySum(), "bigint", "MySum")

        # Test that __call__ returns a Column with UDAF attributes
        udaf_col = sum_udaf(df.value)
        self.assertTrue(hasattr(udaf_col, "_udaf_func"))
        self.assertTrue(hasattr(udaf_col, "_udaf_col"))
        self.assertEqual(udaf_col._udaf_func, sum_udaf)  # type: ignore[attr-defined]

    def test_udaf_multiple_udaf_not_supported(self):
        """Test that multiple UDAFs in agg() raises error."""

        class MySum(Aggregator):
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

        df = self.spark.createDataFrame([(1,), (2,)], ["value"])
        sum_udaf1 = udaf(MySum(), "bigint", "MySum1")
        sum_udaf2 = udaf(MySum(), "bigint", "MySum2")

        with self.assertRaises(PySparkNotImplementedError):
            df.agg(sum_udaf1(df.value), sum_udaf2(df.value))

    def test_udaf_mixed_with_other_agg_not_supported(self):
        """Test that mixing UDAF with other aggregate functions raises error."""

        class MySum(Aggregator):
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

        from pyspark.sql.functions import min as spark_min

        df = self.spark.createDataFrame([(1,), (2,), (3,)], ["value"])
        sum_udaf = udaf(MySum(), "bigint", "MySum")

        with self.assertRaises(PySparkNotImplementedError):
            df.agg(sum_udaf(df.value), spark_min(df.value))

    def test_udaf_with_dict_buffer(self):
        """Test UDAF using dictionary as buffer (complex buffer type)."""

        class WordCount(Aggregator):
            @staticmethod
            def zero():
                return {}

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    buffer = dict(buffer)
                    buffer[value] = buffer.get(value, 0) + 1
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                result = dict(buffer1)
                for k, v in buffer2.items():
                    result[k] = result.get(k, 0) + v
                return result

            @staticmethod
            def finish(reduction):
                return len(reduction)

        df = self.spark.createDataFrame(
            [("apple",), ("banana",), ("apple",), ("cherry",)], ["fruit"]
        )
        count_udaf = udaf(WordCount(), "bigint", "UniqueCount")
        result = df.agg(count_udaf(df.fruit))
        assertDataFrameEqual(result, [Row(**{"UniqueCount(fruit)": 3})])

    # ============ Input Type Tests ============

    def test_udaf_input_type_string(self):
        """Test UDAF with string input type."""

        class ConcatStrings(Aggregator):
            @staticmethod
            def zero():
                return []

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    buffer = list(buffer)
                    buffer.append(value)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return list(buffer1) + list(buffer2)

            @staticmethod
            def finish(reduction):
                return ",".join(sorted(reduction)) if reduction else ""

        df = self.spark.createDataFrame([("c",), ("a",), ("b",)], ["value"])
        concat_udaf = udaf(ConcatStrings(), "string", "ConcatStr")
        result = df.agg(concat_udaf(df.value))
        assertDataFrameEqual(result, [Row(**{"ConcatStr(value)": "a,b,c"})])

    def test_udaf_input_type_boolean(self):
        """Test UDAF with boolean input type."""

        class CountTrue(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is True:
                    return buffer + 1
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame(
            [(True,), (False,), (True,), (True,), (False,)], ["flag"]
        )
        count_udaf = udaf(CountTrue(), "bigint", "CountTrue")
        result = df.agg(count_udaf(df.flag))
        assertDataFrameEqual(result, [Row(**{"CountTrue(flag)": 3})])

    def test_udaf_input_type_date(self):
        """Test UDAF with date input type."""
        import datetime

        class DateRange(Aggregator):
            @staticmethod
            def zero():
                return (None, None)  # (min_date, max_date)

            @staticmethod
            def reduce(buffer, value):
                if value is None:
                    return buffer
                min_d, max_d = buffer
                if min_d is None or value < min_d:
                    min_d = value
                if max_d is None or value > max_d:
                    max_d = value
                return (min_d, max_d)

            @staticmethod
            def merge(buffer1, buffer2):
                min1, max1 = buffer1
                min2, max2 = buffer2
                min_d = min1 if min2 is None or (min1 and min1 < min2) else min2
                max_d = max1 if max2 is None or (max1 and max1 > max2) else max2
                return (min_d, max_d)

            @staticmethod
            def finish(reduction):
                min_d, max_d = reduction
                if min_d is None or max_d is None:
                    return None
                return (max_d - min_d).days

        df = self.spark.createDataFrame(
            [
                (datetime.date(2024, 1, 1),),
                (datetime.date(2024, 1, 10),),
                (datetime.date(2024, 1, 5),),
            ],
            ["dt"],
        )
        range_udaf = udaf(DateRange(), "bigint", "DateRange")
        result = df.agg(range_udaf(df.dt))
        assertDataFrameEqual(result, [Row(**{"DateRange(dt)": 9})])  # 10 - 1 = 9 days

    def test_udaf_input_type_timestamp(self):
        """Test UDAF with timestamp input type."""
        import datetime

        class TimestampCount(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + 1
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        df = self.spark.createDataFrame(
            [
                (datetime.datetime(2024, 1, 1, 10, 0, 0),),
                (datetime.datetime(2024, 1, 1, 11, 0, 0),),
                (datetime.datetime(2024, 1, 1, 12, 0, 0),),
            ],
            ["ts"],
        )
        count_udaf = udaf(TimestampCount(), "bigint", "TsCount")
        result = df.agg(count_udaf(df.ts))
        assertDataFrameEqual(result, [Row(**{"TsCount(ts)": 3})])

    def test_udaf_input_type_decimal(self):
        """Test UDAF with decimal input type."""
        from decimal import Decimal as D
        from pyspark.sql.types import DecimalType, StructField, StructType

        class DecimalSum(Aggregator):
            @staticmethod
            def zero():
                return D("0.00")

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + value
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return float(reduction)

        schema = StructType([StructField("amount", DecimalType(10, 2), True)])
        df = self.spark.createDataFrame(
            [(D("10.50"),), (D("20.25"),), (D("30.25"),)], schema
        )
        sum_udaf = udaf(DecimalSum(), "double", "DecSum")
        result = df.agg(sum_udaf(df.amount))
        collected = result.collect()[0][0]
        self.assertAlmostEqual(collected, 61.0, places=2)

    def test_udaf_input_type_binary(self):
        """Test UDAF with binary input type."""
        from pyspark.sql.types import BinaryType, StructField, StructType

        class BinaryLen(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + len(value)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        schema = StructType([StructField("data", BinaryType(), True)])
        df = self.spark.createDataFrame(
            [(bytearray(b"abc"),), (bytearray(b"de"),), (bytearray(b"f"),)], schema
        )
        len_udaf = udaf(BinaryLen(), "bigint", "TotalLen")
        result = df.agg(len_udaf(df.data))
        assertDataFrameEqual(result, [Row(**{"TotalLen(data)": 6})])  # 3 + 2 + 1

    def test_udaf_input_type_array(self):
        """Test UDAF with array input type."""
        from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType

        class ArraySum(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return buffer + sum(value)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        schema = StructType([StructField("nums", ArrayType(IntegerType()), True)])
        df = self.spark.createDataFrame(
            [([1, 2, 3],), ([4, 5],), ([6],)], schema
        )
        sum_udaf = udaf(ArraySum(), "bigint", "ArraySum")
        result = df.agg(sum_udaf(df.nums))
        assertDataFrameEqual(result, [Row(**{"ArraySum(nums)": 21})])  # 1+2+3+4+5+6

    def test_udaf_input_type_map(self):
        """Test UDAF with map input type."""
        from pyspark.sql.types import IntegerType, MapType, StringType, StructField, StructType

        class MapEntryCount(Aggregator):
            @staticmethod
            def zero():
                return 0

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    # Map comes as list of (key, value) tuples in Arrow
                    return buffer + len(value)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return buffer1 + buffer2

            @staticmethod
            def finish(reduction):
                return reduction

        schema = StructType(
            [StructField("kv", MapType(StringType(), IntegerType()), True)]
        )
        df = self.spark.createDataFrame(
            [({"a": 1, "b": 2},), ({"c": 3},), ({"d": 4, "e": 5},)], schema
        )
        count_udaf = udaf(MapEntryCount(), "bigint", "MapEntryCount")
        result = df.agg(count_udaf(df.kv))
        assertDataFrameEqual(result, [Row(**{"MapEntryCount(kv)": 5})])  # 2+1+2 entries

    def test_udaf_input_type_struct(self):
        """Test UDAF with struct input type."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        class StructAgg(Aggregator):
            @staticmethod
            def zero():
                return (0, 0)  # (total_age, count)

            @staticmethod
            def reduce(buffer, value):
                if value is not None:
                    return (buffer[0] + value["age"], buffer[1] + 1)
                return buffer

            @staticmethod
            def merge(buffer1, buffer2):
                return (buffer1[0] + buffer2[0], buffer1[1] + buffer2[1])

            @staticmethod
            def finish(reduction):
                if reduction[1] == 0:
                    return None
                return reduction[0] / reduction[1]

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
        df = self.spark.createDataFrame(
            [({"name": "Alice", "age": 30},), ({"name": "Bob", "age": 40},)], schema
        )
        avg_udaf = udaf(StructAgg(), "double", "AvgAge")
        result = df.agg(avg_udaf(df.person))
        assertDataFrameEqual(result, [Row(**{"AvgAge(person)": 35.0})])


class UDAFTests(UDAFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.testing.utils import have_pandas, have_pyarrow, pandas_requirement_message

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
