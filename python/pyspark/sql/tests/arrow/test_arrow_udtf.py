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
from typing import Iterator

from pyspark.errors import PySparkAttributeError
from pyspark.errors.exceptions.captured import PythonException
from pyspark.sql.functions import arrow_udtf, lit
from pyspark.sql.types import Row, StructType, StructField, IntegerType
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pyarrow, pyarrow_requirement_message
from pyspark.testing import assertDataFrameEqual

if have_pyarrow:
    import pyarrow as pa


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDTFTests(ReusedSQLTestCase):
    def test_arrow_udtf_zero_args(self):
        @arrow_udtf(returnType="id int, value string")
        class TestUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table(
                    {
                        "id": pa.array([1, 2, 3], type=pa.int32()),
                        "value": pa.array(["a", "b", "c"], type=pa.string()),
                    }
                )
                yield result_table

        # Test direct DataFrame API usage
        result_df = TestUDTF()
        expected_df = self.spark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c")], "id int, value string"
        )
        assertDataFrameEqual(result_df, expected_df)

        # Test SQL registration and usage
        self.spark.udtf.register("test_zero_args_udtf", TestUDTF)
        sql_result_df = self.spark.sql("SELECT * FROM test_zero_args_udtf()")
        assertDataFrameEqual(sql_result_df, expected_df)

    def test_arrow_udtf_scalar_args_only(self):
        @arrow_udtf(returnType="x int, y int, sum int")
        class ScalarArgsUDTF:
            def eval(self, x: "pa.Array", y: "pa.Array") -> Iterator["pa.Table"]:
                assert isinstance(x, pa.Array), f"Expected pa.Array, got {type(x)}"
                assert isinstance(y, pa.Array), f"Expected pa.Array, got {type(y)}"

                x_val = x[0].as_py()
                y_val = y[0].as_py()
                result_table = pa.table(
                    {
                        "x": pa.array([x_val], type=pa.int32()),
                        "y": pa.array([y_val], type=pa.int32()),
                        "sum": pa.array([x_val + y_val], type=pa.int32()),
                    }
                )
                yield result_table

        # Test direct DataFrame API usage
        result_df = ScalarArgsUDTF(lit(5), lit(10))
        expected_df = self.spark.createDataFrame([(5, 10, 15)], "x int, y int, sum int")
        assertDataFrameEqual(result_df, expected_df)

        # Test SQL registration and usage
        self.spark.udtf.register("ScalarArgsUDTF", ScalarArgsUDTF)
        sql_result_df = self.spark.sql("SELECT * FROM ScalarArgsUDTF(5, 10)")
        assertDataFrameEqual(sql_result_df, expected_df)

        # Test with different values via SQL
        sql_result_df2 = self.spark.sql("SELECT * FROM ScalarArgsUDTF(4, 7)")
        expected_df2 = self.spark.createDataFrame([(4, 7, 11)], "x int, y int, sum int")
        assertDataFrameEqual(sql_result_df2, expected_df2)

    def test_arrow_udtf_record_batch_iterator(self):
        @arrow_udtf(returnType="batch_id int, name string, count int")
        class RecordBatchUDTF:
            def eval(self, batch_size: "pa.Array") -> Iterator["pa.RecordBatch"]:
                assert isinstance(
                    batch_size, pa.Array
                ), f"Expected pa.Array, got {type(batch_size)}"

                size = batch_size[0].as_py()

                for batch_id in range(3):
                    batch = pa.record_batch(
                        {
                            "batch_id": pa.array([batch_id] * size, type=pa.int32()),
                            "name": pa.array([f"batch_{batch_id}"] * size, type=pa.string()),
                            "count": pa.array(list(range(size)), type=pa.int32()),
                        }
                    )
                    yield batch

        # Test direct DataFrame API usage
        result_df = RecordBatchUDTF(lit(2))
        expected_data = [
            (0, "batch_0", 0),
            (0, "batch_0", 1),
            (1, "batch_1", 0),
            (1, "batch_1", 1),
            (2, "batch_2", 0),
            (2, "batch_2", 1),
        ]
        expected_df = self.spark.createDataFrame(
            expected_data, "batch_id int, name string, count int"
        )
        assertDataFrameEqual(result_df, expected_df)

        # Test SQL registration and usage
        self.spark.udtf.register("record_batch_udtf", RecordBatchUDTF)
        sql_result_df = self.spark.sql(
            "SELECT * FROM record_batch_udtf(2) ORDER BY batch_id, count"
        )
        assertDataFrameEqual(sql_result_df, expected_df)

        # Test with different batch size via SQL
        sql_result_df2 = self.spark.sql("SELECT * FROM record_batch_udtf(1) ORDER BY batch_id")
        expected_data2 = [
            (0, "batch_0", 0),
            (1, "batch_1", 0),
            (2, "batch_2", 0),
        ]
        expected_df2 = self.spark.createDataFrame(
            expected_data2, "batch_id int, name string, count int"
        )
        assertDataFrameEqual(sql_result_df2, expected_df2)

    def test_arrow_udtf_error_not_iterator(self):
        @arrow_udtf(returnType="x int, y string")
        class NotIteratorUDTF:
            def eval(self) -> "pa.Table":
                return pa.table(
                    {"x": pa.array([1], type=pa.int32()), "y": pa.array(["test"], type=pa.string())}
                )

        with self.assertRaisesRegex(PythonException, "UDTF_RETURN_NOT_ITERABLE"):
            result_df = NotIteratorUDTF()
            result_df.collect()

    def test_arrow_udtf_error_wrong_yield_type(self):
        @arrow_udtf(returnType="x int, y string")
        class WrongYieldTypeUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                yield {"x": [1], "y": ["test"]}

        with self.assertRaisesRegex(PythonException, "UDTF_ARROW_TYPE_CONVERSION_ERROR"):
            result_df = WrongYieldTypeUDTF()
            result_df.collect()

    def test_arrow_udtf_error_invalid_arrow_type(self):
        @arrow_udtf(returnType="x int, y string")
        class InvalidArrowTypeUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                yield "not_an_arrow_table"

        with self.assertRaisesRegex(PythonException, "UDTF_ARROW_TYPE_CONVERSION_ERROR"):
            result_df = InvalidArrowTypeUDTF()
            result_df.collect()

    def test_arrow_udtf_error_mismatched_schema(self):
        @arrow_udtf(returnType="x int, y string")
        class MismatchedSchemaUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table(
                    {
                        "wrong_col": pa.array([1], type=pa.int32()),
                        "another_wrong_col": pa.array([2.5], type=pa.float64()),
                    }
                )
                yield result_table

        with self.assertRaisesRegex(PythonException, "Schema at index 0 was different"):
            result_df = MismatchedSchemaUDTF()
            result_df.collect()

    def test_arrow_udtf_sql_with_aggregation(self):
        @arrow_udtf(returnType="category string, count int")
        class CategoryCountUDTF:
            def eval(self, categories: "pa.Array") -> Iterator["pa.Table"]:
                # The input is a single array element, extract the array contents
                cat_array = categories[0].as_py()  # Get the array from the first (and only) element

                # Count occurrences
                counts = {}
                for cat in cat_array:
                    if cat is not None:
                        counts[cat] = counts.get(cat, 0) + 1

                if counts:
                    result_table = pa.table(
                        {
                            "category": pa.array(list(counts.keys()), type=pa.string()),
                            "count": pa.array(list(counts.values()), type=pa.int32()),
                        }
                    )
                    yield result_table

        self.spark.udtf.register("category_count_udtf", CategoryCountUDTF)

        # Test with array input
        result_df = self.spark.sql(
            "SELECT * FROM category_count_udtf(array('A', 'B', 'A', 'C', 'B', 'A')) "
            "ORDER BY category"
        )
        expected_df = self.spark.createDataFrame(
            [("A", 3), ("B", 2), ("C", 1)], "category string, count int"
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_sql_with_struct_output(self):
        @arrow_udtf(returnType="person struct<name:string,age:int>, status string")
        class PersonStatusUDTF:
            def eval(self, name: "pa.Array", age: "pa.Array") -> Iterator["pa.Table"]:
                name_val = name[0].as_py()
                age_val = age[0].as_py()

                status = "adult" if age_val >= 18 else "minor"

                # Create struct array
                person_array = pa.array(
                    [{"name": name_val, "age": age_val}],
                    type=pa.struct([("name", pa.string()), ("age", pa.int32())]),
                )

                result_table = pa.table(
                    {
                        "person": person_array,
                        "status": pa.array([status], type=pa.string()),
                    }
                )
                yield result_table

        self.spark.udtf.register("person_status_udtf", PersonStatusUDTF)

        result_df = self.spark.sql("SELECT * FROM person_status_udtf('John', 25)")
        # Note: Using Row constructor for the expected struct value
        expected_df = self.spark.createDataFrame(
            [(Row(name="John", age=25), "adult")],
            "person struct<name:string,age:int>, status string",
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_sql_conditional_yield(self):
        @arrow_udtf(returnType="number int, type string")
        class FilterNumbersUDTF:
            def eval(self, start: "pa.Array", end: "pa.Array") -> Iterator["pa.Table"]:
                start_val = start[0].as_py()
                end_val = end[0].as_py()

                numbers = []
                types = []

                for i in range(start_val, end_val + 1):
                    if i % 2 == 0:  # Only yield even numbers
                        numbers.append(i)
                        types.append("even")

                if numbers:  # Only yield if we have data
                    result_table = pa.table(
                        {
                            "number": pa.array(numbers, type=pa.int32()),
                            "type": pa.array(types, type=pa.string()),
                        }
                    )
                    yield result_table

        self.spark.udtf.register("filter_numbers_udtf", FilterNumbersUDTF)

        result_df = self.spark.sql("SELECT * FROM filter_numbers_udtf(1, 10) ORDER BY number")
        expected_df = self.spark.createDataFrame(
            [(2, "even"), (4, "even"), (6, "even"), (8, "even"), (10, "even")],
            "number int, type string",
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_sql_empty_result(self):
        @arrow_udtf(returnType="value int")
        class EmptyResultUDTF:
            def eval(self, condition: "pa.Array") -> Iterator["pa.Table"]:
                # Only yield if condition is true
                if condition[0].as_py():
                    result_table = pa.table(
                        {
                            "value": pa.array([42], type=pa.int32()),
                        }
                    )
                    yield result_table
                # If condition is false, don't yield anything

        self.spark.udtf.register("empty_result_udtf", EmptyResultUDTF)

        # Test with true condition
        result_df_true = self.spark.sql("SELECT * FROM empty_result_udtf(true)")
        expected_df_true = self.spark.createDataFrame([(42,)], "value int")
        assertDataFrameEqual(result_df_true, expected_df_true)

        # Test with false condition (empty result)
        result_df_false = self.spark.sql("SELECT * FROM empty_result_udtf(false)")
        expected_df_false = self.spark.createDataFrame([], "value int")
        assertDataFrameEqual(result_df_false, expected_df_false)

    def test_arrow_udtf_type_coercion_long_to_int(self):
        @arrow_udtf(returnType="id int")
        class LongToIntUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table(
                    {
                        "id": pa.array([1, 2, 3], type=pa.int64()),  # long values
                    }
                )
                yield result_table

        with self.assertRaisesRegex(PythonException, "Schema at index 0 was different"):
            result_df = LongToIntUDTF()
            result_df.collect()

    def test_arrow_udtf_type_coercion_string_to_int(self):
        @arrow_udtf(returnType="id int")
        class StringToIntUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                # Return string values that cannot be coerced to int
                result_table = pa.table(
                    {
                        "id": pa.array(["abc", "def", "xyz"], type=pa.string()),
                    }
                )
                yield result_table

        with self.assertRaisesRegex(PythonException, "Schema at index 0 was different"):
            result_df = StringToIntUDTF()
            result_df.collect()

    def test_arrow_udtf_blocks_analyze_method_none_return_type(self):

        with self.assertRaises(PySparkAttributeError) as cm:
            @arrow_udtf
            class AnalyzeUDTF:
                def eval(self, input_col: "pa.Array") -> Iterator["pa.Table"]:
                    yield pa.table({"result": pa.array([1, 2, 3])})

                @staticmethod
                def analyze(arg):
                    from pyspark.sql.udtf import AnalyzeResult

                    return AnalyzeResult(
                        schema=StructType([StructField("result", IntegerType(), True)])
                    )

        self.assertIn("INVALID_ARROW_UDTF_WITH_ANALYZE", str(cm.exception))

    def test_arrow_udtf_blocks_analyze_method_with_return_type(self):

        with self.assertRaises(PySparkAttributeError) as cm:
            @arrow_udtf(returnType="result: int")
            class AnalyzeUDTF:
                def eval(self, input_col: "pa.Array") -> Iterator["pa.Table"]:
                    yield pa.table({"result": pa.array([1, 2, 3])})

                @staticmethod
                def analyze(arg):
                    from pyspark.sql.udtf import AnalyzeResult

                    return AnalyzeResult(
                        schema=StructType([StructField("result", IntegerType(), True)])
                    )

        self.assertIn("INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE", str(cm.exception))


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
