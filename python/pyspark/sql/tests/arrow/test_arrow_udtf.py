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
from typing import Iterator, Optional

from pyspark.errors import PySparkAttributeError
from pyspark.errors import PythonException
from pyspark.sql.functions import arrow_udtf, lit
from pyspark.sql.types import Row, StructType, StructField, IntegerType
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pyarrow, pyarrow_requirement_message
from pyspark.testing import assertDataFrameEqual
from pyspark.util import is_remote_only

if have_pyarrow:
    import pyarrow as pa
    import pyarrow.compute as pc


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDTFTestsMixin:
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
                    # Create arrays for each column
                    batch_id_array = pa.array([batch_id] * size, type=pa.int32())
                    name_array = pa.array([f"batch_{batch_id}"] * size, type=pa.string())
                    count_array = pa.array(list(range(size)), type=pa.int32())

                    # Create record batch from arrays and names
                    batch = pa.record_batch(
                        [batch_id_array, name_array, count_array],
                        names=["batch_id", "name", "count"],
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

        with self.assertRaisesRegex(
            PythonException,
            "Target schema's field names are not matching the record batch's field names",
        ):
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

        # Should succeed with automatic coercion
        result_df = LongToIntUDTF()
        expected_df = self.spark.createDataFrame([(1,), (2,), (3,)], "id int")
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_type_coercion_string_to_int(self):
        @arrow_udtf(returnType="id int")
        class StringToIntUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                # Return string values that cannot be coerced to int
                result_table = pa.table(
                    {
                        "id": pa.array(["1", "2", "xyz"], type=pa.string()),
                    }
                )
                yield result_table

        # Should fail with Arrow cast exception since string cannot be cast to int
        with self.assertRaisesRegex(
            PythonException,
            "PySparkRuntimeError: \\[RESULT_COLUMNS_MISMATCH_FOR_ARROW_UDTF\\] "
            "Column names of the returned pyarrow.Table or pyarrow.RecordBatch do not match "
            "specified schema. Expected: int32 Actual: string",
        ):
            result_df = StringToIntUDTF()
            result_df.collect()

    def test_arrow_udtf_type_coercion_string_to_int_safe(self):
        @arrow_udtf(returnType="id int")
        class StringToIntUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table(
                    {
                        "id": pa.array(["1", "2", "3"], type=pa.string()),
                    }
                )
                yield result_table

        result_df = StringToIntUDTF()
        expected_df = self.spark.createDataFrame([(1,), (2,), (3,)], "id int")
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_type_corecion_int64_to_int32_safe(self):
        @arrow_udtf(returnType="id int")
        class Int64ToInt32UDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table(
                    {
                        "id": pa.array([1, 2, 3], type=pa.int64()),  # long values
                    }
                )
                yield result_table

        result_df = Int64ToInt32UDTF()
        expected_df = self.spark.createDataFrame([(1,), (2,), (3,)], "id int")
        assertDataFrameEqual(result_df, expected_df)

    def test_return_type_coercion_success(self):
        @arrow_udtf(returnType="value int")
        class CoercionSuccessUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table(
                    {
                        "value": pa.array([10, 20, 30], type=pa.int64()),  # long -> int coercion
                    }
                )
                yield result_table

        result_df = CoercionSuccessUDTF()
        expected_df = self.spark.createDataFrame([(10,), (20,), (30,)], "value int")
        assertDataFrameEqual(result_df, expected_df)

    def test_return_type_coercion_overflow(self):
        @arrow_udtf(returnType="value int")
        class CoercionOverflowUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                # Return values that will cause overflow when casting long to int
                result_table = pa.table(
                    {
                        "value": pa.array([2147483647 + 1], type=pa.int64()),  # int32 max + 1
                    }
                )
                yield result_table

        # Should fail with PyArrow overflow exception
        with self.assertRaises(Exception):
            result_df = CoercionOverflowUDTF()
            result_df.collect()

    def test_return_type_coercion_multiple_columns(self):
        @arrow_udtf(returnType="id int, price float")
        class MultipleColumnCoercionUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table(
                    {
                        "id": pa.array([1, 2, 3], type=pa.int64()),  # long -> int coercion
                        "price": pa.array(
                            [10.5, 20.7, 30.9], type=pa.float64()
                        ),  # double -> float coercion
                    }
                )
                yield result_table

        result_df = MultipleColumnCoercionUDTF()
        expected_df = self.spark.createDataFrame(
            [(1, 10.5), (2, 20.7), (3, 30.9)], "id int, price float"
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_empty_column_result(self):
        @arrow_udtf(returnType=StructType())
        class EmptyResultUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                yield pa.Table.from_struct_array(pa.array([{}] * 3))

        assertDataFrameEqual(EmptyResultUDTF(), [Row(), Row(), Row()])

        @arrow_udtf(returnType="id int")
        class InvalidEmptyResultUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                yield pa.Table.from_struct_array(pa.array([{}] * 3))

        with self.assertRaisesRegex(PythonException, "UDTF_RETURN_SCHEMA_MISMATCH"):
            result_df = InvalidEmptyResultUDTF()
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

    def test_arrow_udtf_with_table_argument_basic(self):
        @arrow_udtf(returnType="filtered_id bigint")  # Use bigint to match int64
        class TableArgUDTF:
            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                assert isinstance(
                    table_data, pa.RecordBatch
                ), f"Expected pa.RecordBatch, got {type(table_data)}"

                # Convert record batch to table to work with it more easily
                table = pa.table(table_data)

                # Filter rows where id > 5
                id_column = table.column("id")
                mask = pa.compute.greater(id_column, pa.scalar(5))
                filtered_table = table.filter(mask)

                if filtered_table.num_rows > 0:
                    result_table = pa.table(
                        {"filtered_id": filtered_table.column("id")}  # Keep original type (int64)
                    )
                    yield result_table

        # Test with DataFrame API using asTable()
        input_df = self.spark.range(8)
        result_df = TableArgUDTF(input_df.asTable())
        expected_df = self.spark.createDataFrame([(6,), (7,)], "filtered_id bigint")
        assertDataFrameEqual(result_df, expected_df)

        # Test SQL registration and usage with TABLE() syntax
        self.spark.udtf.register("test_table_arg_udtf", TableArgUDTF)
        sql_result_df = self.spark.sql(
            "SELECT * FROM test_table_arg_udtf(TABLE(SELECT id FROM range(0, 8)))"
        )
        assertDataFrameEqual(sql_result_df, expected_df)

    def test_arrow_udtf_with_table_argument_and_scalar(self):
        @arrow_udtf(returnType="filtered_id bigint")  # Use bigint to match int64
        class MixedArgsUDTF:
            def eval(
                self, table_data: "pa.RecordBatch", threshold: "pa.Array"
            ) -> Iterator["pa.Table"]:
                assert isinstance(
                    threshold, pa.Array
                ), f"Expected pa.Array for threshold, got {type(threshold)}"
                assert isinstance(
                    table_data, pa.RecordBatch
                ), f"Expected pa.RecordBatch for table_data, got {type(table_data)}"

                threshold_val = threshold[0].as_py()

                # Convert record batch to table
                table = pa.table(table_data)
                id_column = table.column("id")
                mask = pa.compute.greater(id_column, pa.scalar(threshold_val))
                filtered_table = table.filter(mask)

                if filtered_table.num_rows > 0:
                    result_table = pa.table(
                        {"filtered_id": filtered_table.column("id")}  # Keep original type
                    )
                    yield result_table

        # # Test with DataFrame API
        input_df = self.spark.range(8)
        result_df = MixedArgsUDTF(input_df.asTable(), lit(5))
        expected_df = self.spark.createDataFrame([(6,), (7,)], "filtered_id bigint")
        assertDataFrameEqual(result_df, expected_df)

        # Test SQL registration and usage
        self.spark.udtf.register("test_mixed_args_udtf", MixedArgsUDTF)
        sql_result_df = self.spark.sql(
            "SELECT * FROM test_mixed_args_udtf(TABLE(SELECT id FROM range(0, 8)), 5)"
        )
        assertDataFrameEqual(sql_result_df, expected_df)

    def test_arrow_udtf_lateral_join_disallowed(self):
        @arrow_udtf(returnType="x int, result int")
        class SimpleArrowUDTF:
            def eval(self, input_val: "pa.Array") -> Iterator["pa.Table"]:
                val = input_val[0].as_py()
                result_table = pa.table(
                    {
                        "x": pa.array([val], type=pa.int32()),
                        "result": pa.array([val * 2], type=pa.int32()),
                    }
                )
                yield result_table

        self.spark.udtf.register("simple_arrow_udtf", SimpleArrowUDTF)

        test_df = self.spark.createDataFrame([(1,), (2,), (3,)], "id int")
        test_df.createOrReplaceTempView("test_table")

        with self.assertRaisesRegex(Exception, "LATERAL_JOIN_WITH_ARROW_UDTF_UNSUPPORTED"):
            self.spark.sql(
                """
                SELECT t.id, f.x, f.result
                FROM test_table t, LATERAL simple_arrow_udtf(t.id) f
                """
            )

    def test_arrow_udtf_lateral_join_with_table_argument_disallowed(self):
        @arrow_udtf(returnType="filtered_id bigint")
        class MixedArgsUDTF:
            def eval(self, input_table: "pa.Table") -> Iterator["pa.Table"]:
                filtered_data = input_table.filter(pc.greater(input_table["id"], 5))
                result_table = pa.table({"filtered_id": filtered_data["id"]})
                yield result_table

        self.spark.udtf.register("mixed_args_udtf", MixedArgsUDTF)

        test_df1 = self.spark.createDataFrame([(1,), (2,), (3,)], "id int")
        test_df1.createOrReplaceTempView("test_table1")

        test_df2 = self.spark.createDataFrame([(6,), (7,), (8,)], "id bigint")
        test_df2.createOrReplaceTempView("test_table2")

        # Table arguments create nested lateral joins where our CheckAnalysis rule doesn't trigger
        # because the Arrow UDTF is in the inner lateral join, not the outer one our rule checks.
        # So Spark's general lateral join validation catches this first with
        # NON_DETERMINISTIC_LATERAL_SUBQUERIES.
        with self.assertRaisesRegex(
            Exception,
            "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.NON_DETERMINISTIC_LATERAL_SUBQUERIES",
        ):
            self.spark.sql(
                """
                SELECT t1.id, f.filtered_id
                FROM test_table1 t1, LATERAL mixed_args_udtf(table(SELECT * FROM test_table2)) f
                """
            )

    def test_arrow_udtf_with_table_argument_then_lateral_join_allowed(self):
        @arrow_udtf(returnType="processed_id bigint")
        class TableArgUDTF:
            def eval(self, input_table: "pa.Table") -> Iterator["pa.Table"]:
                processed_data = pc.add(input_table["id"], 100)
                result_table = pa.table({"processed_id": processed_data})
                yield result_table

        self.spark.udtf.register("table_arg_udtf", TableArgUDTF)

        source_df = self.spark.createDataFrame([(1,), (2,), (3,)], "id bigint")
        source_df.createOrReplaceTempView("source_table")

        join_df = self.spark.createDataFrame([("A",), ("B",), ("C",)], "label string")
        join_df.createOrReplaceTempView("join_table")

        result_df = self.spark.sql(
            """
            SELECT f.processed_id, j.label
            FROM table_arg_udtf(table(SELECT * FROM source_table)) f,
                join_table j
            ORDER BY f.processed_id, j.label
            """
        )

        expected_data = [
            (101, "A"),
            (101, "B"),
            (101, "C"),
            (102, "A"),
            (102, "B"),
            (102, "C"),
            (103, "A"),
            (103, "B"),
            (103, "C"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, "processed_id bigint, label string")
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_table_argument_with_regular_udtf_lateral_join_allowed(self):
        @arrow_udtf(returnType="computed_value int")
        class ComputeUDTF:
            def eval(self, input_table: "pa.Table") -> Iterator["pa.Table"]:
                total = pc.sum(input_table["value"]).as_py()
                result_table = pa.table({"computed_value": pa.array([total], type=pa.int32())})
                yield result_table

        from pyspark.sql.functions import udtf
        from pyspark.sql.types import StructType, StructField, IntegerType

        @udtf(returnType=StructType([StructField("multiplied", IntegerType())]))
        class MultiplyUDTF:
            def eval(self, input_val: int):
                yield (input_val * 3,)

        self.spark.udtf.register("compute_udtf", ComputeUDTF)
        self.spark.udtf.register("multiply_udtf", MultiplyUDTF)

        values_df = self.spark.createDataFrame([(10,), (20,), (30,)], "value int")
        values_df.createOrReplaceTempView("values_table")

        result_df = self.spark.sql(
            """
            SELECT c.computed_value, m.multiplied
            FROM compute_udtf(table(SELECT * FROM values_table) WITH SINGLE PARTITION) c,
                LATERAL multiply_udtf(c.computed_value) m
            """
        )

        expected_df = self.spark.createDataFrame([(60, 180)], "computed_value int, multiplied int")
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_named_arguments_scalar_only(self):
        @arrow_udtf(returnType="x int, y int, sum int")
        class NamedArgsUDTF:
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

        # Test SQL registration and usage with named arguments
        self.spark.udtf.register("named_args_udtf", NamedArgsUDTF)

        # Test with named arguments in SQL
        sql_result_df = self.spark.sql("SELECT * FROM named_args_udtf(y => 10, x => 5)")
        expected_df = self.spark.createDataFrame([(5, 10, 15)], "x int, y int, sum int")
        assertDataFrameEqual(sql_result_df, expected_df)

        # Test with mixed positional and named arguments
        sql_result_df2 = self.spark.sql("SELECT * FROM named_args_udtf(7, y => 3)")
        expected_df2 = self.spark.createDataFrame([(7, 3, 10)], "x int, y int, sum int")
        assertDataFrameEqual(sql_result_df2, expected_df2)

    def test_arrow_udtf_with_partition_by(self):
        @arrow_udtf(returnType="partition_key int, sum_value int")
        class SumUDTF:
            def __init__(self):
                self._partition_key = None
                self._sum = 0

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                table = pa.table(table_data)
                partition_key = pc.unique(table["partition_key"]).to_pylist()
                assert (
                    len(partition_key) == 1
                ), f"Expected exactly one partition key, got {partition_key}"
                self._partition_key = partition_key[0]
                self._sum += pc.sum(table["value"]).as_py()
                # Don't yield here - accumulate and yield in terminate
                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                if self._partition_key is not None:
                    result_table = pa.table(
                        {
                            "partition_key": pa.array([self._partition_key], type=pa.int32()),
                            "sum_value": pa.array([self._sum], type=pa.int32()),
                        }
                    )
                    yield result_table

        test_data = [
            (1, 10),
            (2, 5),
            (1, 20),
            (2, 15),
            (1, 30),
            (3, 100),
        ]
        input_df = self.spark.createDataFrame(test_data, "partition_key int, value int")

        self.spark.udtf.register("sum_udtf", SumUDTF)
        input_df.createOrReplaceTempView("test_data")

        result_df = self.spark.sql(
            """
            SELECT * FROM sum_udtf(TABLE(test_data) PARTITION BY partition_key)
        """
        )

        expected_data = [
            (1, 60),
            (2, 20),
            (3, 100),
        ]
        expected_df = self.spark.createDataFrame(expected_data, "partition_key int, sum_value int")
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_partition_by_and_terminate(self):
        @arrow_udtf(returnType="partition_key int, count int, sum_value int")
        class TerminateUDTF:
            def __init__(self):
                self._partition_key = None
                self._count = 0
                self._sum = 0

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                import pyarrow.compute as pc

                table = pa.table(table_data)
                # Track partition key
                partition_keys = pc.unique(table["partition_key"]).to_pylist()
                assert len(partition_keys) == 1, f"Expected one partition key, got {partition_keys}"
                self._partition_key = partition_keys[0]

                # Accumulate stats but don't yield here
                self._count += table.num_rows
                self._sum += pc.sum(table["value"]).as_py()
                # Return empty iterator - results come from terminate
                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                # Yield accumulated results for this partition
                if self._partition_key is not None:
                    result_table = pa.table(
                        {
                            "partition_key": pa.array([self._partition_key], type=pa.int32()),
                            "count": pa.array([self._count], type=pa.int32()),
                            "sum_value": pa.array([self._sum], type=pa.int32()),
                        }
                    )
                    yield result_table

        test_data = [
            (3, 50),
            (1, 10),
            (2, 40),
            (1, 20),
            (2, 30),
        ]
        input_df = self.spark.createDataFrame(test_data, "partition_key int, value int")

        self.spark.udtf.register("terminate_udtf", TerminateUDTF)
        input_df.createOrReplaceTempView("test_data_terminate")

        result_df = self.spark.sql(
            """
            SELECT * FROM terminate_udtf(TABLE(test_data_terminate) PARTITION BY partition_key)
            ORDER BY partition_key
            """
        )

        expected_data = [
            (1, 2, 30),  # partition 1: 2 rows, sum = 30
            (2, 2, 70),  # partition 2: 2 rows, sum = 70
            (3, 1, 50),  # partition 3: 1 row, sum = 50
        ]
        expected_df = self.spark.createDataFrame(
            expected_data, "partition_key int, count int, sum_value int"
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_partition_by_and_order_by(self):
        @arrow_udtf(returnType="partition_key int, first_value int, last_value int")
        class OrderByUDTF:
            def __init__(self):
                self._partition_key = None
                self._first_value = None
                self._last_value = None

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                import pyarrow.compute as pc

                table = pa.table(table_data)
                partition_keys = pc.unique(table["partition_key"]).to_pylist()
                assert len(partition_keys) == 1, f"Expected one partition key, got {partition_keys}"
                self._partition_key = partition_keys[0]

                # Track first and last values (should be ordered)
                values = table["value"].to_pylist()
                if values:
                    if self._first_value is None:
                        self._first_value = values[0]
                    self._last_value = values[-1]

                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                if self._partition_key is not None:
                    result_table = pa.table(
                        {
                            "partition_key": pa.array([self._partition_key], type=pa.int32()),
                            "first_value": pa.array([self._first_value], type=pa.int32()),
                            "last_value": pa.array([self._last_value], type=pa.int32()),
                        }
                    )
                    yield result_table

        test_data = [
            (1, 30),
            (1, 10),
            (1, 20),
            (2, 60),
            (2, 40),
            (2, 50),
        ]
        input_df = self.spark.createDataFrame(test_data, "partition_key int, value int")

        self.spark.udtf.register("order_by_udtf", OrderByUDTF)
        input_df.createOrReplaceTempView("test_data_order")

        result_df = self.spark.sql(
            """
            SELECT * FROM order_by_udtf(
                TABLE(test_data_order)
                PARTITION BY partition_key
                ORDER BY value
            )
            ORDER BY partition_key
            """
        )

        expected_data = [
            (1, 10, 30),  # partition 1: first=10 (min), last=30 (max) after ordering
            (2, 40, 60),  # partition 2: first=40 (min), last=60 (max) after ordering
        ]
        expected_df = self.spark.createDataFrame(
            expected_data, "partition_key int, first_value int, last_value int"
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_partition_column_removal(self):
        @arrow_udtf(returnType="col1_sum int, col2_sum int")
        class PartitionColumnTestUDTF:
            def __init__(self):
                self._col1_sum = 0
                self._col2_sum = 0
                self._columns_verified = False

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                import pyarrow.compute as pc

                table = pa.table(table_data)

                # When partitioning by an expression like "col1 + col2",
                # Catalyst adds the expression result as a new column at the beginning.
                # The ArrowUDTFWithPartition._remove_partition_by_exprs method should
                # remove this added column, leaving only the original table columns.

                # Verify columns only once per partition
                if not self._columns_verified:
                    column_names = table.column_names
                    # Verify we only have the original columns, not the partition expression
                    assert "col1" in column_names, f"Expected col1 in columns: {column_names}"
                    assert "col2" in column_names, f"Expected col2 in columns: {column_names}"
                    # The partition expression column should have been removed
                    assert len(column_names) == 2, (
                        f"Expected only col1 and col2 after partition column removal, "
                        f"but got: {column_names}"
                    )
                    self._columns_verified = True

                # Accumulate sums - don't yield here to avoid multiple results per partition
                self._col1_sum += pc.sum(table["col1"]).as_py()
                self._col2_sum += pc.sum(table["col2"]).as_py()

                # Return empty iterator - results come from terminate
                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                # Yield accumulated results for this partition
                result_table = pa.table(
                    {
                        "col1_sum": pa.array([self._col1_sum], type=pa.int32()),
                        "col2_sum": pa.array([self._col2_sum], type=pa.int32()),
                    }
                )
                yield result_table

        test_data = [
            (1, 1),  # partition: 1+1=2
            (1, 2),  # partition: 1+2=3
            (2, 0),  # partition: 2+0=2
            (2, 1),  # partition: 2+1=3
        ]
        input_df = self.spark.createDataFrame(test_data, "col1 int, col2 int")

        self.spark.udtf.register("partition_column_test_udtf", PartitionColumnTestUDTF)
        input_df.createOrReplaceTempView("test_partition_removal")

        # Partition by col1 + col2 expression
        result_df = self.spark.sql(
            """
            SELECT * FROM partition_column_test_udtf(
                TABLE(test_partition_removal)
                PARTITION BY col1 + col2
            )
            ORDER BY col1_sum, col2_sum
            """
        )

        expected_data = [
            (3, 1),  # partition 2: sum of col1s (1+2), sum of col2s (1+0)
            (3, 3),  # partition 3: sum of col1s (1+2), sum of col2s (2+1)
        ]
        expected_df = self.spark.createDataFrame(expected_data, "col1_sum int, col2_sum int")
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_partition_by_all_columns(self):
        from pyspark.sql.functions import SkipRestOfInputTableException

        @arrow_udtf(returnType="product_id int, review_id int, rating int, review string")
        class TopReviewsPerProduct:
            TOP_K = 3

            def __init__(self):
                self._product = None
                self._seen = 0
                self._batches: list[pa.Table] = []
                self._top_table: Optional[pa.Table] = None

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                import pyarrow.compute as pc

                table = pa.table(table_data)
                if table.num_rows == 0:
                    return iter(())

                products = pc.unique(table["product_id"]).to_pylist()
                assert len(products) == 1, f"Expected one product, saw {products}"
                product = products[0]

                if self._product is None:
                    self._product = product
                else:
                    assert (
                        self._product == product
                    ), f"Mixed products {self._product} and {product} in partition"

                self._batches.append(table)
                self._seen += table.num_rows

                if self._seen >= self.TOP_K and self._top_table is None:
                    combined = pa.concat_tables(self._batches)
                    self._top_table = combined.slice(0, self.TOP_K)
                    raise SkipRestOfInputTableException(
                        f"Top {self.TOP_K} reviews ready for product {self._product}"
                    )

                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                if self._product is None:
                    return iter(())

                if self._top_table is None:
                    combined = pa.concat_tables(self._batches) if self._batches else pa.table({})
                    limit = min(self.TOP_K, self._seen)
                    self._top_table = combined.slice(0, limit)

                yield self._top_table

        review_data = [
            (101, 1, 5, "Amazing battery life"),
            (101, 2, 5, "Still great after a month"),
            (101, 3, 4, "Solid build"),
            (101, 4, 3, "Average sound"),
            (202, 5, 5, "My go-to lens"),
            (202, 6, 4, "Sharp and bright"),
            (202, 7, 4, "Great value"),
        ]
        df = self.spark.createDataFrame(
            review_data, "product_id int, review_id int, rating int, review string"
        )
        self.spark.udtf.register("top_reviews_udtf", TopReviewsPerProduct)
        df.createOrReplaceTempView("product_reviews")

        result_df = self.spark.sql(
            """
            SELECT * FROM top_reviews_udtf(
                TABLE(product_reviews)
                PARTITION BY (product_id)
                ORDER BY (rating DESC, review_id)
            )
            ORDER BY product_id, rating DESC, review_id
            """
        )

        expected_df = self.spark.createDataFrame(
            [
                (101, 1, 5, "Amazing battery life"),
                (101, 2, 5, "Still great after a month"),
                (101, 3, 4, "Solid build"),
                (202, 5, 5, "My go-to lens"),
                (202, 6, 4, "Sharp and bright"),
                (202, 7, 4, "Great value"),
            ],
            "product_id int, review_id int, rating int, review string",
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_partition_by_single_partition_multiple_input_partitions(self):
        @arrow_udtf(returnType="partition_key int, count bigint, sum_value bigint")
        class SinglePartitionUDTF:
            def __init__(self):
                self._partition_key = None
                self._count = 0
                self._sum = 0

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                import pyarrow.compute as pc

                table = pa.table(table_data)

                # All rows should have the same partition key (constant value 1)
                partition_keys = pc.unique(table["partition_key"]).to_pylist()
                self._partition_key = partition_keys[0]
                self._count += table.num_rows
                self._sum += pc.sum(table["id"]).as_py()

                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                if self._partition_key is not None:
                    result_table = pa.table(
                        {
                            "partition_key": pa.array([self._partition_key], type=pa.int32()),
                            "count": pa.array([self._count], type=pa.int64()),
                            "sum_value": pa.array([self._sum], type=pa.int64()),
                        }
                    )
                    yield result_table

        # Create DataFrame with 5 input partitions but all data will map to partition_key=1
        # range(1, 10, 1, 5) creates ids from 1 to 9 with 5 partitions
        input_df = self.spark.range(1, 10, 1, 5).selectExpr(
            "1 as partition_key", "id"  # constant partition key
        )

        self.spark.udtf.register("single_partition_udtf", SinglePartitionUDTF)
        input_df.createOrReplaceTempView("test_single_partition")

        result_df = self.spark.sql(
            """
            SELECT * FROM single_partition_udtf(
                TABLE(test_single_partition)
                PARTITION BY partition_key
            )
            """
        )

        # All 9 rows (1 through 9) should be in a single partition with key=1
        expected_data = [(1, 9, 45)]
        expected_df = self.spark.createDataFrame(
            expected_data, "partition_key int, count bigint, sum_value bigint"
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_partition_by_skip_rest_of_input(self):
        from pyspark.sql.functions import SkipRestOfInputTableException

        @arrow_udtf(returnType="partition_key int, rows_processed int, last_value int")
        class SkipRestUDTF:
            def __init__(self):
                self._partition_key = None
                self._rows_processed = 0
                self._last_value = None

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                import pyarrow.compute as pc

                table = pa.table(table_data)
                partition_keys = pc.unique(table["partition_key"]).to_pylist()
                assert len(partition_keys) == 1, f"Expected one partition key, got {partition_keys}"
                self._partition_key = partition_keys[0]

                # Process rows one by one and stop after processing 2 rows per partition
                values = table["value"].to_pylist()
                for value in values:
                    self._rows_processed += 1
                    self._last_value = value

                    # Skip rest of the partition after processing 2 rows
                    if self._rows_processed >= 2:
                        msg = f"Skipping partition {self._partition_key} "
                        msg += f"after {self._rows_processed} rows"
                        raise SkipRestOfInputTableException(msg)

                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                if self._partition_key is not None:
                    result_table = pa.table(
                        {
                            "partition_key": pa.array([self._partition_key], type=pa.int32()),
                            "rows_processed": pa.array([self._rows_processed], type=pa.int32()),
                            "last_value": pa.array([self._last_value], type=pa.int32()),
                        }
                    )
                    yield result_table

        # Create test data with multiple partitions, each having more than 2 rows
        test_data = [
            (1, 10),
            (1, 20),
            (1, 30),  # This should be skipped
            (1, 40),  # This should be skipped
            (2, 50),
            (2, 60),
            (2, 70),  # This should be skipped
            (3, 80),
            (3, 90),
            (3, 100),  # This should be skipped
            (3, 110),  # This should be skipped
        ]
        input_df = self.spark.createDataFrame(test_data, "partition_key int, value int")

        self.spark.udtf.register("skip_rest_udtf", SkipRestUDTF)
        input_df.createOrReplaceTempView("test_skip_rest")

        result_df = self.spark.sql(
            """
            SELECT * FROM skip_rest_udtf(
                TABLE(test_skip_rest)
                PARTITION BY partition_key
                ORDER BY value
            )
            ORDER BY partition_key
            """
        )

        # Each partition should only process 2 rows before skipping the rest
        expected_data = [
            (1, 2, 20),  # Processed rows 10, 20, then skipped 30, 40
            (2, 2, 60),  # Processed rows 50, 60, then skipped 70
            (3, 2, 90),  # Processed rows 80, 90, then skipped 100, 110
        ]
        expected_df = self.spark.createDataFrame(
            expected_data, "partition_key int, rows_processed int, last_value int"
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_partition_by_empty_input_batch(self):
        @arrow_udtf(returnType="count int")
        class EmptyBatchUDTF:
            def __init__(self):
                self._count = 0

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                table = pa.table(table_data)
                self._count += table.num_rows
                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                result_table = pa.table({"count": pa.array([self._count], type=pa.int32())})
                yield result_table

        empty_df = self.spark.createDataFrame([], "partition_key int, value int")
        self.spark.udtf.register("empty_batch_udtf", EmptyBatchUDTF)
        empty_df.createOrReplaceTempView("empty_partition_by")

        result_df = self.spark.sql(
            """
            SELECT * FROM empty_batch_udtf(
                TABLE(empty_partition_by)
                PARTITION BY partition_key
            )
            """
        )

        expected_df = self.spark.createDataFrame([], "count int")
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_partition_by_null_values(self):
        @arrow_udtf(returnType="partition_key int, count int, non_null_sum int")
        class NullPartitionUDTF:
            def __init__(self):
                self._partition_key = None
                self._count = 0
                self._non_null_sum = 0

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                import pyarrow.compute as pc

                table = pa.table(table_data)
                # Handle null partition keys
                partition_keys = table["partition_key"]
                unique_keys = pc.unique(partition_keys).to_pylist()

                # Should have exactly one unique value (either a value or None)
                assert len(unique_keys) == 1, f"Expected one partition key, got {unique_keys}"
                self._partition_key = unique_keys[0]

                # Count rows and sum non-null values
                self._count += table.num_rows
                values = table["value"]
                # Use PyArrow compute to handle nulls properly
                non_null_values = pc.drop_null(values)
                if len(non_null_values) > 0:
                    self._non_null_sum += pc.sum(non_null_values).as_py()

                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                # Return results even for null partition keys
                result_table = pa.table(
                    {
                        "partition_key": pa.array([self._partition_key], type=pa.int32()),
                        "count": pa.array([self._count], type=pa.int32()),
                        "non_null_sum": pa.array([self._non_null_sum], type=pa.int32()),
                    }
                )
                yield result_table

        # Test data with null partition keys and null values
        test_data = [
            (1, 10),
            (1, None),  # null value in partition 1
            (None, 20),  # null partition key
            (None, 30),  # null partition key
            (2, 40),
            (2, None),  # null value in partition 2
            (None, None),  # both null
        ]
        input_df = self.spark.createDataFrame(test_data, "partition_key int, value int")

        self.spark.udtf.register("null_partition_udtf", NullPartitionUDTF)
        input_df.createOrReplaceTempView("test_null_partitions")

        result_df = self.spark.sql(
            """
            SELECT * FROM null_partition_udtf(
                TABLE(test_null_partitions)
                PARTITION BY partition_key
                ORDER BY value
            )
            ORDER BY partition_key NULLS FIRST
            """
        )

        # Expected: null partition gets grouped together, nulls in values are handled
        expected_data = [
            (None, 3, 50),  # null partition: 3 rows, sum of non-null values = 20+30 = 50
            (1, 2, 10),  # partition 1: 2 rows, sum of non-null values = 10
            (2, 2, 40),  # partition 2: 2 rows, sum of non-null values = 40
        ]
        expected_df = self.spark.createDataFrame(
            expected_data, "partition_key int, count int, non_null_sum int"
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_empty_table(self):
        @arrow_udtf(returnType="result string")
        class EmptyTableUDTF:
            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                import pyarrow as pa

                # This should not be called for empty tables
                table = pa.table(table_data)
                if table.num_rows == 0:
                    raise AssertionError("eval should not be called for empty tables")
                result_table = pa.table(
                    {"result": pa.array([f"rows_{table.num_rows}"], type=pa.string())}
                )
                yield result_table

        # Create an empty DataFrame
        empty_df = self.spark.range(0)

        self.spark.udtf.register("empty_table_udtf", EmptyTableUDTF)
        empty_df.createOrReplaceTempView("empty_table")

        result_df = self.spark.sql(
            """
            SELECT * FROM empty_table_udtf(TABLE(empty_table))
            """
        )

        # For empty input, UDTF is not called, resulting in empty output
        # This is consistent with regular UDTFs
        expected_df = self.spark.createDataFrame([], "result string")
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_table_and_struct_arguments(self):
        """Test that TABLE args are RecordBatch while struct args are Array."""

        @arrow_udtf(returnType="table_is_batch boolean, struct_is_array boolean")
        class TypeCheckUDTF:
            def eval(self, table_arg, struct_arg) -> Iterator["pa.Table"]:
                # Verify types and return result
                result = pa.table(
                    {
                        "table_is_batch": pa.array(
                            [isinstance(table_arg, pa.RecordBatch)], pa.bool_()
                        ),
                        "struct_is_array": pa.array(
                            [
                                isinstance(struct_arg, pa.Array)
                                and not isinstance(struct_arg, pa.RecordBatch)
                            ],
                            pa.bool_(),
                        ),
                    }
                )
                yield result

        self.spark.udtf.register("type_check_udtf", TypeCheckUDTF)
        self.spark.range(3).createOrReplaceTempView("test_table")

        result_df = self.spark.sql(
            """
            SELECT * FROM type_check_udtf(
                TABLE(test_table),
                named_struct('a', 10, 'b', 15)
            )
        """
        )

        # All rows should show correct types
        for row in result_df.collect():
            assert row.table_is_batch is True
            assert row.struct_is_array is True

    def test_arrow_udtf_table_partition_by_single_column(self):
        @arrow_udtf(returnType="partition_key string, total_value bigint")
        class PartitionSumUDTF:
            def __init__(self):
                self._category = None
                self._total = 0

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                table = pa.table(table_data)

                # Each partition will have records with the same category
                if table.num_rows > 0:
                    self._category = table.column("category")[0].as_py()
                    self._total += pa.compute.sum(table.column("value")).as_py()
                # Don't yield here - accumulate and yield in terminate
                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                if self._category is not None:
                    result_table = pa.table(
                        {
                            "partition_key": pa.array([self._category], type=pa.string()),
                            "total_value": pa.array([self._total], type=pa.int64()),
                        }
                    )
                    yield result_table

        self.spark.udtf.register("partition_sum_udtf", PartitionSumUDTF)

        # Create test data with categories
        test_data = [("A", 10), ("A", 20), ("B", 30), ("B", 40), ("C", 50)]
        test_df = self.spark.createDataFrame(test_data, "category string, value int")
        test_df.createOrReplaceTempView("partition_test_data")

        result_df = self.spark.sql(
            """
            SELECT * FROM partition_sum_udtf(
                TABLE(partition_test_data) PARTITION BY category
            ) ORDER BY partition_key
        """
        )

        expected_df = self.spark.createDataFrame(
            [("A", 30), ("B", 70), ("C", 50)], "partition_key string, total_value bigint"
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_table_partition_by_multiple_columns(self):
        @arrow_udtf(returnType="dept string, status string, count_employees bigint")
        class DeptStatusCountUDTF:
            def __init__(self):
                self._dept = None
                self._status = None
                self._count = 0

            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                table = pa.table(table_data)

                if table.num_rows > 0:
                    self._dept = table.column("department")[0].as_py()
                    self._status = table.column("status")[0].as_py()
                    self._count += table.num_rows
                # Don't yield here - accumulate and yield in terminate
                return iter(())

            def terminate(self) -> Iterator["pa.Table"]:
                if self._dept is not None and self._status is not None:
                    result_table = pa.table(
                        {
                            "dept": pa.array([self._dept], type=pa.string()),
                            "status": pa.array([self._status], type=pa.string()),
                            "count_employees": pa.array([self._count], type=pa.int64()),
                        }
                    )
                    yield result_table

        self.spark.udtf.register("dept_status_count_udtf", DeptStatusCountUDTF)

        test_data = [
            ("IT", "active"),
            ("IT", "active"),
            ("IT", "inactive"),
            ("HR", "active"),
            ("HR", "inactive"),
            ("Finance", "active"),
        ]
        test_df = self.spark.createDataFrame(test_data, "department string, status string")
        test_df.createOrReplaceTempView("employee_data")

        result_df = self.spark.sql(
            """
            SELECT * FROM dept_status_count_udtf(
                TABLE(SELECT * FROM employee_data)
                PARTITION BY (department, status)
            ) ORDER BY dept, status
        """
        )

        expected_df = self.spark.createDataFrame(
            [
                ("Finance", "active", 1),
                ("HR", "active", 1),
                ("HR", "inactive", 1),
                ("IT", "active", 2),
                ("IT", "inactive", 1),
            ],
            "dept string, status string, count_employees bigint",
        )
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_with_scalar_first_table_second(self):
        @arrow_udtf(returnType="filtered_id bigint")
        class ScalarFirstTableSecondUDTF:
            def eval(
                self, threshold: "pa.Array", table_data: "pa.RecordBatch"
            ) -> Iterator["pa.Table"]:
                assert isinstance(
                    threshold, pa.Array
                ), f"Expected pa.Array for threshold, got {type(threshold)}"
                assert isinstance(
                    table_data, pa.RecordBatch
                ), f"Expected pa.RecordBatch for table_data, got {type(table_data)}"

                threshold_val = threshold[0].as_py()

                # Convert record batch to table
                table = pa.table(table_data)
                id_column = table.column("id")
                mask = pa.compute.greater(id_column, pa.scalar(threshold_val))
                filtered_table = table.filter(mask)

                if filtered_table.num_rows > 0:
                    result_table = pa.table(
                        {"filtered_id": filtered_table.column("id")}  # Keep original type
                    )
                    yield result_table

        # Test with DataFrame API - scalar first, table second
        input_df = self.spark.range(8)
        result_df = ScalarFirstTableSecondUDTF(lit(4), input_df.asTable())
        expected_df = self.spark.createDataFrame([(5,), (6,), (7,)], "filtered_id bigint")
        assertDataFrameEqual(result_df, expected_df)

        # Test SQL registration and usage
        self.spark.udtf.register("test_scalar_first_table_second_udtf", ScalarFirstTableSecondUDTF)
        sql_result_df = self.spark.sql(
            "SELECT * FROM test_scalar_first_table_second_udtf("
            "4, TABLE(SELECT id FROM range(0, 8)))"
        )
        assertDataFrameEqual(sql_result_df, expected_df)

    def test_arrow_udtf_with_table_argument_in_middle(self):
        """Test Arrow UDTF with table argument in the middle of multiple scalar arguments."""

        @arrow_udtf(returnType="filtered_id bigint")
        class TableInMiddleUDTF:
            def eval(
                self,
                min_threshold: "pa.Array",
                table_data: "pa.RecordBatch",
                max_threshold: "pa.Array",
            ) -> Iterator["pa.Table"]:
                assert isinstance(
                    min_threshold, pa.Array
                ), f"Expected pa.Array for min_threshold, got {type(min_threshold)}"
                assert isinstance(
                    table_data, pa.RecordBatch
                ), f"Expected pa.RecordBatch for table_data, got {type(table_data)}"
                assert isinstance(
                    max_threshold, pa.Array
                ), f"Expected pa.Array for max_threshold, got {type(max_threshold)}"

                min_val = min_threshold[0].as_py()
                max_val = max_threshold[0].as_py()

                # Convert record batch to table
                table = pa.table(table_data)
                id_column = table.column("id")

                # Filter rows where min_val < id < max_val
                mask = pa.compute.and_(
                    pa.compute.greater(id_column, pa.scalar(min_val)),
                    pa.compute.less(id_column, pa.scalar(max_val)),
                )
                filtered_table = table.filter(mask)

                if filtered_table.num_rows > 0:
                    result_table = pa.table(
                        {"filtered_id": filtered_table.column("id")}  # Keep original type
                    )
                    yield result_table

        # Test with DataFrame API - scalar, table, scalar
        input_df = self.spark.range(10)
        result_df = TableInMiddleUDTF(lit(2), input_df.asTable(), lit(7))
        expected_df = self.spark.createDataFrame([(3,), (4,), (5,), (6,)], "filtered_id bigint")
        assertDataFrameEqual(result_df, expected_df)

        # Test SQL registration and usage
        self.spark.udtf.register("test_table_in_middle_udtf", TableInMiddleUDTF)
        sql_result_df = self.spark.sql(
            "SELECT * FROM test_table_in_middle_udtf(2, TABLE(SELECT id FROM range(0, 10)), 7)"
        )
        assertDataFrameEqual(sql_result_df, expected_df)

    def test_arrow_udtf_with_named_arguments(self):
        @arrow_udtf(returnType="result_id bigint, multiplier_used int")
        class NamedArgsUDTF:
            def eval(
                self, table_data: "pa.RecordBatch", multiplier: "pa.Array"
            ) -> Iterator["pa.Table"]:
                assert isinstance(
                    table_data, pa.RecordBatch
                ), f"Expected pa.RecordBatch for table_data, got {type(table_data)}"
                assert isinstance(
                    multiplier, pa.Array
                ), f"Expected pa.Array for multiplier, got {type(multiplier)}"

                multiplier_val = multiplier[0].as_py()

                # Convert record batch to table
                table = pa.table(table_data)
                id_column = table.column("id")

                # Multiply each id by the multiplier
                multiplied_ids = pa.compute.multiply(id_column, pa.scalar(multiplier_val))

                result_table = pa.table(
                    {
                        "result_id": multiplied_ids,
                        "multiplier_used": pa.array(
                            [multiplier_val] * table.num_rows, type=pa.int32()
                        ),
                    }
                )
                yield result_table

        # Test with DataFrame API using named arguments
        input_df = self.spark.range(3)  # [0, 1, 2]
        result_df = NamedArgsUDTF(table_data=input_df.asTable(), multiplier=lit(5))
        expected_df = self.spark.createDataFrame(
            [(0, 5), (5, 5), (10, 5)], "result_id bigint, multiplier_used int"
        )
        assertDataFrameEqual(result_df, expected_df)

        # Test with DataFrame API using different named argument order
        result_df2 = NamedArgsUDTF(multiplier=lit(3), table_data=input_df.asTable())
        expected_df2 = self.spark.createDataFrame(
            [(0, 3), (3, 3), (6, 3)], "result_id bigint, multiplier_used int"
        )
        assertDataFrameEqual(result_df2, expected_df2)

        # Test SQL registration and usage with named arguments
        self.spark.udtf.register("test_named_args_udtf", NamedArgsUDTF)

        sql_result_df = self.spark.sql(
            """
            SELECT * FROM test_named_args_udtf(
                table_data => TABLE(SELECT id FROM range(0, 3)),
                multiplier => 5
            )
        """
        )
        assertDataFrameEqual(sql_result_df, expected_df)

        sql_result_df2 = self.spark.sql(
            """
            SELECT * FROM test_named_args_udtf(
                multiplier => 3,
                table_data => TABLE(SELECT id FROM range(0, 3))
            )
        """
        )
        assertDataFrameEqual(sql_result_df2, expected_df2)

    @unittest.skipIf(is_remote_only(), "Requires JVM access")
    def test_arrow_udtf_with_logging(self):
        import pyarrow as pa

        @arrow_udtf(returnType="id bigint, doubled bigint")
        class TestArrowUDTFWithLogging:
            def eval(self, table_data: "pa.RecordBatch") -> Iterator["pa.Table"]:
                assert isinstance(
                    table_data, pa.RecordBatch
                ), f"Expected pa.RecordBatch, got {type(table_data)}"

                logger = logging.getLogger("test_arrow_udtf")
                logger.warning(f"arrow udtf: {table_data.to_pydict()}")

                # Convert record batch to table
                table = pa.table(table_data)

                # Get the id column and create doubled values
                id_column = table.column("id")
                doubled_values = pa.compute.multiply(id_column, pa.scalar(2))

                yield pa.table({"id": id_column, "doubled": doubled_values})

        with self.sql_conf(
            {
                "spark.sql.execution.arrow.maxRecordsPerBatch": "3",
                "spark.sql.pyspark.worker.logging.enabled": "true",
            }
        ):
            assertDataFrameEqual(
                TestArrowUDTFWithLogging(self.spark.range(9, numPartitions=2).asTable()),
                [Row(id=i, doubled=i * 2) for i in range(9)],
            )

            logs = self.spark.tvf.python_worker_logs()

            assertDataFrameEqual(
                logs.select("level", "msg", "context", "logger"),
                [
                    Row(
                        level="WARNING",
                        msg=f"arrow udtf: {dict(id=lst)}",
                        context={"class_name": "TestArrowUDTFWithLogging", "func_name": "eval"},
                        logger="test_arrow_udtf",
                    )
                    for lst in [[0, 1, 2], [3], [4, 5, 6], [7, 8]]
                ],
            )


class ArrowUDTFTests(ArrowUDTFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_udtf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
