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

from pyspark.sql.functions import arrow_udtf, col, lit
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pyarrow, pyarrow_requirement_message
from pyspark.testing import assertDataFrameEqual

if have_pyarrow:
    import pyarrow as pa


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDTFTests(ReusedSQLTestCase):
    """Test cases for PyArrow-native UDTFs."""

    def test_arrow_udtf_zero_args(self):
        @arrow_udtf(returnType="id int, value string")
        class TestUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table({
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'value': pa.array(['a', 'b', 'c'], type=pa.string())
                })
                yield result_table

        result_df = TestUDTF()
        expected_df = self.spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], "id int, value string")
        
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_scalar_args_only(self):
        @arrow_udtf(returnType="x int, y int, sum int")
        class ScalarArgsUDTF:
            def eval(self, x: "pa.Array", y: "pa.Array") -> Iterator["pa.Table"]:
                assert isinstance(x, pa.Array), f"Expected pa.Array, got {type(x)}"
                assert isinstance(y, pa.Array), f"Expected pa.Array, got {type(y)}"
                
                x_val = x[0].as_py()
                y_val = y[0].as_py()
                result_table = pa.table({
                    'x': pa.array([x_val], type=pa.int32()),
                    'y': pa.array([y_val], type=pa.int32()),
                    'sum': pa.array([x_val + y_val], type=pa.int32())
                })
                yield result_table

        result_df = ScalarArgsUDTF(lit(5), lit(10))
        expected_df = self.spark.createDataFrame([(5, 10, 15)], "x int, y int, sum int")
        
        assertDataFrameEqual(result_df, expected_df)
        self.spark.udtf.register("ScalarArgsUDTF", ScalarArgsUDTF)

    def test_arrow_udtf_record_batch_iterator(self):
        @arrow_udtf(returnType="batch_id int, name string, count int")
        class RecordBatchUDTF:
            def eval(self, batch_size: "pa.Array") -> Iterator["pa.RecordBatch"]:
                assert isinstance(batch_size, pa.Array), f"Expected pa.Array, got {type(batch_size)}"
                
                size = batch_size[0].as_py()
                
                for batch_id in range(3):
                    batch = pa.record_batch({
                        'batch_id': pa.array([batch_id] * size, type=pa.int32()),
                        'name': pa.array([f'batch_{batch_id}'] * size, type=pa.string()),
                        'count': pa.array(list(range(size)), type=pa.int32())
                    })
                    yield batch

        result_df = RecordBatchUDTF(lit(2))
        expected_data = [
            (0, 'batch_0', 0), (0, 'batch_0', 1),
            (1, 'batch_1', 0), (1, 'batch_1', 1), 
            (2, 'batch_2', 0), (2, 'batch_2', 1)
        ]
        expected_df = self.spark.createDataFrame(expected_data, "batch_id int, name string, count int")
        
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_error_not_iterator(self):
        @arrow_udtf(returnType="x int, y string")
        class NotIteratorUDTF:
            def eval(self) -> "pa.Table":  # Should be Iterator[pa.Table]
                return pa.table({
                    'x': pa.array([1], type=pa.int32()),
                    'y': pa.array(['test'], type=pa.string())
                })

        # This should raise an error because eval doesn't return an iterator
        with self.assertRaises(Exception):
            result_df = NotIteratorUDTF()
            result_df.collect()

    def test_arrow_udtf_error_wrong_yield_type(self):
        @arrow_udtf(returnType="x int, y string")
        class WrongYieldTypeUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                yield {'x': [1], 'y': ['test']}

        # This should raise an error because we're yielding a dict, not pa.Table/pa.RecordBatch
        with self.assertRaises(Exception):
            result_df = WrongYieldTypeUDTF()
            result_df.collect()

    def test_arrow_udtf_error_mismatched_schema(self):
        @arrow_udtf(returnType="x int, y string")
        class MismatchedSchemaUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                # Return table with wrong column names/types
                result_table = pa.table({
                    'wrong_col': pa.array([1], type=pa.int32()),
                    'another_wrong_col': pa.array([2.5], type=pa.float64())  # Wrong type too
                })
                yield result_table

        # This should raise an error due to schema mismatch
        with self.assertRaises(Exception):
            result_df = MismatchedSchemaUDTF()
            result_df.collect()


if __name__ == "__main__":
    from pyspark.sql.tests.test_arrow_udtf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
