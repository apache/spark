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
import sys

from pyspark.sql.functions import arrow_udtf, col, lit
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pyarrow, pyarrow_requirement_message
from pyspark.testing import assertDataFrameEqual

# Import pyarrow conditionally
if have_pyarrow:
    import pyarrow as pa


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDTFTests(ReusedSQLTestCase):
    """Test cases for PyArrow-native UDTFs."""

    def test_arrow_udtf_zero_args(self):
        @arrow_udtf(returnType="id int, value string")
        class GeneratorUDTF:
            def eval(self) -> Iterator["pa.Table"]:
                result_table = pa.table({
                    'id': pa.array([1, 2, 3], type=pa.int32()),
                    'value': pa.array(['a', 'b', 'c'], type=pa.string())
                })
                yield result_table

        # Test the UDTF
        result_df = GeneratorUDTF()
        expected_df = self.spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], "id int, value string")
        
        assertDataFrameEqual(result_df, expected_df)

    def test_arrow_udtf_scalar_args_only(self):
        @arrow_udtf(returnType="x int, y int, sum int")
        class ScalarArgsUDTF:
            def eval(self, x: "pa.ChunkedArray", y: "pa.ChunkedArray") -> Iterator["pa.Table"]:
                # ChunkedArray for scalar values - extract the single value
                x_val = x.to_pylist()[0]  # Use to_pylist() instead of as_py()
                y_val = y.to_pylist()[0]
                result_table = pa.table({
                    'x': pa.array([x_val], type=pa.int32()),
                    'y': pa.array([y_val], type=pa.int32()),
                    'sum': pa.array([x_val + y_val], type=pa.int32())
                })
                yield result_table

        # Test with scalar inputs
        result_df = ScalarArgsUDTF(lit(5), lit(10))
        expected_df = self.spark.createDataFrame([(5, 10, 15)], "x int, y int, sum int")
        
        assertDataFrameEqual(result_df, expected_df)
        self.spark.udtf.register("ScalarArgsUDTF", ScalarArgsUDTF)


if __name__ == "__main__":
    from pyspark.sql.tests.test_arrow_udtf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
