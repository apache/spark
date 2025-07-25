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


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDTFTests(ReusedSQLTestCase):
    """Test cases for PyArrow-native UDTFs."""

    def test_arrow_udtf_basic_table_input(self):
        """Test arrow_udtf with PyArrow Table input."""
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("PyArrow not available")

        @arrow_udtf(returnType="x int, y int")
        class TestArrowUDTF:
            def eval(self, batch: pa.Table) -> Iterator[pa.Table]:
                # Simple pass-through operation
                yield batch

        df = self.spark.range(3).selectExpr("id as x", "id + 1 as y")
        print(df.asTable())
        TestArrowUDTF(df.asTable()).show()
        results = TestArrowUDTF(df.asTable()).collect()
        # Fix: Use proper DataFrame operation with select and the UDTF
        # result = df.select(TestArrowUDTF(df).alias("result")).select("result.*").collect()
        
        expected = [(0, 1), (1, 2), (2, 3)]
        self.assertEqual(result, expected)

    def test_arrow_udtf_basic_array_input(self):
        """Test arrow_udtf with PyArrow Array inputs."""
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("PyArrow not available")

        @arrow_udtf(returnType="x int, doubled int")
        class TestArrowArrayUDTF:
            def eval(self, x: pa.Array) -> Iterator[pa.Table]:
                # Double the input values
                doubled = pa.compute.multiply(x, pa.scalar(2))
                result_table = pa.table({
                    'x': x,
                    'doubled': doubled
                })
                yield result_table

        df = self.spark.range(3).select(col("id").alias("x"))
        # Fix: Use proper DataFrame operation with the UDTF
        result = df.select(TestArrowArrayUDTF(col("x")).alias("result")).select("result.*").collect()
        
        expected = [(0, 0), (1, 2), (2, 4)]
        self.assertEqual(result, expected)

    def test_arrow_udtf_multiple_array_inputs(self):
        """Test arrow_udtf with multiple PyArrow Array inputs."""
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("PyArrow not available")

        @arrow_udtf(returnType="x int, y int, sum int")
        class TestMultipleArrayUDTF:
            def eval(self, x: pa.Array, y: pa.Array) -> Iterator[pa.Table]:
                # Add two arrays
                sum_arr = pa.compute.add(x, y)
                result_table = pa.table({
                    'x': x,
                    'y': y,
                    'sum': sum_arr
                })
                yield result_table

        df = self.spark.range(3).select(col("id").alias("x"), (col("id") + 10).alias("y"))
        # Fix: Use proper DataFrame operation with the UDTF
        result = df.select(TestMultipleArrayUDTF(col("x"), col("y")).alias("result")).select("result.*").collect()
        
        expected = [(0, 10, 10), (1, 11, 12), (2, 12, 14)]
        self.assertEqual(result, expected)

    def test_arrow_udtf_vectorized_operations(self):
        """Test arrow_udtf with complex vectorized operations."""
        try:
            import pyarrow as pa
            import pyarrow.compute as pc
        except ImportError:
            self.skipTest("PyArrow not available")

        @arrow_udtf(returnType="original int, sqrt double, log double")
        class VectorizedMathUDTF:
            def eval(self, values: pa.Array) -> Iterator[pa.Table]:
                # Perform vectorized mathematical operations
                sqrt_values = pc.sqrt(pc.cast(values, pa.float64()))
                log_values = pc.ln(pc.cast(pc.add(values, pa.scalar(1)), pa.float64()))
                
                result_table = pa.table({
                    'original': values,
                    'sqrt': sqrt_values,
                    'log': log_values
                })
                yield result_table

        df = self.spark.range(1, 5).select(col("id").alias("values"))
        # Fix: Use proper DataFrame operation with the UDTF
        result = df.select(VectorizedMathUDTF(col("values")).alias("result")).select("result.*").collect()
        
        # Verify the structure and some values
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0][0], 1)  # original value
        self.assertAlmostEqual(result[0][1], 1.0, places=5)  # sqrt(1) = 1.0
        self.assertAlmostEqual(result[2][1], 1.732, places=2)  # sqrt(3) ≈ 1.732

    def test_arrow_udtf_error_handling(self):
        """Test error handling in arrow_udtf."""
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("PyArrow not available")

        @arrow_udtf(returnType="x int")
        class ErrorUDTF:
            def eval(self, x: pa.Array) -> Iterator[pa.Table]:
                # This should raise an error for non-PyArrow return
                yield {"x": [1, 2, 3]}  # Invalid return type

        df = self.spark.range(3).select(col("id").alias("x"))
        
        with self.assertRaises(Exception):
            df.select(ErrorUDTF(col("x")).alias("result")).select("result.*").collect()


if __name__ == "__main__":
    from pyspark.sql.tests.test_arrow_udtf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)