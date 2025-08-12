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

from typing import Iterator
from pyspark.sql.tests.arrow.test_arrow_udtf import ArrowUDTFTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.errors.exceptions.connect import PythonException
from pyspark.sql.functions import arrow_udtf
from pyspark.testing.sqlutils import have_pyarrow

if have_pyarrow:
    import pyarrow as pa


class ArrowUDTFParityTests(ArrowUDTFTestsMixin, ReusedConnectTestCase):
    
    # Override the error tests to use connect.PythonException instead of captured.PythonException
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


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.arrow.test_parity_arrow_udtf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
