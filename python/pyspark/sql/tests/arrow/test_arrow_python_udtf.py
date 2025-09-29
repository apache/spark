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

from pyspark.sql.types import BinaryType
from pyspark.testing.sqlutils import (
    have_pyarrow,
    pyarrow_requirement_message,
    ReusedSQLTestCase,
)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowPythonUDTFTests(ReusedSQLTestCase):
    def test_arrow_table_udf_binary_type(self):
        from pyspark.sql.functions import udtf, lit

        @udtf(returnType="type_name: string", useArrow=True)
        class BinaryTypeUDF:
            def eval(self, b):
                # Check the type of the binary input and return type name as string
                yield (type(b).__name__,)

        with self.sql_conf({"spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": "false"}):
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "true"}):
                result = BinaryTypeUDF(lit(b"test_bytes")).collect()
                self.assertEqual(result[0]["type_name"], "bytes")

            with self.sql_conf({"spark.sql.execution.arrow.pyspark.binaryAsBytes": "false"}):
                result = BinaryTypeUDF(lit(b"test_bytearray")).collect()
                self.assertEqual(result[0]["type_name"], "bytearray")


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_python_udtf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
