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
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql.tests.pandas.test_pandas_udf import PandasUDFTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class PandasUDFParityTests(PandasUDFTestsMixin, ReusedConnectTestCase):
    def test_udf_wrong_arg(self):
        self.check_udf_wrong_arg()

    def test_pandas_udf_decorator_with_return_type_string(self):
        @pandas_udf("v double", PandasUDFType.GROUPED_MAP)
        def foo(x):
            return x

        self.assertEqual(foo.returnType, StructType([StructField("v", DoubleType(), True)]))
        self.assertEqual(foo.evalType, PandasUDFType.GROUPED_MAP)

        @pandas_udf(returnType="double", functionType=PandasUDFType.SCALAR)
        def foo(x):
            return x

        self.assertEqual(foo.returnType, DoubleType())
        self.assertEqual(foo.evalType, PandasUDFType.SCALAR)

    def test_pandas_udf_basic_with_return_type_string(self):
        udf = pandas_udf(lambda x: x, "double", PandasUDFType.SCALAR)
        self.assertEqual(udf.returnType, DoubleType())
        self.assertEqual(udf.evalType, PandasUDFType.SCALAR)

        udf = pandas_udf(lambda x: x, "v double", PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType(), True)]))
        self.assertEqual(udf.evalType, PandasUDFType.GROUPED_MAP)

        udf = pandas_udf(lambda x: x, "v double", functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType(), True)]))
        self.assertEqual(udf.evalType, PandasUDFType.GROUPED_MAP)

        udf = pandas_udf(lambda x: x, returnType="v double", functionType=PandasUDFType.GROUPED_MAP)
        self.assertEqual(udf.returnType, StructType([StructField("v", DoubleType(), True)]))
        self.assertEqual(udf.evalType, PandasUDFType.GROUPED_MAP)


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_pandas_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
