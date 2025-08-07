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

from pyspark.util import is_remote_only
from pyspark.ml import functions as SF
from pyspark.testing.connectutils import (
    should_test_connect,
    ReusedMixedTestCase,
)
from pyspark.testing.pandasutils import PandasOnSparkTestUtils

if should_test_connect:
    from pyspark.ml.connect import functions as CF


@unittest.skipIf(is_remote_only(), "Requires JVM access")
class SparkConnectMLFunctionTests(ReusedMixedTestCase, PandasOnSparkTestUtils):
    """These test cases exercise the interface to the proto plan
    generation but do not call Spark."""

    def test_array_vector_conversion(self):
        query = """
            SELECT * FROM VALUES
            (1, 4, ARRAY(1.0, 2.0, 3.0)),
            (1, 2, ARRAY(-1.0, -2.0, -3.0))
            AS tab(a, b, c)
            """

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        self.compare_by_show(
            cdf.select(cdf.b, CF.array_to_vector(cdf.c)),
            sdf.select(sdf.b, SF.array_to_vector(sdf.c)),
        )

        cdf1 = cdf.select("a", CF.array_to_vector(cdf.c).alias("d"))
        sdf1 = sdf.select("a", SF.array_to_vector(sdf.c).alias("d"))

        self.compare_by_show(
            cdf1.select(CF.vector_to_array(cdf1.d)),
            sdf1.select(SF.vector_to_array(sdf1.d)),
        )
        self.compare_by_show(
            cdf1.select(CF.vector_to_array(cdf1.d, "float32")),
            sdf1.select(SF.vector_to_array(sdf1.d, "float32")),
        )
        self.compare_by_show(
            cdf1.select(CF.vector_to_array(cdf1.d, "float64")),
            sdf1.select(SF.vector_to_array(sdf1.d, "float64")),
        )


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_connect_function import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
