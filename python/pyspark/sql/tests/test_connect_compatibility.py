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
from pyspark.testing.sqlutils import ReusedSQLTestCase


class ConnectCompatibilityTestsMixin:
    def test_signature_comparison_between_classic_and_connect(self):
        """Compare method signatures between ClassicDataFrame and ConnectDataFrame."""
        import inspect
        from pyspark.sql.classic.dataframe import DataFrame as ClassicDataFrame
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
        from pyspark.sql.classic.column import Column as ClassicColumn
        from pyspark.sql.connect.column import Column as ConnectColumn
        from pyspark.sql.session import SparkSession as ClassicSparkSession
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        def get_public_methods(cls):
            """Get public methods of a class."""
            return {
                name: method
                for name, method in inspect.getmembers(cls, predicate=inspect.isfunction)
                if not name.startswith("_")
            }

        def compare_method_signatures(classic_cls, connect_cls, cls_name):
            """Compare method signatures between classic and connect classes."""
            classic_methods = get_public_methods(classic_cls)
            connect_methods = get_public_methods(connect_cls)

            common_methods = set(classic_methods.keys()) & set(connect_methods.keys())

            for method in common_methods:
                classic_signature = inspect.signature(classic_methods[method])
                connect_signature = inspect.signature(connect_methods[method])

                # createDataFrame cannot be the same since RDD is not supported from Spark Connect
                if not method == "createDataFrame":
                    self.assertEqual(
                        classic_signature,
                        connect_signature,
                        f"Signature mismatch in {cls_name} method '{method}'\n"
                        f"Classic: {classic_signature}\n"
                        f"Connect: {connect_signature}",
                    )

        def check_missing_methods(classic_cls, connect_cls, cls_name, expected_missing_methods):
            """Check for expected missing methods between classic and connect classes."""
            classic_methods = get_public_methods(classic_cls)
            connect_methods = get_public_methods(connect_cls)

            # Identify missing methods
            classic_only_methods = set(classic_methods.keys()) - set(connect_methods.keys())

            # Compare the actual missing methods with the expected ones
            self.assertEqual(
                classic_only_methods,
                expected_missing_methods,
                f"{cls_name}: Unexpected missing methods in Connect: {classic_only_methods}",
            )

        # DataFrame API signature comparison
        compare_method_signatures(ClassicDataFrame, ConnectDataFrame, "DataFrame")

        # Column API signature comparison
        compare_method_signatures(ClassicColumn, ConnectColumn, "Column")

        # SparkSession API signature comparison
        compare_method_signatures(ClassicSparkSession, ConnectSparkSession, "SparkSession")

        # Expected missing methods for DataFrame
        expected_missing_methods_for_dataframe = {
            "inputFiles",
            "isLocal",
            "semanticHash",
            "isEmpty",
        }

        # DataFrame missing method check
        check_missing_methods(
            ClassicDataFrame, ConnectDataFrame, "DataFrame", expected_missing_methods_for_dataframe
        )

        # Expected missing methods for Column (if any, replace with actual values)
        expected_missing_methods_for_column = set()

        # Column missing method check
        check_missing_methods(
            ClassicColumn, ConnectColumn, "Column", expected_missing_methods_for_column
        )

        # Expected missing methods for SparkSession (if any, replace with actual values)
        expected_missing_methods_for_spark_session = {"newSession"}

        # SparkSession missing method check
        check_missing_methods(
            ClassicSparkSession,
            ConnectSparkSession,
            "SparkSession",
            expected_missing_methods_for_spark_session,
        )


class ConnectCompatibilityTests(ConnectCompatibilityTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_connect_compatibility import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
