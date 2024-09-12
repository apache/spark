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

                self.assertEqual(
                    classic_signature,
                    connect_signature,
                    f"Signature mismatch in {cls_name} method '{method}'\n"
                    f"Classic: {classic_signature}\n"
                    f"Connect: {connect_signature}",
                )

        # DataFrame API signature comparison
        compare_method_signatures(ClassicDataFrame, ConnectDataFrame, "DataFrame")

        # Column API signature comparison
        compare_method_signatures(ClassicColumn, ConnectColumn, "Column")


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
