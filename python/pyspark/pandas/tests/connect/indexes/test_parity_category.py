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

from pyspark.pandas.tests.indexes.test_category import CategoricalIndexTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils, TestUtils


class CategoricalIndexParityTests(
    CategoricalIndexTestsMixin, PandasOnSparkTestUtils, TestUtils, ReusedConnectTestCase
):
    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_append(self):
        super().test_append()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_astype(self):
        super().test_astype()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_factorize(self):
        super().test_factorize()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_intersection(self):
        super().test_intersection()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_remove_categories(self):
        super().test_remove_categories()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_remove_unused_categories(self):
        super().test_remove_unused_categories()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_reorder_categories(self):
        super().test_reorder_categories()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_set_categories(self):
        super().test_set_categories()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_union(self):
        super().test_union()


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.indexes.test_parity_category import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
