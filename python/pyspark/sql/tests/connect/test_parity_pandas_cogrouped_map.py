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

from pyspark.sql.tests.pandas.test_pandas_cogrouped_map import CogroupedApplyInPandasTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class CogroupedApplyInPandasTests(CogroupedApplyInPandasTestsMixin, ReusedConnectTestCase):
    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_different_group_key_cardinality(self):
        super().test_different_group_key_cardinality()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_returning_incompatible_type(self):
        super().test_apply_in_pandas_returning_incompatible_type()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_wrong_args(self):
        super().test_wrong_args()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_not_returning_pandas_dataframe(self):
        super().test_apply_in_pandas_not_returning_pandas_dataframe()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_returning_wrong_column_names(self):
        super().test_apply_in_pandas_returning_wrong_column_names()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_returning_no_column_names_and_wrong_amount(self):
        super().test_apply_in_pandas_returning_no_column_names_and_wrong_amount()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_apply_in_pandas_returning_incompatible_type(self):
        super().test_apply_in_pandas_returning_incompatible_type()

    @unittest.skip(
        "Spark Connect does not support sc._jvm.org.apache.log4j but the test depends on it."
    )
    def test_wrong_return_type(self):
        super().test_wrong_return_type()


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_parity_pandas_cogrouped_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
