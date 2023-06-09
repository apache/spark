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

from pyspark.testing.connectutils import should_test_connect

if should_test_connect:
    from pyspark import sql
    from pyspark.sql.connect.column import Column

    # This is a hack to make the Column instance comparison works in `ColumnTestsMixin`.
    # e.g., `isinstance(col, pyspark.sql.Column)`.
    sql.Column = Column

from pyspark.sql.tests.test_column import ColumnTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class ColumnParityTests(ColumnTestsMixin, ReusedConnectTestCase):
    # TODO(SPARK-42017): df["bad_key"] does not raise AnalysisException
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_access_column(self):
        super().test_access_column()

    @unittest.skip("Requires JVM access.")
    def test_validate_column_types(self):
        super().test_validate_column_types()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_column import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
