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

from pyspark.sql.tests.test_subquery import SubqueryTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class SubqueryParityTests(SubqueryTestsMixin, ReusedConnectTestCase):
    @unittest.skip("TODO(SPARK-50134): Support subquery in connect")
    def test_simple_uncorrelated_scalar_subquery(self):
        super().test_simple_uncorrelated_scalar_subquery()

    @unittest.skip("TODO(SPARK-50134): Support subquery in connect")
    def test_uncorrelated_scalar_subquery_with_view(self):
        super().test_uncorrelated_scalar_subquery_with_view()

    @unittest.skip("TODO(SPARK-50134): Support subquery in connect")
    def test_scalar_subquery_against_local_relations(self):
        super().test_scalar_subquery_against_local_relations()

    @unittest.skip("TODO(SPARK-50134): Support subquery in connect")
    def test_correlated_scalar_subquery(self):
        super().test_correlated_scalar_subquery()

    @unittest.skip("TODO(SPARK-50134): Support subquery in connect")
    def test_exists_subquery(self):
        super().test_exists_subquery()

    @unittest.skip("TODO(SPARK-50134): Support subquery in connect")
    def test_scalar_subquery_with_outer_reference_errors(self):
        super().test_scalar_subquery_with_outer_reference_errors()

    @unittest.skip("TODO(SPARK-50134): Support subquery in connect")
    def test_scalar_subquery_inside_lateral_join(self):
        super().test_scalar_subquery_inside_lateral_join()

    @unittest.skip("TODO(SPARK-50134): Support subquery in connect")
    def test_lateral_join_inside_subquery(self):
        super().test_lateral_join_inside_subquery()


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_parity_subquery import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
