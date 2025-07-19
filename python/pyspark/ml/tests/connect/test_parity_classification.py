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

from pyspark.testing.utils import eventually, timeout
from pyspark.ml.tests.test_classification import ClassificationTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class ClassificationParityTests(ClassificationTestsMixin, ReusedConnectTestCase):
    @eventually(timeout=60, catch_timeout=True)
    @timeout(timeout=10)
    def test_binary_logistic_regression_summary(self):
        super().test_binary_logistic_regression_summary()

    @eventually(timeout=60, catch_timeout=True)
    @timeout(timeout=10)
    def test_multiclass_logistic_regression_summary(self):
        super().test_multiclass_logistic_regression_summary()

    @eventually(timeout=60, catch_timeout=True)
    @timeout(timeout=10)
    def test_linear_svc(self):
        super().test_linear_svc()

    @eventually(timeout=60, catch_timeout=True)
    @timeout(timeout=10)
    def test_factorization_machine(self):
        super().test_factorization_machine()

    @eventually(timeout=60, catch_timeout=True)
    @timeout(timeout=10)
    def test_binary_random_forest_classifier(self):
        super().test_binary_random_forest_classifier()

    @eventually(timeout=60, catch_timeout=True)
    @timeout(timeout=10)
    def test_multiclass_random_forest_classifier(self):
        super().test_multiclass_random_forest_classifier()

    @eventually(timeout=60, catch_timeout=True)
    @timeout(timeout=10)
    def test_mlp(self):
        super().test_mlp()


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_parity_classification import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
