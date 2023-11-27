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
import os
import unittest

from py4j.protocol import Py4JJavaError

from pyspark.testing.utils import PySparkTestCase, eventually
from pyspark.find_spark_home import _find_spark_home


class UtilTests(PySparkTestCase):
    def test_py4j_str(self):
        with self.assertRaises(Py4JJavaError) as context:
            # This attempts java.lang.String(null) which throws an NPE.
            self.sc._jvm.java.lang.String(None)

        self.assertTrue("NullPointerException" in str(context.exception))

    def test_parsing_version_string(self):
        from pyspark.util import VersionUtils

        self.assertRaises(ValueError, lambda: VersionUtils.majorMinorVersion("abced"))

    def test_find_spark_home(self):
        # SPARK-38827: Test find_spark_home without `SPARK_HOME` environment variable set.
        origin = os.environ["SPARK_HOME"]
        try:
            del os.environ["SPARK_HOME"]
            self.assertEqual(origin, _find_spark_home())
        finally:
            os.environ["SPARK_HOME"] = origin

    @eventually(timeout=180, catch_assertions=True)
    def test_eventually_decorator(self):
        import random

        self.assertTrue(random.random() < 0.1)

    def test_eventually_function(self):
        import random

        def condition():
            self.assertTrue(random.random() < 0.1)

        eventually(timeout=180, catch_assertions=True)(condition)()

    def test_eventually_lambda(self):
        import random

        eventually(timeout=180, catch_assertions=True)(
            lambda: self.assertTrue(random.random() < 0.1)
        )()


if __name__ == "__main__":
    from pyspark.tests.test_util import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
