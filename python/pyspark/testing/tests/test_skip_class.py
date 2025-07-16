#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import sys
import unittest


@unittest.skip
class SkipClassTests(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)


if __name__ == "__main__":
    from pyspark.testing.tests.test_skip_class import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    try:
        unittest.main(testRunner=testRunner, verbosity=2)
    except SystemExit as e:
        if sys.version_info >= (3, 12):
            assert e.code == 5, f"status code: {e.code}"
        else:
            assert e.code == 0, f"status code: {e.code}"
