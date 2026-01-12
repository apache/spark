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


def main(module="__main__", output="target/test-reports"):
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output=output, verbosity=2, outsuffix="")
    except ImportError:
        testRunner = None

    # Python 3.12+ incorrectly exits with status code 5 if setUpClass fails.
    # We need to check the result and exit with the correct status code.
    # This is fixed in latest 3.13+ but is not backported to 3.12.
    # To be safe, we apply this on all 3.12 - 3.14.
    if sys.version_info >= (3, 12) and sys.version_info < (3, 15):
        res = unittest.main(module=module, testRunner=testRunner, verbosity=2, exit=False)
        if not res.result.wasSuccessful():
            sys.exit(1)
        elif res.result.testsRun == 0 and len(res.result.skipped) == 0:
            sys.exit(5)
        else:
            sys.exit(0)
    else:
        unittest.main(module=module, testRunner=testRunner, verbosity=2)
