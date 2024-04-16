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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class RaisesTestsMixin:
    def test_raises(self):
        psdf = ps.DataFrame(
            {"a": [1, 2, 6, 4, 4, 6, 4, 3, 7], "b": [4, 2, 7, 3, 3, 1, 1, 1, 2]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        # test raises with incorrect key
        self.assertRaises(ValueError, lambda: psdf.groupby([]))
        self.assertRaises(KeyError, lambda: psdf.groupby("x"))
        self.assertRaises(KeyError, lambda: psdf.groupby(["a", "x"]))
        self.assertRaises(KeyError, lambda: psdf.groupby("a")["x"])
        self.assertRaises(KeyError, lambda: psdf.groupby("a")["b", "x"])
        self.assertRaises(KeyError, lambda: psdf.groupby("a")[["b", "x"]])


class RaisesTests(
    RaisesTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_raises import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
