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

import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils, TestUtils


class ConnectPlottingTests(PandasOnSparkTestUtils, TestUtils, ReusedConnectTestCase):
    @property
    def pdf1(self):
        return pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", None],
            columns=["max_speed", "shield"],
        )

    @property
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    def test_unsupported_functions(self):
        with self.assertRaises(PandasNotImplementedError):
            self.psdf1.plot.hist()

        with self.assertRaises(PandasNotImplementedError):
            self.psdf1.plot.hist(bins=3)

        with self.assertRaises(PandasNotImplementedError):
            self.psdf1.shield.plot.hist()

        with self.assertRaises(PandasNotImplementedError):
            self.psdf1.shield.plot.hist(bins=3)

    def test_unsupported_kinds(self):
        with self.assertRaises(PandasNotImplementedError):
            self.psdf1.plot(kind="hist")

        with self.assertRaises(PandasNotImplementedError):
            self.psdf1.plot(kind="hist", bins=3)

        with self.assertRaises(PandasNotImplementedError):
            self.psdf1.shield.plot(kind="hist")

        with self.assertRaises(PandasNotImplementedError):
            self.psdf1.shield.plot(kind="hist", bins=3)


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.test_connect_plotting import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
