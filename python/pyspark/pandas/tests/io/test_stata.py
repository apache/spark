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
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class StataMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"animal": ["falcon", "parrot", "falcon", "parrot"], "speed": [350, 18, 361, 15]}
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_to_feather(self):
        with self.temp_dir() as dirpath:
            path1 = f"{dirpath}/file1.dta"
            path2 = f"{dirpath}/file2.dta"

            self.pdf.to_stata(path1)
            self.psdf.to_stata(path2)

            self.assert_eq(
                pd.read_stata(path1),
                pd.read_stata(path2),
            )


class StataTests(
    StataMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.io.test_stata import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
