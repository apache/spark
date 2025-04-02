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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Conversion'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#conversion
class FrameConversionMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_astype(self):
        psdf = self.psdf

        msg = "Only a column name can be used for the key in a dtype mappings argument."
        with self.assertRaisesRegex(KeyError, msg):
            psdf.astype({"c": float})

    def test_isnull(self):
        pdf = pd.DataFrame(
            {"x": [1, 2, 3, 4, None, 6], "y": list("abdabd")}, index=np.random.rand(6)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.notnull(), pdf.notnull())
        self.assert_eq(psdf.isnull(), pdf.isnull())


class FrameConversionTests(
    FrameConversionMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_conversion import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
