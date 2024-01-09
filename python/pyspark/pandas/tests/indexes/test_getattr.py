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


class IndexGetattrMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_index_getattr(self):
        psidx = self.psdf.index
        item = "databricks"

        expected_error_message = "'.*Index' object has no attribute '{}'".format(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            psidx.__getattr__(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            ps.from_pandas(pd.date_range("2011-01-01", freq="D", periods=10)).__getattr__(item)

    def test_multi_index_getattr(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)
        psidx = psdf.index
        item = "databricks"

        expected_error_message = "'MultiIndex' object has no attribute '{}'".format(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            psidx.__getattr__(item)


class IndexGetattrTests(
    IndexGetattrMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_getattr import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
