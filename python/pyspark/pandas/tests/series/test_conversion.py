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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.testing.pandasutils import have_tabulate, tabulate_requirement_message


class SeriesConversionMixin:
    @property
    def pser(self):
        return pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    def test_to_numpy(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")

        psser = ps.from_pandas(pser)
        self.assert_eq(psser.to_numpy(), pser.values)

    def test_to_datetime(self):
        pser = pd.Series(["3/11/2000", "3/12/2000", "3/13/2000"] * 100)
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pd.to_datetime(pser, infer_datetime_format=True),
            ps.to_datetime(psser, infer_datetime_format=True),
        )

    def test_to_list(self):
        self.assert_eq(self.psser.tolist(), self.pser.tolist())

    def test_to_frame(self):
        pser = pd.Series(["a", "b", "c"])
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.to_frame(name="a"), psser.to_frame(name="a"))

        # for MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pser = pd.Series(["a", "b", "c"], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.to_frame(name="a"), psser.to_frame(name="a"))

    @unittest.skipIf(not have_tabulate, tabulate_requirement_message)
    def test_to_markdown(self):
        pser = pd.Series(["elk", "pig", "dog", "quetzal"], name="animal")
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.to_markdown(), psser.to_markdown())


class SeriesConversionTests(
    SeriesConversionMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_conversion import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
