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
from pyspark.pandas.tests.groupby.test_stat import GroupbyStatTestingFuncMixin


class GroupbyStatAdvMixin(GroupbyStatTestingFuncMixin):
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [3.1, 4.1, 4.1, 3.1],
                "C": ["a", "b", "b", "a"],
                "D": [True, False, False, True],
            }
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_quantile(self):
        dfs = [
            pd.DataFrame(
                [["a", 1], ["a", 2], ["a", 3], ["b", 1], ["b", 3], ["b", 5]], columns=["key", "val"]
            ),
            pd.DataFrame(
                [["a", True], ["a", True], ["a", False], ["b", True], ["b", True], ["b", False]],
                columns=["key", "val"],
            ),
        ]
        for df in dfs:
            psdf = ps.from_pandas(df)
            # q accept float and int between 0 and 1
            for i in [0, 0.1, 0.5, 1]:
                self.assert_eq(
                    df.groupby("key").quantile(q=i, interpolation="lower"),
                    psdf.groupby("key").quantile(q=i),
                    almost=True,
                )
                self.assert_eq(
                    df.groupby("key")["val"].quantile(q=i, interpolation="lower"),
                    psdf.groupby("key")["val"].quantile(q=i),
                    almost=True,
                )
            # raise ValueError when q not in [0, 1]
            with self.assertRaises(ValueError):
                psdf.groupby("key").quantile(q=1.1)
            with self.assertRaises(ValueError):
                psdf.groupby("key").quantile(q=-0.1)
            with self.assertRaises(ValueError):
                psdf.groupby("key").quantile(q=2)
            with self.assertRaises(ValueError):
                psdf.groupby("key").quantile(q=np.nan)
            # raise TypeError when q type mismatch
            with self.assertRaises(TypeError):
                psdf.groupby("key").quantile(q="0.1")
            # raise NotImplementedError when q is list like type
            with self.assertRaises(NotImplementedError):
                psdf.groupby("key").quantile(q=(0.1, 0.5))
            with self.assertRaises(NotImplementedError):
                psdf.groupby("key").quantile(q=[0.1, 0.5])

    def test_first(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.first())
        self._test_stat_func(lambda groupby_obj: groupby_obj.first(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.first(numeric_only=True))

        pdf = pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [-1.5, np.nan, -3.2, 0.1],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.groupby("A").first().sort_index(), psdf.groupby("A").first().sort_index()
        )
        self.assert_eq(
            pdf.groupby("A").first(min_count=1).sort_index(),
            psdf.groupby("A").first(min_count=1).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("A").first(min_count=2).sort_index(),
            psdf.groupby("A").first(min_count=2).sort_index(),
        )

    def test_last(self):
        self._test_stat_func(lambda groupby_obj: groupby_obj.last())
        self._test_stat_func(lambda groupby_obj: groupby_obj.last(numeric_only=None))
        self._test_stat_func(lambda groupby_obj: groupby_obj.last(numeric_only=True))

        pdf = pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [-1.5, np.nan, -3.2, 0.1],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.groupby("A").last().sort_index(), psdf.groupby("A").last().sort_index())
        self.assert_eq(
            pdf.groupby("A").last(min_count=1).sort_index(),
            psdf.groupby("A").last(min_count=1).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("A").last(min_count=2).sort_index(),
            psdf.groupby("A").last(min_count=2).sort_index(),
        )

    def test_nth(self):
        for n in [0, 1, 2, 128, -1, -2, -128]:
            self._test_stat_func(lambda groupby_obj: groupby_obj.nth(n))

        with self.assertRaisesRegex(NotImplementedError, "slice or list"):
            self.psdf.groupby("B").nth(slice(0, 2))
        with self.assertRaisesRegex(NotImplementedError, "slice or list"):
            self.psdf.groupby("B").nth([0, 1, -1])
        with self.assertRaisesRegex(TypeError, "Invalid index"):
            self.psdf.groupby("B").nth("x")


class GroupbyStatAdvTests(
    GroupbyStatAdvMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_stat_adv import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
