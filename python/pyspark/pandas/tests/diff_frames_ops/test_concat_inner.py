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

import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class ConcatTestingFuncMixin:
    def _test_frames(self):
        pdf1 = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5]}, index=[1, 2, 3])
        pdf1.columns.names = ["AB"]
        pdf2 = pd.DataFrame({"C": [1, 2, 3], "D": [4, 5, 6]}, index=[1, 3, 5])
        pdf2.columns.names = ["CD"]
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        psdf3 = psdf1.copy()
        psdf4 = psdf2.copy()
        pdf3 = pdf1.copy()
        pdf4 = pdf2.copy()

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")], names=["X", "AB"])
        pdf3.columns = columns
        psdf3.columns = columns

        columns = pd.MultiIndex.from_tuples([("X", "C"), ("X", "D")], names=["Y", "CD"])
        pdf4.columns = columns
        psdf4.columns = columns

        pdf5 = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5]}, index=[1, 2, 3])
        pdf6 = pd.DataFrame({"C": [1, 2, 3]}, index=[1, 3, 5])
        psdf5 = ps.from_pandas(pdf5)
        psdf6 = ps.from_pandas(pdf6)

        objs = [
            ([psdf1.A, psdf2.C], [pdf1.A, pdf2.C]),
            # TODO: ([psdf1, psdf2.C], [pdf1, pdf2.C]),
            ([psdf1.A, psdf2], [pdf1.A, pdf2]),
            ([psdf1.A, psdf2.C], [pdf1.A, pdf2.C]),
            ([psdf3[("X", "A")], psdf4[("X", "C")]], [pdf3[("X", "A")], pdf4[("X", "C")]]),
            ([psdf3, psdf4[("X", "C")]], [pdf3, pdf4[("X", "C")]]),
            ([psdf3[("X", "A")], psdf4], [pdf3[("X", "A")], pdf4]),
            ([psdf3, psdf4], [pdf3, pdf4]),
            ([psdf5, psdf6], [pdf5, pdf6]),
            ([psdf6, psdf5], [pdf6, pdf5]),
        ]

        return objs


class ConcatInnerMixin(ConcatTestingFuncMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_concat_column_axis_inner(self):
        join = "inner"

        objs = self._test_frames()
        for i, (psdfs, pdfs) in enumerate(objs):
            for ignore_index in [True, False]:
                with self.subTest(ignore_index=ignore_index, join=join, pdfs=pdfs, pair=i):
                    actual = ps.concat(psdfs, axis=1, ignore_index=ignore_index, join=join)
                    expected = pd.concat(pdfs, axis=1, ignore_index=ignore_index, join=join)
                    self.assert_eq(
                        repr(actual.sort_values(list(actual.columns)).reset_index(drop=True)),
                        repr(expected.sort_values(list(expected.columns)).reset_index(drop=True)),
                    )
                    actual = ps.concat(
                        psdfs, axis=1, ignore_index=ignore_index, join=join, sort=True
                    )
                    expected = pd.concat(
                        pdfs, axis=1, ignore_index=ignore_index, join=join, sort=True
                    )
                    self.assert_eq(
                        repr(actual.reset_index(drop=True)),
                        repr(expected.reset_index(drop=True)),
                    )


class ConcatInnerTests(
    ConcatInnerMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_concat_inner import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
