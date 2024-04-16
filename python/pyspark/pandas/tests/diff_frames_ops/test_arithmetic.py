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
from pyspark.pandas.typedef.typehints import extension_float_dtypes_available


class ArithmeticTestingFuncMixin:
    def _test_arithmetic_frame(self, pdf1, pdf2, *, check_extension):
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        # Series
        self.assert_eq((psdf1.a - psdf2.b).sort_index(), (pdf1.a - pdf2.b).sort_index())

        self.assert_eq((psdf1.a * psdf2.a).sort_index(), (pdf1.a * pdf2.a).sort_index())

        if check_extension and not extension_float_dtypes_available:
            self.assert_eq(
                (psdf1["a"] / psdf2["a"]).sort_index(), (pdf1["a"] / pdf2["a"]).sort_index()
            )
        else:
            self.assert_eq(
                (psdf1["a"] / psdf2["a"]).sort_index(), (pdf1["a"] / pdf2["a"]).sort_index()
            )

        # DataFrame
        self.assert_eq((psdf1 + psdf2).sort_index(), (pdf1 + pdf2).sort_index())

        # Multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        psdf1.columns = columns
        psdf2.columns = columns
        pdf1.columns = columns
        pdf2.columns = columns

        # Series
        self.assert_eq(
            (psdf1[("x", "a")] - psdf2[("x", "b")]).sort_index(),
            (pdf1[("x", "a")] - pdf2[("x", "b")]).sort_index(),
        )

        self.assert_eq(
            (psdf1[("x", "a")] - psdf2["x"]["b"]).sort_index(),
            (pdf1[("x", "a")] - pdf2["x"]["b"]).sort_index(),
        )

        self.assert_eq(
            (psdf1["x"]["a"] - psdf2[("x", "b")]).sort_index(),
            (pdf1["x"]["a"] - pdf2[("x", "b")]).sort_index(),
        )

        # DataFrame
        self.assert_eq((psdf1 + psdf2).sort_index(), (pdf1 + pdf2).sort_index())

    def _test_arithmetic_series(self, pser1, pser2, *, check_extension):
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        # MultiIndex Series
        self.assert_eq((psser1 + psser2).sort_index(), (pser1 + pser2).sort_index())

        self.assert_eq((psser1 - psser2).sort_index(), (pser1 - pser2).sort_index())

        self.assert_eq((psser1 * psser2).sort_index(), (pser1 * pser2).sort_index())

        if check_extension and not extension_float_dtypes_available:
            self.assert_eq((psser1 / psser2).sort_index(), (pser1 / pser2).sort_index())
        else:
            self.assert_eq((psser1 / psser2).sort_index(), (pser1 / pser2).sort_index())


class ArithmeticMixin(ArithmeticTestingFuncMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    @property
    def pdf1(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 10, 11],
        )

    @property
    def pdf2(self):
        return pd.DataFrame(
            {"a": [9, 8, 7, 6, 5, 4, 3, 2, 1], "b": [0, 0, 0, 4, 5, 6, 1, 2, 3]},
            index=list(range(9)),
        )

    @property
    def pdf5(self):
        return pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "b": [4, 5, 6, 3, 2, 1, 0, 0, 0],
                "c": [4, 5, 6, 3, 2, 1, 0, 0, 0],
            },
            index=[0, 1, 3, 5, 6, 8, 9, 10, 11],
        ).set_index(["a", "b"])

    @property
    def pdf6(self):
        return pd.DataFrame(
            {
                "a": [9, 8, 7, 6, 5, 4, 3, 2, 1],
                "b": [0, 0, 0, 4, 5, 6, 1, 2, 3],
                "c": [9, 8, 7, 6, 5, 4, 3, 2, 1],
                "e": [4, 5, 6, 3, 2, 1, 0, 0, 0],
            },
            index=list(range(9)),
        ).set_index(["a", "b"])

    @property
    def pser1(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon", "koala"], ["speed", "weight", "length", "power"]],
            [[0, 3, 1, 1, 1, 2, 2, 2], [0, 2, 0, 3, 2, 0, 1, 3]],
        )
        return pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1], index=midx)

    @property
    def pser2(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        return pd.Series([-45, 200, -1.2, 30, -250, 1.5, 320, 1, -0.3], index=midx)

    @property
    def psdf5(self):
        return ps.from_pandas(self.pdf5)

    @property
    def psdf6(self):
        return ps.from_pandas(self.pdf6)

    def test_arithmetic(self):
        self._test_arithmetic_frame(self.pdf1, self.pdf2, check_extension=False)
        self._test_arithmetic_series(self.pser1, self.pser2, check_extension=False)

    def test_multi_index_arithmetic(self):
        psdf5 = self.psdf5
        psdf6 = self.psdf6
        pdf5 = self.pdf5
        pdf6 = self.pdf6

        # Series
        self.assert_eq((psdf5.c - psdf6.e).sort_index(), (pdf5.c - pdf6.e).sort_index())

        self.assert_eq((psdf5["c"] / psdf6["e"]).sort_index(), (pdf5["c"] / pdf6["e"]).sort_index())

        # DataFrame
        self.assert_eq((psdf5 + psdf6).sort_index(), (pdf5 + pdf6).sort_index(), almost=True)


class ArithmeticTests(
    ArithmeticMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_arithmetic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
