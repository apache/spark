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


class ArithmeticChainTestingFuncMixin:
    def _test_arithmetic_chain_frame(self, pdf1, pdf2, pdf3, *, check_extension):
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)
        psdf3 = ps.from_pandas(pdf3)

        # Series
        self.assert_eq(
            (psdf1.a - psdf2.b - psdf3.c).sort_index(), (pdf1.a - pdf2.b - pdf3.c).sort_index()
        )

        self.assert_eq(
            (psdf1.a * (psdf2.a * psdf3.c)).sort_index(), (pdf1.a * (pdf2.a * pdf3.c)).sort_index()
        )

        if check_extension and not extension_float_dtypes_available:
            self.assert_eq(
                (psdf1["a"] / psdf2["a"] / psdf3["c"]).sort_index(),
                (pdf1["a"] / pdf2["a"] / pdf3["c"]).sort_index(),
            )
        else:
            self.assert_eq(
                (psdf1["a"] / psdf2["a"] / psdf3["c"]).sort_index(),
                (pdf1["a"] / pdf2["a"] / pdf3["c"]).sort_index(),
            )

        # DataFrame
        self.assert_eq((psdf1 + psdf2 - psdf3).sort_index(), (pdf1 + pdf2 - pdf3).sort_index())

        # Multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        psdf1.columns = columns
        psdf2.columns = columns
        pdf1.columns = columns
        pdf2.columns = columns
        columns = pd.MultiIndex.from_tuples([("x", "b"), ("y", "c")])
        psdf3.columns = columns
        pdf3.columns = columns

        # Series
        self.assert_eq(
            (psdf1[("x", "a")] - psdf2[("x", "b")] - psdf3[("y", "c")]).sort_index(),
            (pdf1[("x", "a")] - pdf2[("x", "b")] - pdf3[("y", "c")]).sort_index(),
        )

        self.assert_eq(
            (psdf1[("x", "a")] * (psdf2[("x", "b")] * psdf3[("y", "c")])).sort_index(),
            (pdf1[("x", "a")] * (pdf2[("x", "b")] * pdf3[("y", "c")])).sort_index(),
        )

        # DataFrame
        self.assert_eq((psdf1 + psdf2 - psdf3).sort_index(), (pdf1 + pdf2 - pdf3).sort_index())

    def _test_arithmetic_chain_series(self, pser1, pser2, pser3, *, check_extension):
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)
        psser3 = ps.from_pandas(pser3)

        # MultiIndex Series
        self.assert_eq(
            self.sort_index_with_values(psser1 + psser2 - psser3),
            self.sort_index_with_values(pser1 + pser2 - pser3),
        )

        self.assert_eq(
            self.sort_index_with_values(psser1 * psser2 * psser3),
            self.sort_index_with_values(pser1 * pser2 * pser3),
        )

        if check_extension and not extension_float_dtypes_available:
            self.assert_eq(
                self.sort_index_with_values(psser1 - psser2 / psser3),
                self.sort_index_with_values(pser1 - pser2 / pser3),
            )
        else:
            self.assert_eq(
                self.sort_index_with_values(psser1 - psser2 / psser3),
                self.sort_index_with_values(pser1 - pser2 / pser3),
            )

        self.assert_eq(
            self.sort_index_with_values(psser1 + psser2 * psser3),
            self.sort_index_with_values(pser1 + pser2 * pser3),
        )


class ArithmeticChainMixin(ArithmeticChainTestingFuncMixin):
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
    def pdf3(self):
        return pd.DataFrame(
            {"b": [1, 1, 1, 1, 1, 1, 1, 1, 1], "c": [1, 1, 1, 1, 1, 1, 1, 1, 1]},
            index=list(range(9)),
        )

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
    def pser3(self):
        midx = pd.MultiIndex(
            [["koalas", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [1, 1, 2, 0, 0, 2, 2, 2, 1]],
        )
        return pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)

    def test_arithmetic_chain(self):
        self._test_arithmetic_chain_frame(self.pdf1, self.pdf2, self.pdf3, check_extension=False)
        self._test_arithmetic_chain_series(
            self.pser1, self.pser2, self.pser3, check_extension=False
        )


class ArithmeticChainTests(
    ArithmeticChainMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_arithmetic_chain import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
