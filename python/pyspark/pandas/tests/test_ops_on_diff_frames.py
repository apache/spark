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

from distutils.version import LooseVersion
from itertools import product
import unittest

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.pandas.frame import DataFrame
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.typedef.typehints import (
    extension_dtypes,
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)


class OpsOnDiffFramesEnabledTest(PandasOnSparkTestCase, SQLTestUtils):
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
    def pdf4(self):
        return pd.DataFrame(
            {"e": [2, 2, 2, 2, 2, 2, 2, 2, 2], "f": [2, 2, 2, 2, 2, 2, 2, 2, 2]},
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
    def pser3(self):
        midx = pd.MultiIndex(
            [["koalas", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [1, 1, 2, 0, 0, 2, 2, 2, 1]],
        )
        return pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)

    @property
    def kdf1(self):
        return ps.from_pandas(self.pdf1)

    @property
    def kdf2(self):
        return ps.from_pandas(self.pdf2)

    @property
    def kdf3(self):
        return ps.from_pandas(self.pdf3)

    @property
    def kdf4(self):
        return ps.from_pandas(self.pdf4)

    @property
    def kdf5(self):
        return ps.from_pandas(self.pdf5)

    @property
    def kdf6(self):
        return ps.from_pandas(self.pdf6)

    @property
    def kser1(self):
        return ps.from_pandas(self.pser1)

    @property
    def kser2(self):
        return ps.from_pandas(self.pser2)

    @property
    def kser3(self):
        return ps.from_pandas(self.pser3)

    def test_ranges(self):
        self.assert_eq(
            (ps.range(10) + ps.range(10)).sort_index(),
            (
                ps.DataFrame({"id": list(range(10))}) + ps.DataFrame({"id": list(range(10))})
            ).sort_index(),
        )

    def test_no_matched_index(self):
        with self.assertRaisesRegex(ValueError, "Index names must be exactly matched"):
            ps.DataFrame({"a": [1, 2, 3]}).set_index("a") + ps.DataFrame(
                {"b": [1, 2, 3]}
            ).set_index("b")

    def test_arithmetic(self):
        self._test_arithmetic_frame(self.pdf1, self.pdf2, check_extension=False)
        self._test_arithmetic_series(self.pser1, self.pser2, check_extension=False)

    @unittest.skipIf(not extension_dtypes_available, "pandas extension dtypes are not available")
    def test_arithmetic_extension_dtypes(self):
        self._test_arithmetic_frame(
            self.pdf1.astype("Int64"), self.pdf2.astype("Int64"), check_extension=True
        )
        self._test_arithmetic_series(
            self.pser1.astype(int).astype("Int64"),
            self.pser2.astype(int).astype("Int64"),
            check_extension=True,
        )

    @unittest.skipIf(
        not extension_float_dtypes_available, "pandas extension float dtypes are not available"
    )
    def test_arithmetic_extension_float_dtypes(self):
        self._test_arithmetic_frame(
            self.pdf1.astype("Float64"), self.pdf2.astype("Float64"), check_extension=True
        )
        self._test_arithmetic_series(
            self.pser1.astype("Float64"), self.pser2.astype("Float64"), check_extension=True
        )

    def _test_arithmetic_frame(self, pdf1, pdf2, *, check_extension):
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        def assert_eq(actual, expected):
            if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
                self.assert_eq(actual, expected, check_exact=not check_extension)
                if check_extension:
                    if isinstance(actual, DataFrame):
                        for dtype in actual.dtypes:
                            self.assertTrue(isinstance(dtype, extension_dtypes))
                    else:
                        self.assertTrue(isinstance(actual.dtype, extension_dtypes))
            else:
                self.assert_eq(actual, expected)

        # Series
        assert_eq((kdf1.a - kdf2.b).sort_index(), (pdf1.a - pdf2.b).sort_index())

        assert_eq((kdf1.a * kdf2.a).sort_index(), (pdf1.a * pdf2.a).sort_index())

        if check_extension and not extension_float_dtypes_available:
            self.assert_eq(
                (kdf1["a"] / kdf2["a"]).sort_index(), (pdf1["a"] / pdf2["a"]).sort_index()
            )
        else:
            assert_eq((kdf1["a"] / kdf2["a"]).sort_index(), (pdf1["a"] / pdf2["a"]).sort_index())

        # DataFrame
        assert_eq((kdf1 + kdf2).sort_index(), (pdf1 + pdf2).sort_index())

        # Multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        kdf1.columns = columns
        kdf2.columns = columns
        pdf1.columns = columns
        pdf2.columns = columns

        # Series
        assert_eq(
            (kdf1[("x", "a")] - kdf2[("x", "b")]).sort_index(),
            (pdf1[("x", "a")] - pdf2[("x", "b")]).sort_index(),
        )

        assert_eq(
            (kdf1[("x", "a")] - kdf2["x"]["b"]).sort_index(),
            (pdf1[("x", "a")] - pdf2["x"]["b"]).sort_index(),
        )

        assert_eq(
            (kdf1["x"]["a"] - kdf2[("x", "b")]).sort_index(),
            (pdf1["x"]["a"] - pdf2[("x", "b")]).sort_index(),
        )

        # DataFrame
        assert_eq((kdf1 + kdf2).sort_index(), (pdf1 + pdf2).sort_index())

    def _test_arithmetic_series(self, pser1, pser2, *, check_extension):
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        def assert_eq(actual, expected):
            if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
                self.assert_eq(actual, expected, check_exact=not check_extension)
                if check_extension:
                    self.assertTrue(isinstance(actual.dtype, extension_dtypes))
            else:
                self.assert_eq(actual, expected)

        # MultiIndex Series
        assert_eq((kser1 + kser2).sort_index(), (pser1 + pser2).sort_index())

        assert_eq((kser1 - kser2).sort_index(), (pser1 - pser2).sort_index())

        assert_eq((kser1 * kser2).sort_index(), (pser1 * pser2).sort_index())

        if check_extension and not extension_float_dtypes_available:
            self.assert_eq((kser1 / kser2).sort_index(), (pser1 / pser2).sort_index())
        else:
            assert_eq((kser1 / kser2).sort_index(), (pser1 / pser2).sort_index())

    def test_arithmetic_chain(self):
        self._test_arithmetic_chain_frame(self.pdf1, self.pdf2, self.pdf3, check_extension=False)
        self._test_arithmetic_chain_series(
            self.pser1, self.pser2, self.pser3, check_extension=False
        )

    @unittest.skipIf(not extension_dtypes_available, "pandas extension dtypes are not available")
    def test_arithmetic_chain_extension_dtypes(self):
        self._test_arithmetic_chain_frame(
            self.pdf1.astype("Int64"),
            self.pdf2.astype("Int64"),
            self.pdf3.astype("Int64"),
            check_extension=True,
        )
        self._test_arithmetic_chain_series(
            self.pser1.astype(int).astype("Int64"),
            self.pser2.astype(int).astype("Int64"),
            self.pser3.astype(int).astype("Int64"),
            check_extension=True,
        )

    @unittest.skipIf(
        not extension_float_dtypes_available, "pandas extension float dtypes are not available"
    )
    def test_arithmetic_chain_extension_float_dtypes(self):
        self._test_arithmetic_chain_frame(
            self.pdf1.astype("Float64"),
            self.pdf2.astype("Float64"),
            self.pdf3.astype("Float64"),
            check_extension=True,
        )
        self._test_arithmetic_chain_series(
            self.pser1.astype("Float64"),
            self.pser2.astype("Float64"),
            self.pser3.astype("Float64"),
            check_extension=True,
        )

    def _test_arithmetic_chain_frame(self, pdf1, pdf2, pdf3, *, check_extension):
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)
        kdf3 = ps.from_pandas(pdf3)

        common_columns = set(kdf1.columns).intersection(kdf2.columns).intersection(kdf3.columns)

        def assert_eq(actual, expected):
            if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
                self.assert_eq(actual, expected, check_exact=not check_extension)
                if check_extension:
                    if isinstance(actual, DataFrame):
                        for column, dtype in zip(actual.columns, actual.dtypes):
                            if column in common_columns:
                                self.assertTrue(isinstance(dtype, extension_dtypes))
                            else:
                                self.assertFalse(isinstance(dtype, extension_dtypes))
                    else:
                        self.assertTrue(isinstance(actual.dtype, extension_dtypes))
            else:
                self.assert_eq(actual, expected)

        # Series
        assert_eq((kdf1.a - kdf2.b - kdf3.c).sort_index(), (pdf1.a - pdf2.b - pdf3.c).sort_index())

        assert_eq(
            (kdf1.a * (kdf2.a * kdf3.c)).sort_index(), (pdf1.a * (pdf2.a * pdf3.c)).sort_index()
        )

        if check_extension and not extension_float_dtypes_available:
            self.assert_eq(
                (kdf1["a"] / kdf2["a"] / kdf3["c"]).sort_index(),
                (pdf1["a"] / pdf2["a"] / pdf3["c"]).sort_index(),
            )
        else:
            assert_eq(
                (kdf1["a"] / kdf2["a"] / kdf3["c"]).sort_index(),
                (pdf1["a"] / pdf2["a"] / pdf3["c"]).sort_index(),
            )

        # DataFrame
        if check_extension and (
            LooseVersion("1.0") <= LooseVersion(pd.__version__) < LooseVersion("1.1")
        ):
            self.assert_eq(
                (kdf1 + kdf2 - kdf3).sort_index(), (pdf1 + pdf2 - pdf3).sort_index(), almost=True
            )
        else:
            assert_eq((kdf1 + kdf2 - kdf3).sort_index(), (pdf1 + pdf2 - pdf3).sort_index())

        # Multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        kdf1.columns = columns
        kdf2.columns = columns
        pdf1.columns = columns
        pdf2.columns = columns
        columns = pd.MultiIndex.from_tuples([("x", "b"), ("y", "c")])
        kdf3.columns = columns
        pdf3.columns = columns

        common_columns = set(kdf1.columns).intersection(kdf2.columns).intersection(kdf3.columns)

        # Series
        assert_eq(
            (kdf1[("x", "a")] - kdf2[("x", "b")] - kdf3[("y", "c")]).sort_index(),
            (pdf1[("x", "a")] - pdf2[("x", "b")] - pdf3[("y", "c")]).sort_index(),
        )

        assert_eq(
            (kdf1[("x", "a")] * (kdf2[("x", "b")] * kdf3[("y", "c")])).sort_index(),
            (pdf1[("x", "a")] * (pdf2[("x", "b")] * pdf3[("y", "c")])).sort_index(),
        )

        # DataFrame
        if check_extension and (
            LooseVersion("1.0") <= LooseVersion(pd.__version__) < LooseVersion("1.1")
        ):
            self.assert_eq(
                (kdf1 + kdf2 - kdf3).sort_index(), (pdf1 + pdf2 - pdf3).sort_index(), almost=True
            )
        else:
            assert_eq((kdf1 + kdf2 - kdf3).sort_index(), (pdf1 + pdf2 - pdf3).sort_index())

    def _test_arithmetic_chain_series(self, pser1, pser2, pser3, *, check_extension):
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)
        kser3 = ps.from_pandas(pser3)

        def assert_eq(actual, expected):
            if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
                self.assert_eq(actual, expected, check_exact=not check_extension)
                if check_extension:
                    self.assertTrue(isinstance(actual.dtype, extension_dtypes))
            else:
                self.assert_eq(actual, expected)

        # MultiIndex Series
        assert_eq((kser1 + kser2 - kser3).sort_index(), (pser1 + pser2 - pser3).sort_index())

        assert_eq((kser1 * kser2 * kser3).sort_index(), (pser1 * pser2 * pser3).sort_index())

        if check_extension and not extension_float_dtypes_available:
            if LooseVersion(pd.__version__) >= LooseVersion("1.0"):
                self.assert_eq(
                    (kser1 - kser2 / kser3).sort_index(), (pser1 - pser2 / pser3).sort_index()
                )
            else:
                expected = pd.Series(
                    [249.0, np.nan, 0.0, 0.88, np.nan, np.nan, np.nan, np.nan, np.nan, -np.inf]
                    + [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
                    index=pd.MultiIndex(
                        [
                            ["cow", "falcon", "koala", "koalas", "lama"],
                            ["length", "power", "speed", "weight"],
                        ],
                        [
                            [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 3, 3, 3, 4, 4, 4],
                            [0, 1, 2, 2, 3, 0, 0, 1, 2, 3, 0, 0, 3, 3, 0, 2, 3],
                        ],
                    ),
                )
                self.assert_eq((kser1 - kser2 / kser3).sort_index(), expected)
        else:
            assert_eq((kser1 - kser2 / kser3).sort_index(), (pser1 - pser2 / pser3).sort_index())

        assert_eq((kser1 + kser2 * kser3).sort_index(), (pser1 + pser2 * pser3).sort_index())

    def test_mod(self):
        pser = pd.Series([100, None, -300, None, 500, -700])
        pser_other = pd.Series([-150] * 6)
        kser = ps.from_pandas(pser)
        kser_other = ps.from_pandas(pser_other)

        self.assert_eq(kser.mod(kser_other).sort_index(), pser.mod(pser_other))
        self.assert_eq(kser.mod(kser_other).sort_index(), pser.mod(pser_other))
        self.assert_eq(kser.mod(kser_other).sort_index(), pser.mod(pser_other))

    def test_rmod(self):
        pser = pd.Series([100, None, -300, None, 500, -700])
        pser_other = pd.Series([-150] * 6)
        kser = ps.from_pandas(pser)
        kser_other = ps.from_pandas(pser_other)

        self.assert_eq(kser.rmod(kser_other).sort_index(), pser.rmod(pser_other))
        self.assert_eq(kser.rmod(kser_other).sort_index(), pser.rmod(pser_other))
        self.assert_eq(kser.rmod(kser_other).sort_index(), pser.rmod(pser_other))

    def test_getitem_boolean_series(self):
        pdf1 = pd.DataFrame(
            {"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]}, index=[20, 10, 30, 0, 50]
        )
        pdf2 = pd.DataFrame(
            {"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]},
            index=[0, 30, 10, 20, 50],
        )
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1[pdf2.A > -3].sort_index(), kdf1[kdf2.A > -3].sort_index())

        self.assert_eq(pdf1.A[pdf2.A > -3].sort_index(), kdf1.A[kdf2.A > -3].sort_index())

        self.assert_eq(
            (pdf1.A + 1)[pdf2.A > -3].sort_index(), (kdf1.A + 1)[kdf2.A > -3].sort_index()
        )

    def test_loc_getitem_boolean_series(self):
        pdf1 = pd.DataFrame(
            {"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]}, index=[20, 10, 30, 0, 50]
        )
        pdf2 = pd.DataFrame(
            {"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]},
            index=[20, 10, 30, 0, 50],
        )
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.loc[pdf2.A > -3].sort_index(), kdf1.loc[kdf2.A > -3].sort_index())

        self.assert_eq(pdf1.A.loc[pdf2.A > -3].sort_index(), kdf1.A.loc[kdf2.A > -3].sort_index())

        self.assert_eq(
            (pdf1.A + 1).loc[pdf2.A > -3].sort_index(), (kdf1.A + 1).loc[kdf2.A > -3].sort_index()
        )

    def test_bitwise(self):
        pser1 = pd.Series([True, False, True, False, np.nan, np.nan, True, False, np.nan])
        pser2 = pd.Series([True, False, False, True, True, False, np.nan, np.nan, np.nan])
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        self.assert_eq(pser1 | pser2, (kser1 | kser2).sort_index())
        self.assert_eq(pser1 & pser2, (kser1 & kser2).sort_index())

        pser1 = pd.Series([True, False, np.nan], index=list("ABC"))
        pser2 = pd.Series([False, True, np.nan], index=list("DEF"))
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        self.assert_eq(pser1 | pser2, (kser1 | kser2).sort_index())
        self.assert_eq(pser1 & pser2, (kser1 & kser2).sort_index())

    @unittest.skipIf(
        not extension_object_dtypes_available, "pandas extension object dtypes are not available"
    )
    def test_bitwise_extension_dtype(self):
        def assert_eq(actual, expected):
            if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
                self.assert_eq(actual, expected, check_exact=False)
                self.assertTrue(isinstance(actual.dtype, extension_dtypes))
            else:
                self.assert_eq(actual, expected)

        pser1 = pd.Series(
            [True, False, True, False, np.nan, np.nan, True, False, np.nan], dtype="boolean"
        )
        pser2 = pd.Series(
            [True, False, False, True, True, False, np.nan, np.nan, np.nan], dtype="boolean"
        )
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        assert_eq((kser1 | kser2).sort_index(), pser1 | pser2)
        assert_eq((kser1 & kser2).sort_index(), pser1 & pser2)

        pser1 = pd.Series([True, False, np.nan], index=list("ABC"), dtype="boolean")
        pser2 = pd.Series([False, True, np.nan], index=list("DEF"), dtype="boolean")
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        # a pandas bug?
        # assert_eq((kser1 | kser2).sort_index(), pser1 | pser2)
        # assert_eq((kser1 & kser2).sort_index(), pser1 & pser2)
        assert_eq(
            (kser1 | kser2).sort_index(),
            pd.Series([True, None, None, None, True, None], index=list("ABCDEF"), dtype="boolean"),
        )
        assert_eq(
            (kser1 & kser2).sort_index(),
            pd.Series(
                [None, False, None, False, None, None], index=list("ABCDEF"), dtype="boolean"
            ),
        )

    def test_concat_column_axis(self):
        pdf1 = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5]}, index=[1, 2, 3])
        pdf1.columns.names = ["AB"]
        pdf2 = pd.DataFrame({"C": [1, 2, 3], "D": [4, 5, 6]}, index=[1, 3, 5])
        pdf2.columns.names = ["CD"]
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        kdf3 = kdf1.copy()
        kdf4 = kdf2.copy()
        pdf3 = pdf1.copy()
        pdf4 = pdf2.copy()

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")], names=["X", "AB"])
        pdf3.columns = columns
        kdf3.columns = columns

        columns = pd.MultiIndex.from_tuples([("X", "C"), ("X", "D")], names=["Y", "CD"])
        pdf4.columns = columns
        kdf4.columns = columns

        pdf5 = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5]}, index=[1, 2, 3])
        pdf6 = pd.DataFrame({"C": [1, 2, 3]}, index=[1, 3, 5])
        kdf5 = ps.from_pandas(pdf5)
        kdf6 = ps.from_pandas(pdf6)

        ignore_indexes = [True, False]
        joins = ["inner", "outer"]

        objs = [
            ([kdf1.A, kdf2.C], [pdf1.A, pdf2.C]),
            # TODO: ([kdf1, kdf2.C], [pdf1, pdf2.C]),
            ([kdf1.A, kdf2], [pdf1.A, pdf2]),
            ([kdf1.A, kdf2.C], [pdf1.A, pdf2.C]),
            ([kdf3[("X", "A")], kdf4[("X", "C")]], [pdf3[("X", "A")], pdf4[("X", "C")]]),
            ([kdf3, kdf4[("X", "C")]], [pdf3, pdf4[("X", "C")]]),
            ([kdf3[("X", "A")], kdf4], [pdf3[("X", "A")], pdf4]),
            ([kdf3, kdf4], [pdf3, pdf4]),
            ([kdf5, kdf6], [pdf5, pdf6]),
            ([kdf6, kdf5], [pdf6, pdf5]),
        ]

        for ignore_index, join in product(ignore_indexes, joins):
            for i, (kdfs, pdfs) in enumerate(objs):
                with self.subTest(ignore_index=ignore_index, join=join, pdfs=pdfs, pair=i):
                    actual = ps.concat(kdfs, axis=1, ignore_index=ignore_index, join=join)
                    expected = pd.concat(pdfs, axis=1, ignore_index=ignore_index, join=join)
                    self.assert_eq(
                        repr(actual.sort_values(list(actual.columns)).reset_index(drop=True)),
                        repr(expected.sort_values(list(expected.columns)).reset_index(drop=True)),
                    )

    def test_combine_first(self):
        pser1 = pd.Series({"falcon": 330.0, "eagle": 160.0})
        pser2 = pd.Series({"falcon": 345.0, "eagle": 200.0, "duck": 30.0})
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        self.assert_eq(
            kser1.combine_first(kser2).sort_index(), pser1.combine_first(pser2).sort_index()
        )
        with self.assertRaisesRegex(
            TypeError, "`combine_first` only allows `Series` for parameter `other`"
        ):
            kser1.combine_first(50)

        kser1.name = ("X", "A")
        kser2.name = ("Y", "B")
        pser1.name = ("X", "A")
        pser2.name = ("Y", "B")
        self.assert_eq(
            kser1.combine_first(kser2).sort_index(), pser1.combine_first(pser2).sort_index()
        )

        # MultiIndex
        midx1 = pd.MultiIndex(
            [["lama", "cow", "falcon", "koala"], ["speed", "weight", "length", "power"]],
            [[0, 3, 1, 1, 1, 2, 2, 2], [0, 2, 0, 3, 2, 0, 1, 3]],
        )
        midx2 = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser1 = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1], index=midx1)
        pser2 = pd.Series([-45, 200, -1.2, 30, -250, 1.5, 320, 1, -0.3], index=midx2)
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        self.assert_eq(
            kser1.combine_first(kser2).sort_index(), pser1.combine_first(pser2).sort_index()
        )

        # Series come from same DataFrame
        pdf = pd.DataFrame(
            {
                "A": {"falcon": 330.0, "eagle": 160.0},
                "B": {"falcon": 345.0, "eagle": 200.0, "duck": 30.0},
            }
        )
        pser1 = pdf.A
        pser2 = pdf.B
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        self.assert_eq(
            kser1.combine_first(kser2).sort_index(), pser1.combine_first(pser2).sort_index()
        )

        kser1.name = ("X", "A")
        kser2.name = ("Y", "B")
        pser1.name = ("X", "A")
        pser2.name = ("Y", "B")

        self.assert_eq(
            kser1.combine_first(kser2).sort_index(), pser1.combine_first(pser2).sort_index()
        )

    def test_insert(self):
        #
        # Basic DataFrame
        #
        pdf = pd.DataFrame([1, 2, 3])
        kdf = ps.from_pandas(pdf)

        pser = pd.Series([4, 5, 6])
        kser = ps.from_pandas(pser)
        kdf.insert(1, "y", kser)
        pdf.insert(1, "y", pser)
        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        #
        # DataFrame with Index different from inserting Series'
        #
        pdf = pd.DataFrame([1, 2, 3], index=[10, 20, 30])
        kdf = ps.from_pandas(pdf)

        pser = pd.Series([4, 5, 6])
        kser = ps.from_pandas(pser)
        kdf.insert(1, "y", kser)
        pdf.insert(1, "y", pser)
        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        #
        # DataFrame with Multi-index columns
        #
        pdf = pd.DataFrame({("x", "a"): [1, 2, 3]})
        kdf = ps.from_pandas(pdf)

        pser = pd.Series([4, 5, 6])
        kser = ps.from_pandas(pser)
        pdf = pd.DataFrame({("x", "a", "b"): [1, 2, 3]})
        kdf = ps.from_pandas(pdf)
        kdf.insert(0, "a", kser)
        pdf.insert(0, "a", pser)
        self.assert_eq(kdf.sort_index(), pdf.sort_index())
        kdf.insert(0, ("b", "c", ""), kser)
        pdf.insert(0, ("b", "c", ""), pser)
        self.assert_eq(kdf.sort_index(), pdf.sort_index())

    def test_compare(self):
        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            pser1 = pd.Series(["b", "c", np.nan, "g", np.nan])
            pser2 = pd.Series(["a", "c", np.nan, np.nan, "h"])
            kser1 = ps.from_pandas(pser1)
            kser2 = ps.from_pandas(pser2)
            self.assert_eq(
                pser1.compare(pser2).sort_index(), kser1.compare(kser2).sort_index(),
            )

            # `keep_shape=True`
            self.assert_eq(
                pser1.compare(pser2, keep_shape=True).sort_index(),
                kser1.compare(kser2, keep_shape=True).sort_index(),
            )
            # `keep_equal=True`
            self.assert_eq(
                pser1.compare(pser2, keep_equal=True).sort_index(),
                kser1.compare(kser2, keep_equal=True).sort_index(),
            )
            # `keep_shape=True` and `keep_equal=True`
            self.assert_eq(
                pser1.compare(pser2, keep_shape=True, keep_equal=True).sort_index(),
                kser1.compare(kser2, keep_shape=True, keep_equal=True).sort_index(),
            )

            # MultiIndex
            pser1.index = pd.MultiIndex.from_tuples(
                [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
            )
            pser2.index = pd.MultiIndex.from_tuples(
                [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
            )
            kser1 = ps.from_pandas(pser1)
            kser2 = ps.from_pandas(pser2)
            self.assert_eq(
                pser1.compare(pser2).sort_index(), kser1.compare(kser2).sort_index(),
            )

            # `keep_shape=True` with MultiIndex
            self.assert_eq(
                pser1.compare(pser2, keep_shape=True).sort_index(),
                kser1.compare(kser2, keep_shape=True).sort_index(),
            )
            # `keep_equal=True` with MultiIndex
            self.assert_eq(
                pser1.compare(pser2, keep_equal=True).sort_index(),
                kser1.compare(kser2, keep_equal=True).sort_index(),
            )
            # `keep_shape=True` and `keep_equal=True` with MultiIndex
            self.assert_eq(
                pser1.compare(pser2, keep_shape=True, keep_equal=True).sort_index(),
                kser1.compare(kser2, keep_shape=True, keep_equal=True).sort_index(),
            )
        else:
            kser1 = ps.Series(["b", "c", np.nan, "g", np.nan])
            kser2 = ps.Series(["a", "c", np.nan, np.nan, "h"])
            expected = ps.DataFrame(
                [["b", "a"], ["g", None], [None, "h"]], index=[0, 3, 4], columns=["self", "other"]
            )
            self.assert_eq(expected, kser1.compare(kser2).sort_index())

            # `keep_shape=True`
            expected = ps.DataFrame(
                [["b", "a"], [None, None], [None, None], ["g", None], [None, "h"]],
                index=[0, 1, 2, 3, 4],
                columns=["self", "other"],
            )
            self.assert_eq(
                expected, kser1.compare(kser2, keep_shape=True).sort_index(),
            )
            # `keep_equal=True`
            expected = ps.DataFrame(
                [["b", "a"], ["g", None], [None, "h"]], index=[0, 3, 4], columns=["self", "other"]
            )
            self.assert_eq(
                expected, kser1.compare(kser2, keep_equal=True).sort_index(),
            )
            # `keep_shape=True` and `keep_equal=True`
            expected = ps.DataFrame(
                [["b", "a"], ["c", "c"], [None, None], ["g", None], [None, "h"]],
                index=[0, 1, 2, 3, 4],
                columns=["self", "other"],
            )
            self.assert_eq(
                expected, kser1.compare(kser2, keep_shape=True, keep_equal=True).sort_index(),
            )

            # MultiIndex
            kser1 = ps.Series(
                ["b", "c", np.nan, "g", np.nan],
                index=pd.MultiIndex.from_tuples(
                    [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
                ),
            )
            kser2 = ps.Series(
                ["a", "c", np.nan, np.nan, "h"],
                index=pd.MultiIndex.from_tuples(
                    [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
                ),
            )
            expected = ps.DataFrame(
                [["b", "a"], [None, "h"], ["g", None]],
                index=pd.MultiIndex.from_tuples([("a", "x"), ("q", "l"), ("x", "k")]),
                columns=["self", "other"],
            )
            self.assert_eq(expected, kser1.compare(kser2).sort_index())

            # `keep_shape=True`
            expected = ps.DataFrame(
                [["b", "a"], [None, None], [None, None], [None, "h"], ["g", None]],
                index=pd.MultiIndex.from_tuples(
                    [("a", "x"), ("b", "y"), ("c", "z"), ("q", "l"), ("x", "k")]
                ),
                columns=["self", "other"],
            )
            self.assert_eq(
                expected, kser1.compare(kser2, keep_shape=True).sort_index(),
            )
            # `keep_equal=True`
            expected = ps.DataFrame(
                [["b", "a"], [None, "h"], ["g", None]],
                index=pd.MultiIndex.from_tuples([("a", "x"), ("q", "l"), ("x", "k")]),
                columns=["self", "other"],
            )
            self.assert_eq(
                expected, kser1.compare(kser2, keep_equal=True).sort_index(),
            )
            # `keep_shape=True` and `keep_equal=True`
            expected = ps.DataFrame(
                [["b", "a"], ["c", "c"], [None, None], [None, "h"], ["g", None]],
                index=pd.MultiIndex.from_tuples(
                    [("a", "x"), ("b", "y"), ("c", "z"), ("q", "l"), ("x", "k")]
                ),
                columns=["self", "other"],
            )
            self.assert_eq(
                expected, kser1.compare(kser2, keep_shape=True, keep_equal=True).sort_index(),
            )

        # Different Index
        with self.assertRaisesRegex(
            ValueError, "Can only compare identically-labeled Series objects"
        ):
            kser1 = ps.Series([1, 2, 3, 4, 5], index=pd.Index([1, 2, 3, 4, 5]),)
            kser2 = ps.Series([2, 2, 3, 4, 1], index=pd.Index([5, 4, 3, 2, 1]),)
            kser1.compare(kser2)
        # Different MultiIndex
        with self.assertRaisesRegex(
            ValueError, "Can only compare identically-labeled Series objects"
        ):
            kser1 = ps.Series(
                [1, 2, 3, 4, 5],
                index=pd.MultiIndex.from_tuples(
                    [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
                ),
            )
            kser2 = ps.Series(
                [2, 2, 3, 4, 1],
                index=pd.MultiIndex.from_tuples(
                    [("a", "x"), ("b", "y"), ("c", "a"), ("x", "k"), ("q", "l")]
                ),
            )
            kser1.compare(kser2)

    def test_different_columns(self):
        kdf1 = self.kdf1
        kdf4 = self.kdf4
        pdf1 = self.pdf1
        pdf4 = self.pdf4

        self.assert_eq((kdf1 + kdf4).sort_index(), (pdf1 + pdf4).sort_index(), almost=True)

        # Multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        kdf1.columns = columns
        pdf1.columns = columns
        columns = pd.MultiIndex.from_tuples([("z", "e"), ("z", "f")])
        kdf4.columns = columns
        pdf4.columns = columns

        self.assert_eq((kdf1 + kdf4).sort_index(), (pdf1 + pdf4).sort_index(), almost=True)

    def test_assignment_series(self):
        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kser = kdf.a
        pser = pdf.a
        kdf["a"] = self.kdf2.a
        pdf["a"] = self.pdf2.a

        self.assert_eq(kdf.sort_index(), pdf.sort_index())
        self.assert_eq(kser, pser)

        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kser = kdf.a
        pser = pdf.a
        kdf["a"] = self.kdf2.b
        pdf["a"] = self.pdf2.b

        self.assert_eq(kdf.sort_index(), pdf.sort_index())
        self.assert_eq(kser, pser)

        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kdf["c"] = self.kdf2.a
        pdf["c"] = self.pdf2.a

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        # Multi-index columns
        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        kdf.columns = columns
        pdf.columns = columns
        kdf[("y", "c")] = self.kdf2.a
        pdf[("y", "c")] = self.pdf2.a

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        pdf = pd.DataFrame({"a": [1, 2, 3], "Koalas": [0, 1, 2]}).set_index("Koalas", drop=False)
        kdf = ps.from_pandas(pdf)

        kdf.index.name = None
        kdf["NEW"] = ps.Series([100, 200, 300])

        pdf.index.name = None
        pdf["NEW"] = pd.Series([100, 200, 300])

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

    def test_assignment_frame(self):
        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kser = kdf.a
        pser = pdf.a
        kdf[["a", "b"]] = self.kdf1
        pdf[["a", "b"]] = self.pdf1

        self.assert_eq(kdf.sort_index(), pdf.sort_index())
        self.assert_eq(kser, pser)

        # 'c' does not exist in `kdf`.
        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kser = kdf.a
        pser = pdf.a
        kdf[["b", "c"]] = self.kdf1
        pdf[["b", "c"]] = self.pdf1

        self.assert_eq(kdf.sort_index(), pdf.sort_index())
        self.assert_eq(kser, pser)

        # 'c' and 'd' do not exist in `kdf`.
        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kdf[["c", "d"]] = self.kdf1
        pdf[["c", "d"]] = self.pdf1

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        # Multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kdf.columns = columns
        pdf.columns = columns
        kdf[[("y", "c"), ("z", "d")]] = self.kdf1
        pdf[[("y", "c"), ("z", "d")]] = self.pdf1

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kdf1 = ps.from_pandas(self.pdf1)
        pdf1 = self.pdf1
        kdf1.columns = columns
        pdf1.columns = columns
        kdf[["c", "d"]] = kdf1
        pdf[["c", "d"]] = pdf1

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

    def test_assignment_series_chain(self):
        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kdf["a"] = self.kdf1.a
        pdf["a"] = self.pdf1.a

        kdf["a"] = self.kdf2.b
        pdf["a"] = self.pdf2.b

        kdf["d"] = self.kdf3.c
        pdf["d"] = self.pdf3.c

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

    def test_assignment_frame_chain(self):
        kdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        kdf[["a", "b"]] = self.kdf1
        pdf[["a", "b"]] = self.pdf1

        kdf[["e", "f"]] = self.kdf3
        pdf[["e", "f"]] = self.pdf3

        kdf[["b", "c"]] = self.kdf2
        pdf[["b", "c"]] = self.pdf2

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

    def test_multi_index_arithmetic(self):
        kdf5 = self.kdf5
        kdf6 = self.kdf6
        pdf5 = self.pdf5
        pdf6 = self.pdf6

        # Series
        self.assert_eq((kdf5.c - kdf6.e).sort_index(), (pdf5.c - pdf6.e).sort_index())

        self.assert_eq((kdf5["c"] / kdf6["e"]).sort_index(), (pdf5["c"] / pdf6["e"]).sort_index())

        # DataFrame
        self.assert_eq((kdf5 + kdf6).sort_index(), (pdf5 + pdf6).sort_index(), almost=True)

    def test_multi_index_assignment_series(self):
        kdf = ps.from_pandas(self.pdf5)
        pdf = self.pdf5
        kdf["x"] = self.kdf6.e
        pdf["x"] = self.pdf6.e

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        kdf = ps.from_pandas(self.pdf5)
        pdf = self.pdf5
        kdf["e"] = self.kdf6.e
        pdf["e"] = self.pdf6.e

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        kdf = ps.from_pandas(self.pdf5)
        pdf = self.pdf5
        kdf["c"] = self.kdf6.e
        pdf["c"] = self.pdf6.e

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

    def test_multi_index_assignment_frame(self):
        kdf = ps.from_pandas(self.pdf5)
        pdf = self.pdf5
        kdf[["c"]] = self.kdf5
        pdf[["c"]] = self.pdf5

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        kdf = ps.from_pandas(self.pdf5)
        pdf = self.pdf5
        kdf[["x"]] = self.kdf5
        pdf[["x"]] = self.pdf5

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

        kdf = ps.from_pandas(self.pdf6)
        pdf = self.pdf6
        kdf[["x", "y"]] = self.kdf6
        pdf[["x", "y"]] = self.pdf6

        self.assert_eq(kdf.sort_index(), pdf.sort_index())

    def test_frame_loc_setitem(self):
        pdf_orig = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        kdf_orig = ps.DataFrame(pdf_orig)

        pdf = pdf_orig.copy()
        kdf = kdf_orig.copy()
        pser1 = pdf.max_speed
        pser2 = pdf.shield
        kser1 = kdf.max_speed
        kser2 = kdf.shield

        another_kdf = ps.DataFrame(pdf_orig)

        kdf.loc[["viper", "sidewinder"], ["shield"]] = -another_kdf.max_speed
        pdf.loc[["viper", "sidewinder"], ["shield"]] = -pdf.max_speed
        self.assert_eq(kdf, pdf)
        self.assert_eq(kser1, pser1)
        self.assert_eq(kser2, pser2)

        pdf = pdf_orig.copy()
        kdf = kdf_orig.copy()
        pser1 = pdf.max_speed
        pser2 = pdf.shield
        kser1 = kdf.max_speed
        kser2 = kdf.shield
        kdf.loc[another_kdf.max_speed < 5, ["shield"]] = -kdf.max_speed
        pdf.loc[pdf.max_speed < 5, ["shield"]] = -pdf.max_speed
        self.assert_eq(kdf, pdf)
        self.assert_eq(kser1, pser1)
        self.assert_eq(kser2, pser2)

        pdf = pdf_orig.copy()
        kdf = kdf_orig.copy()
        pser1 = pdf.max_speed
        pser2 = pdf.shield
        kser1 = kdf.max_speed
        kser2 = kdf.shield
        kdf.loc[another_kdf.max_speed < 5, ["shield"]] = -another_kdf.max_speed
        pdf.loc[pdf.max_speed < 5, ["shield"]] = -pdf.max_speed
        self.assert_eq(kdf, pdf)
        self.assert_eq(kser1, pser1)
        self.assert_eq(kser2, pser2)

    def test_frame_iloc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        kdf = ps.DataFrame(pdf)
        another_kdf = ps.DataFrame(pdf)

        kdf.iloc[[0, 1, 2], 1] = -another_kdf.max_speed
        pdf.iloc[[0, 1, 2], 1] = -pdf.max_speed
        self.assert_eq(kdf, pdf)

        with self.assertRaisesRegex(
            ValueError, "shape mismatch",
        ):
            kdf.iloc[[1, 2], [1]] = -another_kdf.max_speed

        kdf.iloc[[0, 1, 2], 1] = 10 * another_kdf.max_speed
        pdf.iloc[[0, 1, 2], 1] = 10 * pdf.max_speed
        self.assert_eq(kdf, pdf)

        with self.assertRaisesRegex(ValueError, "shape mismatch"):
            kdf.iloc[[0], 1] = 10 * another_kdf.max_speed

    def test_series_loc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        kdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        kser = kdf.x
        ksery = kdf.y

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        kser_another = ps.from_pandas(pser_another)

        kser.loc[kser % 2 == 1] = -kser_another
        pser.loc[pser % 2 == 1] = -pser_another
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        kdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        kser = kdf.x
        ksery = kdf.y
        kser.loc[kser_another % 2 == 1] = -kser
        pser.loc[pser_another % 2 == 1] = -pser
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        kdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        kser = kdf.x
        ksery = kdf.y
        kser.loc[kser_another % 2 == 1] = -kser
        pser.loc[pser_another % 2 == 1] = -pser
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        kdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        kser = kdf.x
        ksery = kdf.y
        kser.loc[kser_another % 2 == 1] = -kser_another
        pser.loc[pser_another % 2 == 1] = -pser_another
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        kdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        kser = kdf.x
        ksery = kdf.y
        kser.loc[["viper", "sidewinder"]] = -kser_another
        pser.loc[["viper", "sidewinder"]] = -pser_another
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        kdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        kser = kdf.x
        ksery = kdf.y
        kser.loc[kser_another % 2 == 1] = 10
        pser.loc[pser_another % 2 == 1] = 10
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

    def test_series_iloc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        psery = pdf.y
        kser = kdf.x
        ksery = kdf.y

        pser1 = pser + 1
        kser1 = kser + 1

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        kser_another = ps.from_pandas(pser_another)

        kser.iloc[[0, 1, 2]] = -kser_another
        pser.iloc[[0, 1, 2]] = -pser_another
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            kser.iloc[[1, 2]] = -kser_another

        kser.iloc[[0, 1, 2]] = 10 * kser_another
        pser.iloc[[0, 1, 2]] = 10 * pser_another
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            kser.iloc[[0]] = 10 * kser_another

        kser1.iloc[[0, 1, 2]] = -kser_another
        pser1.iloc[[0, 1, 2]] = -pser_another
        self.assert_eq(kser1, pser1)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            kser1.iloc[[1, 2]] = -kser_another

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        psery = pdf.y
        kser = kdf.x
        ksery = kdf.y

        piloc = pser.iloc
        kiloc = kser.iloc

        kiloc[[0, 1, 2]] = -kser_another
        piloc[[0, 1, 2]] = -pser_another
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            kiloc[[1, 2]] = -kser_another

        kiloc[[0, 1, 2]] = 10 * kser_another
        piloc[[0, 1, 2]] = 10 * pser_another
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)
        self.assert_eq(ksery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            kiloc[[0]] = 10 * kser_another

    def test_update(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        kser = kdf.x
        pser.update(pd.Series([4, 5, 6]))
        kser.update(ps.Series([4, 5, 6]))
        self.assert_eq(kser.sort_index(), pser.sort_index())
        self.assert_eq(kdf.sort_index(), pdf.sort_index())

    def test_where(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 > 100), kdf1.where(kdf2 > 100).sort_index())

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 < -250), kdf1.where(kdf2 < -250).sort_index())

        # multi-index columns
        pdf1 = pd.DataFrame({("X", "A"): [0, 1, 2, 3, 4], ("X", "B"): [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame(
            {("X", "A"): [0, -1, -2, -3, -4], ("X", "B"): [-100, -200, -300, -400, -500]}
        )
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 > 100), kdf1.where(kdf2 > 100).sort_index())

    def test_mask(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 < 100), kdf1.mask(kdf2 < 100).sort_index())

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 > -250), kdf1.mask(kdf2 > -250).sort_index())

        # multi-index columns
        pdf1 = pd.DataFrame({("X", "A"): [0, 1, 2, 3, 4], ("X", "B"): [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame(
            {("X", "A"): [0, -1, -2, -3, -4], ("X", "B"): [-100, -200, -300, -400, -500]}
        )
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 < 100), kdf1.mask(kdf2 < 100).sort_index())

    def test_multi_index_column_assignment_frame(self):
        pdf = pd.DataFrame({"a": [1, 2, 3, 2], "b": [4.0, 2.0, 3.0, 1.0]})
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        kdf = ps.DataFrame(pdf)

        kdf["c"] = ps.Series([10, 20, 30, 20])
        pdf["c"] = pd.Series([10, 20, 30, 20])

        kdf[("d", "x")] = ps.Series([100, 200, 300, 200], name="1")
        pdf[("d", "x")] = pd.Series([100, 200, 300, 200], name="1")

        kdf[("d", "y")] = ps.Series([1000, 2000, 3000, 2000], name=("1", "2"))
        pdf[("d", "y")] = pd.Series([1000, 2000, 3000, 2000], name=("1", "2"))

        kdf["e"] = ps.Series([10000, 20000, 30000, 20000], name=("1", "2", "3"))
        pdf["e"] = pd.Series([10000, 20000, 30000, 20000], name=("1", "2", "3"))

        kdf[[("f", "x"), ("f", "y")]] = ps.DataFrame(
            {"1": [100000, 200000, 300000, 200000], "2": [1000000, 2000000, 3000000, 2000000]}
        )
        pdf[[("f", "x"), ("f", "y")]] = pd.DataFrame(
            {"1": [100000, 200000, 300000, 200000], "2": [1000000, 2000000, 3000000, 2000000]}
        )

        self.assert_eq(repr(kdf.sort_index()), repr(pdf))

        with self.assertRaisesRegex(KeyError, "Key length \\(3\\) exceeds index depth \\(2\\)"):
            kdf[("1", "2", "3")] = ps.Series([100, 200, 300, 200])

    def test_series_dot(self):
        pser = pd.Series([90, 91, 85], index=[2, 4, 1])
        kser = ps.from_pandas(pser)
        pser_other = pd.Series([90, 91, 85], index=[2, 4, 1])
        kser_other = ps.from_pandas(pser_other)

        self.assert_eq(kser.dot(kser_other), pser.dot(pser_other))

        kser_other = ps.Series([90, 91, 85], index=[1, 2, 4])
        pser_other = pd.Series([90, 91, 85], index=[1, 2, 4])

        self.assert_eq(kser.dot(kser_other), pser.dot(pser_other))

        # length of index is different
        kser_other = ps.Series([90, 91, 85, 100], index=[2, 4, 1, 0])
        with self.assertRaisesRegex(ValueError, "matrices are not aligned"):
            kser.dot(kser_other)

        # for MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        kser = ps.from_pandas(pser)
        pser_other = pd.Series([-450, 20, 12, -30, -250, 15, -320, 100, 3], index=midx)
        kser_other = ps.from_pandas(pser_other)
        self.assert_eq(kser.dot(kser_other), pser.dot(pser_other))

        pser = pd.Series([0, 1, 2, 3])
        kser = ps.from_pandas(pser)

        # DataFrame "other" without Index/MultiIndex as columns
        pdf = pd.DataFrame([[0, 1], [-2, 3], [4, -5], [6, 7]])
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kser.dot(kdf), pser.dot(pdf))

        # DataFrame "other" with Index as columns
        pdf.columns = pd.Index(["x", "y"])
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kser.dot(kdf), pser.dot(pdf))
        pdf.columns = pd.Index(["x", "y"], name="cols_name")
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kser.dot(kdf), pser.dot(pdf))

        pdf = pdf.reindex([1, 0, 2, 3])
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kser.dot(kdf), pser.dot(pdf))

        # DataFrame "other" with MultiIndex as columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kser.dot(kdf), pser.dot(pdf))
        pdf.columns = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y")], names=["cols_name1", "cols_name2"]
        )
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kser.dot(kdf), pser.dot(pdf))

        kser = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).b
        pser = kser.to_pandas()
        kdf = ps.DataFrame({"c": [7, 8, 9]})
        pdf = kdf.to_pandas()
        self.assert_eq(kser.dot(kdf), pser.dot(pdf))

    def test_frame_dot(self):
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]])
        kdf = ps.from_pandas(pdf)

        pser = pd.Series([1, 1, 2, 1])
        kser = ps.from_pandas(pser)
        self.assert_eq(kdf.dot(kser), pdf.dot(pser))

        # Index reorder
        pser = pser.reindex([1, 0, 2, 3])
        kser = ps.from_pandas(pser)
        self.assert_eq(kdf.dot(kser), pdf.dot(pser))

        # ser with name
        pser.name = "ser"
        kser = ps.from_pandas(pser)
        self.assert_eq(kdf.dot(kser), pdf.dot(pser))

        # df with MultiIndex as column (ser with MultiIndex)
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pser = pd.Series([1, 1, 2, 1], index=pidx)
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]], columns=pidx)
        kdf = ps.from_pandas(pdf)
        kser = ps.from_pandas(pser)
        self.assert_eq(kdf.dot(kser), pdf.dot(pser))

        # df with Index as column (ser with Index)
        pidx = pd.Index([1, 2, 3, 4], name="number")
        pser = pd.Series([1, 1, 2, 1], index=pidx)
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]], columns=pidx)
        kdf = ps.from_pandas(pdf)
        kser = ps.from_pandas(pser)
        self.assert_eq(kdf.dot(kser), pdf.dot(pser))

        # df with Index
        pdf.index = pd.Index(["x", "y"], name="char")
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kdf.dot(kser), pdf.dot(pser))

        # df with MultiIndex
        pdf.index = pd.MultiIndex.from_arrays([[1, 1], ["red", "blue"]], names=("number", "color"))
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kdf.dot(kser), pdf.dot(pser))

        pdf = pd.DataFrame([[1, 2], [3, 4]])
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kdf.dot(kdf[0]), pdf.dot(pdf[0]))
        self.assert_eq(kdf.dot(kdf[0] * 10), pdf.dot(pdf[0] * 10))
        self.assert_eq((kdf + 1).dot(kdf[0] * 10), (pdf + 1).dot(pdf[0] * 10))

    def test_to_series_comparison(self):
        kidx1 = ps.Index([1, 2, 3, 4, 5])
        kidx2 = ps.Index([1, 2, 3, 4, 5])

        self.assert_eq((kidx1.to_series() == kidx2.to_series()).all(), True)

        kidx1.name = "koalas"
        kidx2.name = "koalas"

        self.assert_eq((kidx1.to_series() == kidx2.to_series()).all(), True)

    def test_series_repeat(self):
        pser1 = pd.Series(["a", "b", "c"], name="a")
        pser2 = pd.Series([10, 20, 30], name="rep")
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        self.assert_eq(kser1.repeat(kser2).sort_index(), pser1.repeat(pser2).sort_index())

    def test_series_ops(self):
        pser1 = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x", index=[11, 12, 13, 14, 15, 16, 17])
        pser2 = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x", index=[11, 12, 13, 14, 15, 16, 17])
        pidx1 = pd.Index([10, 11, 12, 13, 14, 15, 16], name="x")
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)
        kidx1 = ps.from_pandas(pidx1)

        self.assert_eq((kser1 + 1 + 10 * kser2).sort_index(), (pser1 + 1 + 10 * pser2).sort_index())
        self.assert_eq(
            (kser1 + 1 + 10 * kser2.rename()).sort_index(),
            (pser1 + 1 + 10 * pser2.rename()).sort_index(),
        )
        self.assert_eq(
            (kser1.rename() + 1 + 10 * kser2).sort_index(),
            (pser1.rename() + 1 + 10 * pser2).sort_index(),
        )
        self.assert_eq(
            (kser1.rename() + 1 + 10 * kser2.rename()).sort_index(),
            (pser1.rename() + 1 + 10 * pser2.rename()).sort_index(),
        )

        self.assert_eq(kser1 + 1 + 10 * kidx1, pser1 + 1 + 10 * pidx1)
        self.assert_eq(kser1.rename() + 1 + 10 * kidx1, pser1.rename() + 1 + 10 * pidx1)
        self.assert_eq(kser1 + 1 + 10 * kidx1.rename(None), pser1 + 1 + 10 * pidx1.rename(None))
        self.assert_eq(
            kser1.rename() + 1 + 10 * kidx1.rename(None),
            pser1.rename() + 1 + 10 * pidx1.rename(None),
        )

        self.assert_eq(kidx1 + 1 + 10 * kser1, pidx1 + 1 + 10 * pser1)
        self.assert_eq(kidx1 + 1 + 10 * kser1.rename(), pidx1 + 1 + 10 * pser1.rename())
        self.assert_eq(kidx1.rename(None) + 1 + 10 * kser1, pidx1.rename(None) + 1 + 10 * pser1)
        self.assert_eq(
            kidx1.rename(None) + 1 + 10 * kser1.rename(),
            pidx1.rename(None) + 1 + 10 * pser1.rename(),
        )

        pidx2 = pd.Index([11, 12, 13])
        kidx2 = ps.from_pandas(pidx2)

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            kser1 + kidx2

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            kidx2 + kser1

    def test_index_ops(self):
        pidx1 = pd.Index([1, 2, 3, 4, 5], name="x")
        pidx2 = pd.Index([6, 7, 8, 9, 10], name="x")
        kidx1 = ps.from_pandas(pidx1)
        kidx2 = ps.from_pandas(pidx2)

        self.assert_eq(kidx1 * 10 + kidx2, pidx1 * 10 + pidx2)
        self.assert_eq(kidx1.rename(None) * 10 + kidx2, pidx1.rename(None) * 10 + pidx2)

        if LooseVersion(pd.__version__) >= LooseVersion("1.0"):
            self.assert_eq(kidx1 * 10 + kidx2.rename(None), pidx1 * 10 + pidx2.rename(None))
        else:
            self.assert_eq(
                kidx1 * 10 + kidx2.rename(None), (pidx1 * 10 + pidx2.rename(None)).rename(None)
            )

        pidx3 = pd.Index([11, 12, 13])
        kidx3 = ps.from_pandas(pidx3)

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            kidx1 + kidx3

        pidx1 = pd.Index([1, 2, 3, 4, 5], name="a")
        pidx2 = pd.Index([6, 7, 8, 9, 10], name="a")
        pidx3 = pd.Index([11, 12, 13, 14, 15], name="x")
        kidx1 = ps.from_pandas(pidx1)
        kidx2 = ps.from_pandas(pidx2)
        kidx3 = ps.from_pandas(pidx3)

        self.assert_eq(kidx1 * 10 + kidx2, pidx1 * 10 + pidx2)

        if LooseVersion(pd.__version__) >= LooseVersion("1.0"):
            self.assert_eq(kidx1 * 10 + kidx3, pidx1 * 10 + pidx3)
        else:
            self.assert_eq(kidx1 * 10 + kidx3, (pidx1 * 10 + pidx3).rename(None))

    def test_align(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]}, index=[10, 20, 30])
        pdf2 = pd.DataFrame({"a": [4, 5, 6], "c": ["d", "e", "f"]}, index=[10, 11, 12])
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        for join in ["outer", "inner", "left", "right"]:
            for axis in [None, 0]:
                kdf_l, kdf_r = kdf1.align(kdf2, join=join, axis=axis)
                pdf_l, pdf_r = pdf1.align(pdf2, join=join, axis=axis)
                self.assert_eq(kdf_l.sort_index(), pdf_l.sort_index())
                self.assert_eq(kdf_r.sort_index(), pdf_r.sort_index())

        pser1 = pd.Series([7, 8, 9], index=[10, 11, 12])
        pser2 = pd.Series(["g", "h", "i"], index=[10, 20, 30])
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)

        for join in ["outer", "inner", "left", "right"]:
            kser_l, kser_r = kser1.align(kser2, join=join)
            pser_l, pser_r = pser1.align(pser2, join=join)
            self.assert_eq(kser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(kser_r.sort_index(), pser_r.sort_index())

            kdf_l, kser_r = kdf1.align(kser1, join=join, axis=0)
            pdf_l, pser_r = pdf1.align(pser1, join=join, axis=0)
            self.assert_eq(kdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(kser_r.sort_index(), pser_r.sort_index())

            kser_l, kdf_r = kser1.align(kdf1, join=join)
            pser_l, pdf_r = pser1.align(pdf1, join=join)
            self.assert_eq(kser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(kdf_r.sort_index(), pdf_r.sort_index())

        # multi-index columns
        pdf3 = pd.DataFrame(
            {("x", "a"): [4, 5, 6], ("y", "c"): ["d", "e", "f"]}, index=[10, 11, 12]
        )
        kdf3 = ps.from_pandas(pdf3)
        pser3 = pdf3[("y", "c")]
        kser3 = kdf3[("y", "c")]

        for join in ["outer", "inner", "left", "right"]:
            kdf_l, kdf_r = kdf1.align(kdf3, join=join, axis=0)
            pdf_l, pdf_r = pdf1.align(pdf3, join=join, axis=0)
            self.assert_eq(kdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(kdf_r.sort_index(), pdf_r.sort_index())

            kser_l, kser_r = kser1.align(kser3, join=join)
            pser_l, pser_r = pser1.align(pser3, join=join)
            self.assert_eq(kser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(kser_r.sort_index(), pser_r.sort_index())

            kdf_l, kser_r = kdf1.align(kser3, join=join, axis=0)
            pdf_l, pser_r = pdf1.align(pser3, join=join, axis=0)
            self.assert_eq(kdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(kser_r.sort_index(), pser_r.sort_index())

            kser_l, kdf_r = kser3.align(kdf1, join=join)
            pser_l, pdf_r = pser3.align(pdf1, join=join)
            self.assert_eq(kser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(kdf_r.sort_index(), pdf_r.sort_index())

        self.assertRaises(ValueError, lambda: kdf1.align(kdf3, axis=None))
        self.assertRaises(ValueError, lambda: kdf1.align(kdf3, axis=1))

    def test_pow_and_rpow(self):
        pser = pd.Series([1, 2, np.nan])
        kser = ps.from_pandas(pser)
        pser_other = pd.Series([np.nan, 2, 3])
        kser_other = ps.from_pandas(pser_other)

        self.assert_eq(pser.pow(pser_other), kser.pow(kser_other).sort_index())
        self.assert_eq(pser ** pser_other, (kser ** kser_other).sort_index())
        self.assert_eq(pser.rpow(pser_other), kser.rpow(kser_other).sort_index())

    def test_shift(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        kdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.shift().loc[pdf["Col1"] == 20].astype(int), kdf.shift().loc[kdf["Col1"] == 20]
        )
        self.assert_eq(
            pdf["Col2"].shift().loc[pdf["Col1"] == 20].astype(int),
            kdf["Col2"].shift().loc[kdf["Col1"] == 20],
        )

    def test_diff(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        kdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.diff().loc[pdf["Col1"] == 20].astype(int), kdf.diff().loc[kdf["Col1"] == 20]
        )
        self.assert_eq(
            pdf["Col2"].diff().loc[pdf["Col1"] == 20].astype(int),
            kdf["Col2"].diff().loc[kdf["Col1"] == 20],
        )

    def test_rank(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        kdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.rank().loc[pdf["Col1"] == 20], kdf.rank().loc[kdf["Col1"] == 20])
        self.assert_eq(
            pdf["Col2"].rank().loc[pdf["Col1"] == 20], kdf["Col2"].rank().loc[kdf["Col1"] == 20]
        )


class OpsOnDiffFramesDisabledTest(PandasOnSparkTestCase, SQLTestUtils):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", False)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    @property
    def pdf1(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def pdf2(self):
        return pd.DataFrame(
            {"a": [9, 8, 7, 6, 5, 4, 3, 2, 1], "b": [0, 0, 0, 4, 5, 6, 1, 2, 3]},
            index=list(range(9)),
        )

    @property
    def kdf1(self):
        return ps.from_pandas(self.pdf1)

    @property
    def kdf2(self):
        return ps.from_pandas(self.pdf2)

    def test_arithmetic(self):
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            self.kdf1.a - self.kdf2.b

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            self.kdf1.a - self.kdf2.a

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            self.kdf1["a"] - self.kdf2["a"]

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            self.kdf1 - self.kdf2

    def test_assignment(self):
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf = ps.from_pandas(self.pdf1)
            kdf["c"] = self.kdf1.a

    def test_frame_loc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        kdf = ps.DataFrame(pdf)
        another_kdf = ps.DataFrame(pdf)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf.loc[["viper", "sidewinder"], ["shield"]] = another_kdf.max_speed

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf.loc[another_kdf.max_speed < 5, ["shield"]] = -kdf.max_speed

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf.loc[another_kdf.max_speed < 5, ["shield"]] = -another_kdf.max_speed

    def test_frame_iloc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        kdf = ps.DataFrame(pdf)
        another_kdf = ps.DataFrame(pdf)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf.iloc[[1, 2], [1]] = another_kdf.max_speed.iloc[[1, 2]]

    def test_series_loc_setitem(self):
        pser = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        kser = ps.from_pandas(pser)

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        kser_another = ps.from_pandas(pser_another)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kser.loc[kser % 2 == 1] = -kser_another

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kser.loc[kser_another % 2 == 1] = -kser

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kser.loc[kser_another % 2 == 1] = -kser_another

    def test_series_iloc_setitem(self):
        pser = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        kser = ps.from_pandas(pser)

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        kser_another = ps.from_pandas(pser_another)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kser.iloc[[1]] = -kser_another.iloc[[1]]

    def test_where(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf1.where(kdf2 > 100)

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf1.where(kdf2 < -250)

    def test_mask(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf1.mask(kdf2 < 100)

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf1.mask(kdf2 > -250)

    def test_align(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]}, index=[10, 20, 30])
        pdf2 = pd.DataFrame({"a": [4, 5, 6], "c": ["d", "e", "f"]}, index=[10, 11, 12])
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf1.align(kdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kdf1.align(kdf2, axis=0)

    def test_pow_and_rpow(self):
        pser = pd.Series([1, 2, np.nan])
        kser = ps.from_pandas(pser)
        pser_other = pd.Series([np.nan, 2, 3])
        kser_other = ps.from_pandas(pser_other)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kser.pow(kser_other)
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kser ** kser_other
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            kser.rpow(kser_other)


if __name__ == "__main__":
    from pyspark.pandas.tests.test_ops_on_diff_frames import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
