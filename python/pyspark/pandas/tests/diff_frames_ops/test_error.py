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
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class DiffFramesErrorMixin:
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
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    @property
    def psdf2(self):
        return ps.from_pandas(self.pdf2)

    def test_arithmetic(self):
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            self.psdf1.a - self.psdf2.b

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            self.psdf1.a - self.psdf2.a

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            self.psdf1["a"] - self.psdf2["a"]

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            self.psdf1 - self.psdf2

    def test_assignment(self):
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf = ps.from_pandas(self.pdf1)
            psdf["c"] = self.psdf1.a

    def test_frame_loc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf = ps.DataFrame(pdf)
        another_psdf = ps.DataFrame(pdf)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf.loc[["viper", "sidewinder"], ["shield"]] = another_psdf.max_speed

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf.loc[another_psdf.max_speed < 5, ["shield"]] = -psdf.max_speed

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf.loc[another_psdf.max_speed < 5, ["shield"]] = -another_psdf.max_speed

    def test_frame_iloc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf = ps.DataFrame(pdf)
        another_psdf = ps.DataFrame(pdf)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf.iloc[[1, 2], [1]] = another_psdf.max_speed.iloc[[1, 2]]

    def test_series_loc_setitem(self):
        pser = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        psser = ps.from_pandas(pser)

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        psser_another = ps.from_pandas(pser_another)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psser.loc[psser % 2 == 1] = -psser_another

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psser.loc[psser_another % 2 == 1] = -psser

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psser.loc[psser_another % 2 == 1] = -psser_another

    def test_series_iloc_setitem(self):
        pser = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        psser = ps.from_pandas(pser)

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        psser_another = ps.from_pandas(pser_another)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psser.iloc[[1]] = -psser_another.iloc[[1]]

    def test_where(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf1.where(psdf2 > 100)

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf1.where(psdf2 < -250)

    def test_mask(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf1.mask(psdf2 < 100)

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf1.mask(psdf2 > -250)

    def test_align(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]}, index=[10, 20, 30])
        pdf2 = pd.DataFrame({"a": [4, 5, 6], "c": ["d", "e", "f"]}, index=[10, 11, 12])
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf1.align(psdf2)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf1.align(psdf2, axis=0)

    def test_pow_and_rpow(self):
        pser = pd.Series([1, 2, np.nan])
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([np.nan, 2, 3])
        psser_other = ps.from_pandas(pser_other)

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psser.pow(psser_other)
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psser**psser_other
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psser.rpow(psser_other)

    def test_equals(self):
        psidx1 = ps.Index([1, 2, 3, 4])
        psidx2 = ps.Index([1, 2, 3, 4])

        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psidx1.equals(psidx2)

    def test_combine_first(self):
        pdf1 = pd.DataFrame({"A": [None, 0], "B": [4, None]})
        psdf1 = ps.from_pandas(pdf1)

        self.assertRaises(TypeError, lambda: psdf1.combine_first(ps.Series([1, 2])))

        pser1 = pd.Series({"falcon": 330.0, "eagle": 160.0})
        pser2 = pd.Series({"falcon": 345.0, "eagle": 200.0, "duck": 30.0})
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psser1.combine_first(psser2)

        pdf1 = pd.DataFrame({"A": [None, 0], "B": [4, None]})
        psdf1 = ps.from_pandas(pdf1)
        pdf2 = pd.DataFrame({"C": [3, 3], "B": [1, 1]})
        psdf2 = ps.from_pandas(pdf2)
        with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
            psdf1.combine_first(psdf2)

    def test_series_eq(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6], name="x")
        psser = ps.from_pandas(pser)

        others = (
            ps.Series([np.nan, 1, 3, 4, np.nan, 6], name="x"),
            ps.Index([np.nan, 1, 3, 4, np.nan, 6], name="x"),
        )
        for other in others:
            with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
                psser.eq(other)
            with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
                psser == other


class DiffFramesErrorTests(
    DiffFramesErrorMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_error import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
