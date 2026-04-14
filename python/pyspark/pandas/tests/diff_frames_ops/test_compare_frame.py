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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class CompareFrameMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def _assert_compare_eq(self, pdf1, pdf2, **kwargs):
        """Helper to compare pandas and pyspark compare results.

        Uses almost=True when keep_equal is False (the default) because
        pandas promotes integer columns to float when NaN masking is applied
        (before row filtering), while pyspark applies row filtering first, so
        columns that end up with no NaN values may keep their original dtype.
        """
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)
        almost = not kwargs.get("keep_equal", False)
        self.assert_eq(
            pdf1.compare(pdf2, **kwargs).sort_index(),
            psdf1.compare(psdf2, **kwargs).sort_index(),
            almost=almost,
        )

    def test_compare(self):
        pdf1 = pd.DataFrame({"a": ["b", "c", np.nan, "g", np.nan], "b": [1, 2, 3, 4, np.nan]})
        pdf2 = pd.DataFrame({"a": ["a", "c", np.nan, np.nan, "h"], "b": [1, 2, 1, 4, np.nan]})
        self._assert_compare_eq(pdf1, pdf2)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True)
        self._assert_compare_eq(pdf1, pdf2, keep_equal=True)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True, keep_equal=True)

    def test_compare_same_anchor(self):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5], "b": [5, 4, 3, 2, 1]})
        psdf1 = psdf[["a", "b"]]
        psdf2 = psdf[["a", "b"]].rename(columns={"a": "a", "b": "b"})

        pdf = psdf.to_pandas()
        pdf1 = pdf[["a", "b"]]
        pdf2 = pdf[["a", "b"]]

        self.assert_eq(
            pdf1.compare(pdf2).sort_index(),
            psdf1.compare(psdf2).sort_index(),
        )
        self.assert_eq(
            pdf1.compare(pdf2, keep_shape=True).sort_index(),
            psdf1.compare(psdf2, keep_shape=True).sort_index(),
        )
        self.assert_eq(
            pdf1.compare(pdf2, keep_equal=True).sort_index(),
            psdf1.compare(psdf2, keep_equal=True).sort_index(),
        )
        self.assert_eq(
            pdf1.compare(pdf2, keep_shape=True, keep_equal=True).sort_index(),
            psdf1.compare(psdf2, keep_shape=True, keep_equal=True).sort_index(),
        )

    def test_compare_multiindex(self):
        pdf1 = pd.DataFrame({"a": ["b", "c", np.nan, "g", np.nan], "b": [1, 2, 3, 4, np.nan]})
        pdf2 = pd.DataFrame({"a": ["a", "c", np.nan, np.nan, "h"], "b": [1, 2, 1, 4, np.nan]})
        midx = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z"), ("x", "k"), ("q", "l")]
        )
        pdf1.index = midx
        pdf2.index = midx
        self._assert_compare_eq(pdf1, pdf2)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True)
        self._assert_compare_eq(pdf1, pdf2, keep_equal=True)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True, keep_equal=True)

    def test_compare_all_equal(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        pdf2 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        self._assert_compare_eq(pdf1, pdf2)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True)
        self._assert_compare_eq(pdf1, pdf2, keep_equal=True)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True, keep_equal=True)

    def test_compare_all_different(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        pdf2 = pd.DataFrame({"a": [9, 8, 7], "b": [6, 5, 4]})
        self._assert_compare_eq(pdf1, pdf2)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True)
        self._assert_compare_eq(pdf1, pdf2, keep_equal=True)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True, keep_equal=True)

    def test_compare_nans_matching(self):
        pdf1 = pd.DataFrame({"a": [np.nan, np.nan, 1.0], "b": [np.nan, 2.0, np.nan]})
        pdf2 = pd.DataFrame({"a": [np.nan, np.nan, 1.0], "b": [np.nan, 2.0, np.nan]})
        self._assert_compare_eq(pdf1, pdf2)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True)

    def test_compare_nans_different(self):
        pdf1 = pd.DataFrame({"a": [np.nan, 1.0], "b": [2.0, np.nan]})
        pdf2 = pd.DataFrame({"a": [1.0, np.nan], "b": [np.nan, 2.0]})
        self._assert_compare_eq(pdf1, pdf2)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True)
        self._assert_compare_eq(pdf1, pdf2, keep_equal=True)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True, keep_equal=True)

    def test_compare_single_column(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3]})
        pdf2 = pd.DataFrame({"a": [1, 9, 3]})
        self._assert_compare_eq(pdf1, pdf2)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True)
        self._assert_compare_eq(pdf1, pdf2, keep_equal=True)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True, keep_equal=True)

    def test_compare_empty(self):
        pdf1 = pd.DataFrame({"a": pd.Series([], dtype="int64"), "b": pd.Series([], dtype="int64")})
        pdf2 = pd.DataFrame({"a": pd.Series([], dtype="int64"), "b": pd.Series([], dtype="int64")})
        self._assert_compare_eq(pdf1, pdf2)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True)
        self._assert_compare_eq(pdf1, pdf2, keep_equal=True)
        self._assert_compare_eq(pdf1, pdf2, keep_shape=True, keep_equal=True)

    def test_compare_different_index(self):
        psdf1 = ps.DataFrame(
            {"a": [1, 2, 3, 4, 5], "b": [1, 2, 3, 4, 5]},
            index=pd.Index([1, 2, 3, 4, 5]),
        )
        psdf2 = ps.DataFrame(
            {"a": [2, 2, 3, 4, 1], "b": [2, 2, 3, 4, 1]},
            index=pd.Index([5, 4, 3, 2, 1]),
        )
        with self.assertRaisesRegex(
            ValueError, "Can only compare identically-labeled DataFrame objects"
        ):
            psdf1.compare(psdf2)

    def test_compare_different_columns(self):
        psdf1 = ps.DataFrame({"a": [1, 2], "b": [3, 4]})
        psdf2 = ps.DataFrame({"a": [1, 2], "c": [3, 4]})
        with self.assertRaisesRegex(
            ValueError, "can only compare identically-labeled DataFrame objects"
        ):
            psdf1.compare(psdf2)

    def test_compare_type_error(self):
        psdf = ps.DataFrame({"a": [1, 2]})
        with self.assertRaises(TypeError):
            psdf.compare([1, 2])

    def test_compare_eager_check_disabled(self):
        psdf1 = ps.DataFrame({"a": [1, 2, 3, 4, 5]}, index=pd.Index([1, 2, 3, 4, 5]))
        psdf2 = ps.DataFrame({"a": [1, 2, 3, 4, 5, 6]}, index=pd.Index([1, 2, 4, 3, 6, 7]))

        with ps.option_context("compute.eager_check", False):
            result = psdf1.compare(psdf2).sort_index()
            self.assertIsNotNone(result)


class CompareFrameTests(
    CompareFrameMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
