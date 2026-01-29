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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameIdxMaxMinMixin:
    def test_idxmax(self):
        # Test basic axis=0 (default)
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 2],
                "b": [4.0, 2.0, 3.0, 1.0],
                "c": [300, 200, 400, 200],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.idxmax(), pdf.idxmax())
        self.assert_eq(psdf.idxmax(axis=0), pdf.idxmax(axis=0))
        self.assert_eq(psdf.idxmax(axis="index"), pdf.idxmax(axis="index"))

        # Test axis=1
        self.assert_eq(psdf.idxmax(axis=1), pdf.idxmax(axis=1))
        self.assert_eq(psdf.idxmax(axis="columns"), pdf.idxmax(axis="columns"))

        # Test with NAs
        pdf = pd.DataFrame(
            {
                "a": [1.0, None, 3.0],
                "b": [None, 2.0, None],
                "c": [3.0, 4.0, None],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.idxmax(), pdf.idxmax())
        self.assert_eq(psdf.idxmax(axis=0), pdf.idxmax(axis=0))
        self.assert_eq(psdf.idxmax(axis=1), pdf.idxmax(axis=1))

        # Test with all-NA row
        pdf = pd.DataFrame(
            {
                "a": [1.0, None],
                "b": [2.0, None],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.idxmax(axis=1), pdf.idxmax(axis=1))

        # Test with ties (first occurrence should win)
        pdf = pd.DataFrame(
            {
                "a": [3, 2, 1],
                "b": [3, 5, 1],
                "c": [1, 5, 1],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.idxmax(axis=1), pdf.idxmax(axis=1))

        # Test with single column
        pdf = pd.DataFrame({"a": [1, 2, 3]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.idxmax(axis=1), pdf.idxmax(axis=1))

        # Test with empty DataFrame
        pdf = pd.DataFrame({})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.idxmax(axis=1), pdf.idxmax(axis=1))

        # Test with different data types
        pdf = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.5, 2.5, 0.5],
                "negative": [-5, -10, -1],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.idxmax(axis=1), pdf.idxmax(axis=1))

        # Test with custom index
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": [7, 8, 9],
            },
            index=["row1", "row2", "row3"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.idxmax(axis=1), pdf.idxmax(axis=1))

    def test_idxmax_multiindex_columns(self):
        # Test that MultiIndex columns raise NotImplementedError for axis=1
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": [7, 8, 9],
            }
        )
        pdf.columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b"), ("z", "c")])
        psdf = ps.from_pandas(pdf)

        # axis=0 should work fine (it uses pandas internally)
        self.assert_eq(psdf.idxmax(axis=0), pdf.idxmax(axis=0))

        # axis=1 should raise NotImplementedError
        with self.assertRaises(NotImplementedError):
            psdf.idxmax(axis=1)


class FrameIdxMaxMinTests(
    FrameIdxMaxMinMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
