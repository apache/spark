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
from pyspark.pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal


class FrameResetIndexMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def df_pair(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        return pdf, psdf

    def test_reset_index(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)

        assert_frame_equal(psdf.reset_index(), pdf.reset_index())
        assert_index_equal(psdf.reset_index().index, pdf.reset_index().index)
        assert_frame_equal(psdf.reset_index(drop=True), pdf.reset_index(drop=True))

        pdf.index.name = "a"
        psdf.index.name = "a"

        with self.assertRaisesRegex(ValueError, "cannot insert a, already exists"):
            psdf.reset_index()

        assert_frame_equal(psdf.reset_index(drop=True), pdf.reset_index(drop=True))

        # inplace
        pser = pdf.a
        psser = psdf.a
        pdf.reset_index(drop=True, inplace=True)
        psdf.reset_index(drop=True, inplace=True)
        assert_frame_equal(psdf, pdf)
        assert_series_equal(psser, pser)

        pdf.columns = ["index", "b"]
        psdf.columns = ["index", "b"]
        assert_frame_equal(psdf.reset_index(), pdf.reset_index())

    def test_reset_index_with_default_index_types(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)

        with ps.option_context("compute.default_index_type", "sequence"):
            assert_frame_equal(psdf.reset_index(), pdf.reset_index())

        with ps.option_context("compute.default_index_type", "distributed-sequence"):
            assert_frame_equal(psdf.reset_index(), pdf.reset_index())

        with ps.option_context("compute.default_index_type", "distributed"):
            # the index is different.
            assert_frame_equal(
                psdf.reset_index()._to_pandas().reset_index(drop=True), pdf.reset_index()
            )

    def test_reset_index_with_multiindex_columns(self):
        index = pd.MultiIndex.from_tuples(
            [("bird", "falcon"), ("bird", "parrot"), ("mammal", "lion"), ("mammal", "monkey")],
            names=["class", "name"],
        )
        columns = pd.MultiIndex.from_tuples([("speed", "max"), ("species", "type")])
        pdf = pd.DataFrame(
            [(389.0, "fly"), (24.0, "fly"), (80.5, "run"), (np.nan, "jump")],
            index=index,
            columns=columns,
        )
        psdf = ps.from_pandas(pdf)

        assert_frame_equal(psdf, pdf)
        assert_frame_equal(psdf.reset_index(), pdf.reset_index())
        assert_frame_equal(psdf.reset_index(level="class"), pdf.reset_index(level="class"))
        assert_frame_equal(
            psdf.reset_index(level="class", col_level=1),
            pdf.reset_index(level="class", col_level=1),
        )
        assert_frame_equal(
            psdf.reset_index(level="class", col_level=1, col_fill="species"),
            pdf.reset_index(level="class", col_level=1, col_fill="species"),
        )
        assert_frame_equal(
            psdf.reset_index(level="class", col_level=1, col_fill="genus"),
            pdf.reset_index(level="class", col_level=1, col_fill="genus"),
        )

        with self.assertRaisesRegex(IndexError, "Index has only 2 levels, not 3"):
            psdf.reset_index(col_level=2)

        pdf.index.names = [("x", "class"), ("y", "name")]
        psdf.index.names = [("x", "class"), ("y", "name")]

        assert_frame_equal(psdf.reset_index(), pdf.reset_index())

        with self.assertRaisesRegex(ValueError, "Item must have length equal to number of levels."):
            psdf.reset_index(col_level=1)

    def test_index_to_frame_reset_index(self):
        def check(psdf, pdf):
            assert_frame_equal(psdf.reset_index(), pdf.reset_index())
            assert_frame_equal(psdf.reset_index(drop=True), pdf.reset_index(drop=True))

            pdf.reset_index(drop=True, inplace=True)
            psdf.reset_index(drop=True, inplace=True)
            assert_frame_equal(psdf, pdf)

        pdf, psdf = self.df_pair
        check(psdf.index.to_frame(), pdf.index.to_frame())
        check(psdf.index.to_frame(index=False), pdf.index.to_frame(index=False))

        check(psdf.index.to_frame(name="a"), pdf.index.to_frame(name="a"))
        check(psdf.index.to_frame(index=False, name="a"), pdf.index.to_frame(index=False, name="a"))
        check(psdf.index.to_frame(name=("x", "a")), pdf.index.to_frame(name=("x", "a")))
        check(
            psdf.index.to_frame(index=False, name=("x", "a")),
            pdf.index.to_frame(index=False, name=("x", "a")),
        )


class FrameResetIndexTests(
    FrameResetIndexMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_reset_index import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
