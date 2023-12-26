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


class AssignFrameMixin:
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
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    @property
    def psdf2(self):
        return ps.from_pandas(self.pdf2)

    @property
    def psdf3(self):
        return ps.from_pandas(self.pdf3)

    @property
    def psdf4(self):
        return ps.from_pandas(self.pdf4)

    @property
    def psdf5(self):
        return ps.from_pandas(self.pdf5)

    @property
    def psdf6(self):
        return ps.from_pandas(self.pdf6)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_assignment_frame(self):
        psdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        psser = psdf.a
        pser = pdf.a
        psdf[["a", "b"]] = self.psdf1
        pdf[["a", "b"]] = self.pdf1

        self.assert_eq(psdf.sort_index(), pdf.sort_index())
        self.assert_eq(psser, pser)

        # 'c' does not exist in `psdf`.
        psdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        psser = psdf.a
        pser = pdf.a
        psdf[["b", "c"]] = self.psdf1
        pdf[["b", "c"]] = self.pdf1

        self.assert_eq(psdf.sort_index(), pdf.sort_index())
        self.assert_eq(psser, pser)

        # 'c' and 'd' do not exist in `psdf`.
        psdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        psdf[["c", "d"]] = self.psdf1
        pdf[["c", "d"]] = self.pdf1

        self.assert_eq(psdf.sort_index(), pdf.sort_index())

        # Multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b")])
        psdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        psdf.columns = columns
        pdf.columns = columns
        psdf[[("y", "c"), ("z", "d")]] = self.psdf1
        pdf[[("y", "c"), ("z", "d")]] = self.pdf1

        self.assert_eq(psdf.sort_index(), pdf.sort_index())

        psdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        psdf1 = ps.from_pandas(self.pdf1)
        pdf1 = self.pdf1
        psdf1.columns = columns
        pdf1.columns = columns
        psdf[["c", "d"]] = psdf1
        pdf[["c", "d"]] = pdf1

        self.assert_eq(psdf.sort_index(), pdf.sort_index())

    def test_assignment_frame_chain(self):
        psdf = ps.from_pandas(self.pdf1)
        pdf = self.pdf1
        psdf[["a", "b"]] = self.psdf1
        pdf[["a", "b"]] = self.pdf1

        psdf[["e", "f"]] = self.psdf3
        pdf[["e", "f"]] = self.pdf3

        psdf[["b", "c"]] = self.psdf2
        pdf[["b", "c"]] = self.pdf2

        self.assert_eq(psdf.sort_index(), pdf.sort_index())

    def test_multi_index_assignment_frame(self):
        psdf = ps.from_pandas(self.pdf5)
        pdf = self.pdf5
        psdf[["c"]] = self.psdf5
        pdf[["c"]] = self.pdf5

        self.assert_eq(psdf.sort_index(), pdf.sort_index())

        psdf = ps.from_pandas(self.pdf5)
        pdf = self.pdf5
        psdf[["x"]] = self.psdf5
        pdf[["x"]] = self.pdf5

        self.assert_eq(psdf.sort_index(), pdf.sort_index())

        psdf = ps.from_pandas(self.pdf6)
        pdf = self.pdf6
        psdf[["x", "y"]] = self.psdf6
        pdf[["x", "y"]] = self.pdf6

        self.assert_eq(psdf.sort_index(), pdf.sort_index())


class AssignFrameTests(
    AssignFrameMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_assign_frame import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
