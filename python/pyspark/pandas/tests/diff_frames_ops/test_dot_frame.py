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
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.testing.utils import is_ansi_mode_test, ansi_mode_not_supported_message


class DiffFramesDotFrameMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    @unittest.skipIf(is_ansi_mode_test, ansi_mode_not_supported_message)
    def test_frame_dot(self):
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]])
        psdf = ps.from_pandas(pdf)

        pser = pd.Series([1, 1, 2, 1])
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # Index reorder
        pser = pser.reindex([1, 0, 2, 3])
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # ser with name
        pser.name = "ser"
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # df with MultiIndex as column (ser with MultiIndex)
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pser = pd.Series([1, 1, 2, 1], index=pidx)
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]], columns=pidx)
        psdf = ps.from_pandas(pdf)
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # df with Index as column (ser with Index)
        pidx = pd.Index([1, 2, 3, 4], name="number")
        pser = pd.Series([1, 1, 2, 1], index=pidx)
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]], columns=pidx)
        psdf = ps.from_pandas(pdf)
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # df with Index
        pdf.index = pd.Index(["x", "y"], name="char")
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # df with MultiIndex
        pdf.index = pd.MultiIndex.from_arrays([[1, 1], ["red", "blue"]], names=("number", "color"))
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        pdf = pd.DataFrame([[1, 2], [3, 4]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.dot(psdf[0]), pdf.dot(pdf[0]))
        self.assert_eq(psdf.dot(psdf[0] * 10), pdf.dot(pdf[0] * 10))
        self.assert_eq((psdf + 1).dot(psdf[0] * 10), (pdf + 1).dot(pdf[0] * 10))


class DiffFramesDotFrameTests(DiffFramesDotFrameMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_dot_frame import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
