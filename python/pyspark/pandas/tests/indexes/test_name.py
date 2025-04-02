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
from pyspark.loose_version import LooseVersion
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class IndexNameMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_index_names(self):
        psdf = self.psdf
        self.assertIsNone(psdf.index.name)

        idx = pd.Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], name="x")
        pdf = pd.DataFrame(np.random.randn(10, 5), index=idx, columns=list("abcde"))
        psdf = ps.from_pandas(pdf)

        pser = pdf.a
        psser = psdf.a

        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)

        pidx = pdf.index
        psidx = psdf.index
        pidx.name = "renamed"
        psidx.name = "renamed"
        self.assertEqual(psidx.name, pidx.name)
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)
        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)
        self.assertEqual(psser.index.names, pser.index.names)

        pidx.name = None
        psidx.name = None
        self.assertEqual(psidx.name, pidx.name)
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)
        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)
        self.assertEqual(psser.index.names, pser.index.names)

        with self.assertRaisesRegex(ValueError, "Names must be a list-like"):
            psidx.names = "hi"

        expected_error_message = "Length of new names must be {}, got {}".format(
            psdf._internal.index_level, len(["0", "1"])
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            psidx.names = ["0", "1"]

        expected_error_message = "Index.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.Index([1, 2, 3], name=["0", "1"])
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psidx.name = ["renamed"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psidx.name = ["0", "1"]
        # Specifying `names` when creating Index is no longer supported from pandas 2.0.0.
        if LooseVersion(pd.__version__) >= LooseVersion("2.0.0"):
            pass
        else:
            with self.assertRaisesRegex(TypeError, expected_error_message):
                ps.Index([(1, 2), (3, 4)], names=["a", ["b"]])

    def test_multi_index_names(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.names, pdf.index.names)

        pidx = pdf.index
        psidx = psdf.index
        pidx.names = ["renamed_number", "renamed_color"]
        psidx.names = ["renamed_number", "renamed_color"]
        self.assertEqual(psidx.names, pidx.names)

        pidx.names = ["renamed_number", None]
        psidx.names = ["renamed_number", None]
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)

        with self.assertRaises(PandasNotImplementedError):
            psidx.name
        with self.assertRaises(PandasNotImplementedError):
            psidx.name = "renamed"

    def test_multiindex_set_names(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.from_pandas(pidx)

        pidx = pidx.set_names(["set", "new", "names"])
        psidx = psidx.set_names(["set", "new", "names"])
        self.assert_eq(pidx, psidx)

        pidx.set_names(["set", "new", "names"], inplace=True)
        psidx.set_names(["set", "new", "names"], inplace=True)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("first", level=0)
        psidx = psidx.set_names("first", level=0)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("second", level=1)
        psidx = psidx.set_names("second", level=1)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("third", level=2)
        psidx = psidx.set_names("third", level=2)
        self.assert_eq(pidx, psidx)

        pidx.set_names("first", level=0, inplace=True)
        psidx.set_names("first", level=0, inplace=True)
        self.assert_eq(pidx, psidx)

        pidx.set_names("second", level=1, inplace=True)
        psidx.set_names("second", level=1, inplace=True)
        self.assert_eq(pidx, psidx)

        pidx.set_names("third", level=2, inplace=True)
        psidx.set_names("third", level=2, inplace=True)
        self.assert_eq(pidx, psidx)

    def test_multiindex_tuple_column_name(self):
        column_labels = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")])
        pdf = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=column_labels)
        pdf.set_index(("a", "x"), append=True, inplace=True)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf, psdf)


class IndexNameTests(
    IndexNameMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_name import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
