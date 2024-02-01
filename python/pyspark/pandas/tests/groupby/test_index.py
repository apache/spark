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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupbyIndexMixin:
    def test_groupby_multiindex_columns(self):
        pdf = pd.DataFrame(
            {
                (10, "a"): [1, 2, 6, 4, 4, 6, 4, 3, 7],
                (10, "b"): [4, 2, 7, 3, 3, 1, 1, 1, 2],
                (20, "c"): [4, 2, 7, 3, None, 1, 1, 1, 2],
                (30, "d"): list("abcdefght"),
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby((10, "a")).sum().sort_index(), pdf.groupby((10, "a")).sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby((10, "a"), as_index=False)
            .sum()
            .sort_values((10, "a"))
            .reset_index(drop=True),
            pdf.groupby((10, "a"), as_index=False)
            .sum()
            .sort_values((10, "a"))
            .reset_index(drop=True),
        )
        self.assert_eq(
            psdf.groupby((10, "a"))[[(20, "c")]].sum().sort_index(),
            pdf.groupby((10, "a"))[[(20, "c")]].sum().sort_index(),
        )

        # TODO: a pandas bug?
        #  expected = pdf.groupby((10, "a"))[(20, "c")].sum().sort_index()
        expected = pd.Series(
            [4.0, 2.0, 1.0, 4.0, 8.0, 2.0],
            name=(20, "c"),
            index=pd.Index([1, 2, 3, 4, 6, 7], name=(10, "a")),
        )

        self.assert_eq(psdf.groupby((10, "a"))[(20, "c")].sum().sort_index(), expected)

        self.assert_eq(
            psdf[(20, "c")].groupby(psdf[(10, "a")]).sum().sort_index(),
            pdf[(20, "c")].groupby(pdf[(10, "a")]).sum().sort_index(),
        )

    def test_idxmax(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 2, 2, 3] * 3, "b": [1, 2, 3, 4, 5] * 3, "c": [5, 4, 3, 2, 1] * 3}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby(["a"]).idxmax().sort_index(), psdf.groupby(["a"]).idxmax().sort_index()
        )
        self.assert_eq(
            pdf.groupby(["a"]).idxmax(skipna=False).sort_index(),
            psdf.groupby(["a"]).idxmax(skipna=False).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(["a"])["b"].idxmax().sort_index(),
            psdf.groupby(["a"])["b"].idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).idxmax().sort_index(),
            psdf.b.rename().groupby(psdf.a).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).idxmax().sort_index(),
            psdf.b.groupby(psdf.a.rename()).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).idxmax().sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).idxmax().sort_index(),
        )

        with self.assertRaisesRegex(ValueError, "idxmax only support one-level index now"):
            psdf.set_index(["a", "b"]).groupby(["c"]).idxmax()

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.groupby(("x", "a")).idxmax().sort_index(),
            psdf.groupby(("x", "a")).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).idxmax(skipna=False).sort_index(),
            psdf.groupby(("x", "a")).idxmax(skipna=False).sort_index(),
        )

    def test_idxmin(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 2, 2, 3] * 3, "b": [1, 2, 3, 4, 5] * 3, "c": [5, 4, 3, 2, 1] * 3}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby(["a"]).idxmin().sort_index(), psdf.groupby(["a"]).idxmin().sort_index()
        )
        self.assert_eq(
            pdf.groupby(["a"]).idxmin(skipna=False).sort_index(),
            psdf.groupby(["a"]).idxmin(skipna=False).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(["a"])["b"].idxmin().sort_index(),
            psdf.groupby(["a"])["b"].idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).idxmin().sort_index(),
            psdf.b.rename().groupby(psdf.a).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).idxmin().sort_index(),
            psdf.b.groupby(psdf.a.rename()).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).idxmin().sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).idxmin().sort_index(),
        )

        with self.assertRaisesRegex(ValueError, "idxmin only support one-level index now"):
            psdf.set_index(["a", "b"]).groupby(["c"]).idxmin()

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.groupby(("x", "a")).idxmin().sort_index(),
            psdf.groupby(("x", "a")).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).idxmin(skipna=False).sort_index(),
            psdf.groupby(("x", "a")).idxmin(skipna=False).sort_index(),
        )


class GroupbyIndexTests(
    GroupbyIndexMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_index import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
