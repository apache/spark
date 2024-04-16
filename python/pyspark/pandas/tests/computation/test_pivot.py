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


class FramePivotMixin:
    def test_pivot_table_dtypes(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 2, 4, 2, 4],
                "e": [1, 2, 2, 4, 2, 4],
                "c": [1, 2, 9, 4, 7, 4],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        # Skip columns comparison by reset_index
        res_df = psdf.pivot_table(
            index=["c"], columns="a", values=["b"], aggfunc={"b": "mean"}
        ).dtypes.reset_index(drop=True)
        exp_df = pdf.pivot_table(
            index=["c"], columns="a", values=["b"], aggfunc={"b": "mean"}
        ).dtypes.reset_index(drop=True)
        self.assert_eq(res_df, exp_df)

        # Results don't have the same column's name

        # Todo: self.assert_eq(psdf.pivot_table(columns="a", values="b").dtypes,
        #  pdf.pivot_table(columns="a", values="b").dtypes)

        # Todo: self.assert_eq(psdf.pivot_table(index=['c'], columns="a", values="b").dtypes,
        #  pdf.pivot_table(index=['c'], columns="a", values="b").dtypes)

        # Todo: self.assert_eq(psdf.pivot_table(index=['e', 'c'], columns="a", values="b").dtypes,
        #  pdf.pivot_table(index=['e', 'c'], columns="a", values="b").dtypes)

        # Todo: self.assert_eq(psdf.pivot_table(index=['e', 'c'],
        #  columns="a", values="b", fill_value=999).dtypes, pdf.pivot_table(index=['e', 'c'],
        #  columns="a", values="b", fill_value=999).dtypes)

    def test_pivot_table_and_index(self):
        # https://github.com/databricks/koalas/issues/805
        pdf = pd.DataFrame(
            {
                "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
                "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
                "C": [
                    "small",
                    "large",
                    "large",
                    "small",
                    "small",
                    "large",
                    "small",
                    "small",
                    "large",
                ],
                "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
                "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
            },
            columns=["A", "B", "C", "D", "E"],
            index=np.random.rand(9),
        )
        psdf = ps.from_pandas(pdf)

        ptable = pdf.pivot_table(
            values="D", index=["A", "B"], columns="C", aggfunc="sum", fill_value=0
        ).sort_index()
        ktable = psdf.pivot_table(
            values="D", index=["A", "B"], columns="C", aggfunc="sum", fill_value=0
        ).sort_index()

        self.assert_eq(ktable, ptable)
        self.assert_eq(ktable.index, ptable.index)
        self.assert_eq(repr(ktable.index), repr(ptable.index))

    def test_pivot_errors(self):
        psdf = ps.range(10)

        with self.assertRaisesRegex(ValueError, "columns should be set"):
            psdf.pivot(index="id")

        with self.assertRaisesRegex(ValueError, "values should be set"):
            psdf.pivot(index="id", columns="id")

    def test_pivot_table_errors(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 2, 4, 2, 4],
                "e": [1, 2, 2, 4, 2, 4],
                "c": [1, 2, 9, 4, 7, 4],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assertRaises(KeyError, lambda: psdf.pivot_table(index=["c"], columns="a", values=5))

        msg = "index should be a None or a list of columns."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(index="c", columns="a", values="b")

        msg = "pivot_table doesn't support aggfunc as dict and without index."
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.pivot_table(columns="a", values=["b", "e"], aggfunc={"b": "mean", "e": "sum"})

        msg = "columns should be one column name."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(columns=["a"], values=["b"], aggfunc={"b": "mean", "e": "sum"})

        msg = "Columns in aggfunc must be the same as values."
        with self.assertRaisesRegex(ValueError, msg):
            psdf.pivot_table(
                index=["e", "c"], columns="a", values="b", aggfunc={"b": "mean", "e": "sum"}
            )

        msg = "values can't be a list without index."
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.pivot_table(columns="a", values=["b", "e"])

        msg = "Wrong columns A."
        with self.assertRaisesRegex(ValueError, msg):
            psdf.pivot_table(
                index=["c"], columns="A", values=["b", "e"], aggfunc={"b": "mean", "e": "sum"}
            )

        msg = "values should be one column or list of columns."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(columns="a", values=(["b"], ["c"]))

        msg = "aggfunc must be a dict mapping from column name to aggregate functions"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(columns="a", values="b", aggfunc={"a": lambda x: sum(x)})

        psdf = ps.DataFrame(
            {
                "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
                "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
                "C": [
                    "small",
                    "large",
                    "large",
                    "small",
                    "small",
                    "large",
                    "small",
                    "small",
                    "large",
                ],
                "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
                "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
            },
            columns=["A", "B", "C", "D", "E"],
            index=np.random.rand(9),
        )

        msg = "values should be a numeric type."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(
                index=["C"], columns="A", values=["B", "E"], aggfunc={"B": "mean", "E": "sum"}
            )

        msg = "values should be a numeric type."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.pivot_table(index=["C"], columns="A", values="B", aggfunc={"B": "mean"})


class FramePivotTests(
    FramePivotMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_pivot import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
