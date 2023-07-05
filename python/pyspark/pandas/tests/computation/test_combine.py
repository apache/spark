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
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import ComparisonTestBase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Combining / joining / merging'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#combining-joining-merging
class FrameCombineMixin:
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

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43562): Enable DataFrameTests.test_append for pandas 2.0.0.",
    )
    def test_append(self):
        pdf = pd.DataFrame([[1, 2], [3, 4]], columns=list("AB"))
        psdf = ps.from_pandas(pdf)
        other_pdf = pd.DataFrame([[3, 4], [5, 6]], columns=list("BC"), index=[2, 3])
        other_psdf = ps.from_pandas(other_pdf)

        self.assert_eq(psdf.append(psdf), pdf.append(pdf))
        self.assert_eq(psdf.append(psdf, ignore_index=True), pdf.append(pdf, ignore_index=True))

        # Assert DataFrames with non-matching columns
        self.assert_eq(psdf.append(other_psdf), pdf.append(other_pdf))

        # Assert appending a Series fails
        msg = "DataFrames.append() does not support appending Series to DataFrames"
        with self.assertRaises(TypeError, msg=msg):
            psdf.append(psdf["A"])

        # Assert using the sort parameter raises an exception
        msg = "The 'sort' parameter is currently not supported"
        with self.assertRaises(NotImplementedError, msg=msg):
            psdf.append(psdf, sort=True)

        # Assert using 'verify_integrity' only raises an exception for overlapping indices
        self.assert_eq(
            psdf.append(other_psdf, verify_integrity=True),
            pdf.append(other_pdf, verify_integrity=True),
        )
        msg = "Indices have overlapping values"
        with self.assertRaises(ValueError, msg=msg):
            psdf.append(psdf, verify_integrity=True)

        # Skip integrity verification when ignore_index=True
        self.assert_eq(
            psdf.append(psdf, ignore_index=True, verify_integrity=True),
            pdf.append(pdf, ignore_index=True, verify_integrity=True),
        )

        # Assert appending multi-index DataFrames
        multi_index_pdf = pd.DataFrame([[1, 2], [3, 4]], columns=list("AB"), index=[[2, 3], [4, 5]])
        multi_index_psdf = ps.from_pandas(multi_index_pdf)
        other_multi_index_pdf = pd.DataFrame(
            [[5, 6], [7, 8]], columns=list("AB"), index=[[2, 3], [6, 7]]
        )
        other_multi_index_psdf = ps.from_pandas(other_multi_index_pdf)

        self.assert_eq(
            multi_index_psdf.append(multi_index_psdf), multi_index_pdf.append(multi_index_pdf)
        )

        # Assert DataFrames with non-matching columns
        self.assert_eq(
            multi_index_psdf.append(other_multi_index_psdf),
            multi_index_pdf.append(other_multi_index_pdf),
        )

        # Assert using 'verify_integrity' only raises an exception for overlapping indices
        self.assert_eq(
            multi_index_psdf.append(other_multi_index_psdf, verify_integrity=True),
            multi_index_pdf.append(other_multi_index_pdf, verify_integrity=True),
        )
        with self.assertRaises(ValueError, msg=msg):
            multi_index_psdf.append(multi_index_psdf, verify_integrity=True)

        # Skip integrity verification when ignore_index=True
        self.assert_eq(
            multi_index_psdf.append(multi_index_psdf, ignore_index=True, verify_integrity=True),
            multi_index_pdf.append(multi_index_pdf, ignore_index=True, verify_integrity=True),
        )

        # Assert trying to append DataFrames with different index levels
        msg = "Both DataFrames have to have the same number of index levels"
        with self.assertRaises(ValueError, msg=msg):
            psdf.append(multi_index_psdf)

        # Skip index level check when ignore_index=True
        self.assert_eq(
            psdf.append(multi_index_psdf, ignore_index=True),
            pdf.append(multi_index_pdf, ignore_index=True),
        )

        columns = pd.MultiIndex.from_tuples([("A", "X"), ("A", "Y")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.append(psdf), pdf.append(pdf))

    def test_merge(self):
        left_pdf = pd.DataFrame(
            {
                "lkey": ["foo", "bar", "baz", "foo", "bar", "l"],
                "value": [1, 2, 3, 5, 6, 7],
                "x": list("abcdef"),
            },
            columns=["lkey", "value", "x"],
        )
        right_pdf = pd.DataFrame(
            {
                "rkey": ["baz", "foo", "bar", "baz", "foo", "r"],
                "value": [4, 5, 6, 7, 8, 9],
                "y": list("efghij"),
            },
            columns=["rkey", "value", "y"],
        )
        right_pser = pd.Series(list("defghi"), name="x", index=[5, 6, 7, 8, 9, 10])

        left_psdf = ps.from_pandas(left_pdf)
        right_psdf = ps.from_pandas(right_pdf)
        right_psser = ps.from_pandas(right_pser)

        def check(op, right_psdf=right_psdf, right_pdf=right_pdf):
            ps_res = op(left_psdf, right_psdf)
            ps_res = ps_res._to_pandas()
            ps_res = ps_res.sort_values(by=list(ps_res.columns))
            ps_res = ps_res.reset_index(drop=True)
            p_res = op(left_pdf, right_pdf)
            p_res = p_res.sort_values(by=list(p_res.columns))
            p_res = p_res.reset_index(drop=True)
            self.assert_eq(ps_res, p_res)

        check(lambda left, right: left.merge(right))
        check(lambda left, right: left.merge(right, on="value"))
        check(lambda left, right: left.merge(right, on=("value",)))
        check(lambda left, right: left.merge(right, left_on="lkey", right_on="rkey"))
        check(lambda left, right: left.set_index("lkey").merge(right.set_index("rkey")))
        check(
            lambda left, right: left.set_index("lkey").merge(
                right, left_index=True, right_on="rkey"
            )
        )
        check(
            lambda left, right: left.merge(
                right.set_index("rkey"), left_on="lkey", right_index=True
            )
        )
        check(
            lambda left, right: left.set_index("lkey").merge(
                right.set_index("rkey"), left_index=True, right_index=True
            )
        )

        # MultiIndex
        check(
            lambda left, right: left.merge(
                right, left_on=["lkey", "value"], right_on=["rkey", "value"]
            )
        )
        check(
            lambda left, right: left.set_index(["lkey", "value"]).merge(
                right, left_index=True, right_on=["rkey", "value"]
            )
        )
        check(
            lambda left, right: left.merge(
                right.set_index(["rkey", "value"]), left_on=["lkey", "value"], right_index=True
            )
        )
        # TODO: when both left_index=True and right_index=True with multi-index
        # check(lambda left, right: left.set_index(['lkey', 'value']).merge(
        #     right.set_index(['rkey', 'value']), left_index=True, right_index=True))

        # join types
        for how in ["inner", "left", "right", "outer"]:
            check(lambda left, right: left.merge(right, on="value", how=how))
            check(lambda left, right: left.merge(right, left_on="lkey", right_on="rkey", how=how))

        # suffix
        check(
            lambda left, right: left.merge(
                right, left_on="lkey", right_on="rkey", suffixes=["_left", "_right"]
            )
        )

        # Test Series on the right
        check(lambda left, right: left.merge(right), right_psser, right_pser)
        check(
            lambda left, right: left.merge(right, left_on="x", right_on="x"),
            right_psser,
            right_pser,
        )
        check(
            lambda left, right: left.set_index("x").merge(right, left_index=True, right_on="x"),
            right_psser,
            right_pser,
        )

        # Test join types with Series
        for how in ["inner", "left", "right", "outer"]:
            check(lambda left, right: left.merge(right, how=how), right_psser, right_pser)
            check(
                lambda left, right: left.merge(right, left_on="x", right_on="x", how=how),
                right_psser,
                right_pser,
            )

        # suffix with Series
        check(
            lambda left, right: left.merge(
                right,
                suffixes=["_left", "_right"],
                how="outer",
                left_index=True,
                right_index=True,
            ),
            right_psser,
            right_pser,
        )

        # multi-index columns
        left_columns = pd.MultiIndex.from_tuples([(10, "lkey"), (10, "value"), (20, "x")])
        left_pdf.columns = left_columns
        left_psdf.columns = left_columns

        right_columns = pd.MultiIndex.from_tuples([(10, "rkey"), (10, "value"), (30, "y")])
        right_pdf.columns = right_columns
        right_psdf.columns = right_columns

        check(lambda left, right: left.merge(right))
        check(lambda left, right: left.merge(right, on=[(10, "value")]))
        check(
            lambda left, right: (left.set_index((10, "lkey")).merge(right.set_index((10, "rkey"))))
        )
        check(
            lambda left, right: (
                left.set_index((10, "lkey")).merge(
                    right.set_index((10, "rkey")), left_index=True, right_index=True
                )
            )
        )
        # TODO: when both left_index=True and right_index=True with multi-index columns
        # check(lambda left, right: left.merge(right,
        #                                      left_on=[('a', 'lkey')], right_on=[('a', 'rkey')]))
        # check(lambda left, right: (left.set_index(('a', 'lkey'))
        #                            .merge(right, left_index=True, right_on=[('a', 'rkey')])))

        # non-string names
        left_pdf.columns = [10, 100, 1000]
        left_psdf.columns = [10, 100, 1000]

        right_pdf.columns = [20, 100, 2000]
        right_psdf.columns = [20, 100, 2000]

        check(lambda left, right: left.merge(right))
        check(lambda left, right: left.merge(right, on=[100]))
        check(lambda left, right: (left.set_index(10).merge(right.set_index(20))))
        check(
            lambda left, right: (
                left.set_index(10).merge(right.set_index(20), left_index=True, right_index=True)
            )
        )

    def test_merge_same_anchor(self):
        pdf = pd.DataFrame(
            {
                "lkey": ["foo", "bar", "baz", "foo", "bar", "l"],
                "rkey": ["baz", "foo", "bar", "baz", "foo", "r"],
                "value": [1, 1, 3, 5, 6, 7],
                "x": list("abcdef"),
                "y": list("efghij"),
            },
            columns=["lkey", "rkey", "value", "x", "y"],
        )
        psdf = ps.from_pandas(pdf)

        left_pdf = pdf[["lkey", "value", "x"]]
        right_pdf = pdf[["rkey", "value", "y"]]
        left_psdf = psdf[["lkey", "value", "x"]]
        right_psdf = psdf[["rkey", "value", "y"]]

        def check(op, right_psdf=right_psdf, right_pdf=right_pdf):
            k_res = op(left_psdf, right_psdf)
            k_res = k_res._to_pandas()
            k_res = k_res.sort_values(by=list(k_res.columns))
            k_res = k_res.reset_index(drop=True)
            p_res = op(left_pdf, right_pdf)
            p_res = p_res.sort_values(by=list(p_res.columns))
            p_res = p_res.reset_index(drop=True)
            self.assert_eq(k_res, p_res)

        check(lambda left, right: left.merge(right))
        check(lambda left, right: left.merge(right, on="value"))
        check(lambda left, right: left.merge(right, left_on="lkey", right_on="rkey"))
        check(lambda left, right: left.set_index("lkey").merge(right.set_index("rkey")))
        check(
            lambda left, right: left.set_index("lkey").merge(
                right, left_index=True, right_on="rkey"
            )
        )
        check(
            lambda left, right: left.merge(
                right.set_index("rkey"), left_on="lkey", right_index=True
            )
        )
        check(
            lambda left, right: left.set_index("lkey").merge(
                right.set_index("rkey"), left_index=True, right_index=True
            )
        )

    def test_merge_retains_indices(self):
        left_pdf = pd.DataFrame({"A": [0, 1]})
        right_pdf = pd.DataFrame({"B": [1, 2]}, index=[1, 2])
        left_psdf = ps.from_pandas(left_pdf)
        right_psdf = ps.from_pandas(right_pdf)

        self.assert_eq(
            left_psdf.merge(right_psdf, left_index=True, right_index=True),
            left_pdf.merge(right_pdf, left_index=True, right_index=True),
        )
        self.assert_eq(
            left_psdf.merge(right_psdf, left_on="A", right_index=True),
            left_pdf.merge(right_pdf, left_on="A", right_index=True),
        )
        self.assert_eq(
            left_psdf.merge(right_psdf, left_index=True, right_on="B"),
            left_pdf.merge(right_pdf, left_index=True, right_on="B"),
        )
        self.assert_eq(
            left_psdf.merge(right_psdf, left_on="A", right_on="B"),
            left_pdf.merge(right_pdf, left_on="A", right_on="B"),
        )

    def test_merge_how_parameter(self):
        left_pdf = pd.DataFrame({"A": [1, 2]})
        right_pdf = pd.DataFrame({"B": ["x", "y"]}, index=[1, 2])
        left_psdf = ps.from_pandas(left_pdf)
        right_psdf = ps.from_pandas(right_pdf)

        psdf = left_psdf.merge(right_psdf, left_index=True, right_index=True)
        pdf = left_pdf.merge(right_pdf, left_index=True, right_index=True)
        self.assert_eq(
            psdf.sort_values(by=list(psdf.columns)).reset_index(drop=True),
            pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True),
        )

        psdf = left_psdf.merge(right_psdf, left_index=True, right_index=True, how="left")
        pdf = left_pdf.merge(right_pdf, left_index=True, right_index=True, how="left")
        self.assert_eq(
            psdf.sort_values(by=list(psdf.columns)).reset_index(drop=True),
            pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True),
        )

        psdf = left_psdf.merge(right_psdf, left_index=True, right_index=True, how="right")
        pdf = left_pdf.merge(right_pdf, left_index=True, right_index=True, how="right")
        self.assert_eq(
            psdf.sort_values(by=list(psdf.columns)).reset_index(drop=True),
            pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True),
        )

        psdf = left_psdf.merge(right_psdf, left_index=True, right_index=True, how="outer")
        pdf = left_pdf.merge(right_pdf, left_index=True, right_index=True, how="outer")
        self.assert_eq(
            psdf.sort_values(by=list(psdf.columns)).reset_index(drop=True),
            pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True),
        )

    def test_merge_raises(self):
        left = ps.DataFrame(
            {"value": [1, 2, 3, 5, 6], "x": list("abcde")},
            columns=["value", "x"],
            index=["foo", "bar", "baz", "foo", "bar"],
        )
        right = ps.DataFrame(
            {"value": [4, 5, 6, 7, 8], "y": list("fghij")},
            columns=["value", "y"],
            index=["baz", "foo", "bar", "baz", "foo"],
        )

        with self.assertRaisesRegex(ValueError, "No common columns to perform merge on"):
            left[["x"]].merge(right[["y"]])

        with self.assertRaisesRegex(ValueError, "not a combination of both"):
            left.merge(right, on="value", left_on="x")

        with self.assertRaisesRegex(ValueError, "Must pass right_on or right_index=True"):
            left.merge(right, left_on="x")

        with self.assertRaisesRegex(ValueError, "Must pass right_on or right_index=True"):
            left.merge(right, left_index=True)

        with self.assertRaisesRegex(ValueError, "Must pass left_on or left_index=True"):
            left.merge(right, right_on="y")

        with self.assertRaisesRegex(ValueError, "Must pass left_on or left_index=True"):
            left.merge(right, right_index=True)

        with self.assertRaisesRegex(
            ValueError, "len\\(left_keys\\) must equal len\\(right_keys\\)"
        ):
            left.merge(right, left_on="value", right_on=["value", "y"])

        with self.assertRaisesRegex(
            ValueError, "len\\(left_keys\\) must equal len\\(right_keys\\)"
        ):
            left.merge(right, left_on=["value", "x"], right_on="value")

        with self.assertRaisesRegex(ValueError, "['inner', 'left', 'right', 'full', 'outer']"):
            left.merge(right, left_index=True, right_index=True, how="foo")

        with self.assertRaisesRegex(KeyError, "id"):
            left.merge(right, on="id")

    def test_join(self):
        # check basic function
        pdf1 = pd.DataFrame(
            {"key": ["K0", "K1", "K2", "K3"], "A": ["A0", "A1", "A2", "A3"]}, columns=["key", "A"]
        )
        pdf2 = pd.DataFrame(
            {"key": ["K0", "K1", "K2"], "B": ["B0", "B1", "B2"]}, columns=["key", "B"]
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        join_pdf = pdf1.join(pdf2, lsuffix="_left", rsuffix="_right")
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(psdf2, lsuffix="_left", rsuffix="_right")
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf, join_psdf)

        # join with duplicated columns in Series
        with self.assertRaisesRegex(ValueError, "columns overlap but no suffix specified"):
            ks1 = ps.Series(["A1", "A5"], index=[1, 2], name="A")
            psdf1.join(ks1, how="outer")
        # join with duplicated columns in DataFrame
        with self.assertRaisesRegex(ValueError, "columns overlap but no suffix specified"):
            psdf1.join(psdf2, how="outer")

        # check `on` parameter
        join_pdf = pdf1.join(pdf2.set_index("key"), on="key", lsuffix="_left", rsuffix="_right")
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(psdf2.set_index("key"), on="key", lsuffix="_left", rsuffix="_right")
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)
        self.assert_eq(join_pdf.reset_index(drop=True), join_psdf.reset_index(drop=True))

        join_pdf = pdf1.set_index("key").join(
            pdf2.set_index("key"), on="key", lsuffix="_left", rsuffix="_right"
        )
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.set_index("key").join(
            psdf2.set_index("key"), on="key", lsuffix="_left", rsuffix="_right"
        )
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)
        self.assert_eq(join_pdf.reset_index(drop=True), join_psdf.reset_index(drop=True))

        # multi-index columns
        columns1 = pd.MultiIndex.from_tuples([("x", "key"), ("Y", "A")])
        columns2 = pd.MultiIndex.from_tuples([("x", "key"), ("Y", "B")])
        pdf1.columns = columns1
        pdf2.columns = columns2
        psdf1.columns = columns1
        psdf2.columns = columns2

        join_pdf = pdf1.join(pdf2, lsuffix="_left", rsuffix="_right")
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(psdf2, lsuffix="_left", rsuffix="_right")
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf, join_psdf)

        # check `on` parameter
        join_pdf = pdf1.join(
            pdf2.set_index(("x", "key")), on=[("x", "key")], lsuffix="_left", rsuffix="_right"
        )
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(
            psdf2.set_index(("x", "key")), on=[("x", "key")], lsuffix="_left", rsuffix="_right"
        )
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf.reset_index(drop=True), join_psdf.reset_index(drop=True))

        join_pdf = pdf1.set_index(("x", "key")).join(
            pdf2.set_index(("x", "key")), on=[("x", "key")], lsuffix="_left", rsuffix="_right"
        )
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.set_index(("x", "key")).join(
            psdf2.set_index(("x", "key")), on=[("x", "key")], lsuffix="_left", rsuffix="_right"
        )
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf.reset_index(drop=True), join_psdf.reset_index(drop=True))

        # multi-index
        midx1 = pd.MultiIndex.from_tuples(
            [("w", "a"), ("x", "b"), ("y", "c"), ("z", "d")], names=["index1", "index2"]
        )
        midx2 = pd.MultiIndex.from_tuples(
            [("w", "a"), ("x", "b"), ("y", "c")], names=["index1", "index2"]
        )
        pdf1.index = midx1
        pdf2.index = midx2
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        join_pdf = pdf1.join(pdf2, on=["index1", "index2"], rsuffix="_right")
        join_pdf.sort_values(by=list(join_pdf.columns), inplace=True)

        join_psdf = psdf1.join(psdf2, on=["index1", "index2"], rsuffix="_right")
        join_psdf.sort_values(by=list(join_psdf.columns), inplace=True)

        self.assert_eq(join_pdf, join_psdf)

        with self.assertRaisesRegex(
            ValueError, r'len\(left_on\) must equal the number of levels in the index of "right"'
        ):
            psdf1.join(psdf2, on=["index1"], rsuffix="_right")

    def test_update(self):
        # check base function
        def get_data(left_columns=None, right_columns=None):
            left_pdf = pd.DataFrame(
                {"A": ["1", "2", "3", "4"], "B": ["100", "200", np.nan, np.nan]}, columns=["A", "B"]
            )
            right_pdf = pd.DataFrame(
                {"B": ["x", np.nan, "y", np.nan], "C": ["100", "200", "300", "400"]},
                columns=["B", "C"],
            )

            left_psdf = ps.DataFrame(
                {"A": ["1", "2", "3", "4"], "B": ["100", "200", None, None]}, columns=["A", "B"]
            )
            right_psdf = ps.DataFrame(
                {"B": ["x", None, "y", None], "C": ["100", "200", "300", "400"]}, columns=["B", "C"]
            )
            if left_columns is not None:
                left_pdf.columns = left_columns
                left_psdf.columns = left_columns
            if right_columns is not None:
                right_pdf.columns = right_columns
                right_psdf.columns = right_columns
            return left_psdf, left_pdf, right_psdf, right_pdf

        left_psdf, left_pdf, right_psdf, right_pdf = get_data()
        pser = left_pdf.B
        psser = left_psdf.B
        left_pdf.update(right_pdf)
        left_psdf.update(right_psdf)
        self.assert_eq(left_pdf.sort_values(by=["A", "B"]), left_psdf.sort_values(by=["A", "B"]))
        # Skip due to pandas bug: https://github.com/pandas-dev/pandas/issues/47188
        if not (LooseVersion("1.4.0") <= LooseVersion(pd.__version__) <= LooseVersion("1.4.2")):
            self.assert_eq(psser.sort_index(), pser.sort_index())

        left_psdf, left_pdf, right_psdf, right_pdf = get_data()
        left_pdf.update(right_pdf, overwrite=False)
        left_psdf.update(right_psdf, overwrite=False)
        self.assert_eq(left_pdf.sort_values(by=["A", "B"]), left_psdf.sort_values(by=["A", "B"]))

        with self.assertRaises(NotImplementedError):
            left_psdf.update(right_psdf, join="right")

        # multi-index columns
        left_columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")])
        right_columns = pd.MultiIndex.from_tuples([("X", "B"), ("Y", "C")])

        left_psdf, left_pdf, right_psdf, right_pdf = get_data(
            left_columns=left_columns, right_columns=right_columns
        )
        left_pdf.update(right_pdf)
        left_psdf.update(right_psdf)
        self.assert_eq(
            left_pdf.sort_values(by=[("X", "A"), ("X", "B")]),
            left_psdf.sort_values(by=[("X", "A"), ("X", "B")]),
        )

        left_psdf, left_pdf, right_psdf, right_pdf = get_data(
            left_columns=left_columns, right_columns=right_columns
        )
        left_pdf.update(right_pdf, overwrite=False)
        left_psdf.update(right_psdf, overwrite=False)
        self.assert_eq(
            left_pdf.sort_values(by=[("X", "A"), ("X", "B")]),
            left_psdf.sort_values(by=[("X", "A"), ("X", "B")]),
        )

        right_columns = pd.MultiIndex.from_tuples([("Y", "B"), ("Y", "C")])
        left_psdf, left_pdf, right_psdf, right_pdf = get_data(
            left_columns=left_columns, right_columns=right_columns
        )
        left_pdf.update(right_pdf)
        left_psdf.update(right_psdf)
        self.assert_eq(
            left_pdf.sort_values(by=[("X", "A"), ("X", "B")]),
            left_psdf.sort_values(by=[("X", "A"), ("X", "B")]),
        )


class FrameCombineTests(FrameCombineMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_combine import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
