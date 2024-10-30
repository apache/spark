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
from pyspark.errors import AnalysisException
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class MergeAsOfMixin:
    def test_merge_asof(self):
        pdf_left = pd.DataFrame(
            {"a": [1, 5, 10], "b": ["x", "y", "z"], "left_val": ["a", "b", "c"]}, index=[10, 20, 30]
        )
        pdf_right = pd.DataFrame(
            {"a": [1, 2, 3, 6, 7], "b": ["v", "w", "x", "y", "z"], "right_val": [1, 2, 3, 6, 7]},
            index=[100, 101, 102, 103, 104],
        )
        psdf_left = ps.from_pandas(pdf_left)
        psdf_right = ps.from_pandas(pdf_right)

        self.assert_eq(
            pd.merge_asof(pdf_left, pdf_right, on="a").sort_values("a").reset_index(drop=True),
            ps.merge_asof(psdf_left, psdf_right, on="a").sort_values("a").reset_index(drop=True),
        )
        self.assert_eq(
            (
                pd.merge_asof(pdf_left, pdf_right, left_on="a", right_on="a")
                .sort_values("a")
                .reset_index(drop=True)
            ),
            (
                ps.merge_asof(psdf_left, psdf_right, left_on="a", right_on="a")
                .sort_values("a")
                .reset_index(drop=True)
            ),
        )

        self.assert_eq(
            pd.merge_asof(
                pdf_left.set_index("a"), pdf_right, left_index=True, right_on="a"
            ).sort_index(),
            ps.merge_asof(
                psdf_left.set_index("a"), psdf_right, left_index=True, right_on="a"
            ).sort_index(),
        )

        self.assert_eq(
            pd.merge_asof(
                pdf_left, pdf_right.set_index("a"), left_on="a", right_index=True
            ).sort_index(),
            ps.merge_asof(
                psdf_left, psdf_right.set_index("a"), left_on="a", right_index=True
            ).sort_index(),
        )
        self.assert_eq(
            pd.merge_asof(
                pdf_left.set_index("a"), pdf_right.set_index("a"), left_index=True, right_index=True
            ).sort_index(),
            ps.merge_asof(
                psdf_left.set_index("a"),
                psdf_right.set_index("a"),
                left_index=True,
                right_index=True,
            ).sort_index(),
        )
        self.assert_eq(
            (
                pd.merge_asof(pdf_left, pdf_right, on="a", by="b")
                .sort_values("a")
                .reset_index(drop=True)
            ),
            (
                ps.merge_asof(psdf_left, psdf_right, on="a", by="b")
                .sort_values("a")
                .reset_index(drop=True)
            ),
        )
        self.assert_eq(
            (
                pd.merge_asof(pdf_left, pdf_right, on="a", tolerance=1)
                .sort_values("a")
                .reset_index(drop=True)
            ),
            (
                ps.merge_asof(psdf_left, psdf_right, on="a", tolerance=1)
                .sort_values("a")
                .reset_index(drop=True)
            ),
        )
        self.assert_eq(
            (
                pd.merge_asof(pdf_left, pdf_right, on="a", allow_exact_matches=False)
                .sort_values("a")
                .reset_index(drop=True)
            ),
            (
                ps.merge_asof(psdf_left, psdf_right, on="a", allow_exact_matches=False)
                .sort_values("a")
                .reset_index(drop=True)
            ),
        )
        self.assert_eq(
            (
                pd.merge_asof(pdf_left, pdf_right, on="a", direction="forward")
                .sort_values("a")
                .reset_index(drop=True)
            ),
            (
                ps.merge_asof(psdf_left, psdf_right, on="a", direction="forward")
                .sort_values("a")
                .reset_index(drop=True)
            ),
        )
        self.assert_eq(
            (
                pd.merge_asof(pdf_left, pdf_right, on="a", direction="nearest")
                .sort_values("a")
                .reset_index(drop=True)
            ),
            (
                ps.merge_asof(psdf_left, psdf_right, on="a", direction="nearest")
                .sort_values("a")
                .reset_index(drop=True)
            ),
        )
        # Including Series
        self.assert_eq(
            pd.merge_asof(pdf_left["a"], pdf_right, on="a").sort_values("a").reset_index(drop=True),
            ps.merge_asof(psdf_left["a"], psdf_right, on="a")
            .sort_values("a")
            .reset_index(drop=True),
        )
        self.assert_eq(
            pd.merge_asof(pdf_left, pdf_right["a"], on="a").sort_values("a").reset_index(drop=True),
            ps.merge_asof(psdf_left, psdf_right["a"], on="a")
            .sort_values("a")
            .reset_index(drop=True),
        )
        self.assert_eq(
            pd.merge_asof(pdf_left["a"], pdf_right["a"], on="a")
            .sort_values("a")
            .reset_index(drop=True),
            ps.merge_asof(psdf_left["a"], psdf_right["a"], on="a")
            .sort_values("a")
            .reset_index(drop=True),
        )

        self.assertRaises(
            AnalysisException, lambda: ps.merge_asof(psdf_left, psdf_right, on="a", tolerance=-1)
        )
        with self.assertRaisesRegex(
            ValueError,
            'Can only pass argument "on" OR "left_on" and "right_on", not a combination of both.',
        ):
            ps.merge_asof(psdf_left, psdf_right, on="a", left_on="a")
        psdf_multi_index = ps.DataFrame(
            {"a": [1, 2, 3, 6, 7], "b": ["v", "w", "x", "y", "z"], "right_val": [1, 2, 3, 6, 7]},
            index=pd.MultiIndex.from_tuples([(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)]),
        )
        with self.assertRaisesRegex(ValueError, "right can only have one index"):
            ps.merge_asof(psdf_left, psdf_multi_index, right_index=True)
        with self.assertRaisesRegex(ValueError, "left can only have one index"):
            ps.merge_asof(psdf_multi_index, psdf_right, left_index=True)
        with self.assertRaisesRegex(ValueError, "Must pass right_on or right_index=True"):
            ps.merge_asof(psdf_left, psdf_right, left_index=True)
        with self.assertRaisesRegex(ValueError, "Must pass left_on or left_index=True"):
            ps.merge_asof(psdf_left, psdf_right, right_index=True)
        with self.assertRaisesRegex(ValueError, "can only asof on a key for left"):
            ps.merge_asof(psdf_left, psdf_right, right_on="a", left_on=["a", "b"])
        with self.assertRaisesRegex(ValueError, "can only asof on a key for right"):
            ps.merge_asof(psdf_left, psdf_right, right_on=["a", "b"], left_on="a")
        with self.assertRaisesRegex(
            ValueError, 'Can only pass argument "by" OR "left_by" and "right_by".'
        ):
            ps.merge_asof(psdf_left, psdf_right, on="a", by="b", left_by="a")
        with self.assertRaisesRegex(ValueError, "missing right_by"):
            ps.merge_asof(psdf_left, psdf_right, on="a", left_by="b")
        with self.assertRaisesRegex(ValueError, "missing left_by"):
            ps.merge_asof(psdf_left, psdf_right, on="a", right_by="b")
        with self.assertRaisesRegex(ValueError, "left_by and right_by must be same length"):
            ps.merge_asof(psdf_left, psdf_right, on="a", left_by="b", right_by=["a", "b"])
        psdf_right.columns = ["A", "B", "C"]
        with self.assertRaisesRegex(ValueError, "No common columns to perform merge on."):
            ps.merge_asof(psdf_left, psdf_right)


class MergeAsOfTests(
    MergeAsOfMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.reshape.test_merge_asof import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
