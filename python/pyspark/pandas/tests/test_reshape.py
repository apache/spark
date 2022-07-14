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

import datetime
from decimal import Decimal
from distutils.version import LooseVersion

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.utils import name_like_string
from pyspark.sql.utils import AnalysisException
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class ReshapeTest(PandasOnSparkTestCase):
    def test_get_dummies(self):
        for pdf_or_ps in [
            pd.Series([1, 1, 1, 2, 2, 1, 3, 4]),
            # pd.Series([1, 1, 1, 2, 2, 1, 3, 4], dtype='category'),
            # pd.Series(pd.Categorical([1, 1, 1, 2, 2, 1, 3, 4],
            #                          categories=[4, 3, 2, 1])),
            pd.DataFrame(
                {
                    "a": [1, 2, 3, 4, 4, 3, 2, 1],
                    # 'b': pd.Categorical(list('abcdabcd')),
                    "b": list("abcdabcd"),
                }
            ),
            pd.DataFrame({10: [1, 2, 3, 4, 4, 3, 2, 1], 20: list("abcdabcd")}),
        ]:
            psdf_or_psser = ps.from_pandas(pdf_or_ps)

            self.assert_eq(ps.get_dummies(psdf_or_psser), pd.get_dummies(pdf_or_ps, dtype=np.int8))

        psser = ps.Series([1, 1, 1, 2, 2, 1, 3, 4])
        with self.assertRaisesRegex(
            NotImplementedError, "get_dummies currently does not support sparse"
        ):
            ps.get_dummies(psser, sparse=True)

    def test_get_dummies_object(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 4, 3, 2, 1],
                # 'a': pd.Categorical([1, 2, 3, 4, 4, 3, 2, 1]),
                "b": list("abcdabcd"),
                # 'c': pd.Categorical(list('abcdabcd')),
                "c": list("abcdabcd"),
            }
        )
        psdf = ps.from_pandas(pdf)

        # Explicitly exclude object columns
        self.assert_eq(
            ps.get_dummies(psdf, columns=["a", "c"]),
            pd.get_dummies(pdf, columns=["a", "c"], dtype=np.int8),
        )

        self.assert_eq(ps.get_dummies(psdf), pd.get_dummies(pdf, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.b), pd.get_dummies(pdf.b, dtype=np.int8))
        self.assert_eq(
            ps.get_dummies(psdf, columns=["b"]), pd.get_dummies(pdf, columns=["b"], dtype=np.int8)
        )

        self.assertRaises(KeyError, lambda: ps.get_dummies(psdf, columns=("a", "c")))
        self.assertRaises(TypeError, lambda: ps.get_dummies(psdf, columns="b"))

        # non-string names
        pdf = pd.DataFrame(
            {10: [1, 2, 3, 4, 4, 3, 2, 1], 20: list("abcdabcd"), 30: list("abcdabcd")}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.get_dummies(psdf, columns=[10, 30]),
            pd.get_dummies(pdf, columns=[10, 30], dtype=np.int8),
        )

        self.assertRaises(TypeError, lambda: ps.get_dummies(psdf, columns=10))

    def test_get_dummies_date_datetime(self):
        pdf = pd.DataFrame(
            {
                "d": [
                    datetime.date(2019, 1, 1),
                    datetime.date(2019, 1, 2),
                    datetime.date(2019, 1, 1),
                ],
                "dt": [
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                    datetime.datetime(2019, 1, 1, 0, 0, 1),
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                ],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(ps.get_dummies(psdf), pd.get_dummies(pdf, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.d), pd.get_dummies(pdf.d, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.dt), pd.get_dummies(pdf.dt, dtype=np.int8))

    def test_get_dummies_boolean(self):
        pdf = pd.DataFrame({"b": [True, False, True]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(ps.get_dummies(psdf), pd.get_dummies(pdf, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.b), pd.get_dummies(pdf.b, dtype=np.int8))

    def test_get_dummies_decimal(self):
        pdf = pd.DataFrame({"d": [Decimal(1.0), Decimal(2.0), Decimal(1)]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(ps.get_dummies(psdf), pd.get_dummies(pdf, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.d), pd.get_dummies(pdf.d, dtype=np.int8), almost=True)

    def test_get_dummies_kwargs(self):
        # pser = pd.Series([1, 1, 1, 2, 2, 1, 3, 4], dtype='category')
        pser = pd.Series([1, 1, 1, 2, 2, 1, 3, 4])
        psser = ps.from_pandas(pser)
        self.assert_eq(
            ps.get_dummies(psser, prefix="X", prefix_sep="-"),
            pd.get_dummies(pser, prefix="X", prefix_sep="-", dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psser, drop_first=True),
            pd.get_dummies(pser, drop_first=True, dtype=np.int8),
        )

        # nan
        # pser = pd.Series([1, 1, 1, 2, np.nan, 3, np.nan, 5], dtype='category')
        pser = pd.Series([1, 1, 1, 2, np.nan, 3, np.nan, 5])
        psser = ps.from_pandas(pser)
        self.assert_eq(ps.get_dummies(psser), pd.get_dummies(pser, dtype=np.int8), almost=True)

        # dummy_na
        self.assert_eq(
            ps.get_dummies(psser, dummy_na=True), pd.get_dummies(pser, dummy_na=True, dtype=np.int8)
        )

    def test_get_dummies_prefix(self):
        pdf = pd.DataFrame({"A": ["a", "b", "a"], "B": ["b", "a", "c"], "D": [0, 0, 1]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.get_dummies(psdf, prefix=["foo", "bar"]),
            pd.get_dummies(pdf, prefix=["foo", "bar"], dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psdf, prefix=["foo"], columns=["B"]),
            pd.get_dummies(pdf, prefix=["foo"], columns=["B"], dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psdf, prefix={"A": "foo", "B": "bar"}),
            pd.get_dummies(pdf, prefix={"A": "foo", "B": "bar"}, dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psdf, prefix={"B": "foo", "A": "bar"}),
            pd.get_dummies(pdf, prefix={"B": "foo", "A": "bar"}, dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psdf, prefix={"A": "foo", "B": "bar"}, columns=["A", "B"]),
            pd.get_dummies(pdf, prefix={"A": "foo", "B": "bar"}, columns=["A", "B"], dtype=np.int8),
        )

        with self.assertRaisesRegex(NotImplementedError, "string types"):
            ps.get_dummies(psdf, prefix="foo")
        with self.assertRaisesRegex(ValueError, "Length of 'prefix' \\(1\\) .* \\(2\\)"):
            ps.get_dummies(psdf, prefix=["foo"])
        with self.assertRaisesRegex(ValueError, "Length of 'prefix' \\(2\\) .* \\(1\\)"):
            ps.get_dummies(psdf, prefix=["foo", "bar"], columns=["B"])

        pser = pd.Series([1, 1, 1, 2, 2, 1, 3, 4], name="A")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            ps.get_dummies(psser, prefix="foo"), pd.get_dummies(pser, prefix="foo", dtype=np.int8)
        )

        # columns are ignored.
        self.assert_eq(
            ps.get_dummies(psser, prefix=["foo"], columns=["B"]),
            pd.get_dummies(pser, prefix=["foo"], columns=["B"], dtype=np.int8),
        )

    def test_get_dummies_dtype(self):
        pdf = pd.DataFrame(
            {
                # "A": pd.Categorical(['a', 'b', 'a'], categories=['a', 'b', 'c']),
                "A": ["a", "b", "a"],
                "B": [0, 0, 1],
            }
        )
        psdf = ps.from_pandas(pdf)

        exp = pd.get_dummies(pdf)
        exp = exp.astype({"A_a": "float64", "A_b": "float64"})
        res = ps.get_dummies(psdf, dtype="float64")
        self.assert_eq(res, exp)

    def test_get_dummies_multiindex_columns(self):
        pdf = pd.DataFrame(
            {
                ("x", "a", "1"): [1, 2, 3, 4, 4, 3, 2, 1],
                ("x", "b", "2"): list("abcdabcd"),
                ("y", "c", "3"): list("abcdabcd"),
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.get_dummies(psdf),
            pd.get_dummies(pdf, dtype=np.int8).rename(columns=name_like_string),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=[("y", "c", "3"), ("x", "a", "1")]),
            pd.get_dummies(pdf, columns=[("y", "c", "3"), ("x", "a", "1")], dtype=np.int8).rename(
                columns=name_like_string
            ),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=["x"]),
            pd.get_dummies(pdf, columns=["x"], dtype=np.int8).rename(columns=name_like_string),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=("x", "a")),
            pd.get_dummies(pdf, columns=("x", "a"), dtype=np.int8).rename(columns=name_like_string),
        )

        self.assertRaises(KeyError, lambda: ps.get_dummies(psdf, columns=["z"]))
        self.assertRaises(KeyError, lambda: ps.get_dummies(psdf, columns=("x", "c")))
        self.assertRaises(ValueError, lambda: ps.get_dummies(psdf, columns=[("x",), "c"]))
        self.assertRaises(TypeError, lambda: ps.get_dummies(psdf, columns="x"))

        # non-string names
        pdf = pd.DataFrame(
            {
                ("x", 1, "a"): [1, 2, 3, 4, 4, 3, 2, 1],
                ("x", 2, "b"): list("abcdabcd"),
                ("y", 3, "c"): list("abcdabcd"),
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            ps.get_dummies(psdf),
            pd.get_dummies(pdf, dtype=np.int8).rename(columns=name_like_string),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=[("y", 3, "c"), ("x", 1, "a")]),
            pd.get_dummies(pdf, columns=[("y", 3, "c"), ("x", 1, "a")], dtype=np.int8).rename(
                columns=name_like_string
            ),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=["x"]),
            pd.get_dummies(pdf, columns=["x"], dtype=np.int8).rename(columns=name_like_string),
        )
        self.assert_eq(
            ps.get_dummies(psdf, columns=("x", 1)),
            pd.get_dummies(pdf, columns=("x", 1), dtype=np.int8).rename(columns=name_like_string),
        )

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
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(
                pd.merge_asof(
                    pdf_left.set_index("a"), pdf_right, left_index=True, right_on="a"
                ).sort_index(),
                ps.merge_asof(
                    psdf_left.set_index("a"), psdf_right, left_index=True, right_on="a"
                ).sort_index(),
            )
        else:
            expected = pd.DataFrame(
                {
                    "b_x": ["x", "y", "z"],
                    "left_val": ["a", "b", "c"],
                    "a": [1, 3, 7],
                    "b_y": ["v", "x", "z"],
                    "right_val": [1, 3, 7],
                },
                index=pd.Index([1, 5, 10], name="a"),
            )
            self.assert_eq(
                expected,
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

        self.assertRaises(
            AnalysisException, lambda: ps.merge_asof(psdf_left, psdf_right, on="a", tolerance=-1)
        )


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_reshape import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
