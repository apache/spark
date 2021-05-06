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

import itertools

import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.namespace import _get_index_map
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class NamespaceTest(PandasOnSparkTestCase, SQLTestUtils):
    def test_from_pandas(self):
        pdf = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf, pdf)

        pser = pdf.year
        kser = ps.from_pandas(pser)

        self.assert_eq(kser, pser)

        pidx = pdf.index
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx, pidx)

        pmidx = pdf.set_index("year", append=True).index
        kmidx = ps.from_pandas(pmidx)

        self.assert_eq(kmidx, pmidx)

        expected_error_message = "Unknown data type: {}".format(type(kidx).__name__)
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.from_pandas(kidx)

    def test_to_datetime(self):
        pdf = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
        kdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(kdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        self.assert_eq(pd.to_datetime(1490195805, unit="s"), ps.to_datetime(1490195805, unit="s"))
        self.assert_eq(
            pd.to_datetime(1490195805433502912, unit="ns"),
            ps.to_datetime(1490195805433502912, unit="ns"),
        )

        self.assert_eq(
            pd.to_datetime([1, 2, 3], unit="D", origin=pd.Timestamp("1960-01-01")),
            ps.to_datetime([1, 2, 3], unit="D", origin=pd.Timestamp("1960-01-01")),
        )

    def test_date_range(self):
        self.assert_eq(
            ps.date_range(start="1/1/2018", end="1/08/2018"),
            pd.date_range(start="1/1/2018", end="1/08/2018"),
        )
        self.assert_eq(
            ps.date_range(start="1/1/2018", periods=8), pd.date_range(start="1/1/2018", periods=8)
        )
        self.assert_eq(
            ps.date_range(end="1/1/2018", periods=8), pd.date_range(end="1/1/2018", periods=8)
        )
        self.assert_eq(
            ps.date_range(start="2018-04-24", end="2018-04-27", periods=3),
            pd.date_range(start="2018-04-24", end="2018-04-27", periods=3),
        )

        self.assert_eq(
            ps.date_range(start="1/1/2018", periods=5, freq="M"),
            pd.date_range(start="1/1/2018", periods=5, freq="M"),
        )

        self.assert_eq(
            ps.date_range(start="1/1/2018", periods=5, freq="3M"),
            pd.date_range(start="1/1/2018", periods=5, freq="3M"),
        )

        self.assert_eq(
            ps.date_range(start="1/1/2018", periods=5, freq=pd.offsets.MonthEnd(3)),
            pd.date_range(start="1/1/2018", periods=5, freq=pd.offsets.MonthEnd(3)),
        )

        self.assert_eq(
            ps.date_range(start="2017-01-01", end="2017-01-04", closed="left"),
            pd.date_range(start="2017-01-01", end="2017-01-04", closed="left"),
        )

        self.assert_eq(
            ps.date_range(start="2017-01-01", end="2017-01-04", closed="right"),
            pd.date_range(start="2017-01-01", end="2017-01-04", closed="right"),
        )

        self.assertRaises(
            AssertionError, lambda: ps.date_range(start="1/1/2018", periods=5, tz="Asia/Tokyo")
        )
        self.assertRaises(
            AssertionError, lambda: ps.date_range(start="1/1/2018", periods=5, freq="ns")
        )
        self.assertRaises(
            AssertionError, lambda: ps.date_range(start="1/1/2018", periods=5, freq="N")
        )

    def test_concat_index_axis(self):
        pdf = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5], "C": [6, 7, 8]})
        # TODO: pdf.columns.names = ["ABC"]
        kdf = ps.from_pandas(pdf)

        ignore_indexes = [True, False]
        joins = ["inner", "outer"]
        sorts = [True, False]

        objs = [
            ([kdf, kdf], [pdf, pdf]),
            ([kdf, kdf.reset_index()], [pdf, pdf.reset_index()]),
            ([kdf.reset_index(), kdf], [pdf.reset_index(), pdf]),
            ([kdf, kdf[["C", "A"]]], [pdf, pdf[["C", "A"]]]),
            ([kdf[["C", "A"]], kdf], [pdf[["C", "A"]], pdf]),
            ([kdf, kdf["C"]], [pdf, pdf["C"]]),
            ([kdf["C"], kdf], [pdf["C"], pdf]),
            ([kdf["C"], kdf, kdf["A"]], [pdf["C"], pdf, pdf["A"]]),
            ([kdf, kdf["C"], kdf["A"]], [pdf, pdf["C"], pdf["A"]]),
        ]

        for ignore_index, join, sort in itertools.product(ignore_indexes, joins, sorts):
            for i, (kdfs, pdfs) in enumerate(objs):
                with self.subTest(
                    ignore_index=ignore_index, join=join, sort=sort, pdfs=pdfs, pair=i
                ):
                    self.assert_eq(
                        ps.concat(kdfs, ignore_index=ignore_index, join=join, sort=sort),
                        pd.concat(pdfs, ignore_index=ignore_index, join=join, sort=sort),
                        almost=(join == "outer"),
                    )

        self.assertRaisesRegex(TypeError, "first argument must be", lambda: ps.concat(kdf))
        self.assertRaisesRegex(TypeError, "cannot concatenate object", lambda: ps.concat([kdf, 1]))

        kdf2 = kdf.set_index("B", append=True)
        self.assertRaisesRegex(
            ValueError, "Index type and names should be same", lambda: ps.concat([kdf, kdf2])
        )

        self.assertRaisesRegex(ValueError, "No objects to concatenate", lambda: ps.concat([]))

        self.assertRaisesRegex(ValueError, "All objects passed", lambda: ps.concat([None, None]))

        pdf3 = pdf.copy()
        kdf3 = kdf.copy()

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        # TODO: colums.names = ["XYZ", "ABC"]
        pdf3.columns = columns
        kdf3.columns = columns

        objs = [
            ([kdf3, kdf3], [pdf3, pdf3]),
            ([kdf3, kdf3.reset_index()], [pdf3, pdf3.reset_index()]),
            ([kdf3.reset_index(), kdf3], [pdf3.reset_index(), pdf3]),
            ([kdf3, kdf3[[("Y", "C"), ("X", "A")]]], [pdf3, pdf3[[("Y", "C"), ("X", "A")]]]),
            ([kdf3[[("Y", "C"), ("X", "A")]], kdf3], [pdf3[[("Y", "C"), ("X", "A")]], pdf3]),
        ]

        for ignore_index, sort in itertools.product(ignore_indexes, sorts):
            for i, (kdfs, pdfs) in enumerate(objs):
                with self.subTest(
                    ignore_index=ignore_index, join="outer", sort=sort, pdfs=pdfs, pair=i
                ):
                    self.assert_eq(
                        ps.concat(kdfs, ignore_index=ignore_index, join="outer", sort=sort),
                        pd.concat(pdfs, ignore_index=ignore_index, join="outer", sort=sort),
                    )

        # Skip tests for `join="inner" and sort=False` since pandas is flaky.
        for ignore_index in ignore_indexes:
            for i, (kdfs, pdfs) in enumerate(objs):
                with self.subTest(
                    ignore_index=ignore_index, join="inner", sort=True, pdfs=pdfs, pair=i
                ):
                    self.assert_eq(
                        ps.concat(kdfs, ignore_index=ignore_index, join="inner", sort=True),
                        pd.concat(pdfs, ignore_index=ignore_index, join="inner", sort=True),
                    )

        self.assertRaisesRegex(
            ValueError,
            "MultiIndex columns should have the same levels",
            lambda: ps.concat([kdf, kdf3]),
        )
        self.assertRaisesRegex(
            ValueError,
            "MultiIndex columns should have the same levels",
            lambda: ps.concat([kdf3[("Y", "C")], kdf3]),
        )

        pdf4 = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5], "C": [10, 20, 30]})
        kdf4 = ps.from_pandas(pdf4)
        self.assertRaisesRegex(
            ValueError,
            r"Only can inner \(intersect\) or outer \(union\) join the other axis.",
            lambda: ps.concat([kdf, kdf4], join=""),
        )

        self.assertRaisesRegex(
            ValueError,
            r"Only can inner \(intersect\) or outer \(union\) join the other axis.",
            lambda: ps.concat([kdf, kdf4], join="", axis=1),
        )

        self.assertRaisesRegex(
            ValueError,
            r"Only can inner \(intersect\) or outer \(union\) join the other axis.",
            lambda: ps.concat([kdf.A, kdf4.B], join="", axis=1),
        )

        self.assertRaisesRegex(
            ValueError,
            r"Labels have to be unique; however, got duplicated labels \['A'\].",
            lambda: ps.concat([kdf.A, kdf4.A], join="inner", axis=1),
        )

    def test_concat_column_axis(self):
        pdf1 = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5]}, index=[1, 2, 3])
        pdf1.columns.names = ["AB"]
        pdf2 = pd.DataFrame({"C": [1, 2, 3], "D": [4, 5, 6]}, index=[1, 3, 5])
        pdf2.columns.names = ["CD"]
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        kdf3 = kdf1.copy()
        kdf4 = kdf2.copy()
        pdf3 = pdf1.copy()
        pdf4 = pdf2.copy()

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")], names=["X", "AB"])
        pdf3.columns = columns
        kdf3.columns = columns

        columns = pd.MultiIndex.from_tuples([("X", "C"), ("X", "D")], names=["Y", "CD"])
        pdf4.columns = columns
        kdf4.columns = columns

        ignore_indexes = [True, False]
        joins = ["inner", "outer"]

        objs = [
            ([kdf1.A, kdf1.A.rename("B")], [pdf1.A, pdf1.A.rename("B")]),
            ([kdf3[("X", "A")], kdf3[("X", "B")]], [pdf3[("X", "A")], pdf3[("X", "B")]],),
            (
                [kdf3[("X", "A")], kdf3[("X", "B")].rename("ABC")],
                [pdf3[("X", "A")], pdf3[("X", "B")].rename("ABC")],
            ),
            (
                [kdf3[("X", "A")].rename("ABC"), kdf3[("X", "B")]],
                [pdf3[("X", "A")].rename("ABC"), pdf3[("X", "B")]],
            ),
        ]

        for ignore_index, join in itertools.product(ignore_indexes, joins):
            for i, (kdfs, pdfs) in enumerate(objs):
                with self.subTest(ignore_index=ignore_index, join=join, pdfs=pdfs, pair=i):
                    actual = ps.concat(kdfs, axis=1, ignore_index=ignore_index, join=join)
                    expected = pd.concat(pdfs, axis=1, ignore_index=ignore_index, join=join)
                    self.assert_eq(
                        repr(actual.sort_values(list(actual.columns)).reset_index(drop=True)),
                        repr(expected.sort_values(list(expected.columns)).reset_index(drop=True)),
                    )

    # test dataframes equality with broadcast hint.
    def test_broadcast(self):
        kdf = ps.DataFrame(
            {"key": ["K0", "K1", "K2", "K3"], "A": ["A0", "A1", "A2", "A3"]}, columns=["key", "A"]
        )
        self.assert_eq(kdf, ps.broadcast(kdf))

        kdf.columns = ["x", "y"]
        self.assert_eq(kdf, ps.broadcast(kdf))

        kdf.columns = [("a", "c"), ("b", "d")]
        self.assert_eq(kdf, ps.broadcast(kdf))

        kser = ps.Series([1, 2, 3])
        expected_error_message = "Invalid type : expected DataFrame got {}".format(
            type(kser).__name__
        )
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.broadcast(kser)

    def test_get_index_map(self):
        kdf = ps.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
        sdf = kdf.to_spark()
        self.assertEqual(_get_index_map(sdf), (None, None))

        def check(actual, expected):
            actual_scols, actual_labels = actual
            expected_column_names, expected_labels = expected
            self.assertEqual(len(actual_scols), len(expected_column_names))
            for actual_scol, expected_column_name in zip(actual_scols, expected_column_names):
                expected_scol = sdf[expected_column_name]
                self.assertTrue(actual_scol._jc.equals(expected_scol._jc))
            self.assertEqual(actual_labels, expected_labels)

        check(_get_index_map(sdf, "year"), (["year"], [("year",)]))
        check(_get_index_map(sdf, ["year", "month"]), (["year", "month"], [("year",), ("month",)]))

        self.assertRaises(KeyError, lambda: _get_index_map(sdf, ["year", "hour"]))


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_namespace import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
