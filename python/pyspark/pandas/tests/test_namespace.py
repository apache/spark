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
import inspect

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.namespace import _get_index_map, read_delta
from pyspark.pandas.utils import spark_column_equals
from pyspark.pandas.missing.general_functions import MissingPandasLikeGeneralFunctions
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.testing import assert_frame_equal


class NamespaceTestsMixin:
    def test_from_pandas(self):
        pdf = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)

        pser = pdf.year
        psser = ps.from_pandas(pser)

        self.assert_eq(psser, pser)

        pidx = pdf.index
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx, pidx)

        pmidx = pdf.set_index("year", append=True).index
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx, pmidx)

        expected_error_message = "Unknown data type: {}".format(type(psidx).__name__)
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.from_pandas(psidx)

    def test_to_datetime(self):
        pdf = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
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

        pdf = pd.DataFrame({"years": [2015, 2016], "month": [2, 3], "day": [4, 5]})
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        pdf = pd.DataFrame({"years": [2015, 2016], "months": [2, 3], "day": [4, 5]})
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        pdf = pd.DataFrame({"years": [2015, 2016], "months": [2, 3], "days": [4, 5]})
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        # SPARK-36946: Support time for ps.to_datetime
        pdf = pd.DataFrame(
            {
                "year": [2015, 2016],
                "month": [2, 3],
                "day": [4, 5],
                "hour": [2, 3],
                "minute": [10, 30],
                "second": [21, 25],
            }
        )
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        pdf = pd.DataFrame(
            {
                "year": [2015, 2016],
                "month": [2, 3],
                "day": [4, 5],
                "hour": [2, 3],
                "minute": [10, 30],
            }
        )
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        pdf = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5], "hour": [2, 3]})
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        pdf = pd.DataFrame(
            {
                "year": [2015, 2016],
                "month": [2, 3],
                "day": [4, 5],
                "hour": [2, 3],
                "minute": [10, 30],
                "second": [21, 25],
                "ms": [50, 69],
            }
        )
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        pdf = pd.DataFrame(
            {
                "year": [2015, 2016],
                "month": [2, 3],
                "day": [4, 5],
                "hour": [2, 3],
                "minute": [10, 30],
                "second": [21, 25],
                "ms": [50, 69],
                "millisecond": [123, 678],
            }
        )
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

        pdf = pd.DataFrame(
            {
                "Year": [2015, 2016],
                "Month": [2, 3],
                "Day": [4, 5],
                "Hour": [2, 3],
                "Minute": [10, 30],
                "Second": [21, 25],
                "ms": [50, 69],
                "millisecond": [123, 678],
            }
        )
        psdf = ps.from_pandas(pdf)
        dict_from_pdf = pdf.to_dict()

        self.assert_eq(pd.to_datetime(pdf), ps.to_datetime(psdf))
        self.assert_eq(pd.to_datetime(dict_from_pdf), ps.to_datetime(dict_from_pdf))

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
            ps.date_range(start="2017-01-01", end="2017-01-04", inclusive="left"),
            pd.date_range(start="2017-01-01", end="2017-01-04", inclusive="left"),
        )

        self.assert_eq(
            ps.date_range(start="2017-01-01", end="2017-01-04", inclusive="right"),
            pd.date_range(start="2017-01-01", end="2017-01-04", inclusive="right"),
        )

        self.assert_eq(
            ps.date_range(start="2017-01-01", end="2017-01-04", inclusive="both"),
            pd.date_range(start="2017-01-01", end="2017-01-04", inclusive="both"),
        )

        self.assert_eq(
            ps.date_range(start="2017-01-01", end="2017-01-04", inclusive="neither"),
            pd.date_range(start="2017-01-01", end="2017-01-04", inclusive="neither"),
        )

        with self.assertRaisesRegex(
            ValueError, "Inclusive has to be either 'both', 'neither', 'left' or 'right'"
        ):
            ps.date_range(start="2017-01-01", end="2017-01-04", inclusive="test")

        self.assertRaises(
            AssertionError, lambda: ps.date_range(start="1/1/2018", periods=5, tz="Asia/Tokyo")
        )
        self.assertRaises(
            AssertionError, lambda: ps.date_range(start="1/1/2018", periods=5, freq="ns")
        )
        self.assertRaises(
            AssertionError, lambda: ps.date_range(start="1/1/2018", periods=5, freq="N")
        )

    def test_to_timedelta(self):
        self.assert_eq(
            ps.to_timedelta("1 days 06:05:01.00003"),
            pd.to_timedelta("1 days 06:05:01.00003"),
        )
        self.assert_eq(
            ps.to_timedelta("15.5us"),
            pd.to_timedelta("15.5us"),
        )
        self.assert_eq(
            ps.to_timedelta(["1 days 06:05:01.00003", "15.5us", "nan"]),
            pd.to_timedelta(["1 days 06:05:01.00003", "15.5us", "nan"]),
        )
        self.assert_eq(
            ps.to_timedelta(np.arange(5), unit="s"),
            pd.to_timedelta(np.arange(5), unit="s"),
        )
        self.assert_eq(
            ps.to_timedelta(ps.Series([1, 2]), unit="d"),
            pd.to_timedelta(pd.Series([1, 2]), unit="d"),
        )
        self.assert_eq(
            ps.to_timedelta(pd.Series([1, 2]), unit="d"),
            pd.to_timedelta(pd.Series([1, 2]), unit="d"),
        )

    def test_timedelta_range(self):
        self.assert_eq(
            ps.timedelta_range(start="1 day", end="3 days"),
            pd.timedelta_range(start="1 day", end="3 days"),
        )
        self.assert_eq(
            ps.timedelta_range(start="1 day", periods=3),
            pd.timedelta_range(start="1 day", periods=3),
        )
        self.assert_eq(
            ps.timedelta_range(end="3 days", periods=3),
            pd.timedelta_range(end="3 days", periods=3),
        )
        self.assert_eq(
            ps.timedelta_range(end="3 days", periods=3, closed="right"),
            pd.timedelta_range(end="3 days", periods=3, closed="right"),
        )
        self.assert_eq(
            ps.timedelta_range(start="1 day", end="3 days", freq="6H"),
            pd.timedelta_range(start="1 day", end="3 days", freq="6H"),
        )
        self.assert_eq(
            ps.timedelta_range(start="1 day", end="3 days", periods=4),
            pd.timedelta_range(start="1 day", end="3 days", periods=4),
        )

        self.assertRaises(
            AssertionError, lambda: ps.timedelta_range(start="1 day", periods=3, freq="ns")
        )

    def test_concat_multiindex_sort(self):
        # SPARK-39314: Respect ps.concat sort parameter to follow pandas behavior
        idx = pd.MultiIndex.from_tuples([("Y", "A"), ("Y", "B"), ("X", "C"), ("X", "D")])
        pdf = pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8]], columns=idx)
        psdf = ps.from_pandas(pdf)

        ignore_indexes = [True, False]
        joins = ["inner", "outer"]
        sorts = [True, False]
        objs = [
            ([psdf, psdf.reset_index()], [pdf, pdf.reset_index()]),
            ([psdf.reset_index(), psdf], [pdf.reset_index(), pdf]),
        ]
        for ignore_index, join, sort in itertools.product(ignore_indexes, joins, sorts):
            for i, (psdfs, pdfs) in enumerate(objs):
                self.assert_eq(
                    ps.concat(psdfs, ignore_index=ignore_index, join=join, sort=sort),
                    pd.concat(pdfs, ignore_index=ignore_index, join=join, sort=sort),
                )

    def test_concat_index_axis(self):
        pdf = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5], "C": [6, 7, 8]})
        # TODO: pdf.columns.names = ["ABC"]
        psdf = ps.from_pandas(pdf)

        ignore_indexes = [True, False]
        joins = ["inner", "outer"]
        sorts = [True, False]

        objs = [
            ([psdf, psdf], [pdf, pdf]),
            # no Series
            ([psdf, psdf.reset_index()], [pdf, pdf.reset_index()]),
            ([psdf.reset_index(), psdf], [pdf.reset_index(), pdf]),
            ([psdf, psdf[["C", "A"]]], [pdf, pdf[["C", "A"]]]),
            ([psdf[["C", "A"]], psdf], [pdf[["C", "A"]], pdf]),
            # more than two Series
            ([psdf["C"], psdf, psdf["A"]], [pdf["C"], pdf, pdf["A"]]),
            # only Series
            ([psdf["C"], psdf["A"]], [pdf["C"], pdf["A"]]),
        ]

        series_objs = [
            # more than two Series
            ([psdf, psdf["C"], psdf["A"]], [pdf, pdf["C"], pdf["A"]]),
            # only one Series
            ([psdf, psdf["C"]], [pdf, pdf["C"]]),
            ([psdf["C"], psdf], [pdf["C"], pdf]),
        ]
        for psdfs, pdfs in series_objs:
            for ignore_index, join, sort in itertools.product(ignore_indexes, joins, sorts):
                self.assert_eq(
                    ps.concat(psdfs, ignore_index=ignore_index, join=join, sort=sort),
                    pd.concat(pdfs, ignore_index=ignore_index, join=join, sort=sort),
                )

        for ignore_index, join, sort in itertools.product(ignore_indexes, joins, sorts):
            for i, (psdfs, pdfs) in enumerate(objs):
                with self.subTest(
                    ignore_index=ignore_index, join=join, sort=sort, pdfs=pdfs, pair=i
                ):
                    self.assert_eq(
                        ps.concat(psdfs, ignore_index=ignore_index, join=join, sort=sort),
                        pd.concat(pdfs, ignore_index=ignore_index, join=join, sort=sort),
                        almost=(join == "outer"),
                    )

        self.assertRaisesRegex(TypeError, "first argument must be", lambda: ps.concat(psdf))
        self.assertRaisesRegex(TypeError, "cannot concatenate object", lambda: ps.concat([psdf, 1]))

        psdf2 = psdf.set_index("B", append=True)
        self.assertRaisesRegex(
            ValueError, "Index type and names should be same", lambda: ps.concat([psdf, psdf2])
        )

        self.assertRaisesRegex(ValueError, "No objects to concatenate", lambda: ps.concat([]))

        self.assertRaisesRegex(ValueError, "All objects passed", lambda: ps.concat([None, None]))

        pdf3 = pdf.copy()
        psdf3 = psdf.copy()

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        columns.names = ["XYZ", "ABC"]
        pdf3.columns = columns
        psdf3.columns = columns

        objs = [
            ([psdf3, psdf3], [pdf3, pdf3]),
            ([psdf3, psdf3.reset_index()], [pdf3, pdf3.reset_index()]),
            ([psdf3, psdf3[[("Y", "C"), ("X", "A")]]], [pdf3, pdf3[[("Y", "C"), ("X", "A")]]]),
        ]

        objs += [
            ([psdf3.reset_index(), psdf3], [pdf3.reset_index(), pdf3]),
            ([psdf3[[("Y", "C"), ("X", "A")]], psdf3], [pdf3[[("Y", "C"), ("X", "A")]], pdf3]),
        ]

        for ignore_index, sort in itertools.product(ignore_indexes, sorts):
            for i, (psdfs, pdfs) in enumerate(objs):
                with self.subTest(
                    ignore_index=ignore_index, join="outer", sort=sort, pdfs=pdfs, pair=i
                ):
                    self.assert_eq(
                        ps.concat(psdfs, ignore_index=ignore_index, join="outer", sort=sort),
                        pd.concat(pdfs, ignore_index=ignore_index, join="outer", sort=sort),
                    )

        # Skip tests for `join="inner" and sort=False` since pandas is flaky.
        for ignore_index in ignore_indexes:
            for i, (psdfs, pdfs) in enumerate(objs):
                with self.subTest(
                    ignore_index=ignore_index, join="inner", sort=True, pdfs=pdfs, pair=i
                ):
                    self.assert_eq(
                        ps.concat(psdfs, ignore_index=ignore_index, join="inner", sort=True),
                        pd.concat(pdfs, ignore_index=ignore_index, join="inner", sort=True),
                    )

        self.assertRaisesRegex(
            ValueError,
            "MultiIndex columns should have the same levels",
            lambda: ps.concat([psdf, psdf3]),
        )
        self.assert_eq(ps.concat([psdf3[("Y", "C")], psdf3]), pd.concat([pdf3[("Y", "C")], pdf3]))

        pdf4 = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5], "C": [10, 20, 30]})
        psdf4 = ps.from_pandas(pdf4)
        self.assertRaisesRegex(
            ValueError,
            r"Only can inner \(intersect\) or outer \(union\) join the other axis.",
            lambda: ps.concat([psdf, psdf4], join=""),
        )

        self.assertRaisesRegex(
            ValueError,
            r"Only can inner \(intersect\) or outer \(union\) join the other axis.",
            lambda: ps.concat([psdf, psdf4], join="", axis=1),
        )

        self.assertRaisesRegex(
            ValueError,
            r"Only can inner \(intersect\) or outer \(union\) join the other axis.",
            lambda: ps.concat([psdf.A, psdf4.B], join="", axis=1),
        )

        self.assertRaisesRegex(
            ValueError,
            r"Labels have to be unique; however, got duplicated labels \['A'\].",
            lambda: ps.concat([psdf.A, psdf4.A], join="inner", axis=1),
        )

    def test_concat_column_axis(self):
        pdf1 = pd.DataFrame({"A": [0, 2, 4], "B": [1, 3, 5]}, index=[1, 2, 3])
        pdf1.columns.names = ["AB"]
        pdf2 = pd.DataFrame({"C": [1, 2, 3], "D": [4, 5, 6]}, index=[1, 3, 5])
        pdf2.columns.names = ["CD"]
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        psdf3 = psdf1.copy()
        psdf4 = psdf2.copy()
        pdf3 = pdf1.copy()
        pdf4 = pdf2.copy()

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")], names=["X", "AB"])
        pdf3.columns = columns
        psdf3.columns = columns

        columns = pd.MultiIndex.from_tuples([("X", "C"), ("X", "D")], names=["Y", "CD"])
        pdf4.columns = columns
        psdf4.columns = columns

        ignore_indexes = [True, False]
        joins = ["inner", "outer"]

        objs = [
            ([psdf1.A, psdf1.A.rename("B")], [pdf1.A, pdf1.A.rename("B")]),
            (
                [psdf3[("X", "A")], psdf3[("X", "B")]],
                [pdf3[("X", "A")], pdf3[("X", "B")]],
            ),
            (
                [psdf3[("X", "A")], psdf3[("X", "B")].rename("ABC")],
                [pdf3[("X", "A")], pdf3[("X", "B")].rename("ABC")],
            ),
            (
                [psdf3[("X", "A")].rename("ABC"), psdf3[("X", "B")]],
                [pdf3[("X", "A")].rename("ABC"), pdf3[("X", "B")]],
            ),
        ]

        for ignore_index, join in itertools.product(ignore_indexes, joins):
            for i, (psdfs, pdfs) in enumerate(objs):
                with self.subTest(ignore_index=ignore_index, join=join, pdfs=pdfs, pair=i):
                    actual = ps.concat(psdfs, axis=1, ignore_index=ignore_index, join=join)
                    expected = pd.concat(pdfs, axis=1, ignore_index=ignore_index, join=join)
                    self.assert_eq(
                        repr(actual.sort_values(list(actual.columns)).reset_index(drop=True)),
                        repr(expected.sort_values(list(expected.columns)).reset_index(drop=True)),
                    )

    # test dataframes equality with broadcast hint.
    def test_broadcast(self):
        psdf = ps.DataFrame(
            {"key": ["K0", "K1", "K2", "K3"], "A": ["A0", "A1", "A2", "A3"]}, columns=["key", "A"]
        )
        self.assert_eq(psdf, ps.broadcast(psdf))

        psdf.columns = ["x", "y"]
        self.assert_eq(psdf, ps.broadcast(psdf))

        psdf.columns = [("a", "c"), ("b", "d")]
        self.assert_eq(psdf, ps.broadcast(psdf))

        psser = ps.Series([1, 2, 3])
        expected_error_message = "Invalid type : expected DataFrame got {}".format(
            type(psser).__name__
        )
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.broadcast(psser)

    def test_get_index_map(self):
        psdf = ps.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
        sdf = psdf.to_spark()
        self.assertEqual(_get_index_map(sdf), (None, None))

        def check(actual, expected):
            actual_scols, actual_labels = actual
            expected_column_names, expected_labels = expected
            self.assertEqual(len(actual_scols), len(expected_column_names))
            for actual_scol, expected_column_name in zip(actual_scols, expected_column_names):
                expected_scol = sdf[expected_column_name]
                self.assertTrue(spark_column_equals(actual_scol, expected_scol))
            self.assertEqual(actual_labels, expected_labels)

        check(_get_index_map(sdf, "year"), (["year"], [("year",)]))
        check(_get_index_map(sdf, ["year", "month"]), (["year", "month"], [("year",), ("month",)]))

        self.assertRaises(KeyError, lambda: _get_index_map(sdf, ["year", "hour"]))

    def test_read_delta_with_wrong_input(self):
        self.assertRaisesRegex(
            ValueError,
            "version and timestamp cannot be used together",
            lambda: read_delta("fake_path", version="0", timestamp="2021-06-22"),
        )

    def test_to_numeric(self):
        pser = pd.Series(["1", "2", None, "4", "hello"])
        psser = ps.from_pandas(pser)

        # "coerce" and "raise" with Series that contains un-parsable data.
        self.assert_eq(
            pd.to_numeric(pser, errors="coerce"), ps.to_numeric(psser, errors="coerce"), almost=True
        )

        # "raise" with Series that contains parsable data only.
        pser = pd.Series(["1", "2", None, "4", "5.0"])
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pd.to_numeric(pser, errors="raise"), ps.to_numeric(psser, errors="raise"), almost=True
        )

        # "coerce", "ignore" and "raise" with non-Series.
        data = ["1", "2", None, "4", "hello"]
        self.assert_eq(pd.to_numeric(data, errors="coerce"), ps.to_numeric(data, errors="coerce"))
        self.assert_eq(pd.to_numeric(data, errors="ignore"), ps.to_numeric(data, errors="ignore"))

        self.assertRaisesRegex(
            ValueError,
            'Unable to parse string "hello"',
            lambda: ps.to_numeric(data, errors="raise"),
        )

        # "raise" with non-Series that contains parsable data only.
        data = ["1", "2", None, "4", "5.0"]

        self.assert_eq(
            pd.to_numeric(data, errors="raise"), ps.to_numeric(data, errors="raise"), almost=True
        )

        # Wrong string for `errors` parameter.
        self.assertRaisesRegex(
            ValueError,
            "invalid error value specified",
            lambda: ps.to_numeric(psser, errors="errors"),
        )
        # NotImplementedError
        self.assertRaisesRegex(
            NotImplementedError,
            "'ignore' is not implemented yet, when the `arg` is Series.",
            lambda: ps.to_numeric(psser, errors="ignore"),
        )

    def test_json_normalize(self):
        # Basic test case with a simple JSON structure
        data = [
            {"id": 1, "name": "Alice", "address": {"city": "NYC", "zipcode": "10001"}},
            {"id": 2, "name": "Bob", "address": {"city": "SF", "zipcode": "94105"}},
        ]
        assert_frame_equal(pd.json_normalize(data), ps.json_normalize(data))

        # Test case with nested JSON structure
        data = [
            {"id": 1, "name": "Alice", "address": {"city": {"name": "NYC"}, "zipcode": "10001"}},
            {"id": 2, "name": "Bob", "address": {"city": {"name": "SF"}, "zipcode": "94105"}},
        ]
        assert_frame_equal(pd.json_normalize(data), ps.json_normalize(data))

        # Test case with lists included in the JSON structure
        data = [
            {
                "id": 1,
                "name": "Alice",
                "hobbies": ["reading", "swimming"],
                "address": {"city": "NYC", "zipcode": "10001"},
            },
            {
                "id": 2,
                "name": "Bob",
                "hobbies": ["biking"],
                "address": {"city": "SF", "zipcode": "94105"},
            },
        ]
        assert_frame_equal(pd.json_normalize(data), ps.json_normalize(data))

        # Test case with various data types
        data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "is_student": True,
                "address": {"city": "NYC", "zipcode": "10001"},
            },
            {
                "id": 2,
                "name": "Bob",
                "age": 30,
                "is_student": False,
                "address": {"city": "SF", "zipcode": "94105"},
            },
        ]
        assert_frame_equal(pd.json_normalize(data), ps.json_normalize(data))

        # Test case handling empty input data
        data = []
        self.assert_eq(pd.json_normalize(data), ps.json_normalize(data))

    def test_missing(self):
        missing_functions = inspect.getmembers(
            MissingPandasLikeGeneralFunctions, inspect.isfunction
        )
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "The method.*pd.*{}.*not implemented yet.".format(name),
            ):
                getattr(ps, name)()


class NamespaceTests(NamespaceTestsMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_namespace import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
