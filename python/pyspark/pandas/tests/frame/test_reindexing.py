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
from pandas.tseries.offsets import DateOffset

from pyspark import pandas as ps
from pyspark.errors import PySparkValueError
from pyspark.pandas.config import option_context
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.testing.utils import is_ansi_mode_test, ansi_mode_not_supported_message


# This file contains test cases for 'Reindexing / Selection / Label manipulation'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#reindexing-selection-label-manipulation
class FrameReindexingMixin:
    def test_add_prefix(self):
        pdf = pd.DataFrame({"A": [1, 2, 3, 4], "B": [3, 4, 5, 6]}, index=np.random.rand(4))
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.add_prefix("col_"), psdf.add_prefix("col_"))

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.add_prefix("col_"), psdf.add_prefix("col_"))

    def test_add_suffix(self):
        pdf = pd.DataFrame({"A": [1, 2, 3, 4], "B": [3, 4, 5, 6]}, index=np.random.rand(4))
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.add_suffix("first_series"), psdf.add_suffix("first_series"))

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.add_suffix("first_series"), psdf.add_suffix("first_series"))

    def test_at_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pdf = pd.DataFrame({"A": [1, 2, 3, 4]}, index=idx)
        psdf = ps.from_pandas(pdf)
        psdf.at_time("0:20")
        self.assert_eq(
            pdf.at_time("0:20").sort_index(),
            psdf.at_time("0:20").sort_index(),
        )

        # Index name is 'ts'
        pdf.index.name = "ts"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.at_time("0:20").sort_index(),
            psdf.at_time("0:20").sort_index(),
        )

        # Index name is 'ts', column label is 'index'
        pdf.columns = pd.Index(["index"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.at_time("0:40").sort_index(),
            psdf.at_time("0:40").sort_index(),
        )

        # Both index name and column label are 'index'
        pdf.index.name = "index"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.at_time("0:40").sort_index(),
            psdf.at_time("0:40").sort_index(),
        )

        # Index name is 'index', column label is ('X', 'A')
        pdf.columns = pd.MultiIndex.from_arrays([["X"], ["A"]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.at_time("0:40").sort_index(),
            psdf.at_time("0:40").sort_index(),
        )

        with self.assertRaisesRegex(NotImplementedError, "'asof' argument is not supported"):
            psdf.at_time("0:15", asof=True)

        with self.assertRaisesRegex(NotImplementedError, "at_time currently only works for axis=0"):
            psdf.at_time("0:15", axis=1)

        psdf = ps.DataFrame({"A": [1, 2, 3, 4]})
        with self.assertRaisesRegex(TypeError, "Index must be DatetimeIndex"):
            psdf.at_time("0:15")

    def test_between_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pdf = pd.DataFrame({"A": [1, 2, 3, 4]}, index=idx)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        pdf.index.name = "ts"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        # Column label is 'index'
        pdf.columns = pd.Index(["index"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        # Both index name and column label are 'index'
        pdf.index.name = "index"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        # Index name is 'index', column label is ('X', 'A')
        pdf.columns = pd.MultiIndex.from_arrays([["X"], ["A"]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45").sort_index(),
            psdf.between_time("0:15", "0:45").sort_index(),
        )

        with self.assertRaisesRegex(
            NotImplementedError, "between_time currently only works for axis=0"
        ):
            psdf.between_time("0:15", "0:45", axis=1)

        psdf = ps.DataFrame({"A": [1, 2, 3, 4]})
        with self.assertRaisesRegex(TypeError, "Index must be DatetimeIndex"):
            psdf.between_time("0:15", "0:45")

        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.between_time("0:15", "0:45", inclusive="neither").sort_index(),
            psdf.between_time("0:15", "0:45", inclusive="neither").sort_index(),
        )

        self.assert_eq(
            pdf.between_time("0:15", "0:45", inclusive="left").sort_index(),
            psdf.between_time("0:15", "0:45", inclusive="left").sort_index(),
        )

        self.assert_eq(
            pdf.between_time("0:15", "0:45", inclusive="right").sort_index(),
            psdf.between_time("0:15", "0:45", inclusive="right").sort_index(),
        )

        with self.assertRaises(PySparkValueError) as ctx:
            psdf.between_time("0:15", "0:45", inclusive="")

        self.check_error(
            exception=ctx.exception,
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "inclusive",
                "allowed_values": str(["left", "right", "both", "neither"]),
            },
        )

    def test_drop(self):
        pdf = pd.DataFrame({"x": [1, 2], "y": [3, 4], "z": [5, 6]}, index=np.random.rand(2))
        psdf = ps.from_pandas(pdf)

        # Assert 'labels' or 'columns' parameter is set
        expected_error_message = "Need to specify at least one of 'labels' or 'columns'"
        with self.assertRaisesRegex(ValueError, expected_error_message):
            psdf.drop()

        #
        # Drop columns
        #

        # Assert using a str for 'labels' works
        self.assert_eq(psdf.drop("x", axis=1), pdf.drop("x", axis=1))
        self.assert_eq((psdf + 1).drop("x", axis=1), (pdf + 1).drop("x", axis=1))
        # Assert using a list for 'labels' works
        self.assert_eq(psdf.drop(["y", "z"], axis=1), pdf.drop(["y", "z"], axis=1))
        self.assert_eq(psdf.drop(["x", "y", "z"], axis=1), pdf.drop(["x", "y", "z"], axis=1))
        # Assert using 'columns' instead of 'labels' produces the same results
        self.assert_eq(psdf.drop(columns="x"), pdf.drop(columns="x"))
        self.assert_eq(psdf.drop(columns=["y", "z"]), pdf.drop(columns=["y", "z"]))
        self.assert_eq(psdf.drop(columns=["x", "y", "z"]), pdf.drop(columns=["x", "y", "z"]))
        self.assert_eq(psdf.drop(columns=[]), pdf.drop(columns=[]))

        columns = pd.MultiIndex.from_tuples([(1, "x"), (1, "y"), (2, "z")])
        pdf.columns = columns
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.drop(columns=1), pdf.drop(columns=1))
        self.assert_eq(psdf.drop(columns=(1, "x")), pdf.drop(columns=(1, "x")))
        self.assert_eq(psdf.drop(columns=[(1, "x"), 2]), pdf.drop(columns=[(1, "x"), 2]))
        self.assert_eq(
            psdf.drop(columns=[(1, "x"), (1, "y"), (2, "z")]),
            pdf.drop(columns=[(1, "x"), (1, "y"), (2, "z")]),
        )

        self.assertRaises(KeyError, lambda: psdf.drop(columns=3))
        self.assertRaises(KeyError, lambda: psdf.drop(columns=(1, "z")))

        pdf.index = pd.MultiIndex.from_tuples([("i", 0), ("j", 1)])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.drop(columns=[(1, "x"), (1, "y"), (2, "z")]),
            pdf.drop(columns=[(1, "x"), (1, "y"), (2, "z")]),
        )

        # non-string names
        pdf = pd.DataFrame({10: [1, 2], 20: [3, 4], 30: [5, 6]}, index=np.random.rand(2))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.drop(10, axis=1), pdf.drop(10, axis=1))
        self.assert_eq(psdf.drop([20, 30], axis=1), pdf.drop([20, 30], axis=1))

        #
        # Drop rows
        #

        pdf = pd.DataFrame({"X": [1, 2, 3], "Y": [4, 5, 6], "Z": [7, 8, 9]}, index=["A", "B", "C"])
        psdf = ps.from_pandas(pdf)

        # Given labels (and axis = 0)
        self.assert_eq(psdf.drop(labels="A", axis=0), pdf.drop(labels="A", axis=0))
        self.assert_eq(psdf.drop(labels="A"), pdf.drop(labels="A"))
        self.assert_eq((psdf + 1).drop(labels="A"), (pdf + 1).drop(labels="A"))
        self.assert_eq(psdf.drop(labels=["A", "C"], axis=0), pdf.drop(labels=["A", "C"], axis=0))
        self.assert_eq(
            psdf.drop(labels=["A", "B", "C"], axis=0), pdf.drop(labels=["A", "B", "C"], axis=0)
        )

        with ps.option_context("compute.isin_limit", 2):
            self.assert_eq(
                psdf.drop(labels=["A", "B", "C"], axis=0), pdf.drop(labels=["A", "B", "C"], axis=0)
            )

        # Given index
        self.assert_eq(psdf.drop(index="A"), pdf.drop(index="A"))
        self.assert_eq(psdf.drop(index=["A", "C"]), pdf.drop(index=["A", "C"]))
        self.assert_eq(psdf.drop(index=["A", "B", "C"]), pdf.drop(index=["A", "B", "C"]))
        self.assert_eq(psdf.drop(index=[]), pdf.drop(index=[]))

        with ps.option_context("compute.isin_limit", 2):
            self.assert_eq(psdf.drop(index=["A", "B", "C"]), pdf.drop(index=["A", "B", "C"]))

        # Non-string names
        pdf.index = [10, 20, 30]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.drop(labels=10, axis=0), pdf.drop(labels=10, axis=0))
        self.assert_eq(psdf.drop(labels=[10, 30], axis=0), pdf.drop(labels=[10, 30], axis=0))
        self.assert_eq(
            psdf.drop(labels=[10, 20, 30], axis=0), pdf.drop(labels=[10, 20, 30], axis=0)
        )

        with ps.option_context("compute.isin_limit", 2):
            self.assert_eq(
                psdf.drop(labels=[10, 20, 30], axis=0), pdf.drop(labels=[10, 20, 30], axis=0)
            )

        # MultiIndex
        pdf.index = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assertRaises(NotImplementedError, lambda: psdf.drop(labels=[("a", "x")]))

        #
        # Drop rows and columns
        #
        pdf = pd.DataFrame({"X": [1, 2, 3], "Y": [4, 5, 6], "Z": [7, 8, 9]}, index=["A", "B", "C"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.drop(index="A", columns="X"), pdf.drop(index="A", columns="X"))
        self.assert_eq(
            psdf.drop(index=["A", "C"], columns=["X", "Z"]),
            pdf.drop(index=["A", "C"], columns=["X", "Z"]),
        )
        self.assert_eq(
            psdf.drop(index=["A", "B", "C"], columns=["X", "Z"]),
            pdf.drop(index=["A", "B", "C"], columns=["X", "Z"]),
        )
        with ps.option_context("compute.isin_limit", 2):
            self.assert_eq(
                psdf.drop(index=["A", "B", "C"], columns=["X", "Z"]),
                pdf.drop(index=["A", "B", "C"], columns=["X", "Z"]),
            )
        self.assert_eq(
            psdf.drop(index=[], columns=["X", "Z"]),
            pdf.drop(index=[], columns=["X", "Z"]),
        )
        self.assert_eq(
            psdf.drop(index=["A", "B", "C"], columns=[]),
            pdf.drop(index=["A", "B", "C"], columns=[]),
        )
        self.assert_eq(
            psdf.drop(index=[], columns=[]),
            pdf.drop(index=[], columns=[]),
        )
        self.assertRaises(
            ValueError,
            lambda: psdf.drop(labels="A", axis=0, columns="X"),
        )

    def test_droplevel(self):
        pdf = (
            pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
            .set_index([0, 1])
            .rename_axis(["a", "b"])
        )
        pdf.columns = pd.MultiIndex.from_tuples(
            [("c", "e"), ("d", "f")], names=["level_1", "level_2"]
        )
        psdf = ps.from_pandas(pdf)

        self.assertRaises(ValueError, lambda: psdf.droplevel(["a", "b"]))
        self.assertRaises(ValueError, lambda: psdf.droplevel([1, 1, 1, 1, 1]))
        self.assertRaises(IndexError, lambda: psdf.droplevel(2))
        self.assertRaises(IndexError, lambda: psdf.droplevel(-3))
        self.assertRaises(KeyError, lambda: psdf.droplevel({"a"}))
        self.assertRaises(KeyError, lambda: psdf.droplevel({"a": 1}))

        self.assertRaises(ValueError, lambda: psdf.droplevel(["level_1", "level_2"], axis=1))
        self.assertRaises(IndexError, lambda: psdf.droplevel(2, axis=1))
        self.assertRaises(IndexError, lambda: psdf.droplevel(-3, axis=1))
        self.assertRaises(KeyError, lambda: psdf.droplevel({"level_1"}, axis=1))
        self.assertRaises(KeyError, lambda: psdf.droplevel({"level_1": 1}, axis=1))

        self.assert_eq(pdf.droplevel("a"), psdf.droplevel("a"))
        self.assert_eq(pdf.droplevel(["a"]), psdf.droplevel(["a"]))
        self.assert_eq(pdf.droplevel(("a",)), psdf.droplevel(("a",)))
        self.assert_eq(pdf.droplevel(0), psdf.droplevel(0))
        self.assert_eq(pdf.droplevel(-1), psdf.droplevel(-1))

        self.assert_eq(pdf.droplevel("level_1", axis=1), psdf.droplevel("level_1", axis=1))
        self.assert_eq(pdf.droplevel(["level_1"], axis=1), psdf.droplevel(["level_1"], axis=1))
        self.assert_eq(pdf.droplevel(("level_1",), axis=1), psdf.droplevel(("level_1",), axis=1))
        self.assert_eq(pdf.droplevel(0, axis=1), psdf.droplevel(0, axis=1))
        self.assert_eq(pdf.droplevel(-1, axis=1), psdf.droplevel(-1, axis=1))

        # Tupled names
        pdf.columns.names = [("level", 1), ("level", 2)]
        pdf.index.names = [("a", 10), ("x", 20)]
        psdf = ps.from_pandas(pdf)

        self.assertRaises(KeyError, lambda: psdf.droplevel("a"))
        self.assertRaises(KeyError, lambda: psdf.droplevel(("a", 10)))

        self.assert_eq(pdf.droplevel([("a", 10)]), psdf.droplevel([("a", 10)]))
        self.assert_eq(
            pdf.droplevel([("level", 1)], axis=1), psdf.droplevel([("level", 1)], axis=1)
        )

        # non-string names
        pdf = (
            pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
            .set_index([0, 1])
            .rename_axis([10.0, 20.0])
        )
        pdf.columns = pd.MultiIndex.from_tuples([("c", "e"), ("d", "f")], names=[100.0, 200.0])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.droplevel(10.0), psdf.droplevel(10.0))
        self.assert_eq(pdf.droplevel([10.0]), psdf.droplevel([10.0]))
        self.assert_eq(pdf.droplevel((10.0,)), psdf.droplevel((10.0,)))
        self.assert_eq(pdf.droplevel(0), psdf.droplevel(0))
        self.assert_eq(pdf.droplevel(-1), psdf.droplevel(-1))
        self.assert_eq(pdf.droplevel(100.0, axis=1), psdf.droplevel(100.0, axis=1))
        self.assert_eq(pdf.droplevel(0, axis=1), psdf.droplevel(0, axis=1))

    def test_drop_duplicates(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 2, 2, 3], "b": ["a", "a", "a", "c", "d"]}, index=np.random.rand(5)
        )
        psdf = ps.from_pandas(pdf)

        # inplace is False
        for keep in ["first", "last", False]:
            with self.subTest(keep=keep):
                self.assert_eq(
                    pdf.drop_duplicates(keep=keep).sort_index(),
                    psdf.drop_duplicates(keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates("a", keep=keep).sort_index(),
                    psdf.drop_duplicates("a", keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates(["a", "b"], keep=keep).sort_index(),
                    psdf.drop_duplicates(["a", "b"], keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.set_index("a", append=True).drop_duplicates(keep=keep).sort_index(),
                    psdf.set_index("a", append=True).drop_duplicates(keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.set_index("a", append=True).drop_duplicates("b", keep=keep).sort_index(),
                    psdf.set_index("a", append=True).drop_duplicates("b", keep=keep).sort_index(),
                )

        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
        pdf.columns = columns
        psdf.columns = columns

        # inplace is False
        for keep in ["first", "last", False]:
            with self.subTest("multi-index columns", keep=keep):
                self.assert_eq(
                    pdf.drop_duplicates(keep=keep).sort_index(),
                    psdf.drop_duplicates(keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates(("x", "a"), keep=keep).sort_index(),
                    psdf.drop_duplicates(("x", "a"), keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates([("x", "a"), ("y", "b")], keep=keep).sort_index(),
                    psdf.drop_duplicates([("x", "a"), ("y", "b")], keep=keep).sort_index(),
                )
                self.assert_eq(
                    pdf.drop_duplicates(
                        [("x", "a"), ("y", "b")], keep=keep, ignore_index=True
                    ).sort_index(),
                    psdf.drop_duplicates(
                        [("x", "a"), ("y", "b")], keep=keep, ignore_index=True
                    ).sort_index(),
                )

        # inplace is True
        subset_list = [None, "a", ["a", "b"]]
        for subset in subset_list:
            pdf = pd.DataFrame(
                {"a": [1, 2, 2, 2, 3], "b": ["a", "a", "a", "c", "d"]}, index=np.random.rand(5)
            )
            psdf = ps.from_pandas(pdf)
            pser = pdf.a
            psser = psdf.a
            pdf.drop_duplicates(subset=subset, inplace=True)
            psdf.drop_duplicates(subset=subset, inplace=True)
            self.assert_eq(psdf.sort_index(), pdf.sort_index())
            self.assert_eq(psser.sort_index(), pser.sort_index())

        # multi-index columns, inplace is True
        subset_list = [None, ("x", "a"), [("x", "a"), ("y", "b")]]
        for subset in subset_list:
            pdf = pd.DataFrame(
                {"a": [1, 2, 2, 2, 3], "b": ["a", "a", "a", "c", "d"]}, index=np.random.rand(5)
            )
            psdf = ps.from_pandas(pdf)
            columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
            pdf.columns = columns
            psdf.columns = columns
            pser = pdf[("x", "a")]
            psser = psdf[("x", "a")]
            pdf.drop_duplicates(subset=subset, inplace=True)
            pdf.drop_duplicates(subset=subset, inplace=True, ignore_index=True)
            psdf.drop_duplicates(subset=subset, inplace=True)
            psdf.drop_duplicates(subset=subset, inplace=True, ignore_index=True)
            self.assert_eq(psdf.sort_index(), pdf.sort_index())
            self.assert_eq(psser.sort_index(), pser.sort_index())

        # non-string names
        pdf = pd.DataFrame(
            {10: [1, 2, 2, 2, 3], 20: ["a", "a", "a", "c", "d"]}, index=np.random.rand(5)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.drop_duplicates(10, keep=keep).sort_index(),
            psdf.drop_duplicates(10, keep=keep).sort_index(),
        )
        self.assert_eq(
            pdf.drop_duplicates([10, 20], keep=keep).sort_index(),
            psdf.drop_duplicates([10, 20], keep=keep).sort_index(),
        )

    def test_duplicated(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 2, 3], "b": [1, 1, 1, 4], "c": [1, 1, 1, 5]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.duplicated().sort_index(), psdf.duplicated().sort_index())
        self.assert_eq(
            pdf.duplicated(keep="last").sort_index(),
            psdf.duplicated(keep="last").sort_index(),
        )
        self.assert_eq(
            pdf.duplicated(keep=False).sort_index(),
            psdf.duplicated(keep=False).sort_index(),
        )
        self.assert_eq(
            pdf.duplicated(subset="b").sort_index(),
            psdf.duplicated(subset="b").sort_index(),
        )
        self.assert_eq(
            pdf.duplicated(subset=["b"]).sort_index(),
            psdf.duplicated(subset=["b"]).sort_index(),
        )
        with self.assertRaisesRegex(ValueError, "'keep' only supports 'first', 'last' and False"):
            psdf.duplicated(keep="false")
        with self.assertRaisesRegex(KeyError, "'d'"):
            psdf.duplicated(subset=["d"])

        pdf.index.name = "x"
        psdf.index.name = "x"
        self.assert_eq(pdf.duplicated().sort_index(), psdf.duplicated().sort_index())

        # multi-index
        self.assert_eq(
            pdf.set_index("a", append=True).duplicated().sort_index(),
            psdf.set_index("a", append=True).duplicated().sort_index(),
        )
        self.assert_eq(
            pdf.set_index("a", append=True).duplicated(keep=False).sort_index(),
            psdf.set_index("a", append=True).duplicated(keep=False).sort_index(),
        )
        self.assert_eq(
            pdf.set_index("a", append=True).duplicated(subset=["b"]).sort_index(),
            psdf.set_index("a", append=True).duplicated(subset=["b"]).sort_index(),
        )

        # mutli-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.duplicated().sort_index(), psdf.duplicated().sort_index())
        self.assert_eq(
            pdf.duplicated(subset=("x", "b")).sort_index(),
            psdf.duplicated(subset=("x", "b")).sort_index(),
        )
        self.assert_eq(
            pdf.duplicated(subset=[("x", "b")]).sort_index(),
            psdf.duplicated(subset=[("x", "b")]).sort_index(),
        )

        # non-string names
        pdf = pd.DataFrame(
            {10: [1, 1, 2, 3], 20: [1, 1, 1, 4], 30: [1, 1, 1, 5]}, index=np.random.rand(4)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.duplicated().sort_index(), psdf.duplicated().sort_index())
        self.assert_eq(
            pdf.duplicated(subset=10).sort_index(),
            psdf.duplicated(subset=10).sort_index(),
        )

    def test_filter(self):
        pdf = pd.DataFrame(
            {
                "aa": ["aa", "bd", "bc", "ab", "ce"],
                "ba": [1, 2, 3, 4, 5],
                "cb": [1.0, 2.0, 3.0, 4.0, 5.0],
                "db": [1.0, np.nan, 3.0, np.nan, 5.0],
            }
        )
        pdf = pdf.set_index("aa")
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.filter(items=["ab", "aa"], axis=0).sort_index(),
            pdf.filter(items=["ab", "aa"], axis=0).sort_index(),
        )

        with option_context("compute.isin_limit", 0):
            self.assert_eq(
                psdf.filter(items=["ab", "aa"], axis=0).sort_index(),
                pdf.filter(items=["ab", "aa"], axis=0).sort_index(),
            )

        self.assert_eq(
            psdf.filter(items=["ba", "db"], axis=1).sort_index(),
            pdf.filter(items=["ba", "db"], axis=1).sort_index(),
        )

        self.assert_eq(psdf.filter(like="b", axis="index"), pdf.filter(like="b", axis="index"))
        self.assert_eq(psdf.filter(like="c", axis="columns"), pdf.filter(like="c", axis="columns"))

        self.assert_eq(
            psdf.filter(regex="b.*", axis="index"), pdf.filter(regex="b.*", axis="index")
        )
        self.assert_eq(
            psdf.filter(regex="b.*", axis="columns"), pdf.filter(regex="b.*", axis="columns")
        )

        pdf = pdf.set_index("ba", append=True)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.filter(items=[("aa", 1), ("bd", 2)], axis=0).sort_index(),
            pdf.filter(items=[("aa", 1), ("bd", 2)], axis=0).sort_index(),
        )

        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            psdf.filter(items=[["aa", 1], ("bd", 2)], axis=0)

        with self.assertRaisesRegex(ValueError, "The item should not be empty."):
            psdf.filter(items=[(), ("bd", 2)], axis=0)

        self.assert_eq(psdf.filter(like="b", axis=0), pdf.filter(like="b", axis=0))

        self.assert_eq(psdf.filter(regex="b.*", axis=0), pdf.filter(regex="b.*", axis=0))

        with self.assertRaisesRegex(ValueError, "items should be a list-like object"):
            psdf.filter(items="b")

        with self.assertRaisesRegex(ValueError, "No axis named"):
            psdf.filter(regex="b.*", axis=123)

        with self.assertRaisesRegex(TypeError, "Must pass either `items`, `like`"):
            psdf.filter()

        with self.assertRaisesRegex(TypeError, "mutually exclusive"):
            psdf.filter(regex="b.*", like="aaa")

        # multi-index columns
        pdf = pd.DataFrame(
            {
                ("x", "aa"): ["aa", "ab", "bc", "bd", "ce"],
                ("x", "ba"): [1, 2, 3, 4, 5],
                ("y", "cb"): [1.0, 2.0, 3.0, 4.0, 5.0],
                ("z", "db"): [1.0, np.nan, 3.0, np.nan, 5.0],
            }
        )
        pdf = pdf.set_index(("x", "aa"))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.filter(items=["ab", "aa"], axis=0).sort_index(),
            pdf.filter(items=["ab", "aa"], axis=0).sort_index(),
        )
        self.assert_eq(
            psdf.filter(items=[("x", "ba"), ("z", "db")], axis=1).sort_index(),
            pdf.filter(items=[("x", "ba"), ("z", "db")], axis=1).sort_index(),
        )

        self.assert_eq(psdf.filter(like="b", axis="index"), pdf.filter(like="b", axis="index"))
        self.assert_eq(psdf.filter(like="c", axis="columns"), pdf.filter(like="c", axis="columns"))

        self.assert_eq(
            psdf.filter(regex="b.*", axis="index"), pdf.filter(regex="b.*", axis="index")
        )
        self.assert_eq(
            psdf.filter(regex="b.*", axis="columns"), pdf.filter(regex="b.*", axis="columns")
        )

    def test_last(self):
        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pdf = pd.DataFrame([1, 2, 3, 4], index=index)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.last("1D"), psdf.last("1D"))
        self.assert_eq(pdf.last(DateOffset(days=1)), psdf.last(DateOffset(days=1)))
        with self.assertRaisesRegex(TypeError, "'last' only supports a DatetimeIndex"):
            ps.DataFrame([1, 2, 3, 4]).last("1D")

    def test_first(self):
        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pdf = pd.DataFrame([1, 2, 3, 4], index=index)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.first("1D"), psdf.first("1D"))
        self.assert_eq(pdf.first(DateOffset(days=1)), psdf.first(DateOffset(days=1)))
        with self.assertRaisesRegex(TypeError, "'first' only supports a DatetimeIndex"):
            ps.DataFrame([1, 2, 3, 4]).first("1D")

    def test_swaplevel(self):
        # MultiIndex with two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame({"x1": ["a", "b", "c", "d"], "x2": ["a", "b", "c", "d"]}, index=pidx)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.swaplevel(), psdf.swaplevel())
        self.assert_eq(pdf.swaplevel(0, 1), psdf.swaplevel(0, 1))
        self.assert_eq(pdf.swaplevel(1, 1), psdf.swaplevel(1, 1))
        self.assert_eq(pdf.swaplevel("number", "color"), psdf.swaplevel("number", "color"))

        # MultiIndex with more than two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"], ["l", "m", "s", "xs"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color", "size"))
        pdf = pd.DataFrame({"x1": ["a", "b", "c", "d"], "x2": ["a", "b", "c", "d"]}, index=pidx)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.swaplevel(), psdf.swaplevel())
        self.assert_eq(pdf.swaplevel(0, 1), psdf.swaplevel(0, 1))
        self.assert_eq(pdf.swaplevel(0, 2), psdf.swaplevel(0, 2))
        self.assert_eq(pdf.swaplevel(1, 2), psdf.swaplevel(1, 2))
        self.assert_eq(pdf.swaplevel(1, 1), psdf.swaplevel(1, 1))
        self.assert_eq(pdf.swaplevel(-1, -2), psdf.swaplevel(-1, -2))
        self.assert_eq(pdf.swaplevel("number", "color"), psdf.swaplevel("number", "color"))
        self.assert_eq(pdf.swaplevel("number", "size"), psdf.swaplevel("number", "size"))
        self.assert_eq(pdf.swaplevel("color", "size"), psdf.swaplevel("color", "size"))
        self.assert_eq(
            pdf.swaplevel("color", "size", axis="index"),
            psdf.swaplevel("color", "size", axis="index"),
        )
        self.assert_eq(
            pdf.swaplevel("color", "size", axis=0), psdf.swaplevel("color", "size", axis=0)
        )

        pdf = pd.DataFrame(
            {
                "x1": ["a", "b", "c", "d"],
                "x2": ["a", "b", "c", "d"],
                "x3": ["a", "b", "c", "d"],
                "x4": ["a", "b", "c", "d"],
            }
        )
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color", "size"))
        pdf.columns = pidx
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.swaplevel(axis=1), psdf.swaplevel(axis=1))
        self.assert_eq(pdf.swaplevel(0, 1, axis=1), psdf.swaplevel(0, 1, axis=1))
        self.assert_eq(pdf.swaplevel(0, 2, axis=1), psdf.swaplevel(0, 2, axis=1))
        self.assert_eq(pdf.swaplevel(1, 2, axis=1), psdf.swaplevel(1, 2, axis=1))
        self.assert_eq(pdf.swaplevel(1, 1, axis=1), psdf.swaplevel(1, 1, axis=1))
        self.assert_eq(pdf.swaplevel(-1, -2, axis=1), psdf.swaplevel(-1, -2, axis=1))
        self.assert_eq(
            pdf.swaplevel("number", "color", axis=1), psdf.swaplevel("number", "color", axis=1)
        )
        self.assert_eq(
            pdf.swaplevel("number", "size", axis=1), psdf.swaplevel("number", "size", axis=1)
        )
        self.assert_eq(
            pdf.swaplevel("color", "size", axis=1), psdf.swaplevel("color", "size", axis=1)
        )
        self.assert_eq(
            pdf.swaplevel("color", "size", axis="columns"),
            psdf.swaplevel("color", "size", axis="columns"),
        )

        # Error conditions
        self.assertRaises(AssertionError, lambda: ps.DataFrame([1, 2]).swaplevel())
        self.assertRaises(IndexError, lambda: psdf.swaplevel(0, 9, axis=1))
        self.assertRaises(KeyError, lambda: psdf.swaplevel("not_number", "color", axis=1))
        self.assertRaises(ValueError, lambda: psdf.swaplevel(axis=2))

    def test_swapaxes(self):
        pdf = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]], index=["x", "y", "z"], columns=["a", "b", "c"]
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.swapaxes(0, 1), pdf.swapaxes(0, 1))
        self.assert_eq(psdf.swapaxes(1, 0), pdf.swapaxes(1, 0))
        self.assert_eq(psdf.swapaxes("index", "columns"), pdf.swapaxes("index", "columns"))
        self.assert_eq(psdf.swapaxes("columns", "index"), pdf.swapaxes("columns", "index"))
        self.assert_eq((psdf + 1).swapaxes(0, 1), (pdf + 1).swapaxes(0, 1))

        self.assertRaises(AssertionError, lambda: psdf.swapaxes(0, 1, copy=False))
        self.assertRaises(ValueError, lambda: psdf.swapaxes(0, -1))

    @unittest.skipIf(is_ansi_mode_test, ansi_mode_not_supported_message)
    def test_isin(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 9, 4, 2, 4],
                "c": ["one", "three", "six", "seven", "one", "5"],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.isin([4, "six"]), pdf.isin([4, "six"]))
        # Seems like pandas has a bug when passing `np.array` as parameter
        self.assert_eq(psdf.isin(np.array([4, "six"])), pdf.isin([4, "six"]))
        self.assert_eq(
            psdf.isin({"a": [2, 8], "c": ["three", "one"]}),
            pdf.isin({"a": [2, 8], "c": ["three", "one"]}),
        )
        self.assert_eq(
            psdf.isin({"a": np.array([2, 8]), "c": ["three", "one"]}),
            pdf.isin({"a": np.array([2, 8]), "c": ["three", "one"]}),
        )

        msg = "'DataFrame' object has no attribute {'e'}"
        with self.assertRaisesRegex(AttributeError, msg):
            psdf.isin({"e": [5, 7], "a": [1, 6]})

        msg = "DataFrame and Series are not supported"
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.isin(pdf)

        msg = "Values should be iterable, Series, DataFrame or dict."
        with self.assertRaisesRegex(TypeError, msg):
            psdf.isin(1)

        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, None, 9, 4, None, 4],
                "c": [None, 5, None, 3, 2, 1],
            },
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.isin([4, 3, 1, 1, None]), pdf.isin([4, 3, 1, 1, None]))

        self.assert_eq(psdf.isin({"b": [4, 3, 1, 1, None]}), pdf.isin({"b": [4, 3, 1, 1, None]}))

    def test_sample(self):
        psdf = ps.DataFrame({"A": [0, 2, 4]}, index=["x", "y", "z"])

        # Make sure the tests run, but we can't check the result because they are non-deterministic.
        psdf.sample(frac=0.1)
        psdf.sample(frac=0.2, replace=True)
        psdf.sample(frac=0.2, random_state=5)
        psdf["A"].sample(frac=0.2)
        psdf["A"].sample(frac=0.2, replace=True)
        psdf["A"].sample(frac=0.2, random_state=5)

        self.assert_eq(psdf.sample(frac=0.1, ignore_index=True).index.dtype, np.int64)
        self.assert_eq(psdf.sample(frac=0.2, replace=True, ignore_index=True).index.dtype, np.int64)
        self.assert_eq(
            psdf.sample(frac=0.2, random_state=5, ignore_index=True).index.dtype, np.int64
        )
        self.assert_eq(psdf["A"].sample(frac=0.2, ignore_index=True).index.dtype, np.int64)
        self.assert_eq(
            psdf["A"].sample(frac=0.2, replace=True, ignore_index=True).index.dtype, np.int64
        )
        self.assert_eq(
            psdf["A"].sample(frac=0.2, random_state=5, ignore_index=True).index.dtype, np.int64
        )

        with self.assertRaises(ValueError):
            psdf.sample()
        with self.assertRaises(NotImplementedError):
            psdf.sample(n=1)


class FrameReidexingTests(
    FrameReindexingMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_reindexing import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
