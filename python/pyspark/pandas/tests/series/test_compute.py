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
from itertools import product

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.errors import PySparkValueError
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class SeriesComputeMixin:
    def test_duplicated(self):
        for pser in [
            pd.Series(["beetle", None, "beetle", None, "lama", "beetle"], name="objects"),
            pd.Series([1, np.nan, 1, np.nan], name="numbers"),
            pd.Series(
                [
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2022-02-02"),
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2022-02-02"),
                ],
                name="times",
            ),
        ]:
            psser = ps.from_pandas(pser)
            self.assert_eq(psser.duplicated().sort_index(), pser.duplicated())
            self.assert_eq(
                psser.duplicated(keep="first").sort_index(), pser.duplicated(keep="first")
            )
            self.assert_eq(psser.duplicated(keep="last").sort_index(), pser.duplicated(keep="last"))
            self.assert_eq(psser.duplicated(keep=False).sort_index(), pser.duplicated(keep=False))

        pser = pd.Series([1, 2, 1, 2, 3], name="numbers")
        psser = ps.from_pandas(pser)
        self.assert_eq((psser + 1).duplicated().sort_index(), (pser + 1).duplicated())

    def test_drop_duplicates(self):
        pdf = pd.DataFrame({"animal": ["lama", "cow", "lama", "beetle", "lama", "hippo"]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.animal
        psser = psdf.animal

        self.assert_eq(psser.drop_duplicates().sort_index(), pser.drop_duplicates().sort_index())
        self.assert_eq(
            psser.drop_duplicates(keep="last").sort_index(),
            pser.drop_duplicates(keep="last").sort_index(),
        )

        # inplace
        psser.drop_duplicates(keep=False, inplace=True)
        pser.drop_duplicates(keep=False, inplace=True)
        self.assert_eq(psser.sort_index(), pser.sort_index())
        self.assert_eq(psdf, pdf)

    def test_clip(self):
        pdf = pd.DataFrame({"x": [0, 2, 4]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        # Assert list-like values are not accepted for 'lower' and 'upper'
        msg = "List-like value are not supported for 'lower' and 'upper' at the moment"
        with self.assertRaises(TypeError, msg=msg):
            psser.clip(lower=[1])
        with self.assertRaises(TypeError, msg=msg):
            psser.clip(upper=[1])

        # Assert no lower or upper
        self.assert_eq(psser.clip(), pser.clip())
        # Assert lower only
        self.assert_eq(psser.clip(1), pser.clip(1))
        # Assert upper only
        self.assert_eq(psser.clip(upper=3), pser.clip(upper=3))
        # Assert lower and upper
        self.assert_eq(psser.clip(1, 3), pser.clip(1, 3))
        self.assert_eq((psser + 1).clip(1, 3), (pser + 1).clip(1, 3))

        # Assert inplace is True
        pser.clip(1, 3, inplace=True)
        psser.clip(1, 3, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        # Assert behavior on string values
        str_psser = ps.Series(["a", "b", "c"])
        self.assert_eq(str_psser.clip(1, 3), str_psser)

    def test_compare(self):
        pser = pd.Series([1, 2])
        psser = ps.from_pandas(pser)

        res_psdf = psser.compare(psser)
        self.assertTrue(res_psdf.empty)
        self.assert_eq(res_psdf.columns, pd.Index(["self", "other"]))

        self.assert_eq(pser.compare(pser + 1).sort_index(), psser.compare(psser + 1).sort_index())

        pser = pd.Series([1, 2], index=["x", "y"])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.compare(pser + 1).sort_index(), psser.compare(psser + 1).sort_index())

    def test_concat(self):
        pser1 = pd.Series([1, 2, 3], name="0")
        pser2 = pd.Series([4, 5, 6], name="0")
        pser3 = pd.Series([4, 5, 6], index=[3, 4, 5], name="0")
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)
        psser3 = ps.from_pandas(pser3)

        self.assert_eq(ps.concat([psser1, psser2]), pd.concat([pser1, pser2]))
        self.assert_eq(ps.concat([psser1, psser3]), pd.concat([pser1, pser3]))
        self.assert_eq(
            ps.concat([psser1, psser2], ignore_index=True),
            pd.concat([pser1, pser2], ignore_index=True),
        )

    def test_shift(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.shift(2), pser.shift(2))
        self.assert_eq(psser.shift().shift(-1), pser.shift().shift(-1))
        self.assert_eq(psser.shift().sum(), pser.shift().sum())

        self.assert_eq(psser.shift(periods=2, fill_value=0), pser.shift(periods=2, fill_value=0))

        with self.assertRaisesRegex(TypeError, "periods should be an int; however"):
            psser.shift(periods=1.5)

        self.assert_eq(psser.shift(periods=0), pser.shift(periods=0))

    def test_diff(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.diff(2), pser.diff(2))
        self.assert_eq(psser.diff().diff(-1), pser.diff().diff(-1))
        self.assert_eq(psser.diff().sum(), pser.diff().sum())

    def test_aggregate(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        psser = ps.Series(pser)
        msg = "func must be a string or list of strings"
        with self.assertRaisesRegex(TypeError, msg):
            psser.aggregate({"x": ["min", "max"]})
        msg = (
            "If the given function is a list, it " "should only contains function names as strings."
        )
        with self.assertRaisesRegex(ValueError, msg):
            psser.aggregate(["min", max])

    def test_drop(self):
        pdf = pd.DataFrame({"x": [10, 20, 15, 30, 45]})
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        self.assert_eq(psser.drop(1), pser.drop(1))
        self.assert_eq(psser.drop([1, 4]), pser.drop([1, 4]))
        self.assert_eq(psser.drop(columns=1), pser.drop(columns=1))
        self.assert_eq(psser.drop(columns=[1, 4]), pser.drop(columns=[1, 4]))

        msg = "Need to specify at least one of 'labels', 'index' or 'columns'"
        with self.assertRaisesRegex(ValueError, msg):
            psser.drop()
        self.assertRaises(KeyError, lambda: psser.drop((0, 1)))

        psser.drop([2, 3], inplace=True)
        pser.drop([2, 3], inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        n_pser, n_psser = pser + 1, psser + 1
        n_psser.drop([1, 4], inplace=True)
        n_pser.drop([1, 4], inplace=True)
        self.assert_eq(n_psser, n_pser)
        self.assert_eq(psser, pser)

        # For MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )

        pdf = pd.DataFrame({"x": [45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3]}, index=midx)
        psdf = ps.from_pandas(pdf)
        psser, pser = psdf.x, pdf.x

        self.assert_eq(psser.drop("lama"), pser.drop("lama"))
        self.assert_eq(psser.drop(labels="weight", level=1), pser.drop(labels="weight", level=1))
        self.assert_eq(psser.drop(("lama", "weight")), pser.drop(("lama", "weight")))
        self.assert_eq(
            psser.drop([("lama", "speed"), ("falcon", "weight")]),
            pser.drop([("lama", "speed"), ("falcon", "weight")]),
        )
        self.assert_eq(psser.drop({"lama": "speed"}), pser.drop({"lama": "speed"}))

        msg = "'level' should be less than the number of indexes"
        with self.assertRaisesRegex(ValueError, msg):
            psser.drop(labels="weight", level=2)

        msg = (
            "If the given index is a list, it "
            "should only contains names as all tuples or all non tuples "
            "that contain index names"
        )
        with self.assertRaisesRegex(ValueError, msg):
            psser.drop(["lama", ["cow", "falcon"]])

        msg = "Cannot specify both 'labels' and 'index'/'columns'"
        with self.assertRaisesRegex(ValueError, msg):
            psser.drop("lama", index="cow")

        with self.assertRaisesRegex(ValueError, msg):
            psser.drop("lama", columns="cow")

        msg = r"'Key length \(2\) exceeds index depth \(3\)'"
        with self.assertRaisesRegex(KeyError, msg):
            psser.drop(("lama", "speed", "x"))

        psser.drop({"lama": "speed"}, inplace=True)
        pser.drop({"lama": "speed"}, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

    def test_pop(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pdf = pd.DataFrame({"x": [45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3]}, index=midx)
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        self.assert_eq(psser.pop(("lama", "speed")), pser.pop(("lama", "speed")))
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        msg = r"'Key length \(3\) exceeds index depth \(2\)'"
        with self.assertRaisesRegex(KeyError, msg):
            psser.pop(("lama", "speed", "x"))

        msg = "'key' should be string or tuple that contains strings"
        with self.assertRaisesRegex(TypeError, msg):
            psser.pop(["lama", "speed"])

        pser = pd.Series(["a", "b", "c", "a"], dtype="category")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.pop(0), pser.pop(0))
        self.assert_eq(psser, pser)

        self.assert_eq(psser.pop(3), pser.pop(3))
        self.assert_eq(psser, pser)

    def test_duplicates(self):
        psers = {
            "test on texts": pd.Series(
                ["lama", "cow", "lama", "beetle", "lama", "hippo"], name="animal"
            ),
            "test on numbers": pd.Series([1, 1, 2, 4, 3]),
        }
        keeps = ["first", "last", False]

        for (msg, pser), keep in product(psers.items(), keeps):
            with self.subTest(msg, keep=keep):
                psser = ps.Series(pser)

                self.assert_eq(
                    pser.drop_duplicates(keep=keep).sort_values(),
                    psser.drop_duplicates(keep=keep).sort_values(),
                )

    def test_truncate(self):
        pser1 = pd.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 5, 6, 7])
        psser1 = ps.Series(pser1)
        pser2 = pd.Series([10, 20, 30, 40, 50, 60, 70], index=[7, 6, 5, 4, 3, 2, 1])
        psser2 = ps.Series(pser2)

        self.assert_eq(psser1.truncate(), pser1.truncate())
        self.assert_eq(psser1.truncate(before=2), pser1.truncate(before=2))
        self.assert_eq(psser1.truncate(after=5), pser1.truncate(after=5))
        self.assert_eq(psser1.truncate(copy=False), pser1.truncate(copy=False))
        self.assert_eq(psser1.truncate(2, 5, copy=False), pser1.truncate(2, 5, copy=False))
        self.assert_eq(psser2.truncate(4, 6), pser2.truncate(4, 6))
        self.assert_eq(psser2.truncate(4, 6, copy=False), pser2.truncate(4, 6, copy=False))

        psser = ps.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 3, 2, 1])
        msg = "truncate requires a sorted index"
        with self.assertRaisesRegex(ValueError, msg):
            psser.truncate()

        psser = ps.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 5, 6, 7])
        msg = "Truncate: 2 must be after 5"
        with self.assertRaisesRegex(ValueError, msg):
            psser.truncate(5, 2)

    def test_unstack(self):
        pser = pd.Series(
            [10, -2, 4, 7],
            index=pd.MultiIndex.from_tuples(
                [("one", "a", "z"), ("one", "b", "x"), ("two", "a", "c"), ("two", "b", "v")],
                names=["A", "B", "C"],
            ),
        )
        psser = ps.from_pandas(pser)

        levels = [-3, -2, -1, 0, 1, 2]
        for level in levels:
            pandas_result = pser.unstack(level=level)
            pandas_on_spark_result = psser.unstack(level=level).sort_index()
            self.assert_eq(pandas_result, pandas_on_spark_result)
            self.assert_eq(pandas_result.index.names, pandas_on_spark_result.index.names)
            self.assert_eq(pandas_result.columns.names, pandas_on_spark_result.columns.names)

        # non-numeric datatypes
        pser = pd.Series(
            list("abcd"), index=pd.MultiIndex.from_product([["one", "two"], ["a", "b"]])
        )
        psser = ps.from_pandas(pser)

        levels = [-2, -1, 0, 1]
        for level in levels:
            pandas_result = pser.unstack(level=level)
            pandas_on_spark_result = psser.unstack(level=level).sort_index()
            self.assert_eq(pandas_result, pandas_on_spark_result)
            self.assert_eq(pandas_result.index.names, pandas_on_spark_result.index.names)
            self.assert_eq(pandas_result.columns.names, pandas_on_spark_result.columns.names)

        # Exceeding the range of level
        self.assertRaises(IndexError, lambda: psser.unstack(level=3))
        self.assertRaises(IndexError, lambda: psser.unstack(level=-4))
        # Only support for MultiIndex
        psser = ps.Series([10, -2, 4, 7])
        self.assertRaises(ValueError, lambda: psser.unstack())

    def test_abs(self):
        pser = pd.Series([-2, -1, 0, 1])
        psser = ps.from_pandas(pser)

        self.assert_eq(abs(psser), abs(pser))
        self.assert_eq(np.abs(psser), np.abs(pser))

    def test_factorize(self):
        pser = pd.Series(["a", "b", "a", "b"])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([5, 1, 5, 1])
        psser = ps.from_pandas(pser)
        pcodes, puniques = (pser + 1).factorize(sort=True)
        kcodes, kuniques = (psser + 1).factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(["a", "b", "a", "b"], name="ser", index=["w", "x", "y", "z"])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(
            ["a", "b", "a", "b"], index=pd.MultiIndex.from_arrays([[4, 3, 2, 1], [1, 2, 3, 4]])
        )
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        #
        # Deals with None and np.nan
        #
        pser = pd.Series(["a", "b", "a", np.nan])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([1, None, 3, 2, 1])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(["a", None, "a"])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([None, np.nan])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize()
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes, kcodes.to_list())
        # pandas: Index([], dtype='float64')
        self.assert_eq(pd.Index([]), kuniques)

        pser = pd.Series([np.nan, np.nan])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize()
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes, kcodes.to_list())
        # pandas: Index([], dtype='float64')
        self.assert_eq(pd.Index([]), kuniques)

        #
        # Deals with na_sentinel
        #

        pser = pd.Series(["a", "b", "a", np.nan, None])
        psser = ps.from_pandas(pser)

        pcodes, puniques = pser.factorize(sort=True, use_na_sentinel=-2)
        kcodes, kuniques = psser.factorize(use_na_sentinel=-2)
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pcodes, puniques = pser.factorize(sort=True, use_na_sentinel=2)
        kcodes, kuniques = psser.factorize(use_na_sentinel=2)
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pcodes, puniques = pser.factorize(sort=True, use_na_sentinel=None)
        kcodes, kuniques = psser.factorize(use_na_sentinel=None)
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        # puniques is Index(['a', 'b', nan], dtype='object')
        self.assert_eq(ps.Index(["a", "b", None]), kuniques)

        psser = ps.Series([1, 2, np.nan, 4, 5])  # Arrow takes np.nan as null
        psser.loc[3] = np.nan  # Spark takes np.nan as NaN
        kcodes, kuniques = psser.factorize(use_na_sentinel=None)
        pcodes, puniques = psser._to_pandas().factorize(sort=True, use_na_sentinel=None)
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

    def test_explode(self):
        pser = pd.Series([[1, 2, 3], [], None, [3, 4]])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.explode(), psser.explode(), almost=True)

        # MultiIndex
        pser.index = pd.MultiIndex.from_tuples([("a", "w"), ("b", "x"), ("c", "y"), ("d", "z")])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.explode(), psser.explode(), almost=True)

        # non-array type Series
        pser = pd.Series([1, 2, 3, 4])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.explode(), psser.explode())

    def test_between(self):
        pser = pd.Series([np.nan, 1, 2, 3, 4])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.between(1, 4), pser.between(1, 4))
        self.assert_eq(psser.between(1, 4, inclusive="both"), pser.between(1, 4, inclusive="both"))
        self.assert_eq(
            psser.between(1, 4, inclusive="neither"), pser.between(1, 4, inclusive="neither")
        )
        self.assert_eq(psser.between(1, 4, inclusive="left"), pser.between(1, 4, inclusive="left"))
        self.assert_eq(
            psser.between(1, 4, inclusive="right"), pser.between(1, 4, inclusive="right")
        )
        expected_err_msg = (
            "Inclusive has to be either string of 'both'," "'left', 'right', or 'neither'"
        )
        with self.assertRaisesRegex(ValueError, expected_err_msg):
            psser.between(1, 4, inclusive="middle")

        # Test for backward compatibility
        self.assert_eq(psser.between(1, 4, inclusive="both"), pser.between(1, 4, inclusive="both"))
        self.assert_eq(
            psser.between(1, 4, inclusive="neither"), pser.between(1, 4, inclusive="neither")
        )

    def test_between_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pser = pd.Series([1, 2, 3, 4], index=idx)
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            psser.between_time("0:15", "0:45").sort_index(),
        )

        pser.index.name = "ts"
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            psser.between_time("0:15", "0:45").sort_index(),
        )

        pser.index.name = "index"
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            psser.between_time("0:15", "0:45").sort_index(),
        )

        self.assert_eq(
            pser.between_time("0:15", "0:45", inclusive="neither").sort_index(),
            psser.between_time("0:15", "0:45", inclusive="neither").sort_index(),
        )

        self.assert_eq(
            pser.between_time("0:15", "0:45", inclusive="left").sort_index(),
            psser.between_time("0:15", "0:45", inclusive="left").sort_index(),
        )

        self.assert_eq(
            pser.between_time("0:15", "0:45", inclusive="right").sort_index(),
            psser.between_time("0:15", "0:45", inclusive="right").sort_index(),
        )

        with self.assertRaises(PySparkValueError) as ctx:
            psser.between_time("0:15", "0:45", inclusive="")

        self.check_error(
            exception=ctx.exception,
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "inclusive",
                "allowed_values": str(["left", "right", "both", "neither"]),
            },
        )

    def test_at_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pser = pd.Series([1, 2, 3, 4], index=idx)
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(),
            psser.at_time("0:20").sort_index(),
        )

        pser.index.name = "ts"
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(),
            psser.at_time("0:20").sort_index(),
        )

        pser.index.name = "index"
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(),
            psser.at_time("0:20").sort_index(),
        )


class SeriesComputeTests(
    SeriesComputeMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_compute import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
