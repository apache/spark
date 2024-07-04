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


# This file contains test cases for 'Missing data handling'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#missing-data-handling
class FrameMissingDataMixin:
    def test_backfill(self):
        pdf = pd.DataFrame(
            {
                "A": [None, 3, None, None],
                "B": [2, 4, None, 3],
                "C": [None, None, None, 1],
                "D": [0, 1, 5, 4],
            },
            columns=["A", "B", "C", "D"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.backfill(), psdf.backfill())

        # Test `inplace=True`
        pdf.backfill(inplace=True)
        psdf.backfill(inplace=True)
        self.assert_eq(pdf, psdf)

    def _test_dropna(self, pdf, axis):
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.dropna(axis=axis), pdf.dropna(axis=axis))
        self.assert_eq(psdf.dropna(axis=axis, how="all"), pdf.dropna(axis=axis, how="all"))
        self.assert_eq(psdf.dropna(axis=axis, subset=["x"]), pdf.dropna(axis=axis, subset=["x"]))
        self.assert_eq(psdf.dropna(axis=axis, subset="x"), pdf.dropna(axis=axis, subset=["x"]))
        self.assert_eq(
            psdf.dropna(axis=axis, subset=["y", "z"]), pdf.dropna(axis=axis, subset=["y", "z"])
        )
        self.assert_eq(
            psdf.dropna(axis=axis, subset=["y", "z"], how="all"),
            pdf.dropna(axis=axis, subset=["y", "z"], how="all"),
        )

        self.assert_eq(psdf.dropna(axis=axis, thresh=2), pdf.dropna(axis=axis, thresh=2))
        self.assert_eq(
            psdf.dropna(axis=axis, thresh=1, subset=["y", "z"]),
            pdf.dropna(axis=axis, thresh=1, subset=["y", "z"]),
        )

        pdf2 = pdf.copy()
        psdf2 = psdf.copy()
        pser = pdf2[pdf2.columns[0]]
        psser = psdf2[psdf2.columns[0]]
        pdf2.dropna(inplace=True, axis=axis)
        psdf2.dropna(inplace=True, axis=axis)
        self.assert_eq(psdf2, pdf2)
        self.assert_eq(psser, pser)

        # multi-index
        columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")])
        if axis == 0:
            pdf.columns = columns
        else:
            pdf.index = columns
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.dropna(axis=axis), pdf.dropna(axis=axis))
        self.assert_eq(psdf.dropna(axis=axis, how="all"), pdf.dropna(axis=axis, how="all"))
        self.assert_eq(
            psdf.dropna(axis=axis, subset=[("a", "x")]), pdf.dropna(axis=axis, subset=[("a", "x")])
        )
        self.assert_eq(
            psdf.dropna(axis=axis, subset=("a", "x")), pdf.dropna(axis=axis, subset=[("a", "x")])
        )
        self.assert_eq(
            psdf.dropna(axis=axis, subset=[("a", "y"), ("b", "z")]),
            pdf.dropna(axis=axis, subset=[("a", "y"), ("b", "z")]),
        )
        self.assert_eq(
            psdf.dropna(axis=axis, subset=[("a", "y"), ("b", "z")], how="all"),
            pdf.dropna(axis=axis, subset=[("a", "y"), ("b", "z")], how="all"),
        )

        self.assert_eq(psdf.dropna(axis=axis, thresh=2), pdf.dropna(axis=axis, thresh=2))
        self.assert_eq(
            psdf.dropna(axis=axis, thresh=1, subset=[("a", "y"), ("b", "z")]),
            pdf.dropna(axis=axis, thresh=1, subset=[("a", "y"), ("b", "z")]),
        )

    def test_dropna_axis_index(self):
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=np.random.rand(6),
        )

        self._test_dropna(pdf, axis=0)

        # empty
        pdf = pd.DataFrame(index=np.random.rand(6))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.dropna(), pdf.dropna())
        self.assert_eq(psdf.dropna(how="all"), pdf.dropna(how="all"))
        self.assert_eq(psdf.dropna(thresh=0), pdf.dropna(thresh=0))
        self.assert_eq(psdf.dropna(thresh=1), pdf.dropna(thresh=1))

        # Only NA value
        pdf["a"] = [np.nan] * 6
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.dropna(), pdf.dropna())
        self.assert_eq(psdf.dropna(how="all"), pdf.dropna(how="all"))
        self.assert_eq(psdf.dropna(thresh=0), pdf.dropna(thresh=0))
        self.assert_eq(psdf.dropna(thresh=1), pdf.dropna(thresh=1))

        with self.assertRaisesRegex(ValueError, "No axis named foo"):
            psdf.dropna(axis="foo")

        self.assertRaises(KeyError, lambda: psdf.dropna(subset="1"))
        with self.assertRaisesRegex(ValueError, "invalid how option: 1"):
            psdf.dropna(how=1)
        with self.assertRaisesRegex(TypeError, "must specify how or thresh"):
            psdf.dropna(how=None)

    def test_dropna_axis_column(self):
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=[str(r) for r in np.random.rand(6)],
        ).T

        self._test_dropna(pdf, axis=1)

        psdf = ps.from_pandas(pdf)
        with self.assertRaisesRegex(
            ValueError, "The length of each subset must be the same as the index size."
        ):
            psdf.dropna(subset=(["x", "y"]), axis=1)

        # empty
        pdf = pd.DataFrame({"x": [], "y": [], "z": []})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.dropna(axis=1), pdf.dropna(axis=1))
        self.assert_eq(psdf.dropna(axis=1, how="all"), pdf.dropna(axis=1, how="all"))
        self.assert_eq(psdf.dropna(axis=1, thresh=0), pdf.dropna(axis=1, thresh=0))
        self.assert_eq(psdf.dropna(axis=1, thresh=1), pdf.dropna(axis=1, thresh=1))

    def test_fillna(self):
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf.fillna(-1), pdf.fillna(-1))
        self.assert_eq(
            psdf.fillna({"x": -1, "y": -2, "z": -5}), pdf.fillna({"x": -1, "y": -2, "z": -5})
        )
        self.assert_eq(pdf.fillna(method="ffill"), psdf.fillna(method="ffill"))
        self.assert_eq(pdf.fillna(method="ffill", limit=2), psdf.fillna(method="ffill", limit=2))
        self.assert_eq(pdf.fillna(method="bfill"), psdf.fillna(method="bfill"))
        self.assert_eq(pdf.fillna(method="bfill", limit=2), psdf.fillna(method="bfill", limit=2))

        pdf = pdf.set_index(["x", "y"])
        psdf = ps.from_pandas(pdf)
        # check multi index
        self.assert_eq(psdf.fillna(-1), pdf.fillna(-1))
        self.assert_eq(pdf.fillna(method="bfill"), psdf.fillna(method="bfill"))
        self.assert_eq(pdf.fillna(method="ffill"), psdf.fillna(method="ffill"))

        pser = pdf.z
        psser = psdf.z
        pdf.fillna({"x": -1, "y": -2, "z": -5}, inplace=True)
        psdf.fillna({"x": -1, "y": -2, "z": -5}, inplace=True)
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)

        pser = pdf.z
        psser = psdf.z
        pdf.fillna(0, inplace=True)
        psdf.fillna(0, inplace=True)
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)

        s_nan = pd.Series([-1, -2, -5], index=["x", "y", "z"], dtype=int)
        self.assert_eq(psdf.fillna(s_nan), pdf.fillna(s_nan))

        with self.assertRaisesRegex(NotImplementedError, "fillna currently only"):
            psdf.fillna(-1, axis=1)
        with self.assertRaisesRegex(NotImplementedError, "fillna currently only"):
            psdf.fillna(-1, axis="columns")
        with self.assertRaisesRegex(ValueError, "limit parameter for value is not support now"):
            psdf.fillna(-1, limit=1)
        with self.assertRaisesRegex(TypeError, "Unsupported.*DataFrame"):
            psdf.fillna(pd.DataFrame({"x": [-1], "y": [-1], "z": [-1]}))
        with self.assertRaisesRegex(TypeError, "Unsupported.*int64"):
            psdf.fillna({"x": np.int64(-6), "y": np.int64(-4), "z": -5})
        with self.assertRaisesRegex(ValueError, "Expecting 'pad', 'ffill', 'backfill' or 'bfill'."):
            psdf.fillna(method="xxx")
        with self.assertRaisesRegex(
            ValueError, "Must specify a fillna 'value' or 'method' parameter."
        ):
            psdf.fillna()

        # multi-index columns
        pdf = pd.DataFrame(
            {
                ("x", "a"): [np.nan, 2, 3, 4, np.nan, 6],
                ("x", "b"): [1, 2, np.nan, 4, np.nan, np.nan],
                ("y", "c"): [1, 2, 3, 4, np.nan, np.nan],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.fillna(-1), pdf.fillna(-1))
        self.assert_eq(
            psdf.fillna({("x", "a"): -1, ("x", "b"): -2, ("y", "c"): -5}),
            pdf.fillna({("x", "a"): -1, ("x", "b"): -2, ("y", "c"): -5}),
        )
        self.assert_eq(pdf.fillna(method="ffill"), psdf.fillna(method="ffill"))
        self.assert_eq(pdf.fillna(method="ffill", limit=2), psdf.fillna(method="ffill", limit=2))
        self.assert_eq(pdf.fillna(method="bfill"), psdf.fillna(method="bfill"))
        self.assert_eq(pdf.fillna(method="bfill", limit=2), psdf.fillna(method="bfill", limit=2))

        self.assert_eq(psdf.fillna({"x": -1}), pdf.fillna({"x": -1}))
        self.assert_eq(
            psdf.fillna({"x": -1, ("x", "b"): -2}), pdf.fillna({"x": -1, ("x", "b"): -2})
        )
        self.assert_eq(
            psdf.fillna({("x", "b"): -2, "x": -1}), pdf.fillna({("x", "b"): -2, "x": -1})
        )

        # check multi index
        pdf = pdf.set_index([("x", "a"), ("x", "b")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.fillna(-1), pdf.fillna(-1))
        self.assert_eq(
            psdf.fillna({("x", "a"): -1, ("x", "b"): -2, ("y", "c"): -5}),
            pdf.fillna({("x", "a"): -1, ("x", "b"): -2, ("y", "c"): -5}),
        )

    def test_replace(self):
        pdf = pd.DataFrame(
            {
                "name": ["Ironman", "Captain America", "Thor", "Hulk"],
                "weapon": ["Mark-45", "Shield", "Mjolnir", "Smash"],
            },
            index=np.random.rand(4),
        )
        psdf = ps.from_pandas(pdf)

        with self.assertRaisesRegex(
            NotImplementedError, "replace currently works only for method='pad"
        ):
            psdf.replace(method="bfill")
        with self.assertRaisesRegex(
            NotImplementedError, "replace currently works only when limit=None"
        ):
            psdf.replace(limit=10)
        with self.assertRaisesRegex(
            NotImplementedError, "replace currently doesn't supports regex"
        ):
            psdf.replace(regex="")

        with self.assertRaisesRegex(ValueError, "Length of to_replace and value must be same"):
            psdf.replace(to_replace=["Ironman"], value=["Spiderman", "Doctor Strange"])
        with self.assertRaisesRegex(TypeError, "Unsupported type function"):
            psdf.replace("Ironman", lambda x: "Spiderman")
        with self.assertRaisesRegex(TypeError, "Unsupported type function"):
            psdf.replace(lambda x: "Ironman", "Spiderman")

        self.assert_eq(psdf.replace("Ironman", "Spiderman"), pdf.replace("Ironman", "Spiderman"))
        self.assert_eq(
            psdf.replace(["Ironman", "Captain America"], ["Rescue", "Hawkeye"]),
            pdf.replace(["Ironman", "Captain America"], ["Rescue", "Hawkeye"]),
        )
        self.assert_eq(
            psdf.replace(("Ironman", "Captain America"), ("Rescue", "Hawkeye")),
            pdf.replace(("Ironman", "Captain America"), ("Rescue", "Hawkeye")),
        )

        # inplace
        pser = pdf.name
        psser = psdf.name
        pdf.replace("Ironman", "Spiderman", inplace=True)
        psdf.replace("Ironman", "Spiderman", inplace=True)
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)

        pdf = pd.DataFrame(
            {"A": [0, 1, 2, 3, np.nan], "B": [5, 6, 7, 8, np.nan], "C": ["a", "b", "c", "d", None]},
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.replace([0, 1, 2, 3, 5, 6], 4), pdf.replace([0, 1, 2, 3, 5, 6], 4))

        self.assert_eq(
            psdf.replace([0, 1, 2, 3, 5, 6], [6, 5, 4, 3, 2, 1]),
            pdf.replace([0, 1, 2, 3, 5, 6], [6, 5, 4, 3, 2, 1]),
        )

        self.assert_eq(psdf.replace({0: 10, 1: 100, 7: 200}), pdf.replace({0: 10, 1: 100, 7: 200}))

        self.assert_eq(
            psdf.replace({"A": [0, np.nan], "B": [5, np.nan]}, 100),
            pdf.replace({"A": [0, np.nan], "B": [5, np.nan]}, 100),
        )

        self.assert_eq(
            psdf.replace({"A": {0: 100, 4: 400, np.nan: 700}}),
            pdf.replace({"A": {0: 100, 4: 400, np.nan: 700}}),
        )
        self.assert_eq(
            psdf.replace({"X": {0: 100, 4: 400, np.nan: 700}}),
            pdf.replace({"X": {0: 100, 4: 400, np.nan: 700}}),
        )

        self.assert_eq(psdf.replace({"C": ["a", None]}, "e"), pdf.replace({"C": ["a", None]}, "e"))

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.replace([0, 1, 2, 3, 5, 6], 4), pdf.replace([0, 1, 2, 3, 5, 6], 4))

        self.assert_eq(
            psdf.replace([0, 1, 2, 3, 5, 6], [6, 5, 4, 3, 2, 1]),
            pdf.replace([0, 1, 2, 3, 5, 6], [6, 5, 4, 3, 2, 1]),
        )

        self.assert_eq(psdf.replace({0: 10, 1: 100, 7: 200}), pdf.replace({0: 10, 1: 100, 7: 200}))

        self.assert_eq(
            psdf.replace({("X", "A"): [0, np.nan], ("X", "B"): 5}, 100),
            pdf.replace({("X", "A"): [0, np.nan], ("X", "B"): 5}, 100),
        )

        self.assert_eq(
            psdf.replace({("X", "A"): {0: 100, 4: 400, np.nan: 700}}),
            pdf.replace({("X", "A"): {0: 100, 4: 400, np.nan: 700}}),
        )
        self.assert_eq(
            psdf.replace({("X", "B"): {0: 100, 4: 400, np.nan: 700}}),
            pdf.replace({("X", "B"): {0: 100, 4: 400, np.nan: 700}}),
        )

        self.assert_eq(
            psdf.replace({("Y", "C"): ["a", None]}, "e"),
            pdf.replace({("Y", "C"): ["a", None]}, "e"),
        )

    def test_ffill(self):
        idx = np.random.rand(6)
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.ffill(), pdf.ffill())
        self.assert_eq(psdf.ffill(limit=1), pdf.ffill(limit=1))

        pser = pdf.y
        psser = psdf.y

        psdf.ffill(inplace=True)
        pdf.ffill(inplace=True)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)
        self.assert_eq(psser[idx[2]], pser[idx[2]])

    def test_bfill(self):
        idx = np.random.rand(6)
        pdf = pd.DataFrame(
            {
                "x": [np.nan, 2, 3, 4, np.nan, 6],
                "y": [1, 2, np.nan, 4, np.nan, np.nan],
                "z": [1, 2, 3, 4, np.nan, np.nan],
            },
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.bfill(), pdf.bfill())
        self.assert_eq(psdf.bfill(limit=1), pdf.bfill(limit=1))

        pser = pdf.x
        psser = psdf.x

        psdf.bfill(inplace=True)
        pdf.bfill(inplace=True)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psser, pser)
        self.assert_eq(psser[idx[0]], pser[idx[0]])

    def test_pad(self):
        pdf = pd.DataFrame(
            {
                "A": [None, 3, None, None],
                "B": [2, 4, None, 3],
                "C": [None, None, None, 1],
                "D": [0, 1, 5, 4],
            },
            columns=["A", "B", "C", "D"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.pad(), psdf.pad())

        # Test `inplace=True`
        pdf.pad(inplace=True)
        psdf.pad(inplace=True)
        self.assert_eq(pdf, psdf)


class FrameMissingDataTests(
    FrameMissingDataMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_missing_data import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
