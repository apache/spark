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


# This file contains test cases for 'Binary operator functions'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#binary-operator-functions
class FrameBinaryOpsMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_binary_operators(self):
        pdf = pd.DataFrame(
            {"A": [0, 2, 4], "B": [4, 2, 0], "X": [-1, 10, 0]}, index=np.random.rand(3)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf + psdf.copy(), pdf + pdf.copy())
        self.assert_eq(psdf + psdf.loc[:, ["A", "B"]], pdf + pdf.loc[:, ["A", "B"]])
        self.assert_eq(psdf.loc[:, ["A", "B"]] + psdf, pdf.loc[:, ["A", "B"]] + pdf)

        with ps.option_context("compute.ops_on_diff_frames", False):
            self.assertRaisesRegex(
                ValueError,
                "it comes from a different dataframe",
                lambda: ps.range(10).add(ps.range(10)),
            )

        self.assertRaisesRegex(
            TypeError,
            "add with a sequence is currently not supported",
            lambda: ps.range(10).add(ps.range(10).id),
        )

        psdf_other = psdf.copy()
        psdf_other.columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X"), ("C", "C")])
        self.assertRaisesRegex(
            ValueError,
            "cannot join with no overlapping index names",
            lambda: psdf.add(psdf_other),
        )

    def test_binary_operator_add(self):
        # Positive
        pdf = pd.DataFrame({"a": ["x"], "b": ["y"], "c": [1], "d": [2]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["a"] + psdf["b"], pdf["a"] + pdf["b"])
        self.assert_eq(psdf["c"] + psdf["d"], pdf["c"] + pdf["d"])

        # Negative
        ks_err_msg = "Addition can not be applied to given types"

        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] + psdf["c"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["c"] + psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["c"] + "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" + psdf["c"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 + psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] + 1)

    def test_binary_operator_sub(self):
        # Positive
        pdf = pd.DataFrame({"a": [2], "b": [1]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["a"] - psdf["b"], pdf["a"] - pdf["b"])

        # Negative
        psdf = ps.DataFrame({"a": ["x"], "b": [1]})
        ks_err_msg = "Subtraction can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] - psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] - "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" - psdf["b"])

        ks_err_msg = "Subtraction can not be applied to strings"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] - psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 - psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] - 1)

        psdf = ps.DataFrame({"a": ["x"], "b": ["y"]})
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] - psdf["b"])

    def test_binary_operator_truediv(self):
        # Positive
        pdf = pd.DataFrame({"a": [3], "b": [2]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["a"] / psdf["b"], pdf["a"] / pdf["b"])

        # Negative
        psdf = ps.DataFrame({"a": ["x"], "b": [1]})

        ks_err_msg = "True division can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] / psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] / "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" / psdf["b"])

        ks_err_msg = "True division can not be applied to strings"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] / psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 / psdf["a"])

    def test_binary_operator_floordiv(self):
        psdf = ps.DataFrame({"a": ["x"], "b": [1]})

        ks_err_msg = "Floor division can not be applied to strings"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] // psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 // psdf["a"])

        ks_err_msg = "Floor division can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] // psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] // "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" // psdf["b"])

    def test_binary_operator_mod(self):
        # Positive
        pdf = pd.DataFrame({"a": [3], "b": [2]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["a"] % psdf["b"], pdf["a"] % pdf["b"])

        # Negative
        psdf = ps.DataFrame({"a": ["x"], "b": [1]})
        ks_err_msg = "Modulo can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] % psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] % "literal")

        ks_err_msg = "Modulo can not be applied to strings"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] % psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 1 % psdf["a"])

    def test_binary_operator_multiply(self):
        # Positive
        pdf = pd.DataFrame({"a": ["x", "y"], "b": [1, 2], "c": [3, 4]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["b"] * psdf["c"], pdf["b"] * pdf["c"])
        self.assert_eq(psdf["c"] * psdf["b"], pdf["c"] * pdf["b"])
        self.assert_eq(psdf["a"] * psdf["b"], pdf["a"] * pdf["b"])
        self.assert_eq(psdf["b"] * psdf["a"], pdf["b"] * pdf["a"])
        self.assert_eq(psdf["a"] * 2, pdf["a"] * 2)
        self.assert_eq(psdf["b"] * 2, pdf["b"] * 2)
        self.assert_eq(2 * psdf["a"], 2 * pdf["a"])
        self.assert_eq(2 * psdf["b"], 2 * pdf["b"])

        # Negative
        psdf = ps.DataFrame({"a": ["x"], "b": [2]})
        ks_err_msg = "Multiplication can not be applied to given types"
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["b"] * "literal")
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" * psdf["b"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] * "literal")

        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] * psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: psdf["a"] * 0.1)
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: 0.1 * psdf["a"])
        self.assertRaisesRegex(TypeError, ks_err_msg, lambda: "literal" * psdf["a"])

    def test_combine_first(self):
        pdf = pd.DataFrame(
            {("X", "A"): [None, 0], ("X", "B"): [4, None], ("Y", "C"): [3, 3], ("Y", "B"): [1, 1]}
        )
        pdf1, pdf2 = pdf["X"], pdf["Y"]
        psdf = ps.from_pandas(pdf)
        psdf1, psdf2 = psdf["X"], psdf["Y"]

        self.assert_eq(pdf1.combine_first(pdf2), psdf1.combine_first(psdf2))

    def test_dot(self):
        psdf = self.psdf

        with self.assertRaisesRegex(TypeError, "Unsupported type DataFrame"):
            psdf.dot(psdf)

    def test_rfloordiv(self):
        pdf = pd.DataFrame(
            {"angles": [0, 3, 4], "degrees": [360, 180, 360]},
            index=["circle", "triangle", "rectangle"],
            columns=["angles", "degrees"],
        )
        psdf = ps.from_pandas(pdf)

        expected_result = pdf.rfloordiv(10)
        self.assert_eq(psdf.rfloordiv(10), expected_result)


class FrameBinaryOpsTests(
    FrameBinaryOpsMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_binary_ops import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
