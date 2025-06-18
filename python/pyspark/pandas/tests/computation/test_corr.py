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
from pyspark.testing.pandasutils import PandasOnSparkTestCase, SPARK_CONF_ARROW_ENABLED
from pyspark.testing.sqlutils import SQLTestUtils


class FrameCorrMixin:
    def test_dataframe_corr(self):
        pdf = pd.DataFrame(
            index=[
                "".join(
                    np.random.choice(
                        list("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"), 10
                    )
                )
                for _ in range(30)
            ],
            columns=list("ABCD"),
            dtype="float64",
        )
        psdf = ps.from_pandas(pdf)

        with self.assertRaisesRegex(ValueError, "Invalid method"):
            psdf.corr("std")
        with self.assertRaisesRegex(TypeError, "Invalid min_periods type"):
            psdf.corr(min_periods="3")

        for method in ["pearson", "spearman", "kendall"]:
            self.assert_eq(psdf.corr(method=method), pdf.corr(method=method), check_exact=False)
            self.assert_eq(
                psdf.corr(method=method, min_periods=1),
                pdf.corr(method=method, min_periods=1),
                check_exact=False,
            )
            self.assert_eq(
                psdf.corr(method=method, min_periods=3),
                pdf.corr(method=method, min_periods=3),
                check_exact=False,
            )
            self.assert_eq(
                (psdf + 1).corr(method=method, min_periods=2),
                (pdf + 1).corr(method=method, min_periods=2),
                check_exact=False,
            )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
        pdf.columns = columns
        psdf.columns = columns

        for method in ["pearson", "spearman", "kendall"]:
            self.assert_eq(psdf.corr(method=method), pdf.corr(method=method), check_exact=False)
            self.assert_eq(
                psdf.corr(method=method, min_periods=1),
                pdf.corr(method=method, min_periods=1),
                check_exact=False,
            )
            self.assert_eq(
                psdf.corr(method=method, min_periods=3),
                pdf.corr(method=method, min_periods=3),
                check_exact=False,
            )
            self.assert_eq(
                (psdf + 1).corr(method=method, min_periods=2),
                (pdf + 1).corr(method=method, min_periods=2),
                check_exact=False,
            )

        # test with identical values
        pdf = pd.DataFrame(
            {
                "a": [0, 1, 1, 1, 0],
                "b": [2, 2, -1, 1, np.nan],
                "c": [3, 3, 3, 3, 3],
                "d": [np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)

        for method in ["pearson", "spearman", "kendall"]:
            self.assert_eq(psdf.corr(method=method), pdf.corr(method=method), check_exact=False)
            self.assert_eq(
                psdf.corr(method=method, min_periods=1),
                pdf.corr(method=method, min_periods=1),
                check_exact=False,
            )
            self.assert_eq(
                psdf.corr(method=method, min_periods=3),
                pdf.corr(method=method, min_periods=3),
                check_exact=False,
            )

    def test_series_corr(self):
        pdf = pd.DataFrame(
            index=[
                "".join(
                    np.random.choice(
                        list("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"), 10
                    )
                )
                for _ in range(30)
            ],
            columns=list("ABCD"),
            dtype="float64",
        )
        pser1 = pdf.A
        pser2 = pdf.B
        psdf = ps.from_pandas(pdf)
        psser1 = psdf.A
        psser2 = psdf.B

        with self.assertRaisesRegex(ValueError, "Invalid method"):
            psser1.corr(psser2, method="std")
        with self.assertRaisesRegex(TypeError, "Invalid min_periods type"):
            psser1.corr(psser2, min_periods="3")

        for method in ["pearson", "spearman", "kendall"]:
            self.assert_eq(
                psser1.corr(psser2, method=method),
                pser1.corr(pser2, method=method),
                almost=True,
            )
            self.assert_eq(
                psser1.corr(psser2, method=method, min_periods=1),
                pser1.corr(pser2, method=method, min_periods=1),
                almost=True,
            )
            self.assert_eq(
                psser1.corr(psser2, method=method, min_periods=3),
                pser1.corr(pser2, method=method, min_periods=3),
                almost=True,
            )
            self.assert_eq(
                (psser1 + 1).corr(psser2 - 2, method=method, min_periods=2),
                (pser1 + 1).corr(pser2 - 2, method=method, min_periods=2),
                almost=True,
            )

        # different anchors
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        with ps.option_context("compute.ops_on_diff_frames", False):
            with self.assertRaisesRegex(ValueError, "Cannot combine the series or dataframe"):
                psser1.corr(psser2)

        for method in ["pearson", "spearman", "kendall"]:
            with ps.option_context("compute.ops_on_diff_frames", True):
                self.assert_eq(
                    psser1.corr(psser2, method=method),
                    pser1.corr(pser2, method=method),
                    almost=True,
                )
                self.assert_eq(
                    psser1.corr(psser2, method=method, min_periods=1),
                    pser1.corr(pser2, method=method, min_periods=1),
                    almost=True,
                )
                self.assert_eq(
                    psser1.corr(psser2, method=method, min_periods=3),
                    pser1.corr(pser2, method=method, min_periods=3),
                    almost=True,
                )
                self.assert_eq(
                    (psser1 + 1).corr(psser2 - 2, method=method, min_periods=2),
                    (pser1 + 1).corr(pser2 - 2, method=method, min_periods=2),
                    almost=True,
                )

    def test_cov_corr_meta(self):
        # Disable arrow execution since corr() is using UDT internally which is not supported.
        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            pdf = pd.DataFrame(
                {
                    "a": np.array([1, 2, 3], dtype="i1"),
                    "b": np.array([1, 2, 3], dtype="i2"),
                    "c": np.array([1, 2, 3], dtype="i4"),
                    "d": np.array([1, 2, 3]),
                    "e": np.array([1.0, 2.0, 3.0], dtype="f4"),
                    "f": np.array([1.0, 2.0, 3.0]),
                    "g": np.array([True, False, True]),
                    "h": np.array(list("abc")),
                },
                index=pd.Index([1, 2, 3], name="myindex"),
            )
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf.corr(), pdf.corr(numeric_only=True), check_exact=False)


class FrameCorrTests(
    FrameCorrMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_corr import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
