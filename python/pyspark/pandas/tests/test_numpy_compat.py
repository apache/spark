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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class NumPyCompatTestsMixin:
    blacklist = [
        # Pandas-on-Spark does not currently support
        "conj",
        "conjugate",
        "isnat",
        "matmul",
        "frexp",
        # Values are close enough but tests failed.
        "log",  # flaky
        "log10",  # flaky
        "log1p",  # flaky
        "modf",
    ]

    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_np_add_series(self):
        psdf = self.psdf
        pdf = self.pdf

        self.assert_eq(np.add(psdf.a, psdf.b), np.add(pdf.a, pdf.b))

        psdf = self.psdf
        pdf = self.pdf
        self.assert_eq(np.add(psdf.a, 1), np.add(pdf.a, 1))

    def test_np_add_index(self):
        k_index = self.psdf.index
        p_index = self.pdf.index
        self.assert_eq(np.add(k_index, k_index), np.add(p_index, p_index))

    def test_np_unsupported_series(self):
        psdf = self.psdf
        with self.assertRaisesRegex(NotImplementedError, "pandas.*not.*support.*sqrt.*"):
            np.sqrt(psdf.a, psdf.b)

    def test_np_unsupported_frame(self):
        psdf = self.psdf
        with self.assertRaisesRegex(NotImplementedError, "on-Spark.*not.*support.*sqrt.*"):
            np.sqrt(psdf, psdf)

        psdf1 = ps.DataFrame({"A": [1, 2, 3]})
        psdf2 = ps.DataFrame({("A", "B"): [4, 5, 6]})
        with self.assertRaisesRegex(ValueError, "cannot join with no overlapping index names"):
            np.left_shift(psdf1, psdf2)

    def test_np_spark_compat_series(self):
        from pyspark.pandas.numpy_compat import unary_np_spark_mappings, binary_np_spark_mappings

        # Use randomly generated dataFrame
        pdf = pd.DataFrame(
            np.random.randint(-100, 100, size=(np.random.randint(100), 2)), columns=["a", "b"]
        )
        pdf2 = pd.DataFrame(
            np.random.randint(-100, 100, size=(len(pdf), len(pdf.columns))), columns=["a", "b"]
        )
        psdf = ps.from_pandas(pdf)
        psdf2 = ps.from_pandas(pdf2)

        for np_name, spark_func in unary_np_spark_mappings.items():
            np_func = getattr(np, np_name)
            if np_name not in self.blacklist:
                try:
                    # unary ufunc
                    self.assert_eq(np_func(pdf.a), np_func(psdf.a), almost=True)
                except Exception as e:
                    raise AssertionError("Test in '%s' function was failed." % np_name) from e

        for np_name, spark_func in binary_np_spark_mappings.items():
            np_func = getattr(np, np_name)
            if np_name not in self.blacklist:
                try:
                    # binary ufunc
                    self.assert_eq(np_func(pdf.a, pdf.b), np_func(psdf.a, psdf.b), almost=True)
                    self.assert_eq(np_func(pdf.a, 1), np_func(psdf.a, 1), almost=True)
                except Exception as e:
                    raise AssertionError("Test in '%s' function was failed." % np_name) from e

        # Test only top 5 for now. 'compute.ops_on_diff_frames' option increases too much time.
        try:
            set_option("compute.ops_on_diff_frames", True)
            for np_name, spark_func in list(binary_np_spark_mappings.items())[:5]:
                np_func = getattr(np, np_name)
                if np_name not in self.blacklist:
                    try:
                        # binary ufunc
                        self.assert_eq(
                            np_func(pdf.a, pdf2.b).sort_index(),
                            np_func(psdf.a, psdf2.b).sort_index(),
                            almost=True,
                        )
                    except Exception as e:
                        raise AssertionError("Test in '%s' function was failed." % np_name) from e
        finally:
            reset_option("compute.ops_on_diff_frames")

    def test_np_spark_compat_frame(self):
        from pyspark.pandas.numpy_compat import unary_np_spark_mappings, binary_np_spark_mappings

        # Use randomly generated dataFrame
        pdf = pd.DataFrame(
            np.random.randint(-100, 100, size=(np.random.randint(100), 2)), columns=["a", "b"]
        )
        pdf2 = pd.DataFrame(
            np.random.randint(-100, 100, size=(len(pdf), len(pdf.columns))), columns=["a", "b"]
        )
        psdf = ps.from_pandas(pdf)
        psdf2 = ps.from_pandas(pdf2)

        for np_name, spark_func in unary_np_spark_mappings.items():
            np_func = getattr(np, np_name)
            if np_name not in self.blacklist:
                try:
                    # unary ufunc
                    self.assert_eq(np_func(pdf), np_func(psdf), almost=True)
                except Exception as e:
                    raise AssertionError("Test in '%s' function was failed." % np_name) from e

        for np_name, spark_func in binary_np_spark_mappings.items():
            np_func = getattr(np, np_name)
            if np_name not in self.blacklist:
                try:
                    # binary ufunc
                    self.assert_eq(np_func(pdf, pdf), np_func(psdf, psdf), almost=True)
                    self.assert_eq(np_func(pdf, 1), np_func(psdf, 1), almost=True)
                except Exception as e:
                    raise AssertionError("Test in '%s' function was failed." % np_name) from e

        # Test only top 5 for now. 'compute.ops_on_diff_frames' option increases too much time.
        try:
            set_option("compute.ops_on_diff_frames", True)
            for np_name, spark_func in list(binary_np_spark_mappings.items())[:5]:
                np_func = getattr(np, np_name)
                if np_name not in self.blacklist:
                    try:
                        # binary ufunc
                        self.assert_eq(
                            np_func(pdf, pdf2).sort_index(),
                            np_func(psdf, psdf2).sort_index(),
                            almost=True,
                        )

                    except Exception as e:
                        raise AssertionError("Test in '%s' function was failed." % np_name) from e
        finally:
            reset_option("compute.ops_on_diff_frames")


class NumPyCompatTests(
    NumPyCompatTestsMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_numpy_compat import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
