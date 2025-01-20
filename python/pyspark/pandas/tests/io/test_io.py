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
from io import StringIO

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.testing.utils import (
    have_jinja2,
    jinja2_requirement_message,
    have_tabulate,
    tabulate_requirement_message,
)


# This file contains test cases for 'Serialization / IO / Conversion'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#serialization-io-conversion
class FrameIOMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def df_pair(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        return pdf, psdf

    def test_to_numpy(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 9, 4, 2, 4],
                "c": ["one", "three", "six", "seven", "one", "5"],
            },
            index=np.random.rand(6),
        )

        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.to_numpy(), pdf.values)

    def test_to_pandas(self):
        pdf, psdf = self.df_pair
        self.assert_eq(psdf._to_pandas(), pdf)

    def test_to_spark(self):
        psdf = ps.from_pandas(self.pdf)

        with self.assertRaisesRegex(ValueError, "'index_col' cannot be overlapped"):
            psdf.to_spark(index_col="a")

        with self.assertRaisesRegex(ValueError, "length of index columns.*1.*3"):
            psdf.to_spark(index_col=["x", "y", "z"])

    @unittest.skipIf(not have_tabulate, tabulate_requirement_message)
    def test_to_markdown(self):
        pdf = pd.DataFrame(data={"animal_1": ["elk", "pig"], "animal_2": ["dog", "quetzal"]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.to_markdown(), psdf.to_markdown())

    def test_from_dict(self):
        data = {"row_1": [3, 2, 1, 0], "row_2": [10, 20, 30, 40]}
        pdf = pd.DataFrame.from_dict(data)
        psdf = ps.DataFrame.from_dict(data)
        self.assert_eq(pdf, psdf)

        pdf = pd.DataFrame.from_dict(data, dtype="int8")
        psdf = ps.DataFrame.from_dict(data, dtype="int8")
        self.assert_eq(pdf, psdf)

        pdf = pd.DataFrame.from_dict(data, orient="index", columns=["A", "B", "C", "D"])
        psdf = ps.DataFrame.from_dict(data, orient="index", columns=["A", "B", "C", "D"])
        self.assert_eq(pdf, psdf)

    @unittest.skipIf(not have_jinja2, jinja2_requirement_message)
    def test_style(self):
        # Currently, the `style` function returns a pandas object `Styler` as it is,
        # processing only the number of rows declared in `compute.max_rows`.
        # So it's a bit vague to test, but we are doing minimal tests instead of not testing at all.
        pdf = pd.DataFrame(np.random.randn(10, 4), columns=["A", "B", "C", "D"])
        psdf = ps.from_pandas(pdf)

        def style_negative(v, props=""):
            return props if v < 0 else None

        def check_style():
            # If the value is negative, the text color will be displayed as red.
            pdf_style = pdf.style.applymap(style_negative, props="color:red;")
            psdf_style = psdf.style.applymap(style_negative, props="color:red;")

            # Test whether the same shape as pandas table is created including the color.
            self.assert_eq(pdf_style.to_latex(), psdf_style.to_latex())

        check_style()

        with ps.option_context("compute.max_rows", None):
            check_style()

    def test_info(self):
        pdf, psdf = self.df_pair
        pdf_io = StringIO()
        psdf_io = StringIO()

        psdf.info(buf=psdf_io)
        pdf.info(buf=pdf_io, memory_usage=False)

        # Split is using to filter out first line with class name
        # <class 'pyspark.pandas.frame.DataFrame'> vs <class 'pandas.core.frame.DataFrame'>
        self.assert_eq(pdf_io.getvalue().split("\n")[1:], psdf_io.getvalue().split("\n")[1:])
        psdf_io.truncate(0)
        pdf_io.truncate(0)
        psdf.info(buf=psdf_io, max_cols=1)
        pdf.info(buf=pdf_io, max_cols=1, memory_usage=False)
        self.assert_eq(pdf_io.getvalue().split("\n")[1:], psdf_io.getvalue().split("\n")[1:])
        psdf_io.truncate(0)
        pdf_io.truncate(0)
        psdf.info(buf=psdf_io, show_counts=True)
        pdf.info(buf=pdf_io, show_counts=True, memory_usage=False)
        self.assert_eq(pdf_io.getvalue().split("\n")[1:], psdf_io.getvalue().split("\n")[1:])
        psdf_io.truncate(0)
        pdf_io.truncate(0)
        psdf.info(buf=psdf_io, show_counts=False)
        pdf.info(buf=pdf_io, show_counts=False, memory_usage=False)
        self.assert_eq(pdf_io.getvalue().split("\n")[1:], psdf_io.getvalue().split("\n")[1:])


class FrameIOTests(
    FrameIOMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.io.test_io import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
