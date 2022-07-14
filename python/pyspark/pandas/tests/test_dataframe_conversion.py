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

import os
import shutil
import string
import tempfile
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import ComparisonTestBase, TestUtils
from pyspark.testing.sqlutils import SQLTestUtils


class DataFrameConversionTest(ComparisonTestBase, SQLTestUtils, TestUtils):
    """Test cases for "small data" conversion and I/O."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix=DataFrameConversionTest.__name__)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    @property
    def pdf(self):
        return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=[0, 1, 3])

    @staticmethod
    def strip_all_whitespace(str):
        """A helper function to remove all whitespace from a string."""
        return str.translate({ord(c): None for c in string.whitespace})

    def test_to_html(self):
        expected = self.strip_all_whitespace(
            """
            <table border="1" class="dataframe">
              <thead>
                <tr style="text-align: right;"><th></th><th>a</th><th>b</th></tr>
              </thead>
              <tbody>
                <tr><th>0</th><td>1</td><td>4</td></tr>
                <tr><th>1</th><td>2</td><td>5</td></tr>
                <tr><th>3</th><td>3</td><td>6</td></tr>
              </tbody>
            </table>
            """
        )
        got = self.strip_all_whitespace(self.psdf.to_html())
        self.assert_eq(got, expected)

        # with max_rows set
        expected = self.strip_all_whitespace(
            """
            <table border="1" class="dataframe">
              <thead>
                <tr style="text-align: right;"><th></th><th>a</th><th>b</th></tr>
              </thead>
              <tbody>
                <tr><th>0</th><td>1</td><td>4</td></tr>
                <tr><th>1</th><td>2</td><td>5</td></tr>
              </tbody>
            </table>
            """
        )
        got = self.strip_all_whitespace(self.psdf.to_html(max_rows=2))
        self.assert_eq(got, expected)

    @staticmethod
    def get_excel_dfs(pandas_on_spark_location, pandas_location):
        return {
            "got": pd.read_excel(pandas_on_spark_location, index_col=0),
            "expected": pd.read_excel(pandas_location, index_col=0),
        }

    @unittest.skip("openpyxl")
    def test_to_excel(self):
        with self.temp_dir() as dirpath:
            pandas_location = dirpath + "/" + "output1.xlsx"
            pandas_on_spark_location = dirpath + "/" + "output2.xlsx"

            pdf = self.pdf
            psdf = self.psdf
            psdf.to_excel(pandas_on_spark_location)
            pdf.to_excel(pandas_location)
            dataframes = self.get_excel_dfs(pandas_on_spark_location, pandas_location)
            self.assert_eq(dataframes["got"], dataframes["expected"])

            psdf.a.to_excel(pandas_on_spark_location)
            pdf.a.to_excel(pandas_location)
            dataframes = self.get_excel_dfs(pandas_on_spark_location, pandas_location)
            self.assert_eq(dataframes["got"], dataframes["expected"])

            pdf = pd.DataFrame({"a": [1, None, 3], "b": ["one", "two", None]}, index=[0, 1, 3])

            psdf = ps.from_pandas(pdf)

            psdf.to_excel(pandas_on_spark_location, na_rep="null")
            pdf.to_excel(pandas_location, na_rep="null")
            dataframes = self.get_excel_dfs(pandas_on_spark_location, pandas_location)
            self.assert_eq(dataframes["got"], dataframes["expected"])

            pdf = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]}, index=[0, 1, 3])

            psdf = ps.from_pandas(pdf)

            psdf.to_excel(pandas_on_spark_location, float_format="%.1f")
            pdf.to_excel(pandas_location, float_format="%.1f")
            dataframes = self.get_excel_dfs(pandas_on_spark_location, pandas_location)
            self.assert_eq(dataframes["got"], dataframes["expected"])

            psdf.to_excel(pandas_on_spark_location, header=False)
            pdf.to_excel(pandas_location, header=False)
            dataframes = self.get_excel_dfs(pandas_on_spark_location, pandas_location)
            self.assert_eq(dataframes["got"], dataframes["expected"])

            psdf.to_excel(pandas_on_spark_location, index=False)
            pdf.to_excel(pandas_location, index=False)
            dataframes = self.get_excel_dfs(pandas_on_spark_location, pandas_location)
            self.assert_eq(dataframes["got"], dataframes["expected"])

    def test_to_json(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.to_json(orient="records"), pdf.to_json(orient="records"))

    def test_to_json_negative(self):
        psdf = ps.from_pandas(self.pdf)

        with self.assertRaises(NotImplementedError):
            psdf.to_json(orient="table")

        with self.assertRaises(NotImplementedError):
            psdf.to_json(lines=False)

    def test_read_json_negative(self):
        with self.assertRaises(NotImplementedError):
            ps.read_json("invalid", lines=False)

    def test_to_json_with_path(self):
        pdf = pd.DataFrame({"a": [1], "b": ["a"]})
        psdf = ps.DataFrame(pdf)

        psdf.to_json(self.tmp_dir, num_files=1)
        expected = pdf.to_json(orient="records")

        output_paths = [path for path in os.listdir(self.tmp_dir) if path.startswith("part-")]
        assert len(output_paths) > 0
        output_path = "%s/%s" % (self.tmp_dir, output_paths[0])
        self.assertEqual("[%s]" % open(output_path).read().strip(), expected)

    def test_to_json_with_partition_cols(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        psdf = ps.DataFrame(pdf)

        psdf.to_json(self.tmp_dir, partition_cols="b", num_files=1)

        partition_paths = [path for path in os.listdir(self.tmp_dir) if path.startswith("b=")]
        assert len(partition_paths) > 0
        for partition_path in partition_paths:
            column, value = partition_path.split("=")
            expected = pdf[pdf[column] == value].drop("b", axis=1).to_json(orient="records")

            output_paths = [
                path
                for path in os.listdir("%s/%s" % (self.tmp_dir, partition_path))
                if path.startswith("part-")
            ]
            assert len(output_paths) > 0
            output_path = "%s/%s/%s" % (self.tmp_dir, partition_path, output_paths[0])
            self.assertEqual("[%s]" % open(output_path).read().strip(), expected)

    @unittest.skip("Pyperclip could not find a copy/paste mechanism for Linux.")
    def test_to_clipboard(self):
        pdf = self.pdf
        psdf = self.psdf

        self.assert_eq(psdf.to_clipboard(), pdf.to_clipboard())
        self.assert_eq(psdf.to_clipboard(excel=False), pdf.to_clipboard(excel=False))
        self.assert_eq(
            psdf.to_clipboard(sep=";", index=False), pdf.to_clipboard(sep=";", index=False)
        )

    def test_to_latex(self):
        pdf = self.pdf
        psdf = self.psdf

        self.assert_eq(psdf.to_latex(), pdf.to_latex())
        self.assert_eq(psdf.to_latex(col_space=2), pdf.to_latex(col_space=2))
        self.assert_eq(psdf.to_latex(header=True), pdf.to_latex(header=True))
        self.assert_eq(psdf.to_latex(index=False), pdf.to_latex(index=False))
        self.assert_eq(psdf.to_latex(na_rep="-"), pdf.to_latex(na_rep="-"))
        self.assert_eq(psdf.to_latex(float_format="%.1f"), pdf.to_latex(float_format="%.1f"))
        self.assert_eq(psdf.to_latex(sparsify=False), pdf.to_latex(sparsify=False))
        self.assert_eq(psdf.to_latex(index_names=False), pdf.to_latex(index_names=False))
        self.assert_eq(psdf.to_latex(bold_rows=True), pdf.to_latex(bold_rows=True))
        self.assert_eq(psdf.to_latex(decimal=","), pdf.to_latex(decimal=","))

    def test_to_records(self):
        pdf = pd.DataFrame({"A": [1, 2], "B": [0.5, 0.75]}, index=["a", "b"])

        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.to_records(), pdf.to_records())
        self.assert_eq(psdf.to_records(index=False), pdf.to_records(index=False))
        self.assert_eq(psdf.to_records(index_dtypes="<S2"), pdf.to_records(index_dtypes="<S2"))

    def test_from_records(self):
        # Assert using a dict as input
        self.assert_eq(
            ps.DataFrame.from_records({"A": [1, 2, 3]}), pd.DataFrame.from_records({"A": [1, 2, 3]})
        )
        # Assert using a list of tuples as input
        self.assert_eq(
            ps.DataFrame.from_records([(1, 2), (3, 4)]), pd.DataFrame.from_records([(1, 2), (3, 4)])
        )
        # Assert using a NumPy array as input
        self.assert_eq(ps.DataFrame.from_records(np.eye(3)), pd.DataFrame.from_records(np.eye(3)))
        # Asserting using a custom index
        self.assert_eq(
            ps.DataFrame.from_records([(1, 2), (3, 4)], index=[2, 3]),
            pd.DataFrame.from_records([(1, 2), (3, 4)], index=[2, 3]),
        )
        # Assert excluding excluding column(s)
        self.assert_eq(
            ps.DataFrame.from_records({"A": [1, 2, 3], "B": [1, 2, 3]}, exclude=["B"]),
            pd.DataFrame.from_records({"A": [1, 2, 3], "B": [1, 2, 3]}, exclude=["B"]),
        )
        # Assert limiting to certain column(s)
        self.assert_eq(
            ps.DataFrame.from_records({"A": [1, 2, 3], "B": [1, 2, 3]}, columns=["A"]),
            pd.DataFrame.from_records({"A": [1, 2, 3], "B": [1, 2, 3]}, columns=["A"]),
        )
        # Assert limiting to a number of rows
        self.assert_eq(
            ps.DataFrame.from_records([(1, 2), (3, 4)], nrows=1),
            pd.DataFrame.from_records([(1, 2), (3, 4)], nrows=1),
        )


if __name__ == "__main__":
    from pyspark.pandas.tests.test_dataframe_conversion import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
