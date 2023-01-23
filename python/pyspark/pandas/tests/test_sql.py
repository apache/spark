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

from pyspark import pandas as ps
from pyspark.errors import ParseException
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class SQLTest(PandasOnSparkTestCase, SQLTestUtils):
    def test_error_variable_not_exist(self):
        with self.assertRaisesRegex(KeyError, "variable_foo"):
            ps.sql("select * from {variable_foo}")

    def test_error_bad_sql(self):
        with self.assertRaises(ParseException):
            ps.sql("this is not valid sql")

    def test_series_not_referred(self):
        psdf = ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        with self.assertRaisesRegex(ValueError, "The series in {ser}"):
            ps.sql("SELECT {ser} FROM range(10)", ser=psdf.A)

    def test_sql_with_index_col(self):
        import pandas as pd

        # Index
        psdf = ps.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=pd.Index(["a", "b", "c"], name="index")
        )
        psdf_reset_index = psdf.reset_index()
        actual = ps.sql(
            "select * from {psdf_reset_index} where A > 1",
            index_col="index",
            psdf_reset_index=psdf_reset_index,
        )
        expected = psdf.iloc[[1, 2]]
        self.assert_eq(actual, expected)

        # MultiIndex
        psdf = ps.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]},
            index=pd.MultiIndex.from_tuples(
                [("a", "b"), ("c", "d"), ("e", "f")], names=["index1", "index2"]
            ),
        )
        psdf_reset_index = psdf.reset_index()
        actual = ps.sql(
            "select * from {psdf_reset_index} where A > 1",
            index_col=["index1", "index2"],
            psdf_reset_index=psdf_reset_index,
        )
        expected = psdf.iloc[[1, 2]]
        self.assert_eq(actual, expected)

    def test_sql_with_pandas_objects(self):
        import pandas as pd

        pdf = pd.DataFrame({"a": [1, 2, 3, 4]})
        self.assert_eq(ps.sql("SELECT {col} + 1 as a FROM {tbl}", col=pdf.a, tbl=pdf), pdf + 1)

    def test_sql_with_python_objects(self):
        self.assert_eq(
            ps.sql("SELECT {col} as a FROM range(1)", col="lit"), ps.DataFrame({"a": ["lit"]})
        )
        self.assert_eq(
            ps.sql("SELECT id FROM range(10) WHERE id IN {pred}", col="lit", pred=(1, 2, 3)),
            ps.DataFrame({"id": [1, 2, 3]}),
        )

    def test_sql_with_pandas_on_spark_objects(self):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4]})

        self.assert_eq(ps.sql("SELECT {col} FROM {tbl}", col=psdf.a, tbl=psdf), psdf)
        self.assert_eq(ps.sql("SELECT {tbl.a} FROM {tbl}", tbl=psdf), psdf)

        psdf = ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        self.assert_eq(
            ps.sql("SELECT {col}, {col2} FROM {tbl}", col=psdf.A, col2=psdf.B, tbl=psdf), psdf
        )
        self.assert_eq(ps.sql("SELECT {tbl.A}, {tbl.B} FROM {tbl}", tbl=psdf), psdf)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_sql import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
