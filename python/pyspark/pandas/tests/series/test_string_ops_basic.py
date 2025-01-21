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

import pandas as pd
import numpy as np
import re

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class SeriesStringOpsMixin:
    @property
    def pser(self):
        return pd.Series(
            [
                "apples",
                "Bananas",
                "carrots",
                "1",
                "100",
                "",
                "\nleading-whitespace",
                "trailing-Whitespace    \t",
                None,
                np.nan,
            ]
        )

    def check_func(self, func, almost=False):
        self.check_func_on_series(func, self.pser, almost=almost)

    def check_func_on_series(self, func, pser, almost=False):
        self.assert_eq(func(ps.from_pandas(pser)), func(pser), almost=almost)

    def test_string_add_str_num(self):
        pdf = pd.DataFrame(dict(col1=["a"], col2=[1]))
        psdf = ps.from_pandas(pdf)
        with self.assertRaises(TypeError):
            psdf["col1"] + psdf["col2"]

    def test_string_add_assign(self):
        pdf = pd.DataFrame(dict(col1=["a", "b", "c"], col2=["1", "2", "3"]))
        psdf = ps.from_pandas(pdf)
        psdf["col1"] += psdf["col2"]
        pdf["col1"] += pdf["col2"]
        self.assert_eq(psdf["col1"], pdf["col1"])

    def test_string_add_str_str(self):
        pdf = pd.DataFrame(dict(col1=["a", "b", "c"], col2=["1", "2", "3"]))
        psdf = ps.from_pandas(pdf)

        # TODO: Fix the Series names
        self.assert_eq(psdf["col1"] + psdf["col2"], pdf["col1"] + pdf["col2"])
        self.assert_eq(psdf["col2"] + psdf["col1"], pdf["col2"] + pdf["col1"])

    def test_string_add_str_lit(self):
        pdf = pd.DataFrame(dict(col1=["a", "b", "c"]))
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf["col1"] + "_lit", pdf["col1"] + "_lit")
        self.assert_eq("_lit" + psdf["col1"], "_lit" + pdf["col1"])

    def test_string_capitalize(self):
        self.check_func(lambda x: x.str.capitalize())

    def test_string_title(self):
        self.check_func(lambda x: x.str.title())

    def test_string_lower(self):
        self.check_func(lambda x: x.str.lower())

    def test_string_upper(self):
        self.check_func(lambda x: x.str.upper())

    def test_string_swapcase(self):
        self.check_func(lambda x: x.str.swapcase())

    def test_string_startswith(self):
        pattern = "car"
        self.check_func(lambda x: x.str.startswith(pattern))
        self.check_func(lambda x: x.str.startswith(pattern, na=False))

    def test_string_endswith(self):
        pattern = "s"
        self.check_func(lambda x: x.str.endswith(pattern))
        self.check_func(lambda x: x.str.endswith(pattern, na=False))

    def test_string_strip(self):
        self.check_func(lambda x: x.str.strip())
        self.check_func(lambda x: x.str.strip("es\t"))
        self.check_func(lambda x: x.str.strip("1"))

    def test_string_lstrip(self):
        self.check_func(lambda x: x.str.lstrip())
        self.check_func(lambda x: x.str.lstrip("\n1le"))
        self.check_func(lambda x: x.str.lstrip("s"))

    def test_string_rstrip(self):
        self.check_func(lambda x: x.str.rstrip())
        self.check_func(lambda x: x.str.rstrip("\t ec"))
        self.check_func(lambda x: x.str.rstrip("0"))

    def test_string_get(self):
        self.check_func(lambda x: x.str.get(6))
        self.check_func(lambda x: x.str.get(-1))

    def test_string_isalnum(self):
        self.check_func(lambda x: x.str.isalnum())

    def test_string_isalpha(self):
        self.check_func(lambda x: x.str.isalpha())

    def test_string_isdigit(self):
        self.check_func(lambda x: x.str.isdigit())

    def test_string_isspace(self):
        self.check_func(lambda x: x.str.isspace())

    def test_string_islower(self):
        self.check_func(lambda x: x.str.islower())

    def test_string_isupper(self):
        self.check_func(lambda x: x.str.isupper())

    def test_string_istitle(self):
        self.check_func(lambda x: x.str.istitle())

    def test_string_isnumeric(self):
        self.check_func(lambda x: x.str.isnumeric())

    def test_string_isdecimal(self):
        self.check_func(lambda x: x.str.isdecimal())

    def test_string_cat(self):
        psser = ps.from_pandas(self.pser)
        with self.assertRaises(NotImplementedError):
            psser.str.cat()

    def test_string_center(self):
        self.check_func(lambda x: x.str.center(0))
        self.check_func(lambda x: x.str.center(10))
        self.check_func(lambda x: x.str.center(10, "x"))

    def test_string_contains(self):
        self.check_func(lambda x: x.str.contains("le", regex=False))
        self.check_func(lambda x: x.str.contains("White", case=True, regex=False))
        self.check_func(lambda x: x.str.contains("apples|carrots", regex=True))
        self.check_func(lambda x: x.str.contains("BANANAS", flags=re.IGNORECASE, na=False))

    def test_string_count(self):
        self.check_func(lambda x: x.str.count("wh|Wh"))
        self.check_func(lambda x: x.str.count("WH", flags=re.IGNORECASE))


class SeriesStringOpsTests(
    SeriesStringOpsMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.series.test_string_ops_basic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
