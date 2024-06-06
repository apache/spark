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

import datetime
from decimal import Decimal

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class GetDummiesMixin:
    def test_get_dummies(self):
        for pdf_or_ps in [
            pd.Series([1, 1, 1, 2, 2, 1, 3, 4]),
            # pd.Series([1, 1, 1, 2, 2, 1, 3, 4], dtype='category'),
            # pd.Series(pd.Categorical([1, 1, 1, 2, 2, 1, 3, 4],
            #                          categories=[4, 3, 2, 1])),
            pd.DataFrame(
                {
                    "a": [1, 2, 3, 4, 4, 3, 2, 1],
                    # 'b': pd.Categorical(list('abcdabcd')),
                    "b": list("abcdabcd"),
                }
            ),
            pd.DataFrame({10: [1, 2, 3, 4, 4, 3, 2, 1], 20: list("abcdabcd")}),
        ]:
            psdf_or_psser = ps.from_pandas(pdf_or_ps)

            self.assert_eq(ps.get_dummies(psdf_or_psser), pd.get_dummies(pdf_or_ps, dtype=np.int8))

        psser = ps.Series([1, 1, 1, 2, 2, 1, 3, 4])
        with self.assertRaisesRegex(
            NotImplementedError, "get_dummies currently does not support sparse"
        ):
            ps.get_dummies(psser, sparse=True)
        with self.assertRaisesRegex(NotImplementedError, "get_dummies currently only accept"):
            ps.get_dummies(ps.Series([b"1"]))
        with self.assertRaisesRegex(NotImplementedError, "get_dummies currently only accept"):
            ps.get_dummies(ps.Series([None]))

    def test_get_dummies_date_datetime(self):
        pdf = pd.DataFrame(
            {
                "d": [
                    datetime.date(2019, 1, 1),
                    datetime.date(2019, 1, 2),
                    datetime.date(2019, 1, 1),
                ],
                "dt": [
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                    datetime.datetime(2019, 1, 1, 0, 0, 1),
                    datetime.datetime(2019, 1, 1, 0, 0, 0),
                ],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(ps.get_dummies(psdf), pd.get_dummies(pdf, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.d), pd.get_dummies(pdf.d, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.dt), pd.get_dummies(pdf.dt, dtype=np.int8))

    def test_get_dummies_boolean(self):
        pdf = pd.DataFrame({"b": [True, False, True]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(ps.get_dummies(psdf), pd.get_dummies(pdf, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.b), pd.get_dummies(pdf.b, dtype=np.int8))

    def test_get_dummies_decimal(self):
        pdf = pd.DataFrame({"d": [Decimal(1.0), Decimal(2.0), Decimal(1)]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(ps.get_dummies(psdf), pd.get_dummies(pdf, dtype=np.int8))
        self.assert_eq(ps.get_dummies(psdf.d), pd.get_dummies(pdf.d, dtype=np.int8), almost=True)

    def test_get_dummies_dtype(self):
        pdf = pd.DataFrame(
            {
                # "A": pd.Categorical(['a', 'b', 'a'], categories=['a', 'b', 'c']),
                "A": ["a", "b", "a"],
                "B": [0, 0, 1],
            }
        )
        psdf = ps.from_pandas(pdf)

        exp = pd.get_dummies(pdf)
        exp = exp.astype({"A_a": "float64", "A_b": "float64"})
        res = ps.get_dummies(psdf, dtype="float64")
        self.assert_eq(res, exp)


class GetDummiesTests(
    GetDummiesMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.reshape.test_get_dummies import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
