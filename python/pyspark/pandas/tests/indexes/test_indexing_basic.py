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

import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, compare_both


class BasicIndexingTestsMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"month": [1, 4, 7, 10], "year": [2012, 2014, 2013, 2014], "sale": [55, 40, 84, 31]}
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    @compare_both(almost=False)
    def test_indexing(self, df):
        df1 = df.set_index("month")
        yield df1

        yield df.set_index("month", drop=False)
        yield df.set_index("month", append=True)
        yield df.set_index(["year", "month"])
        yield df.set_index(["year", "month"], drop=False)
        yield df.set_index(["year", "month"], append=True)

        yield df1.set_index("year", drop=False, append=True)

        df2 = df1.copy()
        df2.set_index("year", append=True, inplace=True)
        yield df2

        self.assertRaisesRegex(KeyError, "unknown", lambda: df.set_index("unknown"))
        self.assertRaisesRegex(KeyError, "unknown", lambda: df.set_index(["month", "unknown"]))

        for d in [df, df1, df2]:
            yield d.reset_index()
            yield d.reset_index(drop=True)

        yield df1.reset_index(level=0)
        yield df2.reset_index(level=1)
        yield df2.reset_index(level=[1, 0])
        yield df1.reset_index(level="month")
        yield df2.reset_index(level="year")
        yield df2.reset_index(level=["month", "year"])
        yield df2.reset_index(level="month", drop=True)
        yield df2.reset_index(level=["month", "year"], drop=True)

        self.assertRaisesRegex(
            IndexError,
            "Too many levels: Index has only 1 level, not 3",
            lambda: df1.reset_index(level=2),
        )
        self.assertRaisesRegex(
            IndexError,
            "Too many levels: Index has only 1 level, not 4",
            lambda: df1.reset_index(level=[3, 2]),
        )
        self.assertRaisesRegex(KeyError, "unknown.*month", lambda: df1.reset_index(level="unknown"))
        self.assertRaisesRegex(
            KeyError, "Level unknown not found", lambda: df2.reset_index(level="unknown")
        )

        df3 = df2.copy()
        df3.reset_index(inplace=True)
        yield df3

        yield df1.sale.reset_index()
        yield df1.sale.reset_index(level=0)
        yield df2.sale.reset_index(level=[1, 0])
        yield df1.sale.reset_index(drop=True)
        yield df1.sale.reset_index(name="s")
        yield df1.sale.reset_index(name="s", drop=True)

        s = df1.sale
        self.assertRaisesRegex(
            TypeError,
            "Cannot reset_index inplace on a Series to create a DataFrame",
            lambda: s.reset_index(inplace=True),
        )
        s.reset_index(drop=True, inplace=True)
        yield s
        yield df1

        # multi-index columns
        df4 = df.copy()
        df4.columns = pd.MultiIndex.from_tuples(
            [("cal", "month"), ("cal", "year"), ("num", "sale")]
        )
        df5 = df4.set_index(("cal", "month"))
        yield df5
        yield df4.set_index([("cal", "month"), ("num", "sale")])

        self.assertRaises(KeyError, lambda: df5.reset_index(level=("cal", "month")))

        yield df5.reset_index(level=[("cal", "month")])

        # non-string names
        df6 = df.copy()
        df6.columns = [10.0, 20.0, 30.0]
        df7 = df6.set_index(10.0)
        yield df7
        yield df6.set_index([10.0, 30.0])

        yield df7.reset_index(level=10.0)
        yield df7.reset_index(level=[10.0])

        df8 = df.copy()
        df8.columns = pd.MultiIndex.from_tuples([(10, "month"), (10, "year"), (20, "sale")])
        df9 = df8.set_index((10, "month"))
        yield df9
        yield df8.set_index([(10, "month"), (20, "sale")])

        yield df9.reset_index(level=[(10, "month")])

    def test_from_pandas_with_explicit_index(self):
        pdf = self.pdf

        df1 = ps.from_pandas(pdf.set_index("month"))
        self.assertPandasEqual(df1._to_pandas(), pdf.set_index("month"))

        df2 = ps.from_pandas(pdf.set_index(["year", "month"]))
        self.assertPandasEqual(df2._to_pandas(), pdf.set_index(["year", "month"]))

    def test_limitations(self):
        df = self.psdf.set_index("month")

        self.assertRaisesRegex(
            ValueError,
            "Level should be all int or all string.",
            lambda: df.reset_index([1, "month"]),
        )


class BasicIndexingTests(
    BasicIndexingTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_indexing_basic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
