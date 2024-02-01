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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameTruncateMixin:
    def test_truncate(self):
        pdf1 = pd.DataFrame(
            {
                "A": ["a", "b", "c", "d", "e", "f", "g"],
                "B": ["h", "i", "j", "k", "l", "m", "n"],
                "C": ["o", "p", "q", "r", "s", "t", "u"],
            },
            index=[-500, -20, -1, 0, 400, 550, 1000],
        )
        psdf1 = ps.from_pandas(pdf1)
        pdf2 = pd.DataFrame(
            {
                "A": ["a", "b", "c", "d", "e", "f", "g"],
                "B": ["h", "i", "j", "k", "l", "m", "n"],
                "C": ["o", "p", "q", "r", "s", "t", "u"],
            },
            index=[1000, 550, 400, 0, -1, -20, -500],
        )
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(psdf1.truncate(), pdf1.truncate())
        self.assert_eq(psdf1.truncate(before=-20), pdf1.truncate(before=-20))
        self.assert_eq(psdf1.truncate(after=400), pdf1.truncate(after=400))
        self.assert_eq(psdf1.truncate(copy=False), pdf1.truncate(copy=False))
        self.assert_eq(psdf1.truncate(-20, 400, copy=False), pdf1.truncate(-20, 400, copy=False))
        self.assert_eq(psdf2.truncate(0, 550), pdf2.truncate(0, 550))
        self.assert_eq(psdf2.truncate(0, 550, copy=False), pdf2.truncate(0, 550, copy=False))

        # axis = 1
        self.assert_eq(psdf1.truncate(axis=1), pdf1.truncate(axis=1))
        self.assert_eq(psdf1.truncate(before="B", axis=1), pdf1.truncate(before="B", axis=1))
        self.assert_eq(psdf1.truncate(after="A", axis=1), pdf1.truncate(after="A", axis=1))
        self.assert_eq(psdf1.truncate(copy=False, axis=1), pdf1.truncate(copy=False, axis=1))
        self.assert_eq(psdf2.truncate("B", "C", axis=1), pdf2.truncate("B", "C", axis=1))
        self.assert_eq(
            psdf1.truncate("B", "C", copy=False, axis=1),
            pdf1.truncate("B", "C", copy=False, axis=1),
        )

        # MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X"), ("C", "Z")])
        pdf1.columns = columns
        psdf1.columns = columns
        pdf2.columns = columns
        psdf2.columns = columns

        self.assert_eq(psdf1.truncate(), pdf1.truncate())
        self.assert_eq(psdf1.truncate(before=-20), pdf1.truncate(before=-20))
        self.assert_eq(psdf1.truncate(after=400), pdf1.truncate(after=400))
        self.assert_eq(psdf1.truncate(copy=False), pdf1.truncate(copy=False))
        self.assert_eq(psdf1.truncate(-20, 400, copy=False), pdf1.truncate(-20, 400, copy=False))
        self.assert_eq(psdf2.truncate(0, 550), pdf2.truncate(0, 550))
        self.assert_eq(psdf2.truncate(0, 550, copy=False), pdf2.truncate(0, 550, copy=False))
        # axis = 1
        self.assert_eq(psdf1.truncate(axis=1), pdf1.truncate(axis=1))
        self.assert_eq(psdf1.truncate(before="B", axis=1), pdf1.truncate(before="B", axis=1))
        self.assert_eq(psdf1.truncate(after="A", axis=1), pdf1.truncate(after="A", axis=1))
        self.assert_eq(psdf1.truncate(copy=False, axis=1), pdf1.truncate(copy=False, axis=1))
        self.assert_eq(psdf2.truncate("B", "C", axis=1), pdf2.truncate("B", "C", axis=1))
        self.assert_eq(
            psdf1.truncate("B", "C", copy=False, axis=1),
            pdf1.truncate("B", "C", copy=False, axis=1),
        )

        # Exceptions
        psdf = ps.DataFrame(
            {
                "A": ["a", "b", "c", "d", "e", "f", "g"],
                "B": ["h", "i", "j", "k", "l", "m", "n"],
                "C": ["o", "p", "q", "r", "s", "t", "u"],
            },
            index=[-500, 100, 400, 0, -1, 550, -20],
        )
        msg = "truncate requires a sorted index"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.truncate()

        psdf = ps.DataFrame(
            {
                "A": ["a", "b", "c", "d", "e", "f", "g"],
                "B": ["h", "i", "j", "k", "l", "m", "n"],
                "C": ["o", "p", "q", "r", "s", "t", "u"],
            },
            index=[-500, -20, -1, 0, 400, 550, 1000],
        )
        msg = "Truncate: -20 must be after 400"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.truncate(400, -20)
        msg = "Truncate: B must be after C"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.truncate("C", "B", axis=1)


class FrameTruncateTests(FrameTruncateMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_truncate import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
