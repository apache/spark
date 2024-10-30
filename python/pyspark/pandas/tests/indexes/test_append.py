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


class AppendMixin:
    def test_append(self):
        # Index
        pidx = pd.Index(range(10000))
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.append(pidx), psidx.append(psidx))

        # Index with name
        pidx1 = pd.Index(range(10000), name="a")
        pidx2 = pd.Index(range(10000), name="b")
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(pidx1.append(pidx2), psidx1.append(psidx2))

        self.assert_eq(pidx2.append(pidx1), psidx2.append(psidx1))

        # Index from DataFrame
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=["a", "b", "c"])
        pdf2 = pd.DataFrame({"a": [7, 8, 9], "d": [10, 11, None]}, index=["x", "y", "z"])
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        pidx1 = pdf1.set_index("a").index
        pidx2 = pdf2.set_index("d").index
        psidx1 = psdf1.set_index("a").index
        psidx2 = psdf2.set_index("d").index

        self.assert_eq(pidx1.append(pidx2), psidx1.append(psidx2))

        self.assert_eq(pidx2.append(pidx1), psidx2.append(psidx1))

        # Index from DataFrame with MultiIndex columns
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        pdf2 = pd.DataFrame({"a": [7, 8, 9], "d": [10, 11, 12]})
        pdf1.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        pdf2.columns = pd.MultiIndex.from_tuples([("a", "x"), ("d", "y")])
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        pidx1 = pdf1.set_index(("a", "x")).index
        pidx2 = pdf2.set_index(("d", "y")).index
        psidx1 = psdf1.set_index(("a", "x")).index
        psidx2 = psdf2.set_index(("d", "y")).index

        self.assert_eq(pidx1.append(pidx2), psidx1.append(psidx2))

        self.assert_eq(pidx2.append(pidx1), psidx2.append(psidx1))

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.append(pmidx), psmidx.append(psmidx))

        # MultiIndex with names
        pmidx1 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["x", "y", "z"]
        )
        pmidx2 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["p", "q", "r"]
        )
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)

        self.assert_eq(pmidx1.append(pmidx2), psmidx1.append(psmidx2))
        self.assert_eq(pmidx2.append(pmidx1), psmidx2.append(psmidx1))
        self.assert_eq(pmidx1.append(pmidx2).names, psmidx1.append(psmidx2).names)

        # Index & MultiIndex is currently not supported
        expected_error_message = r"append\(\) between Index & MultiIndex is currently not supported"
        with self.assertRaisesRegex(NotImplementedError, expected_error_message):
            psidx.append(psmidx)
        with self.assertRaisesRegex(NotImplementedError, expected_error_message):
            psmidx.append(psidx)

        # MultiIndexs with different levels is currently not supported
        psmidx3 = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        expected_error_message = (
            r"append\(\) between MultiIndexs with different levels is currently not supported"
        )
        with self.assertRaisesRegex(NotImplementedError, expected_error_message):
            psmidx.append(psmidx3)


class AppendTests(
    AppendMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_append import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
