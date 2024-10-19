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


class MonotonicMixin:
    def test_monotonic(self):
        # test monotonic_increasing & monotonic_decreasing for MultiIndex.
        # Since the Behavior for null value was changed in pandas >= 1.0.0,
        # several cases are tested differently.
        datas = []

        # increasing / decreasing ordered each index level with string
        datas.append([("w", "a"), ("x", "b"), ("y", "c"), ("z", "d")])
        datas.append([("w", "d"), ("x", "c"), ("y", "b"), ("z", "a")])
        datas.append([("z", "a"), ("y", "b"), ("x", "c"), ("w", "d")])
        datas.append([("z", "d"), ("y", "c"), ("x", "b"), ("w", "a")])
        # mixed order each index level with string
        datas.append([("z", "a"), ("x", "b"), ("y", "c"), ("w", "d")])
        datas.append([("z", "a"), ("y", "c"), ("x", "b"), ("w", "d")])

        # increasing / decreasing ordered each index level with integer
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(1, 500), (2, 400), (3, 300), (4, 200), (5, 100)])
        datas.append([(5, 100), (4, 200), (3, 300), (2, 400), (1, 500)])
        datas.append([(5, 500), (4, 400), (3, 300), (2, 200), (1, 100)])
        # mixed order each index level with integer
        datas.append([(1, 500), (3, 400), (2, 300), (4, 200), (5, 100)])
        datas.append([(1, 100), (2, 300), (3, 200), (4, 400), (5, 500)])

        # integer / negative mixed tests
        datas.append([("a", -500), ("b", -400), ("c", -300), ("d", -200), ("e", -100)])
        datas.append([("e", -500), ("d", -400), ("c", -300), ("b", -200), ("a", -100)])
        datas.append([(-5, "a"), (-4, "b"), (-3, "c"), (-2, "d"), (-1, "e")])
        datas.append([(-5, "e"), (-4, "d"), (-3, "c"), (-2, "b"), (-1, "a")])
        datas.append([(-5, "e"), (-3, "d"), (-2, "c"), (-4, "b"), (-1, "a")])
        datas.append([(-5, "e"), (-4, "c"), (-3, "b"), (-2, "d"), (-1, "a")])

        # boolean type tests
        datas.append([(True, True), (True, True)])
        datas.append([(True, True), (True, False)])
        datas.append([(True, False), (True, True)])
        datas.append([(False, True), (False, True)])
        datas.append([(False, True), (False, False)])
        datas.append([(False, False), (False, True)])
        datas.append([(True, True), (False, True)])
        datas.append([(True, True), (False, False)])
        datas.append([(True, False), (False, True)])
        datas.append([(False, True), (True, True)])
        datas.append([(False, True), (True, False)])
        datas.append([(False, False), (True, True)])

        # duplicated index value tests
        datas.append([("x", "d"), ("y", "c"), ("y", "b"), ("z", "a")])
        datas.append([("x", "d"), ("y", "b"), ("y", "c"), ("z", "a")])

        # more depth tests
        datas.append([("x", "d", "o"), ("y", "c", "p"), ("y", "c", "q"), ("z", "a", "r")])
        datas.append([("x", "d", "o"), ("y", "c", "q"), ("y", "c", "p"), ("z", "a", "r")])

        # None type tests (None type is treated as False from pandas >= 1.1.4)
        # Refer https://github.com/pandas-dev/pandas/issues/37220
        datas.append([(1, 100), (2, 200), (None, 300), (4, 400), (5, 500)])
        datas.append([(1, 100), (2, 200), (None, None), (4, 400), (5, 500)])
        datas.append([("x", "d"), ("y", "c"), ("y", None), ("z", "a")])
        datas.append([("x", "d"), ("y", "c"), ("y", "b"), (None, "a")])
        datas.append([("x", "d"), ("y", "b"), ("y", "c"), (None, "a")])
        datas.append([("x", "d", "o"), ("y", "c", "p"), ("y", "c", None), ("z", "a", "r")])

        for data in datas:
            with self.subTest(data=data):
                pmidx = pd.MultiIndex.from_tuples(data)
                psmidx = ps.from_pandas(pmidx)
                self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
                self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # datas below return different result depends on pandas version.
        # Because the behavior of handling null values is changed in pandas >= 1.1.4.
        # Since Koalas follows latest pandas, all of them should return `False`.
        datas = []
        datas.append([(1, 100), (2, 200), (3, None), (4, 400), (5, 500)])
        datas.append([(1, None), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (5, None)])
        datas.append([(False, None), (True, True)])
        datas.append([(None, False), (True, True)])
        datas.append([(False, False), (True, None)])
        datas.append([(False, False), (None, True)])
        datas.append([("x", "d"), ("y", None), ("y", None), ("z", "a")])
        datas.append([("x", "d", "o"), ("y", "c", None), ("y", "c", None), ("z", "a", "r")])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (None, 500)])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (None, None)])
        datas.append([(5, 100), (4, 200), (3, None), (2, 400), (1, 500)])
        datas.append([(5, None), (4, 200), (3, 300), (2, 400), (1, 500)])
        datas.append([(5, 100), (4, 200), (3, None), (2, 400), (1, 500)])
        datas.append([(5, 100), (4, 200), (3, 300), (2, 400), (1, None)])
        datas.append([(True, None), (True, True)])
        datas.append([(None, True), (True, True)])
        datas.append([(True, True), (None, True)])
        datas.append([(True, True), (True, None)])
        datas.append([(None, 100), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(None, None), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([("x", "d"), ("y", None), ("y", "c"), ("z", "a")])
        datas.append([("x", "d", "o"), ("y", "c", None), ("y", "c", "q"), ("z", "a", "r")])

        for data in datas:
            with self.subTest(data=data):
                pmidx = pd.MultiIndex.from_tuples(data)
                psmidx = ps.from_pandas(pmidx)
                self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
                self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # For [(-5, None), (-4, None), (-3, None), (-2, None), (-1, None)]
        psdf = ps.DataFrame({"a": [-5, -4, -3, -2, -1], "b": [1, 1, 1, 1, 1]})
        psdf["b"] = None
        psmidx = psdf.set_index(["a", "b"]).index
        pmidx = psmidx._to_pandas()
        self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
        self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # For [(None, "e"), (None, "c"), (None, "b"), (None, "d"), (None, "a")]
        psdf = ps.DataFrame({"a": [1, 1, 1, 1, 1], "b": ["e", "c", "b", "d", "a"]})
        psdf["a"] = None
        psmidx = psdf.set_index(["a", "b"]).index
        pmidx = psmidx._to_pandas()
        self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
        self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # For [(None, None), (None, None), (None, None), (None, None), (None, None)]
        psdf = ps.DataFrame({"a": [1, 1, 1, 1, 1], "b": [1, 1, 1, 1, 1]})
        psdf["a"] = None
        psdf["b"] = None
        psmidx = psdf.set_index(["a", "b"]).index
        pmidx = psmidx._to_pandas()
        self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
        self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # For [(None, None)]
        psdf = ps.DataFrame({"a": [1], "b": [1]})
        psdf["a"] = None
        psdf["b"] = None
        psmidx = psdf.set_index(["a", "b"]).index
        pmidx = psmidx._to_pandas()
        self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
        self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)


class MonotonicTests(
    MonotonicMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_monotonic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
