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

import pyspark.pandas as ps
from pyspark.testing.pandasutils import ComparisonTestBase, TestUtils, SPARK_CONF_ARROW_ENABLED


class IndexesSlowTestsMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

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

    def test_union(self):
        # Index
        pidx1 = pd.Index([1, 2, 3, 4])
        pidx2 = pd.Index([3, 4, 5, 6])
        pidx3 = pd.Index([7.0, 8.0, 9.0, 10.0])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        psidx3 = ps.from_pandas(pidx3)

        self.assert_eq(psidx1.union(psidx2), pidx1.union(pidx2))
        self.assert_eq(psidx2.union(psidx1), pidx2.union(pidx1))
        self.assert_eq(psidx1.union(psidx3), pidx1.union(pidx3))

        self.assert_eq(psidx1.union([3, 4, 5, 6]), pidx1.union([3, 4, 5, 6]), almost=True)
        self.assert_eq(psidx2.union([1, 2, 3, 4]), pidx2.union([1, 2, 3, 4]), almost=True)
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 5, 6])), pidx1.union(pd.Series([3, 4, 5, 6])), almost=True
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4])), pidx2.union(pd.Series([1, 2, 3, 4])), almost=True
        )

        # Testing if the result is correct after sort=False.
        self.assert_eq(
            psidx1.union(psidx2, sort=False).sort_values(),
            pidx1.union(pidx2, sort=False).sort_values(),
        )
        self.assert_eq(
            psidx2.union(psidx1, sort=False).sort_values(),
            pidx2.union(pidx1, sort=False).sort_values(),
        )
        self.assert_eq(
            psidx1.union([3, 4, 5, 6], sort=False).sort_values(),
            pidx1.union([3, 4, 5, 6], sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx2.union([1, 2, 3, 4], sort=False).sort_values(),
            pidx2.union([1, 2, 3, 4], sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 5, 6]), sort=False).sort_values(),
            pidx1.union(pd.Series([3, 4, 5, 6]), sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4]), sort=False).sort_values(),
            pidx2.union(pd.Series([1, 2, 3, 4]), sort=False).sort_values(),
            almost=True,
        )

        pidx1 = pd.Index([1, 2, 3, 4, 3, 4, 3, 4])
        pidx2 = pd.Index([3, 4, 3, 4, 5, 6])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(psidx1.union(psidx2), pidx1.union(pidx2))
        self.assert_eq(
            psidx1.union([3, 4, 3, 3, 5, 6]), pidx1.union([3, 4, 3, 4, 5, 6]), almost=True
        )
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 3, 3, 5, 6])),
            pidx1.union(pd.Series([3, 4, 3, 4, 5, 6])),
            almost=True,
        )

        # Manually create the expected result here since there is a bug in Index.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.Index([1, 2, 3, 3, 3, 4, 4, 4, 5, 6])
        self.assert_eq(psidx2.union(psidx1), expected)
        self.assert_eq(
            psidx2.union([1, 2, 3, 4, 3, 4, 3, 4]),
            expected,
            almost=True,
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4, 3, 4, 3, 4])),
            expected,
            almost=True,
        )

        # MultiIndex
        pmidx1 = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")])
        pmidx2 = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")])
        pmidx3 = pd.MultiIndex.from_tuples([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)])
        pmidx4 = pd.MultiIndex.from_tuples([(1, 3), (1, 4), (1, 5), (1, 6)])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        psmidx3 = ps.from_pandas(pmidx3)
        psmidx4 = ps.from_pandas(pmidx4)

        # Manually create the expected result here since there is a bug in MultiIndex.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "a"), ("x", "b"), ("x", "b"), ("x", "c"), ("x", "d")]
        )
        self.assert_eq(psmidx1.union(psmidx2), expected)
        self.assert_eq(psmidx2.union(psmidx1), expected)
        self.assert_eq(
            psmidx1.union([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")]),
            expected,
        )
        self.assert_eq(
            psmidx2.union([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")]),
            expected,
        )

        expected = pd.MultiIndex.from_tuples(
            [(1, 1), (1, 2), (1, 3), (1, 3), (1, 4), (1, 4), (1, 5), (1, 6)]
        )
        self.assert_eq(psmidx3.union(psmidx4), expected)
        self.assert_eq(psmidx4.union(psmidx3), expected)
        self.assert_eq(
            psmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)]),
            expected,
        )
        self.assert_eq(
            psmidx4.union([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)]),
            expected,
        )

        # Testing if the result is correct after sort=False.
        # Manually create the expected result here since there is a bug in MultiIndex.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "a"), ("x", "b"), ("x", "b"), ("x", "c"), ("x", "d")]
        )
        self.assert_eq(psmidx1.union(psmidx2, sort=False).sort_values(), expected)
        self.assert_eq(psmidx2.union(psmidx1, sort=False).sort_values(), expected)
        self.assert_eq(
            psmidx1.union(
                [("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")], sort=False
            ).sort_values(),
            expected,
        )
        self.assert_eq(
            psmidx2.union(
                [("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")], sort=False
            ).sort_values(),
            expected,
        )

        expected = pd.MultiIndex.from_tuples(
            [(1, 1), (1, 2), (1, 3), (1, 3), (1, 4), (1, 4), (1, 5), (1, 6)]
        )
        self.assert_eq(psmidx3.union(psmidx4, sort=False).sort_values(), expected)
        self.assert_eq(psmidx4.union(psmidx3, sort=False).sort_values(), expected)
        self.assert_eq(
            psmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)], sort=False).sort_values(), expected
        )
        self.assert_eq(
            psmidx4.union(
                [(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)], sort=False
            ).sort_values(),
            expected,
        )

        self.assertRaises(NotImplementedError, lambda: psidx1.union(psmidx1))
        self.assertRaises(TypeError, lambda: psmidx1.union(psidx1))
        self.assertRaises(TypeError, lambda: psmidx1.union(["x", "a"]))
        self.assertRaises(ValueError, lambda: psidx1.union(ps.range(2)))

    def test_intersection(self):
        pidx = pd.Index([1, 2, 3, 4], name="Koalas")
        psidx = ps.from_pandas(pidx)

        # other = Index
        pidx_other = pd.Index([3, 4, 5, 6], name="Koalas")
        psidx_other = ps.from_pandas(pidx_other)
        self.assert_eq(pidx.intersection(pidx_other), psidx.intersection(psidx_other).sort_values())
        self.assert_eq(
            (pidx + 1).intersection(pidx_other), (psidx + 1).intersection(psidx_other).sort_values()
        )

        pidx_other_different_name = pd.Index([3, 4, 5, 6], name="Databricks")
        psidx_other_different_name = ps.from_pandas(pidx_other_different_name)
        self.assert_eq(
            pidx.intersection(pidx_other_different_name),
            psidx.intersection(psidx_other_different_name).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pidx_other_different_name),
            (psidx + 1).intersection(psidx_other_different_name).sort_values(),
        )

        pidx_other_from_frame = pd.DataFrame({"a": [3, 4, 5, 6]}).set_index("a").index
        psidx_other_from_frame = ps.from_pandas(pidx_other_from_frame)
        self.assert_eq(
            pidx.intersection(pidx_other_from_frame),
            psidx.intersection(psidx_other_from_frame).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pidx_other_from_frame),
            (psidx + 1).intersection(psidx_other_from_frame).sort_values(),
        )

        # other = MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(
            pidx.intersection(pmidx), psidx.intersection(psmidx).sort_values(), almost=True
        )
        self.assert_eq(
            (pidx + 1).intersection(pmidx),
            (psidx + 1).intersection(psmidx).sort_values(),
            almost=True,
        )

        # other = Series
        pser = pd.Series([3, 4, 5, 6])
        psser = ps.from_pandas(pser)
        self.assert_eq(pidx.intersection(pser), psidx.intersection(psser).sort_values())
        self.assert_eq((pidx + 1).intersection(pser), (psidx + 1).intersection(psser).sort_values())

        pser_different_name = pd.Series([3, 4, 5, 6], name="Databricks")
        psser_different_name = ps.from_pandas(pser_different_name)
        self.assert_eq(
            pidx.intersection(pser_different_name),
            psidx.intersection(psser_different_name).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pser_different_name),
            (psidx + 1).intersection(psser_different_name).sort_values(),
        )

        others = ([3, 4, 5, 6], (3, 4, 5, 6), {3: None, 4: None, 5: None, 6: None})
        for other in others:
            self.assert_eq(pidx.intersection(other), psidx.intersection(other).sort_values())
            self.assert_eq(
                (pidx + 1).intersection(other), (psidx + 1).intersection(other).sort_values()
            )

        # MultiIndex / other = Index
        self.assert_eq(
            pmidx.intersection(pidx), psmidx.intersection(psidx).sort_values(), almost=True
        )
        self.assert_eq(
            pmidx.intersection(pidx_other_from_frame),
            psmidx.intersection(psidx_other_from_frame).sort_values(),
            almost=True,
        )

        # MultiIndex / other = MultiIndex
        pmidx_other = pd.MultiIndex.from_tuples([("c", "z"), ("d", "w")])
        psmidx_other = ps.from_pandas(pmidx_other)
        self.assert_eq(
            pmidx.intersection(pmidx_other), psmidx.intersection(psmidx_other).sort_values()
        )

        # MultiIndex / other = list
        other = [("c", "z"), ("d", "w")]
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex / other = tuple
        other = (("c", "z"), ("d", "w"))
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex / other = dict
        other = {("c", "z"): None, ("d", "w"): None}
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex with different names.
        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")], names=["X", "Y"])
        pmidx2 = pd.MultiIndex.from_tuples([("c", "z"), ("d", "w")], names=["A", "B"])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.intersection(pmidx2), psmidx1.intersection(psmidx2).sort_values())

        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx.intersection(4)
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection(4)
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection(ps.Series([3, 4, 5, 6]))
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection([("c", "z"), ["d", "w"]])
        with self.assertRaisesRegex(ValueError, "Index data must be 1-dimensional"):
            psidx.intersection(ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))
        with self.assertRaisesRegex(ValueError, "Index data must be 1-dimensional"):
            psmidx.intersection(ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))
        # other = list of tuple
        with self.assertRaisesRegex(ValueError, "Names should be list-like for a MultiIndex"):
            psidx.intersection([(1, 2), (3, 4)])


class IndexesSlowTests(IndexesSlowTestsMixin, ComparisonTestBase, TestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_base_slow import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
