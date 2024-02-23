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
from datetime import datetime

import pandas as pd

from pyspark import pandas as ps
from pyspark.loose_version import LooseVersion
from pyspark.testing.pandasutils import PandasOnSparkTestCase, SPARK_CONF_ARROW_ENABLED
from pyspark.testing.sqlutils import SQLTestUtils


class ConversionMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_index_from_series(self):
        pser = pd.Series([1, 2, 3], name="a", index=[10, 20, 30])
        psser = ps.from_pandas(pser)

        self.assert_eq(ps.Index(psser), pd.Index(pser))
        self.assert_eq(ps.Index(psser, dtype="float"), pd.Index(pser, dtype="float"))
        self.assert_eq(ps.Index(psser, name="x"), pd.Index(pser, name="x"))

        self.assert_eq(ps.Index(psser, dtype="int64"), pd.Index(pser, dtype="int64"))
        self.assert_eq(ps.Index(psser, dtype="float64"), pd.Index(pser, dtype="float64"))

        pser = pd.Series([datetime(2021, 3, 1), datetime(2021, 3, 2)], name="x", index=[10, 20])
        psser = ps.from_pandas(pser)

        self.assert_eq(ps.Index(psser), pd.Index(pser))
        self.assert_eq(ps.DatetimeIndex(psser), pd.DatetimeIndex(pser))

    def test_index_from_index(self):
        pidx = pd.Index([1, 2, 3], name="a")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(ps.Index(psidx), pd.Index(pidx))
        self.assert_eq(ps.Index(psidx, dtype="float"), pd.Index(pidx, dtype="float"))
        self.assert_eq(ps.Index(psidx, name="x"), pd.Index(pidx, name="x"))
        self.assert_eq(ps.Index(psidx, copy=True), pd.Index(pidx, copy=True))

        self.assert_eq(ps.Index(psidx, dtype="int64"), pd.Index(pidx, dtype="int64"))
        self.assert_eq(ps.Index(psidx, dtype="float64"), pd.Index(pidx, dtype="float64"))

        pidx = pd.DatetimeIndex(["2021-03-01", "2021-03-02"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(ps.Index(psidx), pd.Index(pidx))
        self.assert_eq(ps.DatetimeIndex(psidx), pd.DatetimeIndex(pidx))

    def test_multiindex_from_arrays(self):
        arrays = [["a", "a", "b", "b"], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays)
        psidx = ps.MultiIndex.from_arrays(arrays)

        self.assert_eq(pidx, psidx)

    def test_multiindex_from_tuples(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "blue")]
        pidx = pd.MultiIndex.from_tuples(tuples)
        psidx = ps.MultiIndex.from_tuples(tuples)

        self.assert_eq(pidx, psidx)

    def test_multiindex_from_product(self):
        iterables = [[0, 1, 2], ["green", "purple"]]
        pidx = pd.MultiIndex.from_product(iterables)
        psidx = ps.MultiIndex.from_product(iterables)

        self.assert_eq(pidx, psidx)

    def test_multi_index_from_index(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "blue")]
        pmidx = pd.Index(tuples)
        psmidx = ps.Index(tuples)

        self.assertTrue(isinstance(psmidx, ps.MultiIndex))
        self.assert_eq(pmidx, psmidx)

        # Specify the `names`
        # Specify the `names` while Index creating is no longer supported from pandas 2.0.0.
        if LooseVersion(pd.__version__) >= LooseVersion("2.0.0"):
            pmidx = pd.Index(tuples)
            pmidx.names = ["Hello", "Koalas"]
            psmidx = ps.Index(tuples)
            psmidx.names = ["Hello", "Koalas"]
        else:
            pmidx = pd.Index(tuples, names=["Hello", "Koalas"])
            psmidx = ps.Index(tuples, names=["Hello", "Koalas"])

        self.assertTrue(isinstance(psmidx, ps.MultiIndex))
        self.assert_eq(pmidx, psmidx)

    def test_multiindex_from_frame(self):
        pdf = pd.DataFrame(
            [["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]], columns=["a", "b"]
        )
        psdf = ps.from_pandas(pdf)
        pidx = pd.MultiIndex.from_frame(pdf)
        psidx = ps.MultiIndex.from_frame(psdf)

        self.assert_eq(pidx, psidx)

        # Specify `names`
        pidx = pd.MultiIndex.from_frame(pdf, names=["state", "observation"])
        psidx = ps.MultiIndex.from_frame(psdf, names=["state", "observation"])
        self.assert_eq(pidx, psidx)

        pidx = pd.MultiIndex.from_frame(pdf, names=("state", "observation"))
        psidx = ps.MultiIndex.from_frame(psdf, names=("state", "observation"))
        self.assert_eq(pidx, psidx)

        # MultiIndex columns
        pidx = pd.MultiIndex.from_tuples([("a", "w"), ("b", "x")])
        pdf.columns = pidx
        psdf = ps.from_pandas(pdf)

        pidx = pd.MultiIndex.from_frame(pdf)
        psidx = ps.MultiIndex.from_frame(psdf)

        self.assert_eq(pidx, psidx)

        # tuples for names
        pidx = pd.MultiIndex.from_frame(pdf, names=[("a", "w"), ("b", "x")])
        psidx = ps.MultiIndex.from_frame(psdf, names=[("a", "w"), ("b", "x")])

        self.assert_eq(pidx, psidx)

        err_msg = "Input must be a DataFrame"
        with self.assertRaisesRegex(TypeError, err_msg):
            ps.MultiIndex.from_frame({"a": [1, 2, 3], "b": [4, 5, 6]})

        self.assertRaises(TypeError, lambda: ps.MultiIndex.from_frame(psdf, names="ab"))

        # non-string names
        self.assert_eq(
            ps.MultiIndex.from_frame(psdf, names=[0, 1]),
            pd.MultiIndex.from_frame(pdf, names=[0, 1]),
        )
        self.assert_eq(
            ps.MultiIndex.from_frame(psdf, names=[("x", 0), ("y", 1)]),
            pd.MultiIndex.from_frame(pdf, names=[("x", 0), ("y", 1)]),
        )

        pdf = pd.DataFrame([["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(ps.MultiIndex.from_frame(psdf), pd.MultiIndex.from_frame(pdf))

    def test_to_series(self):
        pidx = self.pdf.index
        psidx = self.psdf.index

        self.assert_eq(psidx.to_series(), pidx.to_series())
        self.assert_eq(psidx.to_series(name="a"), pidx.to_series(name="a"))

        # With name
        pidx.name = "Koalas"
        psidx.name = "Koalas"
        self.assert_eq(psidx.to_series(), pidx.to_series())
        self.assert_eq(psidx.to_series(name=("x", "a")), pidx.to_series(name=("x", "a")))

        # With tupled name
        pidx.name = ("x", "a")
        psidx.name = ("x", "a")
        self.assert_eq(psidx.to_series(), pidx.to_series())
        self.assert_eq(psidx.to_series(name="a"), pidx.to_series(name="a"))

        self.assert_eq((psidx + 1).to_series(), (pidx + 1).to_series())

        pidx = self.pdf.set_index("b", append=True).index
        psidx = self.psdf.set_index("b", append=True).index

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            self.assert_eq(psidx.to_series(), pidx.to_series(), check_exact=False)
            self.assert_eq(psidx.to_series(name="a"), pidx.to_series(name="a"), check_exact=False)

        expected_error_message = "Series.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psidx.to_series(name=["x", "a"])

    def test_to_frame(self):
        pidx = self.pdf.index
        psidx = self.psdf.index

        self.assert_eq(psidx.to_frame(), pidx.to_frame())
        self.assert_eq(psidx.to_frame(index=False), pidx.to_frame(index=False))

        pidx.name = "a"
        psidx.name = "a"

        self.assert_eq(psidx.to_frame(), pidx.to_frame())
        self.assert_eq(psidx.to_frame(index=False), pidx.to_frame(index=False))

        self.assert_eq(psidx.to_frame(name="x"), pidx.to_frame(name="x"))
        self.assert_eq(psidx.to_frame(index=False, name="x"), pidx.to_frame(index=False, name="x"))

        self.assertRaises(TypeError, lambda: psidx.to_frame(name=["x"]))

        # non-string name
        self.assert_eq(psidx.to_frame(name=10), pidx.to_frame(name=10))
        self.assert_eq(psidx.to_frame(name=("x", 10)), pidx.to_frame(name=("x", 10)))

        pidx = self.pdf.set_index("b", append=True).index
        psidx = self.psdf.set_index("b", append=True).index

        self.assert_eq(psidx.to_frame(), pidx.to_frame())
        self.assert_eq(psidx.to_frame(index=False), pidx.to_frame(index=False))

        self.assert_eq(psidx.to_frame(name=["x", "y"]), pidx.to_frame(name=["x", "y"]))
        self.assert_eq(psidx.to_frame(name=("x", "y")), pidx.to_frame(name=("x", "y")))
        self.assert_eq(
            psidx.to_frame(index=False, name=["x", "y"]),
            pidx.to_frame(index=False, name=["x", "y"]),
        )

        self.assertRaises(TypeError, lambda: psidx.to_frame(name="x"))
        self.assertRaises(ValueError, lambda: psidx.to_frame(name=["x"]))

        # non-string names
        self.assert_eq(psidx.to_frame(name=[10, 20]), pidx.to_frame(name=[10, 20]))
        self.assert_eq(psidx.to_frame(name=("x", 10)), pidx.to_frame(name=("x", 10)))
        if LooseVersion(pd.__version__) < LooseVersion("1.5.0"):
            self.assert_eq(
                psidx.to_frame(name=[("x", 10), ("y", 20)]),
                pidx.to_frame(name=[("x", 10), ("y", 20)]),
            )
        else:
            # Since pandas 1.5.0, the result is changed as below:
            #      (x, 10)  (y, 20)
            #   b
            # 0 4        0        4
            # 1 5        1        5
            # 3 6        3        6
            # 5 3        5        3
            # 6 2        6        2
            # 8 1        8        1
            # 9 0        9        0
            #   0        9        0
            #   0        9        0
            #
            # The columns should be `Index([('x', 20), ('y', 20)], dtype='object')`,
            # but pandas API on Spark doesn't support such a way for creating Index.
            # So, we currently cannot follow the behavior of pandas.
            expected_result = ps.DataFrame(
                {("x", 10): [0, 1, 3, 5, 6, 8, 9, 9, 9], ("y", 20): [4, 5, 6, 3, 2, 1, 0, 0, 0]},
                index=ps.MultiIndex.from_tuples(
                    [(0, 4), (1, 5), (3, 6), (5, 3), (6, 2), (8, 1), (9, 0), (9, 0), (9, 0)],
                    names=[None, "b"],
                ),
            )
            self.assert_eq(psidx.to_frame(name=[("x", 10), ("y", 20)]), expected_result)

    def test_to_list(self):
        # Index
        pidx = pd.Index([1, 2, 3, 4, 5])
        psidx = ps.from_pandas(pidx)
        # MultiIndex
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "green")]
        pmidx = pd.MultiIndex.from_tuples(tuples)
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psidx.tolist(), pidx.tolist())
        self.assert_eq(psmidx.tolist(), pmidx.tolist())

    def test_to_numpy(self):
        pidx = pd.Index([1, 2, 3, 4])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.to_numpy(copy=True), psidx.to_numpy(copy=True))


class ConversionTests(
    ConversionMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_conversion import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
