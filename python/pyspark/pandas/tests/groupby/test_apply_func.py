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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupbyApplyFuncMixin:
    def test_apply(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("b").apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b").apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").apply(len).sort_index(),
            pdf.groupby("b").apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"]
            .apply(lambda x, y, z: x + x.min() + y * z, 10, z=20)
            .sort_index(),
            pdf.groupby("b")["a"].apply(lambda x, y, z: x + x.min() + y * z, 10, z=20).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")[["a"]].apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"])
            .apply(lambda x, y, z: x + x.min() + y + z, 1, z=2)
            .sort_index(),
            pdf.groupby(["a", "b"]).apply(lambda x, y, z: x + x.min() + y + z, 1, z=2).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].apply(lambda x: 1).sort_index(),
            pdf.groupby(["b"])["c"].apply(lambda x: 1).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].apply(len).sort_index(),
            pdf.groupby(["b"])["c"].apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5).apply(lambda x: x + x.min()).sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)["a"].apply(lambda x: x + x.min()).sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].apply(lambda x: x + x.min()).sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].apply(len).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].apply(len).sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.groupby(pdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
        )

        with self.assertRaisesRegex(TypeError, "int object is not callable"):
            psdf.groupby("b").apply(1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).apply(lambda x: 1).sort_index(),
            pdf.groupby(("x", "b")).apply(lambda x: 1).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("x", "b")).apply(len).sort_index(),
            pdf.groupby(("x", "b")).apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).apply(len).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).apply(len).sort_index(),
        )

    def test_apply_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_apply()

    def test_apply_with_type_hint(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)

        def add_max1(x) -> ps.DataFrame[int, int, int]:
            return x + x.min()

        # Type hints set the default column names, and we use default index for
        # pandas API on Spark. Here we ignore both diff.
        actual = psdf.groupby("b").apply(add_max1).sort_index()
        expected = pdf.groupby("b").apply(add_max1).sort_index()
        self.assert_eq(sorted(actual["c0"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c1"].to_numpy()), sorted(expected["b"].to_numpy()))
        self.assert_eq(sorted(actual["c2"].to_numpy()), sorted(expected["c"].to_numpy()))

        def add_max2(
            x,
        ) -> ps.DataFrame[slice("a", int), slice("b", int), slice("c", int)]:  # noqa: F405
            return x + x.min()

        actual = psdf.groupby("b").apply(add_max2).sort_index()
        expected = pdf.groupby("b").apply(add_max2).sort_index()
        self.assert_eq(sorted(actual["a"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c"].to_numpy()), sorted(expected["c"].to_numpy()))
        self.assert_eq(sorted(actual["c"].to_numpy()), sorted(expected["c"].to_numpy()))

    def test_apply_negative(self):
        def func(_) -> ps.Series[int]:
            return pd.Series([1])

        with self.assertRaisesRegex(TypeError, "Series as a return type hint at frame groupby"):
            ps.range(10).groupby("id").apply(func)

    def test_apply_with_new_dataframe(self):
        pdf = pd.DataFrame(
            {"timestamp": [0.0, 0.5, 1.0, 0.0, 0.5], "car_id": ["A", "A", "A", "B", "B"]}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
            pdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
        )

        self.assert_eq(
            psdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
            pdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
        )

        # dataframe with 1000+ records
        pdf = pd.DataFrame(
            {
                "timestamp": [0.0, 0.5, 1.0, 0.0, 0.5] * 300,
                "car_id": ["A", "A", "A", "B", "B"] * 300,
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
            pdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
        )

        self.assert_eq(
            psdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
            pdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
        )

    def test_apply_infer_schema_without_shortcut(self):
        # SPARK-39054: Ensure infer schema accuracy in GroupBy.apply
        with option_context("compute.shortcut_limit", 0):
            dfs = (
                {"timestamp": [0.0], "car_id": ["A"]},
                {"timestamp": [0.0, 0.0], "car_id": ["A", "A"]},
            )
            func = lambda _: pd.DataFrame({"column": [0.0]})  # noqa: E731
            for df in dfs:
                pdf = pd.DataFrame(df)
                psdf = ps.from_pandas(pdf)
                self.assert_eq(
                    psdf.groupby("car_id").apply(func).sort_index(),
                    pdf.groupby("car_id").apply(func).sort_index(),
                )

    def test_apply_with_new_dataframe_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_apply_with_new_dataframe()

    def test_apply_key_handling(self):
        pdf = pd.DataFrame(
            {"d": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0], "v": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("d").apply(sum).sort_index(), pdf.groupby("d").apply(sum).sort_index()
        )

        with ps.option_context("compute.shortcut_limit", 1):
            self.assert_eq(
                psdf.groupby("d").apply(sum).sort_index(), pdf.groupby("d").apply(sum).sort_index()
            )

    def test_apply_with_side_effect(self):
        pdf = pd.DataFrame(
            {"d": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0], "v": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}
        )
        psdf = ps.from_pandas(pdf)

        acc = ps.utils.default_session().sparkContext.accumulator(0)

        def sum_with_acc_frame(x) -> ps.DataFrame[np.float64, np.float64]:
            nonlocal acc
            acc += 1
            return np.sum(x)

        actual = psdf.groupby("d").apply(sum_with_acc_frame)
        actual.columns = ["d", "v"]
        self.assert_eq(
            actual._to_pandas().sort_index(),
            pdf.groupby("d").apply(sum).sort_index().reset_index(drop=True),
        )
        self.assert_eq(acc.value, 2)

        def sum_with_acc_series(x) -> np.float64:
            nonlocal acc
            acc += 1
            return np.sum(x)

        self.assert_eq(
            psdf.groupby("d")["v"].apply(sum_with_acc_series)._to_pandas().sort_index(),
            pdf.groupby("d")["v"].apply(sum).sort_index().reset_index(drop=True),
        )
        self.assert_eq(acc.value, 4)

    def test_apply_return_series(self):
        # SPARK-36907: Fix DataFrameGroupBy.apply without shortcut.
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").apply(lambda x: x.iloc[0]).sort_index(),
            pdf.groupby("b").apply(lambda x: x.iloc[0]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").apply(lambda x: x["a"]).sort_index(),
            pdf.groupby("b").apply(lambda x: x["a"]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b", "c"]).apply(lambda x: x.iloc[0]).sort_index(),
            pdf.groupby(["b", "c"]).apply(lambda x: x.iloc[0]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b", "c"]).apply(lambda x: x["a"]).sort_index(),
            pdf.groupby(["b", "c"]).apply(lambda x: x["a"]).sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).apply(lambda x: x.iloc[0]).sort_index(),
            pdf.groupby(("x", "b")).apply(lambda x: x.iloc[0]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("x", "b")).apply(lambda x: x[("x", "a")]).sort_index(),
            pdf.groupby(("x", "b")).apply(lambda x: x[("x", "a")]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "b"), ("y", "c")]).apply(lambda x: x.iloc[0]).sort_index(),
            pdf.groupby([("x", "b"), ("y", "c")]).apply(lambda x: x.iloc[0]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "b"), ("y", "c")]).apply(lambda x: x[("x", "a")]).sort_index(),
            pdf.groupby([("x", "b"), ("y", "c")]).apply(lambda x: x[("x", "a")]).sort_index(),
        )

    def test_apply_return_series_without_shortcut(self):
        # SPARK-36907: Fix DataFrameGroupBy.apply without shortcut.
        with ps.option_context("compute.shortcut_limit", 2):
            self.test_apply_return_series()

    def test_apply_explicitly_infer(self):
        # SPARK-39317
        from pyspark.pandas.utils import SPARK_CONF_ARROW_ENABLED

        def plus_min(x):
            return x + x.min()

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            df = ps.DataFrame({"A": ["a", "a", "b"], "B": [1, 2, 3]}, columns=["A", "B"])
            g = df.groupby("A")
            g.apply(plus_min).sort_index()

    def test_transform(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("b").transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b").transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")["a"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")[["a"]].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(["a", "b"]).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(["b"])["c"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)["a"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.groupby(pdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
        )
        with self.assertRaisesRegex(TypeError, "str object is not callable"):
            psdf.groupby("a").transform("sum")

        def udf(col) -> int:
            return col + 10

        with self.assertRaisesRegex(
            TypeError,
            "Expected the return type of this function to be of Series type, "
            "but found type ScalarType\\[LongType\\(\\)\\]",
        ):
            psdf.groupby("a").transform(udf)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(("x", "b")).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).transform(lambda x: x + x.min()).sort_index(),
        )

    def test_transform_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_transform()

    def test_filter(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby("b").filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"].filter(lambda x: any(x == 2)).sort_index(),
            pdf.groupby("b")["a"].filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby("b")[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(["a", "b"]).filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5).filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5).filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5)["a"].filter(lambda x: any(x == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5)["a"].filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5)[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5)[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.rename().groupby(pdf.b).filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.groupby(pdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
        )

        with self.assertRaisesRegex(TypeError, "int object is not callable"):
            psdf.groupby("b").filter(1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).filter(lambda x: any(x[("x", "a")] == 2)).sort_index(),
            pdf.groupby(("x", "b")).filter(lambda x: any(x[("x", "a")] == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")])
            .filter(lambda x: any(x[("x", "a")] == 2))
            .sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")])
            .filter(lambda x: any(x[("x", "a")] == 2))
            .sort_index(),
        )


class GroupbyApplyFuncTests(
    GroupbyApplyFuncMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_apply_func import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
