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
import sys
import unittest
import inspect

from pyspark.sql.functions import mean, lit
from pyspark.testing.sqlutils import ReusedSQLTestCase, \
    have_pandas, have_pyarrow, pandas_requirement_message, \
    pyarrow_requirement_message
from pyspark.sql.pandas.typehints import infer_eval_type
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType

if have_pandas:
    import pandas as pd
    from pandas.util.testing import assert_frame_equal

python_requirement_message = "pandas UDF with type hints are supported with Python 3.6+."


@unittest.skipIf(
    not have_pandas or not have_pyarrow or sys.version_info[:2] < (3, 6),
    pandas_requirement_message or pyarrow_requirement_message or python_requirement_message)
class PandasUDFTypeHintsTests(ReusedSQLTestCase):
    # Note that, we should remove `exec` once we drop Python 2 in this class.

    def setUp(self):
        self.local = {'pd': pd}

    def test_type_annotation_scalar(self):
        exec(
            "def func(col: pd.Series) -> pd.Series: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR)

        exec(
            "def func(col: pd.DataFrame, col1: pd.Series) -> pd.DataFrame: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR)

        exec(
            "def func(col: pd.DataFrame, *args: pd.Series) -> pd.Series: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR)

        exec(
            "def func(col: pd.Series, *args: pd.Series, **kwargs: pd.DataFrame) -> pd.Series:\n"
            "    pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR)

        exec(
            "def func(col: pd.Series, *, col2: pd.DataFrame) -> pd.DataFrame:\n"
            "    pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR)

        exec(
            "from typing import Union\n"
            "def func(col: Union[pd.Series, pd.DataFrame], *, col2: pd.DataFrame) -> pd.Series:\n"
            "    pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR)

    def test_type_annotation_scalar_iter(self):
        exec(
            "from typing import Iterator\n"
            "def func(iter: Iterator[pd.Series]) -> Iterator[pd.Series]: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR_ITER)

        exec(
            "from typing import Iterator, Tuple\n"
            "def func(iter: Iterator[Tuple[pd.DataFrame, pd.Series]]) -> Iterator[pd.DataFrame]:\n"
            "    pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR_ITER)

        exec(
            "from typing import Iterator, Tuple\n"
            "def func(iter: Iterator[Tuple[pd.DataFrame, ...]]) -> Iterator[pd.Series]: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR_ITER)

        exec(
            "from typing import Iterator, Tuple, Union\n"
            "def func(iter: Iterator[Tuple[Union[pd.DataFrame, pd.Series], ...]])"
            " -> Iterator[pd.Series]: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.SCALAR_ITER)

    def test_type_annotation_group_agg(self):
        exec(
            "def func(col: pd.Series) -> str: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.GROUPED_AGG)

        exec(
            "def func(col: pd.DataFrame, col1: pd.Series) -> int: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.GROUPED_AGG)

        exec(
            "from pyspark.sql import Row\n"
            "def func(col: pd.DataFrame, *args: pd.Series) -> Row: pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.GROUPED_AGG)

        exec(
            "def func(col: pd.Series, *args: pd.Series, **kwargs: pd.DataFrame) -> str:\n"
            "    pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.GROUPED_AGG)

        exec(
            "def func(col: pd.Series, *, col2: pd.DataFrame) -> float:\n"
            "    pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.GROUPED_AGG)

        exec(
            "from typing import Union\n"
            "def func(col: Union[pd.Series, pd.DataFrame], *, col2: pd.DataFrame) -> float:\n"
            "    pass",
            self.local)
        self.assertEqual(
            infer_eval_type(inspect.signature(self.local['func'])), PandasUDFType.GROUPED_AGG)

    def test_type_annotation_negative(self):
        exec(
            "def func(col: str) -> pd.Series: pass",
            self.local)
        self.assertRaisesRegex(
            NotImplementedError,
            "Unsupported signature.*str",
            infer_eval_type, inspect.signature(self.local['func']))

        exec(
            "def func(col: pd.DataFrame, col1: int) -> pd.DataFrame: pass",
            self.local)
        self.assertRaisesRegex(
            NotImplementedError,
            "Unsupported signature.*int",
            infer_eval_type, inspect.signature(self.local['func']))

        exec(
            "from typing import Union\n"
            "def func(col: Union[pd.DataFrame, str], col1: int) -> pd.DataFrame: pass",
            self.local)
        self.assertRaisesRegex(
            NotImplementedError,
            "Unsupported signature.*str",
            infer_eval_type, inspect.signature(self.local['func']))

        exec(
            "from typing import Tuple\n"
            "def func(col: pd.Series) -> Tuple[pd.DataFrame]: pass",
            self.local)
        self.assertRaisesRegex(
            NotImplementedError,
            "Unsupported signature.*Tuple",
            infer_eval_type, inspect.signature(self.local['func']))

        exec(
            "def func(col, *args: pd.Series) -> pd.Series: pass",
            self.local)
        self.assertRaisesRegex(
            ValueError,
            "should be specified.*Series",
            infer_eval_type, inspect.signature(self.local['func']))

        exec(
            "def func(col: pd.Series, *args: pd.Series, **kwargs: pd.DataFrame):\n"
            "    pass",
            self.local)
        self.assertRaisesRegex(
            ValueError,
            "should be specified.*Series",
            infer_eval_type, inspect.signature(self.local['func']))

        exec(
            "def func(col: pd.Series, *, col2) -> pd.DataFrame:\n"
            "    pass",
            self.local)
        self.assertRaisesRegex(
            ValueError,
            "should be specified.*Series",
            infer_eval_type, inspect.signature(self.local['func']))

    def test_scalar_udf_type_hint(self):
        df = self.spark.range(10).selectExpr("id", "id as v")

        exec(
            "import typing\n"
            "def plus_one(v: typing.Union[pd.Series, pd.DataFrame]) -> pd.Series:\n"
            "    return v + 1",
            self.local)

        plus_one = pandas_udf("long")(self.local["plus_one"])

        actual = df.select(plus_one(df.v).alias("plus_one"))
        expected = df.selectExpr("(v + 1) as plus_one")
        assert_frame_equal(expected.toPandas(), actual.toPandas())

    def test_scalar_iter_udf_type_hint(self):
        df = self.spark.range(10).selectExpr("id", "id as v")

        exec(
            "import typing\n"
            "def plus_one(itr: typing.Iterator[pd.Series]) -> typing.Iterator[pd.Series]:\n"
            "    for s in itr:\n"
            "        yield s + 1",
            self.local)

        plus_one = pandas_udf("long")(self.local["plus_one"])

        actual = df.select(plus_one(df.v).alias("plus_one"))
        expected = df.selectExpr("(v + 1) as plus_one")
        assert_frame_equal(expected.toPandas(), actual.toPandas())

    def test_group_agg_udf_type_hint(self):
        df = self.spark.range(10).selectExpr("id", "id as v")
        exec(
            "import numpy as np\n"
            "def weighted_mean(v: pd.Series, w: pd.Series) -> float:\n"
            "    return np.average(v, weights=w)",
            self.local)

        weighted_mean = pandas_udf("double")(self.local["weighted_mean"])

        actual = df.groupby('id').agg(weighted_mean(df.v, lit(1.0))).sort('id')
        expected = df.groupby('id').agg(mean(df.v).alias('weighted_mean(v, 1.0)')).sort('id')
        assert_frame_equal(expected.toPandas(), actual.toPandas())

    def test_ignore_type_hint_in_group_apply_in_pandas(self):
        df = self.spark.range(10)
        exec(
            "def pandas_plus_one(v: pd.DataFrame) -> pd.DataFrame:\n"
            "    return v + 1",
            self.local)

        pandas_plus_one = self.local["pandas_plus_one"]

        actual = df.groupby('id').applyInPandas(pandas_plus_one, schema=df.schema).sort('id')
        expected = df.selectExpr("id + 1 as id")
        assert_frame_equal(expected.toPandas(), actual.toPandas())

    def test_ignore_type_hint_in_cogroup_apply_in_pandas(self):
        df = self.spark.range(10)
        exec(
            "def pandas_plus_one(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:\n"
            "    return left + 1",
            self.local)

        pandas_plus_one = self.local["pandas_plus_one"]

        actual = df.groupby('id').cogroup(
            self.spark.range(10).groupby("id")
        ).applyInPandas(pandas_plus_one, schema=df.schema).sort('id')
        expected = df.selectExpr("id + 1 as id")
        assert_frame_equal(expected.toPandas(), actual.toPandas())

    def test_ignore_type_hint_in_map_in_pandas(self):
        df = self.spark.range(10)
        exec(
            "from typing import Iterator\n"
            "def pandas_plus_one(iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:\n"
            "    return map(lambda v: v + 1, iter)",
            self.local)

        pandas_plus_one = self.local["pandas_plus_one"]

        actual = df.mapInPandas(pandas_plus_one, schema=df.schema)
        expected = df.selectExpr("id + 1 as id")
        assert_frame_equal(expected.toPandas(), actual.toPandas())


if __name__ == "__main__":
    from pyspark.sql.tests.test_pandas_udf_typehints import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
