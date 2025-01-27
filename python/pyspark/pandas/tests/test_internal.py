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

from pyspark.sql.types import LongType, StructType, StructField
from pyspark.pandas.internal import (
    InternalFrame,
    SPARK_DEFAULT_INDEX_NAME,
    SPARK_INDEX_NAME_FORMAT,
)
from pyspark.pandas.utils import spark_column_equals
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class InternalFrameTestsMixin:
    def test_from_pandas(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        internal = InternalFrame.from_pandas(pdf)
        sdf = internal.spark_frame

        self.assert_eq(internal.index_spark_column_names, [SPARK_DEFAULT_INDEX_NAME])
        self.assert_eq(internal.index_names, [None])
        self.assert_eq(internal.column_labels, [("a",), ("b",)])
        self.assert_eq(internal.data_spark_column_names, ["a", "b"])
        self.assertTrue(spark_column_equals(internal.spark_column_for(("a",)), sdf["a"]))
        self.assertTrue(spark_column_equals(internal.spark_column_for(("b",)), sdf["b"]))

        self.assert_eq(internal.to_pandas_frame, pdf)

        # non-string column name
        pdf1 = pd.DataFrame({0: [1, 2, 3], 1: [4, 5, 6]})

        internal = InternalFrame.from_pandas(pdf1)
        sdf = internal.spark_frame

        self.assert_eq(internal.index_spark_column_names, [SPARK_DEFAULT_INDEX_NAME])
        self.assert_eq(internal.index_names, [None])
        self.assert_eq(internal.column_labels, [(0,), (1,)])
        self.assert_eq(internal.data_spark_column_names, ["0", "1"])
        self.assertTrue(spark_column_equals(internal.spark_column_for((0,)), sdf["0"]))
        self.assertTrue(spark_column_equals(internal.spark_column_for((1,)), sdf["1"]))

        self.assert_eq(internal.to_pandas_frame, pdf1)

        # categorical column
        pdf2 = pd.DataFrame({0: [1, 2, 3], 1: pd.Categorical([4, 5, 6])})
        internal = InternalFrame.from_pandas(pdf2)
        sdf = internal.spark_frame

        self.assert_eq(internal.index_spark_column_names, [SPARK_DEFAULT_INDEX_NAME])
        self.assert_eq(internal.index_names, [None])
        self.assert_eq(internal.column_labels, [(0,), (1,)])
        self.assert_eq(internal.data_spark_column_names, ["0", "1"])
        self.assertTrue(spark_column_equals(internal.spark_column_for((0,)), sdf["0"]))
        self.assertTrue(spark_column_equals(internal.spark_column_for((1,)), sdf["1"]))

        self.assert_eq(internal.to_pandas_frame, pdf2)

        # multi-index
        pdf.set_index("a", append=True, inplace=True)

        internal = InternalFrame.from_pandas(pdf)
        sdf = internal.spark_frame

        self.assert_eq(
            internal.index_spark_column_names,
            [SPARK_INDEX_NAME_FORMAT(0), SPARK_INDEX_NAME_FORMAT(1)],
        )
        self.assert_eq(internal.index_names, [None, ("a",)])
        self.assert_eq(internal.column_labels, [("b",)])
        self.assert_eq(internal.data_spark_column_names, ["b"])
        self.assertTrue(spark_column_equals(internal.spark_column_for(("b",)), sdf["b"]))

        self.assert_eq(internal.to_pandas_frame, pdf)

        # multi-index columns
        pdf.columns = pd.MultiIndex.from_tuples([("x", "b")])

        internal = InternalFrame.from_pandas(pdf)
        sdf = internal.spark_frame

        self.assert_eq(
            internal.index_spark_column_names,
            [SPARK_INDEX_NAME_FORMAT(0), SPARK_INDEX_NAME_FORMAT(1)],
        )
        self.assert_eq(internal.index_names, [None, ("a",)])
        self.assert_eq(internal.column_labels, [("x", "b")])
        self.assert_eq(internal.data_spark_column_names, ["(x, b)"])
        self.assertTrue(spark_column_equals(internal.spark_column_for(("x", "b")), sdf["(x, b)"]))

        self.assert_eq(internal.to_pandas_frame, pdf)

    def test_attach_distributed_column(self):
        sdf1 = self.spark.range(10)
        self.assert_eq(
            InternalFrame.attach_distributed_sequence_column(sdf1, "index").schema,
            StructType(
                [
                    StructField("index", LongType(), False),
                    StructField("id", LongType(), False),
                ]
            ),
        )

        # zero columns
        sdf2 = self.spark.range(10).select()
        self.assert_eq(
            InternalFrame.attach_distributed_sequence_column(sdf2, "index").schema,
            StructType([StructField("index", LongType(), False)]),
        )

        # empty dataframe, zero columns
        sdf3 = self.spark.range(10).where("id < 0").select()
        self.assert_eq(
            InternalFrame.attach_distributed_sequence_column(sdf3, "index").schema,
            StructType([StructField("index", LongType(), False)]),
        )


class InternalFrameTests(InternalFrameTestsMixin, PandasOnSparkTestCase, SQLTestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_internal import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
