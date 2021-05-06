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

import os

import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils
from pyspark.testing.sqlutils import SQLTestUtils


class SparkFrameMethodsTest(PandasOnSparkTestCase, SQLTestUtils, TestUtils):
    def test_frame_apply_negative(self):
        with self.assertRaisesRegex(
            ValueError, "The output of the function.* pyspark.sql.DataFrame.*int"
        ):
            ps.range(10).spark.apply(lambda scol: 1)

    def test_hint(self):
        pdf1 = pd.DataFrame(
            {"lkey": ["foo", "bar", "baz", "foo"], "value": [1, 2, 3, 5]}
        ).set_index("lkey")
        pdf2 = pd.DataFrame(
            {"rkey": ["foo", "bar", "baz", "foo"], "value": [5, 6, 7, 8]}
        ).set_index("rkey")
        kdf1 = ps.from_pandas(pdf1)
        kdf2 = ps.from_pandas(pdf2)

        hints = ["broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]

        for hint in hints:
            self.assert_eq(
                pdf1.merge(pdf2, left_index=True, right_index=True).sort_values(
                    ["value_x", "value_y"]
                ),
                kdf1.merge(kdf2.spark.hint(hint), left_index=True, right_index=True).sort_values(
                    ["value_x", "value_y"]
                ),
                almost=True,
            )
            self.assert_eq(
                pdf1.merge(pdf2 + 1, left_index=True, right_index=True).sort_values(
                    ["value_x", "value_y"]
                ),
                kdf1.merge(
                    (kdf2 + 1).spark.hint(hint), left_index=True, right_index=True
                ).sort_values(["value_x", "value_y"]),
                almost=True,
            )

    def test_repartition(self):
        kdf = ps.DataFrame({"age": [5, 5, 2, 2], "name": ["Bob", "Bob", "Alice", "Alice"]})
        num_partitions = kdf.to_spark().rdd.getNumPartitions() + 1

        num_partitions += 1
        new_kdf = kdf.spark.repartition(num_partitions)
        self.assertEqual(new_kdf.to_spark().rdd.getNumPartitions(), num_partitions)
        self.assert_eq(kdf.sort_index(), new_kdf.sort_index())

        # Reserves Index
        kdf = kdf.set_index("age")
        num_partitions += 1
        new_kdf = kdf.spark.repartition(num_partitions)
        self.assertEqual(new_kdf.to_spark().rdd.getNumPartitions(), num_partitions)
        self.assert_eq(kdf.sort_index(), new_kdf.sort_index())

        # Reflects internal changes
        kdf = kdf.reset_index()
        kdf = kdf.set_index("name")
        kdf2 = kdf + 1
        num_partitions += 1
        self.assert_eq(kdf2.sort_index(), (kdf + 1).spark.repartition(num_partitions).sort_index())

        # Reserves MultiIndex
        kdf = ps.DataFrame({"a": ["a", "b", "c"]}, index=[[1, 2, 3], [4, 5, 6]])
        num_partitions = kdf.to_spark().rdd.getNumPartitions() + 1
        new_kdf = kdf.spark.repartition(num_partitions)
        self.assertEqual(new_kdf.to_spark().rdd.getNumPartitions(), num_partitions)
        self.assert_eq(kdf.sort_index(), new_kdf.sort_index())

    def test_coalesce(self):
        num_partitions = 10
        kdf = ps.DataFrame({"age": [5, 5, 2, 2], "name": ["Bob", "Bob", "Alice", "Alice"]})
        kdf = kdf.spark.repartition(num_partitions)

        num_partitions -= 1
        new_kdf = kdf.spark.coalesce(num_partitions)
        self.assertEqual(new_kdf.to_spark().rdd.getNumPartitions(), num_partitions)
        self.assert_eq(kdf.sort_index(), new_kdf.sort_index())

        # Reserves Index
        kdf = kdf.set_index("age")
        num_partitions -= 1
        new_kdf = kdf.spark.coalesce(num_partitions)
        self.assertEqual(new_kdf.to_spark().rdd.getNumPartitions(), num_partitions)
        self.assert_eq(kdf.sort_index(), new_kdf.sort_index())

        # Reflects internal changes
        kdf = kdf.reset_index()
        kdf = kdf.set_index("name")
        kdf2 = kdf + 1
        num_partitions -= 1
        self.assert_eq(kdf2.sort_index(), (kdf + 1).spark.coalesce(num_partitions).sort_index())

        # Reserves MultiIndex
        kdf = ps.DataFrame({"a": ["a", "b", "c"]}, index=[[1, 2, 3], [4, 5, 6]])
        num_partitions -= 1
        kdf = kdf.spark.repartition(num_partitions)

        num_partitions -= 1
        new_kdf = kdf.spark.coalesce(num_partitions)
        self.assertEqual(new_kdf.to_spark().rdd.getNumPartitions(), num_partitions)
        self.assert_eq(kdf.sort_index(), new_kdf.sort_index())

    def test_checkpoint(self):
        with self.temp_dir() as tmp:
            self.spark.sparkContext.setCheckpointDir(tmp)
            kdf = ps.DataFrame({"a": ["a", "b", "c"]})
            new_kdf = kdf.spark.checkpoint()
            self.assertIsNotNone(os.listdir(tmp))
            self.assert_eq(kdf, new_kdf)

    def test_local_checkpoint(self):
        kdf = ps.DataFrame({"a": ["a", "b", "c"]})
        new_kdf = kdf.spark.local_checkpoint()
        self.assert_eq(kdf, new_kdf)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_frame_spark import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
