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
import time
import unittest

from pyspark.sql.functions import col
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pyarrow,
    pyarrow_requirement_message,
)


if have_pyarrow:
    import pyarrow as pa
    import pyarrow.compute as pac


@unittest.skipIf(
    not have_pyarrow,
    pyarrow_requirement_message,  # type: ignore[arg-type]
)
class CogroupedMapInArrowTests(ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.sc.environment["TZ"] = tz
        cls.spark.conf.set("spark.sql.session.timeZone", tz)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        ReusedSQLTestCase.tearDownClass()

    def test_apply_in_arrow(self):
        def func1(left, right):
            assert isinstance(left, pa.Table)
            assert isinstance(right, pa.Table)
            assert left.schema.names == ["id", "value"]
            assert right.schema.names == ["id", "value"]

            left_ids = left.to_pydict()["id"]
            right_ids = right.to_pydict()["id"]
            result = {
                "metric": ["min", "max", "len", "sum"],
                "left": [min(left_ids), max(left_ids), len(left_ids), sum(left_ids)],
                "right": [min(right_ids), max(right_ids), len(right_ids), sum(right_ids)],
            }
            return pa.Table.from_pydict(result)

        def func2(key, left, right):
            assert isinstance(key, tuple)
            assert all(isinstance(scalar, pa.Scalar) for scalar in key)
            assert isinstance(left, pa.Table)
            assert isinstance(right, pa.Table)
            assert left.schema.names == ["id", "value"]
            assert right.schema.names == ["id", "value"]
            assert all(
                (pac.divide(k, pa.scalar(4)).cast(pa.int32()),) == key for k in left.column("id")
            )
            assert all(
                (pac.divide(k, pa.scalar(4)).cast(pa.int32()),) == key for k in right.column("id")
            )

            left_ids = left.to_pydict()["id"]
            right_ids = right.to_pydict()["id"]
            result = {
                "key": [",".join(str(k.as_py()) for k in key)] * 4,
                "metric": ["min", "max", "len", "sum"],
                "left": [min(left_ids), max(left_ids), len(left_ids), sum(left_ids)],
                "right": [min(right_ids), max(right_ids), len(right_ids), sum(right_ids)],
            }
            return pa.Table.from_pydict(result)

        def func3(key, left, right):
            return func2(
                tuple(pa.scalar(k) for k in key),
                pa.Table.from_pandas(left),
                pa.Table.from_pandas(right),
            ).to_pandas()

        left_df = self.spark.range(0, 10, 2, 3).withColumn("value", col("id") * 10)
        right_df = self.spark.range(0, 10, 3, 3).withColumn("value", col("id") * 10)

        grouped_left_df = left_df.groupBy((col("id") / 4).cast("int"))
        grouped_right_df = right_df.groupBy((col("id") / 4).cast("int"))
        cogrouped_df = grouped_left_df.cogroup(grouped_right_df)
        schema = "metric string, left long, right long"
        expected = cogrouped_df.applyInPandas(func3, "key string, " + schema)

        actual = cogrouped_df.applyInArrow(func1, schema).collect()
        self.assertEqual(actual, expected.drop("key").collect())

        actual2 = cogrouped_df.applyInArrow(func2, "key string, " + schema).collect()
        self.assertEqual(actual2, expected.collect())


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_cogrouped_map import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
