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
class GroupedMapInArrowTests(ReusedSQLTestCase):
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
        def func1(group):
            assert isinstance(group, pa.Table)
            assert group.schema.names == ["id", "value"]
            return group

        def func2(key, group):
            assert isinstance(key, tuple)
            assert all(isinstance(scalar, pa.Scalar) for scalar in key)
            assert isinstance(group, pa.Table)
            assert group.schema.names == ["id", "value"]
            assert all(
                (pac.divide(k, pa.scalar(4)).cast(pa.int32()),) == key for k in group.column("id")
            )
            return group

        df = self.spark.range(10).withColumn("value", col("id") * 10)
        grouped_df = df.groupBy((col("id") / 4).cast("int"))
        expected = df.collect()

        actual = grouped_df.applyInArrow(func1, "id long, value long").collect()
        self.assertEqual(actual, expected)

        actual2 = grouped_df.applyInArrow(func2, "id long, value long").collect()
        self.assertEqual(actual2, expected)


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_grouped_map import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
