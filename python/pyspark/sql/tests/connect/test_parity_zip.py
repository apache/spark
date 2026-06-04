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

from pyspark.errors import PySparkNotImplementedError
from pyspark.testing.connectutils import ReusedConnectTestCase


class ZipParityTests(ReusedConnectTestCase):
    """`DataFrame.zip` is classic-only for now; assert the Connect stub raises a clean
    NOT_IMPLEMENTED instead of falling through to a generic error or appearing to work."""

    def test_zip_raises_not_implemented(self):
        df = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        with self.assertRaises(PySparkNotImplementedError) as ctx:
            df.select("a").zip(df.select("b"))
        self.assertEqual(ctx.exception.getCondition(), "NOT_IMPLEMENTED")
        self.assertIn("zip", str(ctx.exception))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
