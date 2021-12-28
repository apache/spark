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
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class SparkIndexOpsMethodsTest(PandasOnSparkTestCase, SQLTestUtils):
    @property
    def pser(self):
        return pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    def test_series_transform_negative(self):
        with self.assertRaisesRegex(
            ValueError, "The output of the function.* pyspark.sql.Column.*int"
        ):
            self.psser.spark.transform(lambda scol: 1)

        with self.assertRaisesRegex(AnalysisException, "Column.*non-existent.*does not exist"):
            self.psser.spark.transform(lambda scol: F.col("non-existent"))

    def test_multiindex_transform_negative(self):
        with self.assertRaisesRegex(
            NotImplementedError, "MultiIndex does not support spark.transform yet"
        ):
            midx = pd.MultiIndex(
                [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
                [[0, 0, 0, 1, 1, 1, 2, 2, 2], [1, 1, 1, 1, 1, 2, 1, 2, 2]],
            )
            s = ps.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
            s.index.spark.transform(lambda scol: scol)

    def test_series_apply_negative(self):
        with self.assertRaisesRegex(
            ValueError, "The output of the function.* pyspark.sql.Column.*int"
        ):
            self.psser.spark.apply(lambda scol: 1)

        with self.assertRaisesRegex(AnalysisException, "Column.*non-existent.*does not exist"):
            self.psser.spark.transform(lambda scol: F.col("non-existent"))


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_indexops_spark import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
