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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class DefaultIndexTest(PandasOnSparkTestCase):
    def test_default_index_sequence(self):
        with ps.option_context("compute.default_index_type", "sequence"):
            sdf = self.spark.range(1000)
            self.assert_eq(ps.DataFrame(sdf), pd.DataFrame({"id": list(range(1000))}))

    def test_default_index_distributed_sequence(self):
        with ps.option_context("compute.default_index_type", "distributed-sequence"):
            sdf = self.spark.range(1000)
            self.assert_eq(ps.DataFrame(sdf), pd.DataFrame({"id": list(range(1000))}))

    def test_default_index_distributed(self):
        with ps.option_context("compute.default_index_type", "distributed"):
            sdf = self.spark.range(1000)
            pdf = ps.DataFrame(sdf).to_pandas()
            self.assertEqual(len(set(pdf.index)), len(pdf))


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_default_index import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
