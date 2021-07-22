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
from pyspark.sql import functions as func, Row, SparkSession
from pyspark.sql.observation import Observation
from pyspark.testing.utils import ReusedPySparkTestCase


class ObservationTests(ReusedPySparkTestCase):

    def test_observe(self):
        df = SparkSession(self.sc).createDataFrame([
            (1, 1.0, 'one'),
            (2, 2.0, 'two'),
            (3, 3.0, 'three'),
        ], ['id', 'val', 'label'])

        observation = Observation("metric")
        observed = df.orderBy('id').observe(
            observation,
            func.count(func.lit(1)).alias('cnt'),
            func.sum(func.col("id")).alias('sum'),
            func.mean(func.col("val")).alias('mean')
        )

        # test that observe works transparently
        actual = observed.collect()
        self.assertEqual([
            {'id': 1, 'val': 1.0, 'label': 'one'},
            {'id': 2, 'val': 2.0, 'label': 'two'},
            {'id': 3, 'val': 3.0, 'label': 'three'},
        ], [row.asDict() for row in actual])

        # test that we retrieve the metrics
        self.assertEqual(observation.get, Row(cnt=3, sum=6, mean=2.0))


if __name__ == "__main__":
    from pyspark.tests.test_observation import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
