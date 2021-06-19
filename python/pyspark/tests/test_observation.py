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
from pyspark.sql import DataFrame, functions as func, Row, SparkSession
from pyspark.sql.observation import Observation
from pyspark.testing.utils import ReusedPySparkTestCase


class ObservationTests(ReusedPySparkTestCase):

    @classmethod
    def get_df(cls) -> DataFrame:
        return SparkSession(cls.sc).createDataFrame([
            (1, 1.0, 'one'),
            (2, 2.0, 'two'),
            (3, 3.0, 'three'),
        ], ['id', 'val', 'label'])

    def assert_collected(self, actual):
        self.assertEqual([
            {'id': 1, 'val': 1.0, 'label': 'one'},
            {'id': 2, 'val': 2.0, 'label': 'two'},
            {'id': 3, 'val': 3.0, 'label': 'three'},
        ], [row.asDict() for row in actual])

    def test_observe(self):
        df = self.get_df()

        # we do not bother obtaining the metric, this just
        # tests that Observation.on works transparently
        observed = Observation("metrics").on(df, func.count(func.lit(1)), func.sum(func.col("val")))
        actual = observed.orderBy('id').collect()
        self.assert_collected(actual)

    def test_observation(self):
        df = self.get_df()
        observation = Observation("metric")
        observed = df.orderBy('id').observe(
            observation,
            func.count(func.lit(1)).alias('cnt'),
            func.sum(func.col("id")).alias('sum'),
            func.mean(func.col("val")).alias('mean')
        )
        self.assertFalse(observation.wait_completed(1000))

        actual = observed.collect()
        self.assert_collected(actual)

        self.assertTrue(observation.wait_completed(1000))
        self.assertEqual(observation.get, Row(cnt=3, sum=6, mean=2.0))

        observed.collect()
        self.assertTrue(observation.wait_completed(1000))
        self.assertEqual(observation.get, Row(cnt=3, sum=6, mean=2.0))


if __name__ == "__main__":
    from pyspark.tests.test_join import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
