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
import unittest

from pyspark.statcounter import StatCounter
from pyspark.testing.utils import ReusedPySparkTestCase


class StatCounterTests(ReusedPySparkTestCase):
    def test_as_dict(self):
        stats = self.sc.parallelize([1.0, 2.0, 3.0, 4.0]).stats()
        self.assertDictEqual(
            stats.asDict(),
            {
                "count": 4,
                "max": 4.0,
                "mean": 2.5,
                "min": 1.0,
                "stdev": 1.2909944487358056,
                "sum": 10.0,
                "variance": 1.6666666666666667,
            },
        )

    def test_merge(self):
        stats = StatCounter([1.0, 2.0, 3.0, 4.0])
        stats.merge(5.0)
        self.assertDictEqual(
            stats.asDict(),
            {
                "count": 5,
                "max": 5.0,
                "mean": 3.0,
                "min": 1.0,
                "stdev": 1.5811388300841898,
                "sum": 15.0,
                "variance": 2.5,
            },
        )

    def test_merge_stats(self):
        stats1 = StatCounter([1.0, 2.0, 3.0, 4.0])
        stats2 = StatCounter([1.0, 2.0, 3.0, 4.0])
        merged = stats1.mergeStats(stats2)
        self.assertDictEqual(
            merged.asDict(),
            {
                "count": 8,
                "max": 4.0,
                "mean": 2.5,
                "min": 1.0,
                "stdev": 1.1952286093343936,
                "sum": 20.0,
                "variance": 1.4285714285714286,
            },
        )

    def test_merge_stats_with_self(self):
        stats = StatCounter([1.0, 2.0, 3.0, 4.0])
        stats.mergeStats(stats)
        self.assertDictEqual(
            stats.asDict(),
            {
                "count": 8,
                "max": 4.0,
                "mean": 2.5,
                "min": 1.0,
                "stdev": 1.1952286093343936,
                "sum": 20.0,
                "variance": 1.4285714285714286,
            },
        )


if __name__ == "__main__":
    from pyspark.tests.test_statcounter import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
