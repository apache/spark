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
from pyspark.statcounter import StatCounter
from pyspark.testing.utils import ReusedPySparkTestCase


class StatCounterTests(ReusedPySparkTestCase):
    def test_base(self):
        stats = self.sc.parallelize([1.0, 2.0, 3.0, 4.0]).stats()
        self.assertEqual(stats.count(), 4)
        self.assertEqual(stats.max(), 4.0)
        self.assertEqual(stats.mean(), 2.5)
        self.assertEqual(stats.min(), 1.0)
        self.assertAlmostEqual(stats.stdev(), 1.118033988749895)
        self.assertAlmostEqual(stats.sampleStdev(), 1.2909944487358056)
        self.assertEqual(stats.sum(), 10.0)
        self.assertAlmostEqual(stats.variance(), 1.25)
        self.assertAlmostEqual(stats.sampleVariance(), 1.6666666666666667)

    def test_as_dict(self):
        stats = self.sc.parallelize([1.0, 2.0, 3.0, 4.0]).stats().asDict()
        self.assertEqual(stats["count"], 4)
        self.assertEqual(stats["max"], 4.0)
        self.assertEqual(stats["mean"], 2.5)
        self.assertEqual(stats["min"], 1.0)
        self.assertAlmostEqual(stats["stdev"], 1.2909944487358056)
        self.assertEqual(stats["sum"], 10.0)
        self.assertAlmostEqual(stats["variance"], 1.6666666666666667)

        stats = self.sc.parallelize([1.0, 2.0, 3.0, 4.0]).stats().asDict(sample=True)
        self.assertEqual(stats["count"], 4)
        self.assertEqual(stats["max"], 4.0)
        self.assertEqual(stats["mean"], 2.5)
        self.assertEqual(stats["min"], 1.0)
        self.assertAlmostEqual(stats["stdev"], 1.118033988749895)
        self.assertEqual(stats["sum"], 10.0)
        self.assertAlmostEqual(stats["variance"], 1.25)

    def test_merge(self):
        stats = StatCounter([1.0, 2.0, 3.0, 4.0])
        stats.merge(5.0)
        self.assertEqual(stats.count(), 5)
        self.assertEqual(stats.max(), 5.0)
        self.assertEqual(stats.mean(), 3.0)
        self.assertEqual(stats.min(), 1.0)
        self.assertAlmostEqual(stats.stdev(), 1.414213562373095)
        self.assertAlmostEqual(stats.sampleStdev(), 1.5811388300841898)
        self.assertEqual(stats.sum(), 15.0)
        self.assertAlmostEqual(stats.variance(), 2.0)
        self.assertAlmostEqual(stats.sampleVariance(), 2.5)

    def test_merge_stats(self):
        stats1 = StatCounter([1.0, 2.0, 3.0, 4.0])
        stats2 = StatCounter([1.0, 2.0, 3.0, 4.0])
        stats = stats1.mergeStats(stats2)
        self.assertEqual(stats.count(), 8)
        self.assertEqual(stats.max(), 4.0)
        self.assertEqual(stats.mean(), 2.5)
        self.assertEqual(stats.min(), 1.0)
        self.assertAlmostEqual(stats.stdev(), 1.118033988749895)
        self.assertAlmostEqual(stats.sampleStdev(), 1.1952286093343936)
        self.assertEqual(stats.sum(), 20.0)
        self.assertAlmostEqual(stats.variance(), 1.25)
        self.assertAlmostEqual(stats.sampleVariance(), 1.4285714285714286)

    def test_merge_stats_with_self(self):
        stats = StatCounter([1.0, 2.0, 3.0, 4.0])
        stats.mergeStats(stats)
        self.assertEqual(stats.count(), 8)
        self.assertEqual(stats.max(), 4.0)
        self.assertEqual(stats.mean(), 2.5)
        self.assertEqual(stats.min(), 1.0)
        self.assertAlmostEqual(stats.stdev(), 1.118033988749895)
        self.assertAlmostEqual(stats.sampleStdev(), 1.1952286093343936)
        self.assertEqual(stats.sum(), 20.0)
        self.assertAlmostEqual(stats.variance(), 1.25)
        self.assertAlmostEqual(stats.sampleVariance(), 1.4285714285714286)


if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_statcounter import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
