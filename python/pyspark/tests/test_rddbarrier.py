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
from pyspark.testing.utils import ReusedPySparkTestCase


class RDDBarrierTests(ReusedPySparkTestCase):
    def test_map_partitions(self):
        """Test RDDBarrier.mapPartitions"""
        rdd = self.sc.parallelize(range(12), 4)
        self.assertFalse(rdd._is_barrier())

        rdd1 = rdd.barrier().mapPartitions(lambda it: it)
        self.assertTrue(rdd1._is_barrier())

    def test_map_partitions_with_index(self):
        """Test RDDBarrier.mapPartitionsWithIndex"""
        rdd = self.sc.parallelize(range(12), 4)
        self.assertFalse(rdd._is_barrier())

        def f(index, iterator):
            yield index
        rdd1 = rdd.barrier().mapPartitionsWithIndex(f)
        self.assertTrue(rdd1._is_barrier())
        self.assertEqual(rdd1.collect(), [0, 1, 2, 3])


if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_rddbarrier import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
