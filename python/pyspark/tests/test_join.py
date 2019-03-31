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


class JoinTests(ReusedPySparkTestCase):

    def test_narrow_dependency_in_join(self):
        rdd = self.sc.parallelize(range(10)).map(lambda x: (x, x))
        parted = rdd.partitionBy(2)
        self.assertEqual(2, parted.union(parted).getNumPartitions())
        self.assertEqual(rdd.getNumPartitions() + 2, parted.union(rdd).getNumPartitions())
        self.assertEqual(rdd.getNumPartitions() + 2, rdd.union(parted).getNumPartitions())

        tracker = self.sc.statusTracker()

        self.sc.setJobGroup("test1", "test", True)
        d = sorted(parted.join(parted).collect())
        self.assertEqual(10, len(d))
        self.assertEqual((0, (0, 0)), d[0])
        jobId = tracker.getJobIdsForGroup("test1")[0]
        self.assertEqual(2, len(tracker.getJobInfo(jobId).stageIds))

        self.sc.setJobGroup("test2", "test", True)
        d = sorted(parted.join(rdd).collect())
        self.assertEqual(10, len(d))
        self.assertEqual((0, (0, 0)), d[0])
        jobId = tracker.getJobIdsForGroup("test2")[0]
        self.assertEqual(3, len(tracker.getJobInfo(jobId).stageIds))

        self.sc.setJobGroup("test3", "test", True)
        d = sorted(parted.cogroup(parted).collect())
        self.assertEqual(10, len(d))
        self.assertEqual([[0], [0]], list(map(list, d[0][1])))
        jobId = tracker.getJobIdsForGroup("test3")[0]
        self.assertEqual(2, len(tracker.getJobInfo(jobId).stageIds))

        self.sc.setJobGroup("test4", "test", True)
        d = sorted(parted.cogroup(rdd).collect())
        self.assertEqual(10, len(d))
        self.assertEqual([[0], [0]], list(map(list, d[0][1])))
        jobId = tracker.getJobIdsForGroup("test4")[0]
        self.assertEqual(3, len(tracker.getJobInfo(jobId).stageIds))


if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_join import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
