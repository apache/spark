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


class JoinTestsMixin:
    """Join / cogroup assertions without JVM stage tracking."""

    def _verify_join_collect_results(self) -> None:
        rdd = self.sc.parallelize(range(10)).map(lambda x: (x, x))
        parted = rdd.partitionBy(2)
        self.assertEqual(2, parted.union(parted).getNumPartitions())
        self.assertEqual(rdd.getNumPartitions() + 2, parted.union(rdd).getNumPartitions())
        self.assertEqual(rdd.getNumPartitions() + 2, rdd.union(parted).getNumPartitions())

        d = sorted(parted.join(parted).collect())
        self.assertEqual(10, len(d))
        self.assertEqual((0, (0, 0)), d[0])

        d = sorted(parted.join(rdd).collect())
        self.assertEqual(10, len(d))
        self.assertEqual((0, (0, 0)), d[0])

        d = sorted(parted.cogroup(parted).collect())
        self.assertEqual(10, len(d))
        self.assertEqual([[0], [0]], list(map(list, d[0][1])))

        d = sorted(parted.cogroup(rdd).collect())
        self.assertEqual(10, len(d))
        self.assertEqual([[0], [0]], list(map(list, d[0][1])))


class JoinTests(JoinTestsMixin, ReusedPySparkTestCase):
    def test_narrow_dependency_in_join(self):
        self._verify_join_collect_results()

        tracker = self.sc.statusTracker()

        rdd = self.sc.parallelize(range(10)).map(lambda x: (x, x))
        parted = rdd.partitionBy(2)

        self.sc.setJobGroup("test1", "test", True)
        sorted(parted.join(parted).collect())
        job_id = tracker.getJobIdsForGroup("test1")[0]
        self.assertEqual(2, len(tracker.getJobInfo(job_id).stageIds))

        self.sc.setJobGroup("test2", "test", True)
        sorted(parted.join(rdd).collect())
        job_id = tracker.getJobIdsForGroup("test2")[0]
        self.assertEqual(3, len(tracker.getJobInfo(job_id).stageIds))

        self.sc.setJobGroup("test3", "test", True)
        sorted(parted.cogroup(parted).collect())
        job_id = tracker.getJobIdsForGroup("test3")[0]
        self.assertEqual(2, len(tracker.getJobInfo(job_id).stageIds))

        self.sc.setJobGroup("test4", "test", True)
        sorted(parted.cogroup(rdd).collect())
        job_id = tracker.getJobIdsForGroup("test4")[0]
        self.assertEqual(3, len(tracker.getJobInfo(job_id).stageIds))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
