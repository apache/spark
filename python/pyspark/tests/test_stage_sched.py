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

import os
import tempfile
import unittest
import time
import shutil

from pyspark import SparkConf, SparkContext
from pyspark.resource.profile import ResourceProfileBuilder
from pyspark.resource.requests import TaskResourceRequests


class StageSchedulingTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        if getattr(self, "sc", None) is not None:
            self.sc.stop()
            self.sc = None

    def _test_stage_scheduling(
            self, master, num_tasks, resource_profile,
            expected_max_concurrent_tasks,
    ):
        conf = SparkConf()
        conf.setMaster(master).set("spark.task.maxFailures", "1")
        self.sc = SparkContext(conf=conf)
        temp_dir = self.temp_dir

        def mapper(_):
            from pyspark.taskcontext import TaskContext
            task_id = TaskContext.get().partitionId()
            pid_file_path = os.path.join(temp_dir, str(task_id))
            with open(pid_file_path, mode="w"):
                pass
            time.sleep(0.1)
            num_concurrent_tasks = os.listdir(temp_dir)
            time.sleep(1)
            os.remove(pid_file_path)
            return num_concurrent_tasks
        results = self.sc.parallelize(range(num_tasks), num_tasks) \
            .withResources(resource_profile).map(mapper).collect()
        assert max(results) == expected_max_concurrent_tasks

    def test_stage_scheduling(self):
        rp = ResourceProfileBuilder().require(TaskResourceRequests().cpus(4)).build
        self._test_stage_scheduling(
            "local-cluster[1,4,1024]",
            num_tasks=2,
            resource_profile=rp,
            expected_max_concurrent_tasks=1,
        )
        rp = ResourceProfileBuilder().require(TaskResourceRequests().cpus(1)).build
        self._test_stage_scheduling(
            "local-cluster[1,4,1024]",
            num_tasks=4,
            resource_profile=rp,
            expected_max_concurrent_tasks=4,
        )


if __name__ == "__main__":
    from pyspark.tests.test_memory_profiler import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
