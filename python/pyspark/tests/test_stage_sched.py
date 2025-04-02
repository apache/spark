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
import json

from pyspark import SparkConf, SparkContext
from pyspark.resource.profile import ResourceProfileBuilder
from pyspark.resource.requests import TaskResourceRequests
from pyspark.taskcontext import TaskContext


class StageSchedulingTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        if getattr(self, "sc", None) is not None:
            self.sc.stop()
            self.sc = None

    def _test_stage_scheduling(
        self,
        cpus_per_worker,
        gpus_per_worker,
        num_tasks,
        resource_profile,
        expected_max_concurrent_tasks,
    ):
        conf = SparkConf()
        conf.setMaster(f"local-cluster[1,{cpus_per_worker},1024]").set(
            "spark.task.maxFailures", "1"
        )

        if gpus_per_worker:
            worker_res_config_file = os.path.join(self.temp_dir, "worker_res.json")
            worker_res = [
                {
                    "id": {
                        "componentName": "spark.worker",
                        "resourceName": "gpu",
                    },
                    "addresses": [str(i) for i in range(gpus_per_worker)],
                }
            ]
            with open(worker_res_config_file, "w") as fp:
                json.dump(worker_res, fp)

            conf.set("spark.worker.resource.gpu.amount", str(gpus_per_worker))
            conf.set("spark.worker.resourcesFile", worker_res_config_file)
            conf.set("spark.executor.resource.gpu.amount", str(gpus_per_worker))

        self.sc = SparkContext(conf=conf)
        pids_output_dir = os.path.join(self.temp_dir, "pids")
        os.mkdir(pids_output_dir)

        def mapper(_):
            task_id = TaskContext.get().partitionId()
            pid_file_path = os.path.join(pids_output_dir, str(task_id))
            with open(pid_file_path, mode="w"):
                pass
            time.sleep(0.1)
            num_concurrent_tasks = len(os.listdir(pids_output_dir))
            time.sleep(1)
            os.remove(pid_file_path)
            return num_concurrent_tasks

        results = (
            self.sc.parallelize(range(num_tasks), num_tasks)
            .withResources(resource_profile)
            .map(mapper)
            .collect()
        )
        self.assertEqual(max(results), expected_max_concurrent_tasks)

    def test_stage_scheduling_3_cpu_per_task(self):
        rp = ResourceProfileBuilder().require(TaskResourceRequests().cpus(3)).build
        self._test_stage_scheduling(
            cpus_per_worker=4,
            gpus_per_worker=0,
            num_tasks=2,
            resource_profile=rp,
            expected_max_concurrent_tasks=1,
        )

    def test_stage_scheduling_2_cpu_per_task(self):
        rp = ResourceProfileBuilder().require(TaskResourceRequests().cpus(2)).build
        self._test_stage_scheduling(
            cpus_per_worker=4,
            gpus_per_worker=0,
            num_tasks=4,
            resource_profile=rp,
            expected_max_concurrent_tasks=2,
        )

    def test_stage_scheduling_2_cpus_2_gpus_per_task(self):
        rp = (
            ResourceProfileBuilder()
            .require(TaskResourceRequests().cpus(2).resource("gpu", 2))
            .build
        )
        self._test_stage_scheduling(
            cpus_per_worker=4,
            gpus_per_worker=4,
            num_tasks=4,
            resource_profile=rp,
            expected_max_concurrent_tasks=2,
        )

    def test_stage_scheduling_2_cpus_3_gpus_per_task(self):
        rp = (
            ResourceProfileBuilder()
            .require(TaskResourceRequests().cpus(2).resource("gpu", 3))
            .build
        )
        self._test_stage_scheduling(
            cpus_per_worker=4,
            gpus_per_worker=4,
            num_tasks=2,
            resource_profile=rp,
            expected_max_concurrent_tasks=1,
        )


if __name__ == "__main__":
    from pyspark.tests.test_stage_sched import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
