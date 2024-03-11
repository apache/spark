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

from pyspark.resource import ResourceProfileBuilder, TaskResourceRequests, ExecutorResourceRequests
from pyspark.sql import SparkSession


class ResourceProfileTests(unittest.TestCase):
    def test_profile_before_sc_for_connect(self):
        rpb = ResourceProfileBuilder()
        treqs = TaskResourceRequests().cpus(2)
        # no exception for building ResourceProfile
        rp = rpb.require(treqs).build

        # check taskResources, similar to executorResources.
        self.assertEqual(rp.taskResources["cpus"].amount, 2.0)

        # SparkContext is not initialized and is not remote.
        with self.assertRaisesRegex(
            RuntimeError, "SparkContext must be created to get the profile id."
        ):
            rp.id

        # Remote mode.
        spark = SparkSession.builder.remote("local-cluster[1, 2, 1024]").getOrCreate()
        # Still can access taskResources, similar to executorResources.
        self.assertEqual(rp.taskResources["cpus"].amount, 2.0)
        rp.id
        df = spark.range(10)
        df.mapInPandas(lambda x: x, df.schema, False, rp).collect()
        df.mapInArrow(lambda x: x, df.schema, False, rp).collect()

        def assert_request_contents(exec_reqs, task_reqs):
            self.assertEqual(len(exec_reqs), 6)
            self.assertEqual(exec_reqs["cores"].amount, 2)
            self.assertEqual(exec_reqs["memory"].amount, 6144)
            self.assertEqual(exec_reqs["memoryOverhead"].amount, 1024)
            self.assertEqual(exec_reqs["pyspark.memory"].amount, 2048)
            self.assertEqual(exec_reqs["offHeap"].amount, 3072)
            self.assertEqual(exec_reqs["gpu"].amount, 2)
            self.assertEqual(exec_reqs["gpu"].discoveryScript, "testGpus")
            self.assertEqual(exec_reqs["gpu"].resourceName, "gpu")
            self.assertEqual(exec_reqs["gpu"].vendor, "nvidia.com")
            self.assertEqual(len(task_reqs), 2)
            self.assertEqual(task_reqs["cpus"].amount, 2.0)
            self.assertEqual(task_reqs["gpu"].amount, 2.0)

        rpb = ResourceProfileBuilder()
        ereqs = ExecutorResourceRequests().cores(2).memory("6g").memoryOverhead("1g")
        ereqs.pysparkMemory("2g").offheapMemory("3g").resource("gpu", 2, "testGpus", "nvidia.com")
        treqs = TaskResourceRequests().cpus(2).resource("gpu", 2)
        rp = rpb.require(ereqs).require(treqs).build
        assert_request_contents(rp.executorResources, rp.taskResources)

        spark.stop()


if __name__ == "__main__":
    from pyspark.resource.tests.test_connect_resources import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
