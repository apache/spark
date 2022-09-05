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

from pyspark.resource import ExecutorResourceRequests, ResourceProfileBuilder, TaskResourceRequests


class ResourceProfileTests(unittest.TestCase):
    def test_profile_before_sc(self):
        rpb = ResourceProfileBuilder()
        ereqs = ExecutorResourceRequests().cores(2).memory("6g").memoryOverhead("1g")
        ereqs.pysparkMemory("2g").offheapMemory("3g").resource("gpu", 2, "testGpus", "nvidia.com")
        treqs = TaskResourceRequests().cpus(2).resource("gpu", 2)

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

        assert_request_contents(ereqs.requests, treqs.requests)
        rp = rpb.require(ereqs).require(treqs).build
        assert_request_contents(rp.executorResources, rp.taskResources)
        from pyspark import SparkContext, SparkConf

        sc = SparkContext(conf=SparkConf())
        rdd = sc.parallelize(range(10)).withResources(rp)
        return_rp = rdd.getResourceProfile()
        assert_request_contents(return_rp.executorResources, return_rp.taskResources)
        # intermix objects created before SparkContext init and after
        rpb2 = ResourceProfileBuilder()
        # use reqs created before SparkContext with Builder after
        rpb2.require(ereqs)
        rpb2.require(treqs)
        rp2 = rpb2.build
        self.assertTrue(rp2.id > 0)
        rdd2 = sc.parallelize(range(10)).withResources(rp2)
        return_rp2 = rdd2.getResourceProfile()
        assert_request_contents(return_rp2.executorResources, return_rp2.taskResources)
        ereqs2 = ExecutorResourceRequests().cores(2).memory("6g").memoryOverhead("1g")
        ereqs.pysparkMemory("2g").resource("gpu", 2, "testGpus", "nvidia.com")
        treqs2 = TaskResourceRequests().cpus(2).resource("gpu", 2)
        # use reqs created after SparkContext with Builder before
        rpb.require(ereqs2)
        rpb.require(treqs2)
        rp3 = rpb.build
        assert_request_contents(rp3.executorResources, rp3.taskResources)
        sc.stop()


if __name__ == "__main__":
    from pyspark.resource.tests.test_resources import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
