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
import random
import stat
import sys
import tempfile
import time
import unittest

from pyspark import SparkConf, SparkContext, TaskContext, BarrierTaskContext
from pyspark.testing.utils import PySparkTestCase, SPARK_HOME, eventually


class TaskContextTests(PySparkTestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        # Allow retries even though they are normally disabled in local mode
        self.sc = SparkContext('local[4, 2]', class_name)

    def test_stage_id(self):
        """Test the stage ids are available and incrementing as expected."""
        rdd = self.sc.parallelize(range(10))
        stage1 = rdd.map(lambda x: TaskContext.get().stageId()).take(1)[0]
        stage2 = rdd.map(lambda x: TaskContext.get().stageId()).take(1)[0]
        # Test using the constructor directly rather than the get()
        stage3 = rdd.map(lambda x: TaskContext().stageId()).take(1)[0]
        self.assertEqual(stage1 + 1, stage2)
        self.assertEqual(stage1 + 2, stage3)
        self.assertEqual(stage2 + 1, stage3)

    def test_resources(self):
        """Test the resources are empty by default."""
        rdd = self.sc.parallelize(range(10))
        resources1 = rdd.map(lambda x: TaskContext.get().resources()).take(1)[0]
        # Test using the constructor directly rather than the get()
        resources2 = rdd.map(lambda x: TaskContext().resources()).take(1)[0]
        self.assertEqual(len(resources1), 0)
        self.assertEqual(len(resources2), 0)

    def test_partition_id(self):
        """Test the partition id."""
        rdd1 = self.sc.parallelize(range(10), 1)
        rdd2 = self.sc.parallelize(range(10), 2)
        pids1 = rdd1.map(lambda x: TaskContext.get().partitionId()).collect()
        pids2 = rdd2.map(lambda x: TaskContext.get().partitionId()).collect()
        self.assertEqual(0, pids1[0])
        self.assertEqual(0, pids1[9])
        self.assertEqual(0, pids2[0])
        self.assertEqual(1, pids2[9])

    def test_attempt_number(self):
        """Verify the attempt numbers are correctly reported."""
        rdd = self.sc.parallelize(range(10))
        # Verify a simple job with no failures
        attempt_numbers = rdd.map(lambda x: TaskContext.get().attemptNumber()).collect()
        map(lambda attempt: self.assertEqual(0, attempt), attempt_numbers)

        def fail_on_first(x):
            """Fail on the first attempt so we get a positive attempt number"""
            tc = TaskContext.get()
            attempt_number = tc.attemptNumber()
            partition_id = tc.partitionId()
            attempt_id = tc.taskAttemptId()
            if attempt_number == 0 and partition_id == 0:
                raise RuntimeError("Failing on first attempt")
            else:
                return [x, partition_id, attempt_number, attempt_id]
        result = rdd.map(fail_on_first).collect()
        # We should re-submit the first partition to it but other partitions should be attempt 0
        self.assertEqual([0, 0, 1], result[0][0:3])
        self.assertEqual([9, 3, 0], result[9][0:3])
        first_partition = filter(lambda x: x[1] == 0, result)
        map(lambda x: self.assertEqual(1, x[2]), first_partition)
        other_partitions = filter(lambda x: x[1] != 0, result)
        map(lambda x: self.assertEqual(0, x[2]), other_partitions)
        # The task attempt id should be different
        self.assertTrue(result[0][3] != result[9][3])

    def test_tc_on_driver(self):
        """Verify that getting the TaskContext on the driver returns None."""
        tc = TaskContext.get()
        self.assertTrue(tc is None)

    def test_get_local_property(self):
        """Verify that local properties set on the driver are available in TaskContext."""
        key = "testkey"
        value = "testvalue"
        self.sc.setLocalProperty(key, value)
        try:
            rdd = self.sc.parallelize(range(1), 1)
            prop1 = rdd.map(lambda _: TaskContext.get().getLocalProperty(key)).collect()[0]
            self.assertEqual(prop1, value)
            prop2 = rdd.map(lambda _: TaskContext.get().getLocalProperty("otherkey")).collect()[0]
            self.assertTrue(prop2 is None)
        finally:
            self.sc.setLocalProperty(key, None)

    def test_barrier(self):
        """
        Verify that BarrierTaskContext.barrier() performs global sync among all barrier tasks
        within a stage.
        """
        rdd = self.sc.parallelize(range(10), 4)

        def f(iterator):
            yield sum(iterator)

        def context_barrier(x):
            tc = BarrierTaskContext.get()
            time.sleep(random.randint(1, 5) * 2)
            tc.barrier()
            return time.time()

        times = rdd.barrier().mapPartitions(f).map(context_barrier).collect()
        self.assertTrue(max(times) - min(times) < 2)

    def test_all_gather(self):
        """
        Verify that BarrierTaskContext.allGather() performs global sync among all barrier tasks
        within a stage and passes messages properly.
        """
        rdd = self.sc.parallelize(range(10), 4)

        def f(iterator):
            yield sum(iterator)

        def context_barrier(x):
            tc = BarrierTaskContext.get()
            time.sleep(random.randint(1, 10))
            out = tc.allGather(str(tc.partitionId()))
            pids = [int(e) for e in out]
            return pids

        pids = rdd.barrier().mapPartitions(f).map(context_barrier).collect()[0]
        self.assertEqual(pids, [0, 1, 2, 3])

    def test_barrier_infos(self):
        """
        Verify that BarrierTaskContext.getTaskInfos() returns a list of all task infos in the
        barrier stage.
        """
        rdd = self.sc.parallelize(range(10), 4)

        def f(iterator):
            yield sum(iterator)

        taskInfos = rdd.barrier().mapPartitions(f).map(lambda x: BarrierTaskContext.get()
                                                       .getTaskInfos()).collect()
        self.assertTrue(len(taskInfos) == 4)
        self.assertTrue(len(taskInfos[0]) == 4)

    def test_context_get(self):
        """
        Verify that TaskContext.get() works both in or not in a barrier stage.
        """
        rdd = self.sc.parallelize(range(10), 4)

        def f(iterator):
            taskContext = TaskContext.get()
            if isinstance(taskContext, BarrierTaskContext):
                yield taskContext.partitionId() + 1
            elif isinstance(taskContext, TaskContext):
                yield taskContext.partitionId() + 2
            else:
                yield -1

        # for normal stage
        result1 = rdd.mapPartitions(f).collect()
        self.assertTrue(result1 == [2, 3, 4, 5])
        # for barrier stage
        result2 = rdd.barrier().mapPartitions(f).collect()
        self.assertTrue(result2 == [1, 2, 3, 4])

    def test_barrier_context_get(self):
        """
        Verify that BarrierTaskContext.get() should only works in a barrier stage.
        """
        rdd = self.sc.parallelize(range(10), 4)

        def f(iterator):
            try:
                taskContext = BarrierTaskContext.get()
            except Exception:
                yield -1
            else:
                yield taskContext.partitionId()

        # for normal stage
        result1 = rdd.mapPartitions(f).collect()
        self.assertTrue(result1 == [-1, -1, -1, -1])
        # for barrier stage
        result2 = rdd.barrier().mapPartitions(f).collect()
        self.assertTrue(result2 == [0, 1, 2, 3])


class TaskContextTestsWithWorkerReuse(unittest.TestCase):

    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.worker.reuse", "true")
        self.sc = SparkContext('local[2]', class_name, conf=conf)

    def test_barrier_with_python_worker_reuse(self):
        """
        Regression test for SPARK-25921: verify that BarrierTaskContext.barrier() with
        reused python worker.
        """
        # start a normal job first to start all workers and get all worker pids
        worker_pids = self.sc.parallelize(range(2), 2).map(lambda x: os.getpid()).collect()
        # the worker will reuse in this barrier job
        rdd = self.sc.parallelize(range(10), 2)

        def f(iterator):
            yield sum(iterator)

        def context_barrier(x):
            tc = BarrierTaskContext.get()
            time.sleep(random.randint(1, 5) * 2)
            tc.barrier()
            return (time.time(), os.getpid())

        result = rdd.barrier().mapPartitions(f).map(context_barrier).collect()
        times = list(map(lambda x: x[0], result))
        pids = list(map(lambda x: x[1], result))
        # check both barrier and worker reuse effect
        self.assertTrue(max(times) - min(times) < 2)
        for pid in pids:
            self.assertTrue(pid in worker_pids)

    def check_task_context_correct_with_python_worker_reuse(self):
        """Verify the task context correct when reused python worker"""
        # start a normal job first to start all workers and get all worker pids
        worker_pids = self.sc.parallelize(range(2), 2).map(lambda x: os.getpid()).collect()
        # the worker will reuse in this barrier job
        rdd = self.sc.parallelize(range(10), 2)

        def context(iterator):
            tp = TaskContext.get().partitionId()
            try:
                bp = BarrierTaskContext.get().partitionId()
            except Exception:
                bp = -1

            yield (tp, bp, os.getpid())

        # normal stage after normal stage
        normal_result = rdd.mapPartitions(context).collect()
        tps, bps, pids = zip(*normal_result)
        self.assertTrue(tps == (0, 1))
        self.assertTrue(bps == (-1, -1))
        for pid in pids:
            self.assertTrue(pid in worker_pids)
        # barrier stage after normal stage
        barrier_result = rdd.barrier().mapPartitions(context).collect()
        tps, bps, pids = zip(*barrier_result)
        self.assertTrue(tps == (0, 1))
        self.assertTrue(bps == (0, 1))
        for pid in pids:
            self.assertTrue(pid in worker_pids)
        # normal stage after barrier stage
        normal_result2 = rdd.mapPartitions(context).collect()
        tps, bps, pids = zip(*normal_result2)
        self.assertTrue(tps == (0, 1))
        self.assertTrue(bps == (-1, -1))
        for pid in pids:
            self.assertTrue(pid in worker_pids)
        return True

    def test_task_context_correct_with_python_worker_reuse(self):
        # Retrying the check as the PIDs from Python workers might be different even
        # when reusing Python workers is enabled if a Python worker is dead for some reasons
        # (e.g., socket connection failure) and new Python worker is created.
        eventually(
            self.check_task_context_correct_with_python_worker_reuse, catch_assertions=True)

    def tearDown(self):
        self.sc.stop()


class TaskContextTestsWithResources(unittest.TestCase):

    def setUp(self):
        class_name = self.__class__.__name__
        self.tempFile = tempfile.NamedTemporaryFile(delete=False)
        self.tempFile.write(b'echo {\\"name\\": \\"gpu\\", \\"addresses\\": [\\"0\\"]}')
        self.tempFile.close()
        # create temporary directory for Worker resources coordination
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)
        os.chmod(self.tempFile.name, stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP |
                 stat.S_IROTH | stat.S_IXOTH)
        conf = SparkConf().set("spark.test.home", SPARK_HOME)
        conf = conf.set("spark.worker.resource.gpu.discoveryScript", self.tempFile.name)
        conf = conf.set("spark.worker.resource.gpu.amount", 1)
        conf = conf.set("spark.task.resource.gpu.amount", "1")
        conf = conf.set("spark.executor.resource.gpu.amount", "1")
        self.sc = SparkContext('local-cluster[2,1,1024]', class_name, conf=conf)

    def test_resources(self):
        """Test the resources are available."""
        rdd = self.sc.parallelize(range(10))
        resources = rdd.map(lambda x: TaskContext.get().resources()).take(1)[0]
        self.assertEqual(len(resources), 1)
        self.assertTrue('gpu' in resources)
        self.assertEqual(resources['gpu'].name, 'gpu')
        self.assertEqual(resources['gpu'].addresses, ['0'])

    def tearDown(self):
        os.unlink(self.tempFile.name)
        self.sc.stop()

if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_taskcontext import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
