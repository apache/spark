# -*- encoding: utf-8 -*-
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
import threading
import time
import unittest
has_resource_module = True
try:
    import resource  # noqa: F401
except ImportError:
    has_resource_module = False

from py4j.protocol import Py4JJavaError

from pyspark import SparkConf, SparkContext
from pyspark.testing.utils import ReusedPySparkTestCase, PySparkTestCase, QuietTest


class WorkerTests(ReusedPySparkTestCase):
    def test_cancel_task(self):
        temp = tempfile.NamedTemporaryFile(delete=True)
        temp.close()
        path = temp.name

        def sleep(x):
            import os
            import time
            with open(path, 'w') as f:
                f.write("%d %d" % (os.getppid(), os.getpid()))
            time.sleep(100)

        # start job in background thread
        def run():
            try:
                self.sc.parallelize(range(1), 1).foreach(sleep)
            except Exception:
                pass
        import threading
        t = threading.Thread(target=run)
        t.daemon = True
        t.start()

        daemon_pid, worker_pid = 0, 0
        while True:
            if os.path.exists(path):
                with open(path) as f:
                    data = f.read().split(' ')
                daemon_pid, worker_pid = map(int, data)
                break
            time.sleep(0.1)

        # cancel jobs
        self.sc.cancelAllJobs()
        t.join()

        for i in range(50):
            try:
                os.kill(worker_pid, 0)
                time.sleep(0.1)
            except OSError:
                break  # worker was killed
        else:
            self.fail("worker has not been killed after 5 seconds")

        try:
            os.kill(daemon_pid, 0)
        except OSError:
            self.fail("daemon had been killed")

        # run a normal job
        rdd = self.sc.parallelize(range(100), 1)
        self.assertEqual(100, rdd.map(str).count())

    def test_after_exception(self):
        def raise_exception(_):
            raise Exception()
        rdd = self.sc.parallelize(range(100), 1)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: rdd.foreach(raise_exception))
        self.assertEqual(100, rdd.map(str).count())

    def test_after_non_exception_error(self):
        # SPARK-33339: Pyspark application will hang due to non Exception
        def raise_system_exit(_):
            raise SystemExit()
        rdd = self.sc.parallelize(range(100), 1)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: rdd.foreach(raise_system_exit))
        self.assertEqual(100, rdd.map(str).count())

    def test_after_jvm_exception(self):
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write(b"Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name, 1)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: filtered_data.count())

        rdd = self.sc.parallelize(range(100), 1)
        self.assertEqual(100, rdd.map(str).count())

    def test_accumulator_when_reuse_worker(self):
        from pyspark.accumulators import INT_ACCUMULATOR_PARAM
        acc1 = self.sc.accumulator(0, INT_ACCUMULATOR_PARAM)
        self.sc.parallelize(range(100), 20).foreach(lambda x: acc1.add(x))
        self.assertEqual(sum(range(100)), acc1.value)

        acc2 = self.sc.accumulator(0, INT_ACCUMULATOR_PARAM)
        self.sc.parallelize(range(100), 20).foreach(lambda x: acc2.add(x))
        self.assertEqual(sum(range(100)), acc2.value)
        self.assertEqual(sum(range(100)), acc1.value)

    def test_reuse_worker_after_take(self):
        rdd = self.sc.parallelize(range(100000), 1)
        self.assertEqual(0, rdd.first())

        def count():
            try:
                rdd.count()
            except Exception:
                pass

        t = threading.Thread(target=count)
        t.daemon = True
        t.start()
        t.join(5)
        self.assertTrue(not t.is_alive())
        self.assertEqual(100000, rdd.count())

    def test_with_different_versions_of_python(self):
        rdd = self.sc.parallelize(range(10))
        rdd.count()
        version = self.sc.pythonVer
        self.sc.pythonVer = "2.0"
        try:
            with QuietTest(self.sc):
                self.assertRaises(Py4JJavaError, lambda: rdd.count())
        finally:
            self.sc.pythonVer = version

    def test_python_exception_non_hanging(self):
        # SPARK-21045: exceptions with no ascii encoding shall not hanging PySpark.
        try:
            def f():
                raise Exception("exception with 中 and \xd6\xd0")

            self.sc.parallelize([1]).map(lambda x: f()).count()
        except Py4JJavaError as e:
            self.assertRegex(str(e), "exception with 中")


class WorkerReuseTest(PySparkTestCase):

    def test_reuse_worker_of_parallelize_range(self):
        rdd = self.sc.parallelize(range(20), 8)
        previous_pids = rdd.map(lambda x: os.getpid()).collect()
        current_pids = rdd.map(lambda x: os.getpid()).collect()
        for pid in current_pids:
            self.assertTrue(pid in previous_pids)


@unittest.skipIf(
    not has_resource_module,
    "Memory limit feature in Python worker is dependent on "
    "Python's 'resource' module; however, not found.")
class WorkerMemoryTest(unittest.TestCase):

    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.executor.pyspark.memory", "2g")
        self.sc = SparkContext('local[4]', class_name, conf=conf)

    def test_memory_limit(self):
        rdd = self.sc.parallelize(range(1), 1)

        def getrlimit():
            import resource
            return resource.getrlimit(resource.RLIMIT_AS)

        actual = rdd.map(lambda _: getrlimit()).collect()
        self.assertTrue(len(actual) == 1)
        self.assertTrue(len(actual[0]) == 2)
        [(soft_limit, hard_limit)] = actual
        self.assertEqual(soft_limit, 2 * 1024 * 1024 * 1024)
        self.assertEqual(hard_limit, 2 * 1024 * 1024 * 1024)

    def tearDown(self):
        self.sc.stop()

if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_worker import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
