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
import shutil
import tempfile
import threading
import time
import unittest
from collections import namedtuple

from pyspark import SparkFiles, SparkContext
from pyspark.testing.utils import ReusedPySparkTestCase, PySparkTestCase, QuietTest, SPARK_HOME


class CheckpointTests(ReusedPySparkTestCase):

    def setUp(self):
        self.checkpointDir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.checkpointDir.name)
        self.sc.setCheckpointDir(self.checkpointDir.name)

    def tearDown(self):
        shutil.rmtree(self.checkpointDir.name)

    def test_basic_checkpointing(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)
        self.assertEqual("file:" + self.checkpointDir.name,
                         os.path.dirname(os.path.dirname(flatMappedRDD.getCheckpointFile())))

    def test_checkpoint_and_restore(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: [x])

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        flatMappedRDD.count()  # forces a checkpoint to be computed
        time.sleep(1)  # 1 second

        self.assertTrue(flatMappedRDD.getCheckpointFile() is not None)
        recovered = self.sc._checkpointFile(flatMappedRDD.getCheckpointFile(),
                                            flatMappedRDD._jrdd_deserializer)
        self.assertEqual([1, 2, 3, 4], recovered.collect())


class LocalCheckpointTests(ReusedPySparkTestCase):

    def test_basic_localcheckpointing(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertFalse(flatMappedRDD.isLocallyCheckpointed())

        flatMappedRDD.localCheckpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.isLocallyCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)


class AddFileTests(PySparkTestCase):

    def test_add_py_file(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this job fails due to `userlibrary` not being on the Python path:
        # disable logging in log4j temporarily
        def func(x):
            from userlibrary import UserClass
            return UserClass().hello()
        with QuietTest(self.sc):
            self.assertRaises(Exception, self.sc.parallelize(range(2)).map(func).first)

        # Add the file, so the job should now succeed:
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addPyFile(path)
        res = self.sc.parallelize(range(2)).map(func).first()
        self.assertEqual("Hello World!", res)

    def test_add_file_locally(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        self.sc.addFile(path)
        download_path = SparkFiles.get("hello.txt")
        self.assertNotEqual(path, download_path)
        with open(download_path) as test_file:
            self.assertEqual("Hello World!\n", test_file.readline())

    def test_add_file_recursively_locally(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello")
        self.sc.addFile(path, True)
        download_path = SparkFiles.get("hello")
        self.assertNotEqual(path, download_path)
        with open(download_path + "/hello.txt") as test_file:
            self.assertEqual("Hello World!\n", test_file.readline())
        with open(download_path + "/sub_hello/sub_hello.txt") as test_file:
            self.assertEqual("Sub Hello World!\n", test_file.readline())

    def test_add_py_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlibrary import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addPyFile(path)
        from userlibrary import UserClass
        self.assertEqual("Hello World!", UserClass().hello())

    def test_add_egg_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlib import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlib-0.1.zip")
        self.sc.addPyFile(path)
        from userlib import UserClass
        self.assertEqual("Hello World from inside a package!", UserClass().hello())

    def test_overwrite_system_module(self):
        self.sc.addPyFile(os.path.join(SPARK_HOME, "python/test_support/SimpleHTTPServer.py"))

        import SimpleHTTPServer
        self.assertEqual("My Server", SimpleHTTPServer.__name__)

        def func(x):
            import SimpleHTTPServer
            return SimpleHTTPServer.__name__

        self.assertEqual(["My Server"], self.sc.parallelize(range(1)).map(func).collect())


class ContextTests(unittest.TestCase):

    def test_failed_sparkcontext_creation(self):
        # Regression test for SPARK-1550
        self.assertRaises(Exception, lambda: SparkContext("an-invalid-master-name"))

    def test_get_or_create(self):
        with SparkContext.getOrCreate() as sc:
            self.assertTrue(SparkContext.getOrCreate() is sc)

    def test_parallelize_eager_cleanup(self):
        with SparkContext() as sc:
            temp_files = os.listdir(sc._temp_dir)
            rdd = sc.parallelize([0, 1, 2])
            post_parallalize_temp_files = os.listdir(sc._temp_dir)
            self.assertEqual(temp_files, post_parallalize_temp_files)

    def test_set_conf(self):
        # This is for an internal use case. When there is an existing SparkContext,
        # SparkSession's builder needs to set configs into SparkContext's conf.
        sc = SparkContext()
        sc._conf.set("spark.test.SPARK16224", "SPARK16224")
        self.assertEqual(sc._jsc.sc().conf().get("spark.test.SPARK16224"), "SPARK16224")
        sc.stop()

    def test_stop(self):
        sc = SparkContext()
        self.assertNotEqual(SparkContext._active_spark_context, None)
        sc.stop()
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with(self):
        with SparkContext() as sc:
            self.assertNotEqual(SparkContext._active_spark_context, None)
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with_exception(self):
        try:
            with SparkContext() as sc:
                self.assertNotEqual(SparkContext._active_spark_context, None)
                raise Exception()
        except:
            pass
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with_stop(self):
        with SparkContext() as sc:
            self.assertNotEqual(SparkContext._active_spark_context, None)
            sc.stop()
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_progress_api(self):
        with SparkContext() as sc:
            sc.setJobGroup('test_progress_api', '', True)
            rdd = sc.parallelize(range(10)).map(lambda x: time.sleep(100))

            def run():
                try:
                    rdd.count()
                except Exception:
                    pass
            t = threading.Thread(target=run)
            t.daemon = True
            t.start()
            # wait for scheduler to start
            time.sleep(1)

            tracker = sc.statusTracker()
            jobIds = tracker.getJobIdsForGroup('test_progress_api')
            self.assertEqual(1, len(jobIds))
            job = tracker.getJobInfo(jobIds[0])
            self.assertEqual(1, len(job.stageIds))
            stage = tracker.getStageInfo(job.stageIds[0])
            self.assertEqual(rdd.getNumPartitions(), stage.numTasks)

            sc.cancelAllJobs()
            t.join()
            # wait for event listener to update the status
            time.sleep(1)

            job = tracker.getJobInfo(jobIds[0])
            self.assertEqual('FAILED', job.status)
            self.assertEqual([], tracker.getActiveJobsIds())
            self.assertEqual([], tracker.getActiveStageIds())

            sc.stop()

    def test_startTime(self):
        with SparkContext() as sc:
            self.assertGreater(sc.startTime, 0)

    def test_forbid_insecure_gateway(self):
        # Fail immediately if you try to create a SparkContext
        # with an insecure gateway
        parameters = namedtuple('MockGatewayParameters', 'auth_token')(None)
        mock_insecure_gateway = namedtuple('MockJavaGateway', 'gateway_parameters')(parameters)
        with self.assertRaises(ValueError) as context:
            SparkContext(gateway=mock_insecure_gateway)
        self.assertIn("insecure Py4j gateway", str(context.exception))


if __name__ == "__main__":
    from pyspark.tests.test_context import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
