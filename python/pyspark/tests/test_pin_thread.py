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
import time
import threading
import unittest

from pyspark import SparkContext, SparkConf, InheritableThread


class PinThreadTests(unittest.TestCase):
    # These tests are in a separate class because it uses
    # 'PYSPARK_PIN_THREAD' environment variable to test thread pin feature.

    @classmethod
    def setUpClass(cls):
        cls.old_pin_thread = os.environ.get("PYSPARK_PIN_THREAD")
        os.environ["PYSPARK_PIN_THREAD"] = "true"
        cls.sc = SparkContext("local[4]", cls.__name__, conf=SparkConf())

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        if cls.old_pin_thread is not None:
            os.environ["PYSPARK_PIN_THREAD"] = cls.old_pin_thread
        else:
            del os.environ["PYSPARK_PIN_THREAD"]

    def test_pinned_thread(self):
        threads = []
        exceptions = []
        property_name = "test_property_%s" % PinThreadTests.__name__
        jvm_thread_ids = []

        for i in range(10):

            def test_local_property():
                jvm_thread_id = self.sc._jvm.java.lang.Thread.currentThread().getId()
                jvm_thread_ids.append(jvm_thread_id)

                # If a property is set in this thread, later it should get the same property
                # within this thread.
                self.sc.setLocalProperty(property_name, str(i))

                # 5 threads, 1 second sleep. 5 threads without a sleep.
                time.sleep(i % 2)

                try:
                    assert self.sc.getLocalProperty(property_name) == str(i)

                    # Each command might create a thread in multi-threading mode in Py4J.
                    # This assert makes sure that the created thread is being reused.
                    assert jvm_thread_id == self.sc._jvm.java.lang.Thread.currentThread().getId()
                except Exception as e:
                    exceptions.append(e)

            threads.append(threading.Thread(target=test_local_property))

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        for e in exceptions:
            raise e

        # Created JVM threads should be 10 because Python thread are 10.
        assert len(set(jvm_thread_ids)) == 10

    def test_multiple_group_jobs(self):
        # SPARK-22340 Add a mode to pin Python thread into JVM's

        group_a = "job_ids_to_cancel"
        group_b = "job_ids_to_run"

        threads = []
        thread_ids = range(4)
        thread_ids_to_cancel = [i for i in thread_ids if i % 2 == 0]
        thread_ids_to_run = [i for i in thread_ids if i % 2 != 0]

        # A list which records whether job is cancelled.
        # The index of the array is the thread index which job run in.
        is_job_cancelled = [False for _ in thread_ids]

        def run_job(job_group, index):
            """
            Executes a job with the group ``job_group``. Each job waits for 3 seconds
            and then exits.
            """
            try:
                self.sc.setJobGroup(job_group, "test rdd collect with setting job group")
                self.sc.parallelize([15]).map(lambda x: time.sleep(x)).collect()
                is_job_cancelled[index] = False
            except Exception:
                # Assume that exception means job cancellation.
                is_job_cancelled[index] = True

        # Test if job succeeded when not cancelled.
        run_job(group_a, 0)
        self.assertFalse(is_job_cancelled[0])

        # Run jobs
        for i in thread_ids_to_cancel:
            t = threading.Thread(target=run_job, args=(group_a, i))
            t.start()
            threads.append(t)

        for i in thread_ids_to_run:
            t = threading.Thread(target=run_job, args=(group_b, i))
            t.start()
            threads.append(t)

        # Wait to make sure all jobs are executed.
        time.sleep(3)
        # And then, cancel one job group.
        self.sc.cancelJobGroup(group_a)

        # Wait until all threads launching jobs are finished.
        for t in threads:
            t.join()

        for i in thread_ids_to_cancel:
            self.assertTrue(
                is_job_cancelled[i], "Thread {i}: Job in group A was not cancelled.".format(i=i)
            )

        for i in thread_ids_to_run:
            self.assertFalse(
                is_job_cancelled[i], "Thread {i}: Job in group B did not succeeded.".format(i=i)
            )

    def test_inheritable_local_property(self):
        self.sc.setLocalProperty("a", "hi")
        expected = []

        def get_inner_local_prop():
            expected.append(self.sc.getLocalProperty("b"))

        def get_outer_local_prop():
            expected.append(self.sc.getLocalProperty("a"))
            self.sc.setLocalProperty("b", "hello")
            t2 = InheritableThread(target=get_inner_local_prop)
            t2.start()
            t2.join()

        t1 = InheritableThread(target=get_outer_local_prop)
        t1.start()
        t1.join()

        self.assertEqual(self.sc.getLocalProperty("b"), None)
        self.assertEqual(expected, ["hi", "hello"])


if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_pin_thread import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
