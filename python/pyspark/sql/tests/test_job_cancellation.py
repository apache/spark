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
import threading
import time

from pyspark import InheritableThread, inheritable_thread_target
from pyspark.testing.sqlutils import ReusedSQLTestCase


class JobCancellationTestsMixin:
    def test_tags(self):
        self.spark.clearTags()
        self.spark.addTag("a")
        self.assertEqual(self.spark.getTags(), {"a"})
        self.spark.addTag("b")
        self.spark.removeTag("a")
        self.assertEqual(self.spark.getTags(), {"b"})
        self.spark.addTag("c")
        self.spark.clearTags()
        self.assertEqual(self.spark.getTags(), set())
        self.spark.clearTags()

    def test_tags_multithread(self):
        output1 = None
        output2 = None

        def tag1():
            nonlocal output1

            self.spark.addTag("tag1")
            output1 = self.spark.getTags()

        def tag2():
            nonlocal output2

            self.spark.addTag("tag2")
            output2 = self.spark.getTags()

        t1 = threading.Thread(target=tag1)
        t1.start()
        t1.join()
        t2 = threading.Thread(target=tag2)
        t2.start()
        t2.join()

        self.assertIsNotNone(output1)
        self.assertEquals(output1, {"tag1"})
        self.assertIsNotNone(output2)
        self.assertEquals(output2, {"tag2"})

    def check_job_cancellation(
        self, setter, canceller, thread_ids, thread_ids_to_cancel, thread_ids_to_run
    ):
        job_id_a = "job_ids_to_cancel"
        job_id_b = "job_ids_to_run"
        threads = []

        # A list which records whether job is cancelled.
        # The index of the array is the thread index which job run in.
        is_job_cancelled = [False for _ in thread_ids]

        def run_job(job_id, index):
            """
            Executes a job with the group ``job_group``. Each job waits for 3 seconds
            and then exits.
            """
            try:
                setter(job_id)

                def func(itr):
                    for pdf in itr:
                        time.sleep(pdf._1.iloc[0])
                        yield pdf

                self.spark.createDataFrame([[20]]).repartition(1).mapInPandas(
                    func, schema="_1 LONG"
                ).collect()
                is_job_cancelled[index] = False
            except Exception:
                # Assume that exception means job cancellation.
                is_job_cancelled[index] = True

        # Test if job succeeded when not cancelled.
        run_job(job_id_a, 0)
        self.assertFalse(is_job_cancelled[0])
        self.spark.clearTags()

        # Run jobs
        for i in thread_ids_to_cancel:
            t = threading.Thread(target=run_job, args=(job_id_a, i))
            t.start()
            threads.append(t)

        for i in thread_ids_to_run:
            t = threading.Thread(target=run_job, args=(job_id_b, i))
            t.start()
            threads.append(t)

        # Wait to make sure all jobs are executed.
        time.sleep(10)
        # And then, cancel one job group.
        canceller(job_id_a)

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

    def test_inheritable_tags(self):
        self.check_inheritable_tags(
            create_thread=lambda target, session: InheritableThread(target, session=session)
        )
        self.check_inheritable_tags(
            create_thread=lambda target, session: threading.Thread(
                target=inheritable_thread_target(session)(target)
            )
        )

    def check_inheritable_tags(self, create_thread):
        spark = self.spark
        spark.addTag("a")
        first = set()
        second = set()

        def get_inner_local_prop():
            spark.addTag("c")
            second.update(spark.getTags())

        def get_outer_local_prop():
            spark.addTag("b")
            first.update(spark.getTags())
            t2 = create_thread(target=get_inner_local_prop, session=spark)
            t2.start()
            t2.join()

        t1 = create_thread(target=get_outer_local_prop, session=spark)
        t1.start()
        t1.join()

        self.assertEqual(spark.getTags(), {"a"})
        self.assertEqual(first, {"a", "b"})
        self.assertEqual(second, {"a", "b", "c"})

    def test_interrupt_tag(self):
        thread_ids = range(4)
        self.check_job_cancellation(
            lambda job_group: self.spark.addTag(job_group),
            lambda job_group: self.spark.interruptTag(job_group),
            thread_ids,
            [i for i in thread_ids if i % 2 == 0],
            [i for i in thread_ids if i % 2 != 0],
        )
        self.spark.clearTags()

    def test_interrupt_all(self):
        thread_ids = range(4)
        self.check_job_cancellation(
            lambda job_group: None,
            lambda job_group: self.spark.interruptAll(),
            thread_ids,
            thread_ids,
            [],
        )
        self.spark.clearTags()


class JobCancellationTests(JobCancellationTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_job_cancellation import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
