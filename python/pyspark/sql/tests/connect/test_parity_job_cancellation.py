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

import threading
from unittest import mock

from pyspark import inheritable_thread_target
from pyspark.errors import PySparkNotImplementedError, PySparkTypeError
from pyspark.sql.tests.test_job_cancellation import JobCancellationTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.core.context import SparkContext as SparkContextFacade
from pyspark.core.connect.context import SparkContext as ConnectSparkContext


class JobCancellationParityTests(JobCancellationTestsMixin, ReusedConnectTestCase):
    def _verify_connect_spark_context_facades(self) -> None:
        """Connect SparkContext facade ctor rules (``SparkContext`` from ``pyspark.core``)."""

        self.assertIs(SparkContextFacade.getOrCreate(), self.spark.sparkContext)
        self.assertIs(SparkContextFacade.getOrCreate(), SparkContextFacade.getOrCreate())
        wrapped = ConnectSparkContext(self.spark)
        self.assertIs(wrapped._session, self.spark)
        self.assertIs(wrapped, self.spark.sparkContext)
        with self.assertRaises(PySparkTypeError):
            ConnectSparkContext(self.spark, appName="should-not-be-used")

        with self.assertRaises(PySparkNotImplementedError):
            ConnectSparkContext("local", self.__class__.__name__, gateway=mock.Mock())

        with self.assertRaises(PySparkNotImplementedError):
            ConnectSparkContext(
                "local",
                self.__class__.__name__,
                jsc=mock.Mock(),
            )

    def test_interrupt_all(self):
        self._verify_connect_spark_context_facades()

        thread_ids = range(4)
        self.check_job_cancellation(
            lambda job_group: None,
            lambda job_group: self.spark.interruptAll(),
            thread_ids,
            thread_ids,
            [],
        )
        self.spark.clearTags()

        self.check_job_cancellation(
            lambda job_group: None,
            lambda _: self.spark.sparkContext.cancelAllJobs(),
            thread_ids,
            thread_ids,
            [],
        )
        self.spark.clearTags()

    def test_inheritable_tags_with_deco(self):
        @inheritable_thread_target(self.spark)
        def func(target):
            return target()

        self.check_inheritable_tags(
            create_thread=lambda target, session: threading.Thread(target=func, args=(target,))
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
