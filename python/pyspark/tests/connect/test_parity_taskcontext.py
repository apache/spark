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

from pyspark.tests.test_taskcontext import TaskContextTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class TaskContextParityTests(TaskContextTestsMixin, ReusedConnectTestCase):
    @property
    def sc(self):
        return self.spark.sparkContext

    @unittest.skip(
        "Connect SparkContext does not expose setLocalProperty/getLocalProperty to propagate "
        "driver local properties into TaskContext yet."
    )
    def test_get_local_property(self):
        super().test_get_local_property()

    @unittest.skip(
        "Spark Connect does not mirror classic speculative task retry semantics for recoverable "
        "Python failures in mapInArrow; the job aborts instead of exposing a second attempt "
        "on the failing partition."
    )
    def test_attempt_number(self):
        super().test_attempt_number()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
