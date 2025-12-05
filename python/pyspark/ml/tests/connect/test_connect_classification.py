# -*- coding: utf-8 -*-
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
import os

from pyspark.util import is_remote_only
from pyspark.ml.tests.connect.test_legacy_mode_classification import ClassificationTestsMixin
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.utils import have_torch, torch_requirement_message
from pyspark.testing.connectutils import ReusedConnectTestCase


@unittest.skipIf(
    not should_test_connect or not have_torch or is_remote_only(),
    connect_requirement_message
    or torch_requirement_message
    or "Requires PySpark core library in Spark Connect server",
)
class ClassificationTestsOnConnect(ClassificationTestsMixin, ReusedConnectTestCase):
    @classmethod
    def conf(cls):
        config = super(ClassificationTestsOnConnect, cls).conf()
        config.set("spark.sql.artifact.copyFromLocalToFs.allowDestLocal", "true")
        return config

    @classmethod
    def master(cls):
        return os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[2]")


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_connect_classification import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
