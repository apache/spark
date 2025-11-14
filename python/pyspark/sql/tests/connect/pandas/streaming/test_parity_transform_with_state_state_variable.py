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

from pyspark.sql.tests.pandas.streaming.test_transform_with_state_state_variable import (
    TransformWithStateInPySparkStateVariableTestsMixin,
)
from pyspark import SparkConf
from pyspark.testing.connectutils import ReusedConnectTestCase


class TransformWithStateInPySparkStateVariableParityTests(
    TransformWithStateInPySparkStateVariableTestsMixin, ReusedConnectTestCase
):
    """
    Spark connect parity tests for TransformWithStateInPySparkStateVariable. Run every test case in
     `TransformWithStateInPySparkStateVariableTestsMixin` in spark connect mode.
    """

    @classmethod
    def conf(cls):
        # Due to multiple inheritance from the same level, we need to explicitly setting configs in
        # both TransformWithStateInPySparkStateVariableTestsMixin and ReusedConnectTestCase here
        cfg = SparkConf(loadDefaults=False)
        for base in cls.__bases__:
            if hasattr(base, "conf"):
                parent_cfg = base.conf()
                for k, v in parent_cfg.getAll():
                    cfg.set(k, v)

        # Extra removing config for connect suites
        if cfg._jconf is not None:
            cfg._jconf.remove("spark.master")

        return cfg


if __name__ == "__main__":
    from pyspark.sql.tests.connect.pandas.streaming.test_parity_transform_with_state_state_variable import *  # noqa: F401,E501

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
