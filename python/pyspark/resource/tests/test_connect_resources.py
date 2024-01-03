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

from pyspark.resource import ResourceProfileBuilder, TaskResourceRequests
from pyspark.sql import SparkSession


class ResourceProfileTests(unittest.TestCase):
    def test_profile_before_sc_for_connect(self):
        rpb = ResourceProfileBuilder()
        treqs = TaskResourceRequests().cpus(2)
        # no exception for building ResourceProfile
        rp = rpb.require(treqs).build

        spark = SparkSession.builder.remote("local-cluster[1, 2, 1024]").getOrCreate()
        df = spark.range(10)
        df.mapInPandas(lambda x: x, df.schema, False, rp).collect()
        df.mapInArrow(lambda x: x, df.schema, False, rp).collect()
        spark.stop()


if __name__ == "__main__":
    from pyspark.resource.tests.test_connect_resources import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
