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

from pyspark import TaskContext
from pyspark.resource import TaskResourceRequests, ResourceProfileBuilder
from pyspark.sql import SparkSession
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import ReusedPySparkTestCase


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class ResourceProfileTestsMixin(object):
    def test_map_in_arrow_without_profile(self):
        def func(iterator):
            tc = TaskContext.get()
            assert tc.cpus() == 1
            for batch in iterator:
                yield batch

        df = self.spark.range(10)
        df.mapInArrow(func, "id long").show(n=10)

    def test_map_in_arrow_with_profile(self):
        def func(iterator):
            tc = TaskContext.get()
            assert tc.cpus() == 3
            for batch in iterator:
                yield batch

        df = self.spark.range(10)

        treqs = TaskResourceRequests().cpus(3)
        rp = ResourceProfileBuilder().require(treqs).build
        df.mapInArrow(func, "id long", False, rp).show(n=10)

    def test_map_in_pandas_without_profile(self):
        def func(iterator):
            tc = TaskContext.get()
            assert tc.cpus() == 1
            for batch in iterator:
                yield batch

        df = self.spark.range(10)
        df.mapInPandas(func, "id long").show(n=10)

    def test_map_in_pandas_with_profile(self):
        def func(iterator):
            tc = TaskContext.get()
            assert tc.cpus() == 3
            for batch in iterator:
                yield batch

        df = self.spark.range(10)

        treqs = TaskResourceRequests().cpus(3)
        rp = ResourceProfileBuilder().require(treqs).build
        df.mapInPandas(func, "id long", False, rp).show(n=10)


class ResourceProfileTests(ResourceProfileTestsMixin, ReusedPySparkTestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.core.context import SparkContext

        cls.sc = SparkContext("local-cluster[1, 4, 1024]", cls.__name__, conf=cls.conf())
        cls.spark = SparkSession(cls.sc)

    @classmethod
    def tearDownClass(cls):
        super(ResourceProfileTests, cls).tearDownClass()
        cls.spark.stop()


if __name__ == "__main__":
    from pyspark.sql.tests.test_resources import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
