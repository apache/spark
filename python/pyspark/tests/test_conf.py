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
import random
import unittest

from pyspark import SparkContext, SparkConf


class ConfTests(unittest.TestCase):
    def test_memory_conf(self):
        memoryList = ["1T", "1G", "1M", "1024K"]
        for memory in memoryList:
            sc = SparkContext(conf=SparkConf().set("spark.python.worker.memory", memory))
            l = list(range(1024))
            random.shuffle(l)
            rdd = sc.parallelize(l, 4)
            self.assertEqual(sorted(l), rdd.sortBy(lambda x: x).collect())
            sc.stop()


if __name__ == "__main__":
    from pyspark.tests.test_conf import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
