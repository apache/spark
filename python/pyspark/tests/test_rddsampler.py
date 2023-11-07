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
from pyspark.testing.utils import ReusedPySparkTestCase
from pyspark.rddsampler import RDDSampler, RDDStratifiedSampler


class RDDSamplerTests(ReusedPySparkTestCase):
    def test_rdd_sampler_func(self):
        # SPARK-38879: Test case to improve test coverage for RDDSampler
        # RDDSampler.func
        rdd = self.sc.parallelize(range(20), 2)
        sample_count = rdd.mapPartitionsWithIndex(RDDSampler(False, 0.4, 10).func).count()
        self.assertGreater(sample_count, 3)
        self.assertLess(sample_count, 10)
        sample_data = rdd.mapPartitionsWithIndex(RDDSampler(True, 1, 10).func).collect()
        sample_data.sort()
        # check if at least one element is repeated.
        self.assertTrue(
            any(sample_data[i] == sample_data[i - 1] for i in range(1, len(sample_data)))
        )

    def test_rdd_stratified_sampler_func(self):
        # SPARK-38879: Test case to improve test coverage for RDDSampler
        # RDDStratifiedSampler.func

        fractions = {"a": 0.8, "b": 0.2}
        rdd = self.sc.parallelize(fractions.keys()).cartesian(self.sc.parallelize(range(0, 100)))
        sample_data = dict(
            rdd.mapPartitionsWithIndex(
                RDDStratifiedSampler(False, fractions, 10).func, True
            ).countByKey()
        )
        # Since a have higher sampling rate (0.8),
        # it will occur more number of times than b.
        self.assertGreater(sample_data["a"], sample_data["b"])
        self.assertGreater(sample_data["a"], 60)
        self.assertLess(sample_data["a"], 90)
        self.assertGreater(sample_data["b"], 15)
        self.assertLess(sample_data["b"], 30)


if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_rddsampler import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
