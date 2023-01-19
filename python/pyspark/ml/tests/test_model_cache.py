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

from pyspark.ml.model_cache import ModelCache
from pyspark.testing.mlutils import SparkSessionTestCase
from uuid import uuid4


class ModelCacheTests(SparkSessionTestCase):
    def setUp(self):
        super(ModelCacheTests, self).setUp()

    def test_cache(self):
        def predict_fn(inputs):
            return inputs

        # add 10 items, expect last 3 items in cache
        uuids = [uuid4() for i in range(10)]
        for uuid in uuids:
            ModelCache.add(uuid, predict_fn)

        self.assertTrue(len(ModelCache._models) == 3)
        self.assertTrue(list(ModelCache._models.keys()) == uuids[7:10])

        # get item, expect it to become most recently used
        _ = ModelCache.get(uuids[8])
        expected_uuids = uuids[7:8] + uuids[9:10] + [uuids[8]]
        self.assertTrue(list(ModelCache._models.keys()) == expected_uuids)


if __name__ == "__main__":
    from pyspark.ml.tests.test_model_cache import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
