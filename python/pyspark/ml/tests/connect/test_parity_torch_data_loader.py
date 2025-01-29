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

from pyspark.util import is_remote_only
from pyspark.sql import SparkSession
from pyspark.testing.utils import have_torch, torch_requirement_message

if not is_remote_only():
    from pyspark.ml.torch.tests.test_data_loader import TorchDistributorDataLoaderUnitTests

    @unittest.skipIf(
        not have_torch or is_remote_only(), torch_requirement_message or "Requires JVM access"
    )
    class TorchDistributorBaselineUnitTestsOnConnect(TorchDistributorDataLoaderUnitTests):
        def setUp(self) -> None:
            self.spark = (
                SparkSession.builder.remote("local[1]")
                .config("spark.default.parallelism", "1")
                .getOrCreate()
            )


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_parity_torch_data_loader import *  # noqa: F401,F403

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
