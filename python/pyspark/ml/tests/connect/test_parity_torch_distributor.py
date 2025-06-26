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

import os
import shutil
import unittest

from pyspark.util import is_remote_only
from pyspark.sql import SparkSession
from pyspark.testing import should_test_connect, connect_requirement_message
from pyspark.testing.utils import have_torch, torch_requirement_message

if not is_remote_only() and should_test_connect:
    from pyspark.ml.torch.tests.test_distributor import (
        TorchDistributorBaselineUnitTestsMixin,
        TorchDistributorLocalUnitTestsMixin,
        TorchDistributorDistributedUnitTestsMixin,
        TorchWrapperUnitTestsMixin,
        set_up_test_dirs,
        get_local_mode_conf,
        get_distributed_mode_conf,
    )

    @unittest.skipIf(
        not should_test_connect or not have_torch or is_remote_only(),
        connect_requirement_message or torch_requirement_message or "Requires JVM access",
    )
    class TorchDistributorBaselineUnitTestsOnConnect(
        TorchDistributorBaselineUnitTestsMixin, unittest.TestCase
    ):
        @classmethod
        def setUpClass(cls):
            cls.spark = SparkSession.builder.remote("local[4]").getOrCreate()

        @classmethod
        def tearDownClass(cls):
            cls.spark.stop()

    # @unittest.skipIf(
    #     not have_torch or is_remote_only(), torch_requirement_message or "Requires JVM access"
    # )
    # TODO(SPARK-50864): Re-enable this test after fixing the slowness
    @unittest.skip("Disabled due to slowness")
    class TorchDistributorLocalUnitTestsOnConnect(
        TorchDistributorLocalUnitTestsMixin, unittest.TestCase
    ):
        @classmethod
        def setUpClass(cls):
            (cls.gpu_discovery_script_file_name, cls.mnist_dir_path) = set_up_test_dirs()
            builder = SparkSession.builder.appName(cls.__name__)
            for k, v in get_local_mode_conf().items():
                builder = builder.config(k, v)
            builder = builder.config(
                "spark.driver.resource.gpu.discoveryScript", cls.gpu_discovery_script_file_name
            )
            cls.spark = builder.remote("local-cluster[2,2,512]").getOrCreate()

        @classmethod
        def tearDownClass(cls):
            shutil.rmtree(cls.mnist_dir_path)
            os.unlink(cls.gpu_discovery_script_file_name)
            cls.spark.stop()

        def _get_inputs_for_test_local_training_succeeds(self):
            return [
                ("0,1,2", 1, True, "0,1,2"),
                ("0,1,2", 3, True, "0,1,2"),
                ("0,1,2", 2, False, "0,1,2"),
                (None, 3, False, "NONE"),
            ]

    # @unittest.skipIf(
    #     not have_torch or is_remote_only(), torch_requirement_message or "Requires JVM access"
    # )
    # TODO(SPARK-50864): Re-enable this test after fixing the slowness
    @unittest.skip("Disabled due to slowness")
    class TorchDistributorLocalUnitTestsIIOnConnect(
        TorchDistributorLocalUnitTestsMixin, unittest.TestCase
    ):
        @classmethod
        def setUpClass(cls):
            (cls.gpu_discovery_script_file_name, cls.mnist_dir_path) = set_up_test_dirs()
            builder = SparkSession.builder.appName(cls.__name__)
            for k, v in get_local_mode_conf().items():
                builder = builder.config(k, v)

            builder = builder.config(
                "spark.driver.resource.gpu.discoveryScript", cls.gpu_discovery_script_file_name
            )
            cls.spark = builder.remote("local[4]").getOrCreate()

        @classmethod
        def tearDownClass(cls):
            shutil.rmtree(cls.mnist_dir_path)
            os.unlink(cls.gpu_discovery_script_file_name)
            cls.spark.stop()

        def _get_inputs_for_test_local_training_succeeds(self):
            return [
                ("0,1,2", 1, True, "0,1,2"),
                ("0,1,2", 3, True, "0,1,2"),
                ("0,1,2", 2, False, "0,1,2"),
                (None, 3, False, "NONE"),
            ]

    # @unittest.skipIf(
    #     not have_torch or is_remote_only(), torch_requirement_message or "Requires JVM access"
    # )
    # TODO(SPARK-50864): Re-enable this test after fixing the slowness
    @unittest.skip("Disabled due to slowness")
    class TorchDistributorDistributedUnitTestsOnConnect(
        TorchDistributorDistributedUnitTestsMixin, unittest.TestCase
    ):
        @classmethod
        def setUpClass(cls):
            (cls.gpu_discovery_script_file_name, cls.mnist_dir_path) = set_up_test_dirs()
            builder = SparkSession.builder.appName(cls.__name__)
            for k, v in get_distributed_mode_conf().items():
                builder = builder.config(k, v)

            builder = builder.config(
                "spark.worker.resource.gpu.discoveryScript", cls.gpu_discovery_script_file_name
            )
            cls.spark = builder.remote("local-cluster[2,2,512]").getOrCreate()

        @classmethod
        def tearDownClass(cls):
            shutil.rmtree(cls.mnist_dir_path)
            os.unlink(cls.gpu_discovery_script_file_name)
            cls.spark.stop()

    @unittest.skipIf(
        not should_test_connect or not have_torch or is_remote_only(),
        connect_requirement_message or torch_requirement_message or "Requires JVM access",
    )
    class TorchWrapperUnitTestsOnConnect(TorchWrapperUnitTestsMixin, unittest.TestCase):
        pass


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_parity_torch_distributor import *  # noqa: F401,F403

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
