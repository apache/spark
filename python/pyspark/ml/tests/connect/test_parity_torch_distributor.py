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
import tempfile
import unittest

have_torch = True
try:
    import torch  # noqa: F401
except ImportError:
    have_torch = False

from pyspark.sql import SparkSession

from pyspark.ml.torch.distributor import TorchDistributor

from pyspark.ml.torch.tests.test_distributor import (
    TorchDistributorBaselineUnitTestsMixin,
    TorchDistributorLocalUnitTestsMixin,
    TorchDistributorDistributedUnitTestsMixin,
    TorchWrapperUnitTestsMixin,
)


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorBaselineUnitTestsOnConnect(
    TorchDistributorBaselineUnitTestsMixin, unittest.TestCase
):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.remote("local[4]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()

    def test_get_num_tasks_fails(self) -> None:
        inputs = [1, 5, 4]

        # This is when the conf isn't set and we request GPUs
        for num_processes in inputs:
            with self.subTest():
                # TODO(SPARK-42994): Support sc.resources
                # with self.assertRaisesRegex(RuntimeError, "driver"):
                #     TorchDistributor(num_processes, True, True)
                with self.assertRaisesRegex(RuntimeError, "unset"):
                    TorchDistributor(num_processes, False, True)


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorLocalUnitTestsOnConnect(
    TorchDistributorLocalUnitTestsMixin, unittest.TestCase
):
    def setUp(self) -> None:
        class_name = self.__class__.__name__
        conf = self._get_spark_conf()
        builder = SparkSession.builder.appName(class_name)
        for k, v in conf.getAll():
            if k not in ["spark.master", "spark.remote", "spark.app.name"]:
                builder = builder.config(k, v)
        self.spark = builder.remote("local-cluster[2,2,1024]").getOrCreate()
        self.mnist_dir_path = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.mnist_dir_path)
        os.unlink(self.gpu_discovery_script_file.name)
        self.spark.stop()

    # TODO(SPARK-42994): Support sc.resources
    @unittest.skip("need to support sc.resources")
    def test_get_num_tasks_locally(self):
        super().test_get_num_tasks_locally()

    # TODO(SPARK-42994): Support sc.resources
    @unittest.skip("need to support sc.resources")
    def test_get_gpus_owned_local(self):
        super().test_get_gpus_owned_local()

    # TODO(SPARK-42994): Support sc.resources
    @unittest.skip("need to support sc.resources")
    def test_local_training_succeeds(self):
        super().test_local_training_succeeds()


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorDistributedUnitTestsOnConnect(
    TorchDistributorDistributedUnitTestsMixin, unittest.TestCase
):
    def setUp(self) -> None:
        class_name = self.__class__.__name__
        conf = self._get_spark_conf()
        builder = SparkSession.builder.appName(class_name)
        for k, v in conf.getAll():
            if k not in ["spark.master", "spark.remote", "spark.app.name"]:
                builder = builder.config(k, v)

        self.spark = builder.remote("local-cluster[2,2,1024]").getOrCreate()
        self.mnist_dir_path = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.mnist_dir_path)
        os.unlink(self.gpu_discovery_script_file.name)
        self.spark.stop()


@unittest.skipIf(not have_torch, "torch is required")
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
