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

from pyspark.ml.torch.tests.test_distributor import (
    TorchDistributorBaselineUnitTestsMixin,
    TorchDistributorLocalUnitTestsMixin,
    TorchDistributorDistributedUnitTestsMixin,
    TorchWrapperUnitTestsMixin,
)
from pyspark.testing.utils import timeout


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorBaselineUnitTestsOnConnect(
    TorchDistributorBaselineUnitTestsMixin, unittest.TestCase
):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.remote("local[4]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


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

    def _get_inputs_for_test_local_training_succeeds(self):
        return [
            ("0,1,2", 1, True, "0,1,2"),
            ("0,1,2", 3, True, "0,1,2"),
            ("0,1,2", 2, False, "0,1,2"),
            (None, 3, False, "NONE"),
        ]

    def test_get_num_tasks_locally(self):
        with timeout(20):
            super().test_get_num_tasks_locally()

    def test_get_gpus_owned_local(self):
        with timeout(20):
            super().test_get_gpus_owned_local()

    def test_local_training_succeeds(self):
        with timeout(20):
            super().test_local_training_succeeds()

    def test_local_file_with_pytorch(self):
        with timeout(120):
            super().test_local_file_with_pytorch()

    def test_end_to_end_run_locally(self):
        with timeout(120):
            super().test_end_to_end_run_locally()


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorLocalUnitTestsIIOnConnect(
    TorchDistributorLocalUnitTestsMixin, unittest.TestCase
):
    def setUp(self) -> None:
        class_name = self.__class__.__name__
        conf = self._get_spark_conf()
        builder = SparkSession.builder.appName(class_name)
        for k, v in conf.getAll():
            if k not in ["spark.master", "spark.remote", "spark.app.name"]:
                builder = builder.config(k, v)
        self.spark = builder.remote("local[4]").getOrCreate()
        self.mnist_dir_path = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.mnist_dir_path)
        os.unlink(self.gpu_discovery_script_file.name)
        self.spark.stop()

    def _get_inputs_for_test_local_training_succeeds(self):
        return [
            ("0,1,2", 1, True, "0,1,2"),
            ("0,1,2", 3, True, "0,1,2"),
            ("0,1,2", 2, False, "0,1,2"),
            (None, 3, False, "NONE"),
        ]

    def test_get_num_tasks_locally(self):
        with timeout(20):
            super().test_get_num_tasks_locally()

    def test_get_gpus_owned_local(self):
        with timeout(20):
            super().test_get_gpus_owned_local()

    def test_local_training_succeeds(self):
        with timeout(20):
            super().test_local_training_succeeds()

    def test_local_file_with_pytorch(self):
        with timeout(120):
            super().test_local_file_with_pytorch()

    def test_end_to_end_run_locally(self):
        with timeout(120):
            super().test_end_to_end_run_locally()


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

    def test_get_num_tasks_distributed(self):
        with timeout(120):
            super().test_get_num_tasks_distributed()

    def test_dist_training_succeeds(self):
        with timeout(120):
            super().test_dist_training_succeeds()

    def test_distributed_file_with_pytorch(self):
        with timeout(120):
            super().test_distributed_file_with_pytorch()

    def test_end_to_end_run_distributedly(self):
        with timeout(120):
            super().test_end_to_end_run_distributedly()


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
