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
import stat
import tempfile
import unittest

have_torch = True
try:
    import torch  # noqa: F401
except ImportError:
    have_torch = False

from pyspark.sql import SparkSession
from pyspark.testing.utils import SPARK_HOME

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
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.remote("local[4]").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorLocalUnitTestsOnConnect(
    TorchDistributorLocalUnitTestsMixin, unittest.TestCase
):
    @classmethod
    def setUpClass(cls):
        cls.gpu_discovery_script_file = tempfile.NamedTemporaryFile(delete=False)
        cls.gpu_discovery_script_file.write(
            b'echo {\\"name\\": \\"gpu\\", \\"addresses\\": [\\"0\\",\\"1\\",\\"2\\"]}'
        )
        cls.gpu_discovery_script_file.close()
        # create temporary directory for Worker resources coordination
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        os.chmod(
            cls.gpu_discovery_script_file.name,
            stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP | stat.S_IROTH | stat.S_IXOTH,
        )
        cls.mnist_dir_path = tempfile.mkdtemp()

        cls.spark = (
            SparkSession.builder.appName("TorchDistributorLocalUnitTestsOnConnect")
            .config("spark.test.home", SPARK_HOME)
            .config("spark.driver.resource.gpu.amount", "3")
            .config("spark.driver.resource.gpu.discoveryScript", cls.gpu_discovery_script_file.name)
            .remote("local-cluster[2,2,1024]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.mnist_dir_path)
        os.unlink(cls.gpu_discovery_script_file.name)
        cls.spark.stop()

    def _get_inputs_for_test_local_training_succeeds(self):
        return [
            ("0,1,2", 1, True, "0,1,2"),
            ("0,1,2", 3, True, "0,1,2"),
            ("0,1,2", 2, False, "0,1,2"),
            (None, 3, False, "NONE"),
        ]


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorLocalUnitTestsIIOnConnect(
    TorchDistributorLocalUnitTestsMixin, unittest.TestCase
):
    @classmethod
    def setUpClass(cls):
        cls.gpu_discovery_script_file = tempfile.NamedTemporaryFile(delete=False)
        cls.gpu_discovery_script_file.write(
            b'echo {\\"name\\": \\"gpu\\", \\"addresses\\": [\\"0\\",\\"1\\",\\"2\\"]}'
        )
        cls.gpu_discovery_script_file.close()
        # create temporary directory for Worker resources coordination
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        os.chmod(
            cls.gpu_discovery_script_file.name,
            stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP | stat.S_IROTH | stat.S_IXOTH,
        )
        cls.mnist_dir_path = tempfile.mkdtemp()

        cls.spark = (
            SparkSession.builder.appName("TorchDistributorLocalUnitTestsOnConnect")
            .config("spark.test.home", SPARK_HOME)
            .config("spark.driver.resource.gpu.amount", "3")
            .config("spark.driver.resource.gpu.discoveryScript", cls.gpu_discovery_script_file.name)
            .remote("local[4]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.mnist_dir_path)
        os.unlink(cls.gpu_discovery_script_file.name)
        cls.spark.stop()

    def _get_inputs_for_test_local_training_succeeds(self):
        return [
            ("0,1,2", 1, True, "0,1,2"),
            ("0,1,2", 3, True, "0,1,2"),
            ("0,1,2", 2, False, "0,1,2"),
            (None, 3, False, "NONE"),
        ]


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorDistributedUnitTestsOnConnect(
    TorchDistributorDistributedUnitTestsMixin, unittest.TestCase
):
    @classmethod
    def setUpClass(cls):
        cls.gpu_discovery_script_file = tempfile.NamedTemporaryFile(delete=False)
        cls.gpu_discovery_script_file.write(
            b'echo {\\"name\\": \\"gpu\\", \\"addresses\\": [\\"0\\",\\"1\\",\\"2\\"]}'
        )
        cls.gpu_discovery_script_file.close()
        # create temporary directory for Worker resources coordination
        tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(tempdir.name)
        os.chmod(
            cls.gpu_discovery_script_file.name,
            stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP | stat.S_IROTH | stat.S_IXOTH,
        )
        cls.mnist_dir_path = tempfile.mkdtemp()

        cls.spark = (
            SparkSession.builder.appName("TorchDistributorDistributedUnitTestsOnConnect")
            .config("spark.test.home", SPARK_HOME)
            .config("spark.worker.resource.gpu.discoveryScript", cls.gpu_discovery_script_file.name)
            .config("spark.worker.resource.gpu.amount", "3")
            .config("spark.task.cpus", "2")
            .config("spark.task.resource.gpu.amount", "1")
            .config("spark.executor.resource.gpu.amount", "1")
            .remote("local-cluster[2,2,1024]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.mnist_dir_path)
        os.unlink(cls.gpu_discovery_script_file.name)
        cls.spark.stop()


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
