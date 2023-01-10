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
import stat
import tempfile
import unittest

from pyspark import SparkConf, SparkContext
from pyspark.ml.torch.distributor import TorchDistributor
from pyspark.sql import SparkSession
from pyspark.testing.utils import SPARK_HOME


class TorchDistributorBaselineUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        conf = SparkConf()
        self.sc = SparkContext("local[4]", conf=conf)
        self.spark = SparkSession(self.sc)

    def tearDown(self) -> None:
        self.spark.stop()

    def test_validate_correct_inputs(self) -> None:
        inputs = [
            (1, True, False),
            (100, True, False),
            (1, False, False),
            (100, False, False),
        ]
        for num_processes, local_mode, use_gpu in inputs:
            with self.subTest():
                TorchDistributor(num_processes, local_mode, use_gpu)

    def test_validate_incorrect_inputs(self) -> None:
        inputs = [
            (0, False, False, ValueError, "positive"),
        ]
        for num_processes, local_mode, use_gpu, error, message in inputs:
            with self.subTest():
                with self.assertRaisesRegex(error, message):
                    TorchDistributor(num_processes, local_mode, use_gpu)

    def test_encryption_passes(self) -> None:
        inputs = [
            ("spark.ssl.enabled", "false", "pytorch.spark.distributor.ignoreSsl", "true"),
            ("spark.ssl.enabled", "false", "pytorch.spark.distributor.ignoreSsl", "false"),
            ("spark.ssl.enabled", "true", "pytorch.spark.distributor.ignoreSsl", "true"),
        ]
        for ssl_conf_key, ssl_conf_value, pytorch_conf_key, pytorch_conf_value in inputs:
            with self.subTest():
                self.spark.sparkContext._conf.set(ssl_conf_key, ssl_conf_value)
                self.spark.sparkContext._conf.set(pytorch_conf_key, pytorch_conf_value)
                distributor = TorchDistributor(1, True, False)
                distributor._check_encryption()

    def test_encryption_fails(self) -> None:
        # this is the only combination that should fail
        inputs = [("spark.ssl.enabled", "true", "pytorch.spark.distributor.ignoreSsl", "false")]
        for ssl_conf_key, ssl_conf_value, pytorch_conf_key, pytorch_conf_value in inputs:
            with self.subTest():
                with self.assertRaisesRegex(Exception, "encryption"):
                    self.spark.sparkContext._conf.set(ssl_conf_key, ssl_conf_value)
                    self.spark.sparkContext._conf.set(pytorch_conf_key, pytorch_conf_value)
                    distributor = TorchDistributor(1, True, False)
                    distributor._check_encryption()

    def test_get_num_tasks_fails(self) -> None:
        inputs = [1, 5, 4]

        # This is when the conf isn't set and we request GPUs
        for num_processes in inputs:
            with self.subTest():
                with self.assertRaisesRegex(RuntimeError, "driver"):
                    TorchDistributor(num_processes, True, True)
                with self.assertRaisesRegex(RuntimeError, "unset"):
                    TorchDistributor(num_processes, False, True)


class TorchDistributorLocalUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        class_name = self.__class__.__name__
        self.tempFile = tempfile.NamedTemporaryFile(delete=False)
        self.tempFile.write(
            b'echo {\\"name\\": \\"gpu\\", \\"addresses\\": [\\"0\\",\\"1\\",\\"2\\"]}'
        )
        self.tempFile.close()
        # create temporary directory for Worker resources coordination
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)
        os.chmod(
            self.tempFile.name,
            stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP | stat.S_IROTH | stat.S_IXOTH,
        )
        conf = SparkConf().set("spark.test.home", SPARK_HOME)

        conf = conf.set("spark.driver.resource.gpu.amount", "3")
        conf = conf.set("spark.driver.resource.gpu.discoveryScript", self.tempFile.name)

        self.sc = SparkContext("local-cluster[2,2,1024]", class_name, conf=conf)
        self.spark = SparkSession(self.sc)

    def tearDown(self) -> None:
        os.unlink(self.tempFile.name)
        self.spark.stop()

    def test_get_num_tasks_locally(self) -> None:
        succeeds = [1, 2]
        fails = [4, 8]
        for num_processes in succeeds:
            with self.subTest():
                expected_output = num_processes
                distributor = TorchDistributor(num_processes, True, True)
                self.assertEqual(distributor._get_num_tasks(), expected_output)

        for num_processes in fails:
            with self.subTest():
                with self.assertWarns(RuntimeWarning):
                    distributor = TorchDistributor(num_processes, True, True)
                    distributor.num_processes = 3


class TorchDistributorDistributedUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        class_name = self.__class__.__name__
        self.tempFile = tempfile.NamedTemporaryFile(delete=False)
        self.tempFile.write(
            b'echo {\\"name\\": \\"gpu\\", \\"addresses\\": [\\"0\\",\\"1\\",\\"2\\"]}'
        )
        self.tempFile.close()
        # create temporary directory for Worker resources coordination
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)
        os.chmod(
            self.tempFile.name,
            stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP | stat.S_IROTH | stat.S_IXOTH,
        )
        conf = SparkConf().set("spark.test.home", SPARK_HOME)

        conf = conf.set("spark.worker.resource.gpu.discoveryScript", self.tempFile.name)
        conf = conf.set("spark.worker.resource.gpu.amount", "3")
        conf = conf.set("spark.task.cpus", "2")
        conf = conf.set("spark.task.resource.gpu.amount", "1")
        conf = conf.set("spark.executor.resource.gpu.amount", "1")

        self.sc = SparkContext("local-cluster[2,2,1024]", class_name, conf=conf)
        self.spark = SparkSession(self.sc)

    def tearDown(self) -> None:
        os.unlink(self.tempFile.name)
        self.spark.stop()

    def test_get_num_tasks_distributed(self) -> None:
        inputs = [(1, 8, 8), (2, 8, 4), (3, 8, 3)]

        for spark_conf_value, num_processes, expected_output in inputs:
            with self.subTest():
                self.spark.sparkContext._conf.set(
                    "spark.task.resource.gpu.amount", str(spark_conf_value)
                )
                distributor = TorchDistributor(num_processes, False, True)
                self.assertEqual(distributor._get_num_tasks(), expected_output)

        self.spark.sparkContext._conf.set("spark.task.resource.gpu.amount", "1")


if __name__ == "__main__":
    from pyspark.ml.torch.tests.test_distributor import *  # noqa: F401,F403 type: ignore

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
