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

# from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import PySparkTestCase
import contextlib
import os
from pyspark.ml.torch.distributor import (
    PyTorchDistributor,
    create_torchrun_command,
    execute_command,
    get_gpus_owned,
)
from pyspark.sql import SparkSession
from six import StringIO  # type: ignore
import sys
import unittest


@contextlib.contextmanager
def patch_stdout() -> StringIO:
    """patch stdout and give an output"""
    sys_stdout = sys.stdout
    io_out = StringIO()
    sys.stdout = io_out
    try:
        yield io_out
    finally:
        sys.stdout = sys_stdout


class TestPyTorchDistributor(PySparkTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.spark = SparkSession(self.sc)

    def test_validate_correct_inputs(self) -> None:
        inputs = [
            ("pytorch", 1, True, True),
            ("pytorch", 100, True, False),
            ("pytorch-lightning", 1, False, True),
            ("pytorch-lightning", 100, False, False),
        ]
        for framework, num_processes, local_mode, use_gpu in inputs:
            with self.subTest():
                PyTorchDistributor(framework, num_processes, local_mode, use_gpu)

    def test_validate_incorrect_inputs(self) -> None:
        inputs = [
            ("tensorflow", 1, True, True, ValueError, "framework"),
            ("pytroch", 100, True, False, ValueError, "framework"),
            ("pytorchlightning", 1, False, True, ValueError, "framework"),
            ("pytorch-lightning", 0, False, False, ValueError, "positive"),
        ]
        for framework, num_processes, local_mode, use_gpu, error, message in inputs:
            with self.subTest():
                with self.assertRaisesRegex(error, message):
                    PyTorchDistributor(framework, num_processes, local_mode, use_gpu)

    def test_get_correct_num_tasks_when_spark_conf_is_set(self) -> None:
        inputs = [(1, 8, 8), (2, 8, 4), (3, 8, 3)]
        # this is when the sparkconf isn't set
        for _, num_processes, _ in inputs:
            with self.subTest():
                distributor = PyTorchDistributor("pytorch", num_processes, True, True)
                self.assertEqual(distributor._get_num_tasks(), num_processes)

        # this is when the sparkconf is set
        for spark_conf_value, num_processes, expected_output in inputs:
            with self.subTest():
                self.spark.sparkContext._conf.set(
                    "spark.task.resource.gpu.amount", str(spark_conf_value)
                )
                distributor = PyTorchDistributor("pytorch", num_processes, True, True)
                self.assertEqual(distributor._get_num_tasks(), expected_output)

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
                distributor = PyTorchDistributor("pytorch", 1, True, True)
                distributor._check_encryption()

    def test_encryption_fails(self) -> None:
        # this is the only combination that should fail
        inputs = [("spark.ssl.enabled", "true", "pytorch.spark.distributor.ignoreSsl", "false")]
        for ssl_conf_key, ssl_conf_value, pytorch_conf_key, pytorch_conf_value in inputs:
            with self.subTest():
                with self.assertRaisesRegex(Exception, "encryption"):
                    self.spark.sparkContext._conf.set(ssl_conf_key, ssl_conf_value)
                    self.spark.sparkContext._conf.set(pytorch_conf_key, pytorch_conf_value)
                    distributor = PyTorchDistributor("pytorch", 1, True, True)
                    distributor._check_encryption()

    def test_get_gpus_owned(self) -> None:
        addresses = ["0", "1", "2"]
        self.assertEqual(get_gpus_owned(addresses), addresses)

        env_vars = {"CUDA_VISIBLE_DEVICES": "3,4,5"}
        self.setup_env_vars(env_vars)
        addresses = ["2", "0", "1"]
        self.assertEqual(get_gpus_owned(addresses), ["5", "3", "4"])
        self.delete_env_vars(env_vars)

    def test_execute_command(self) -> None:
        """Test that run command runs the process and logs are written correctly"""

        with patch_stdout() as output:
            stdout_command = ["echo", "hello_stdout"]
            execute_command(stdout_command)
            self.assertIn(
                "hello_stdout", output.getvalue().strip(), "hello_stdout should print to stdout"
            )

        with patch_stdout() as output:
            stderr_command = ["bash", "-c", "echo hello_stderr >&2"]
            execute_command(stderr_command)
            self.assertIn(
                "hello_stderr", output.getvalue().strip(), "hello_stderr should print to stdout"
            )

        # include command in the exception message
        with self.assertRaisesRegexp(RuntimeError, "exit 1"):  # pylint: disable=deprecated-method
            error_command = ["bash", "-c", "exit 1"]
            execute_command(error_command)

        with self.assertRaisesRegexp(RuntimeError, "abcdef"):  # pylint: disable=deprecated-method
            error_command = ["bash", "-c", "'abc''def'"]
            execute_command(error_command)

    def setup_env_vars(self, input_map: dict[str, str]) -> None:
        for key, value in input_map.items():
            os.environ[key] = value

    def delete_env_vars(self, input_map: dict[str, str]) -> None:
        for key in input_map.keys():
            del os.environ[key]

    def test_create_torchrun_command(self) -> None:
        train_path = "train.py"
        args_string = ["1", "3"]
        local_mode_input_params = {"num_processes": 4, "local_mode": True}

        expected_local_mode_output = (
            "torchrun --standalone --nnodes=1 --nproc_per_node=4 train.py 1 3".split(" ")
        )
        self.assertEqual(
            create_torchrun_command(local_mode_input_params, train_path, *args_string),
            expected_local_mode_output,
        )

        distributed_mode_input_params = {"num_processes": 4, "local_mode": False}
        input_env_vars = {"MASTER_ADDR": "localhost", "MASTER_PORT": "9350", "RANK": "3"}

        args_number = [1, 3]  # testing conversion to strings
        self.setup_env_vars(input_env_vars)
        expected_distributed_mode_output = (
            "torchrun --nnodes=4 --node_rank=3 "
            "--rdzv_endpoint=localhost:9350 "
            "--rdzv_id=0 --nproc_per_node=1 train.py 1 3".split(" ")
        )
        self.assertEqual(
            create_torchrun_command(distributed_mode_input_params, train_path, *args_number),
            expected_distributed_mode_output,
        )
        self.delete_env_vars(input_env_vars)

    def test_run(self) -> None:
        # TODO(SPARK-41592): This will need to be created after distributed training is done.
        pass


if __name__ == "__main__":
    from pyspark.ml.torch.tests.test_distributor import *  # noqa: F401,F403 type: ignore

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
