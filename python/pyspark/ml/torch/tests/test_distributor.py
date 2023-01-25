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

import contextlib
import os
import shutil
from six import StringIO
import stat
import subprocess
import sys
import time
import tempfile
import threading
from typing import Callable, Dict, Any
import unittest
from unittest.mock import patch

have_torch = True
try:
    import torch  # noqa: F401
except ImportError:
    have_torch = False

from pyspark import SparkConf, SparkContext
from pyspark.ml.torch.distributor import TorchDistributor, get_gpus_owned
from pyspark.ml.torch.torch_run_process_wrapper import clean_and_terminate, check_parent_alive
from pyspark.sql import SparkSession
from pyspark.testing.utils import SPARK_HOME


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


def create_training_function(mnist_dir_path: str) -> Callable:
    import torch.nn as nn
    import torch.nn.functional as F
    from torchvision import transforms, datasets

    batch_size = 100
    num_epochs = 1
    momentum = 0.5

    train_dataset = datasets.MNIST(
        mnist_dir_path,
        train=True,
        download=True,
        transform=transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
        ),
    )

    class Net(nn.Module):
        def __init__(self) -> None:
            super(Net, self).__init__()
            self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
            self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
            self.conv2_drop = nn.Dropout2d()
            self.fc1 = nn.Linear(320, 50)
            self.fc2 = nn.Linear(50, 10)

        def forward(self, x: Any) -> Any:
            x = F.relu(F.max_pool2d(self.conv1(x), 2))
            x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
            x = x.view(-1, 320)
            x = F.relu(self.fc1(x))
            x = F.dropout(x, training=self.training)
            x = self.fc2(x)
            return F.log_softmax(x)

    def train_fn(learning_rate: float) -> Any:
        import torch
        import torch.optim as optim
        import torch.distributed as dist
        from torch.nn.parallel import DistributedDataParallel as DDP
        from torch.utils.data.distributed import DistributedSampler

        dist.init_process_group("gloo")

        train_sampler = DistributedSampler(dataset=train_dataset)
        data_loader = torch.utils.data.DataLoader(
            train_dataset, batch_size=batch_size, sampler=train_sampler
        )

        model = Net()
        ddp_model = DDP(model)
        optimizer = optim.SGD(ddp_model.parameters(), lr=learning_rate, momentum=momentum)
        for epoch in range(1, num_epochs + 1):
            model.train()
            for _, (data, target) in enumerate(data_loader):
                optimizer.zero_grad()
                output = model(data)
                loss = F.nll_loss(output, target)
                loss.backward()
                optimizer.step()
            print(f"epoch {epoch} finished.")

        return "success"

    return train_fn


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorBaselineUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        conf = SparkConf()
        self.sc = SparkContext("local[4]", conf=conf)
        self.spark = SparkSession(self.sc)

    def tearDown(self) -> None:
        self.spark.stop()

    def setup_env_vars(self, input_map: Dict[str, str]) -> None:
        for key, value in input_map.items():
            os.environ[key] = value

    def delete_env_vars(self, input_map: Dict[str, str]) -> None:
        for key in input_map.keys():
            del os.environ[key]

    def test_validate_correct_inputs(self) -> None:
        inputs = [
            (1, True, False),
            (100, True, False),
            (1, False, False),
            (100, False, False),
        ]
        for num_processes, local_mode, use_gpu in inputs:
            with self.subTest():
                expected_params = {
                    "num_processes": num_processes,
                    "local_mode": local_mode,
                    "use_gpu": use_gpu,
                    "num_tasks": num_processes,
                }
                dist = TorchDistributor(num_processes, local_mode, use_gpu)
                self.assertEqual(expected_params, dist.input_params)

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

    def test_execute_command(self) -> None:
        """Test that run command runs the process and logs are written correctly"""

        with patch_stdout() as output:
            stdout_command = ["echo", "hello_stdout"]
            TorchDistributor._execute_command(stdout_command)
            self.assertIn(
                "hello_stdout", output.getvalue().strip(), "hello_stdout should print to stdout"
            )

        with patch_stdout() as output:
            stderr_command = ["bash", "-c", "echo hello_stderr >&2"]
            TorchDistributor._execute_command(stderr_command)
            self.assertIn(
                "hello_stderr", output.getvalue().strip(), "hello_stderr should print to stdout"
            )

        # include command in the exception message
        with self.assertRaisesRegex(RuntimeError, "exit 1"):
            error_command = ["bash", "-c", "exit 1"]
            TorchDistributor._execute_command(error_command)

        with self.assertRaisesRegex(RuntimeError, "abcdef"):
            error_command = ["bash", "-c", "'abc''def'"]
            TorchDistributor._execute_command(error_command)

    def test_create_torchrun_command(self) -> None:
        train_path = "train.py"
        args_string = ["1", "3"]
        local_mode_input_params = {"num_processes": 4, "local_mode": True}

        expected_local_mode_output = [
            sys.executable,
            "-m",
            "pyspark.ml.torch.torch_run_process_wrapper",
            "--standalone",
            "--nnodes=1",
            "--nproc_per_node=4",
            "train.py",
            "1",
            "3",
        ]
        self.assertEqual(
            TorchDistributor._create_torchrun_command(
                local_mode_input_params, train_path, *args_string
            ),
            expected_local_mode_output,
        )

        distributed_mode_input_params = {"num_processes": 4, "local_mode": False}
        input_env_vars = {"MASTER_ADDR": "localhost", "MASTER_PORT": "9350", "RANK": "3"}

        args_number = [1, 3]  # testing conversion to strings
        self.setup_env_vars(input_env_vars)
        expected_distributed_mode_output = [
            sys.executable,
            "-m",
            "pyspark.ml.torch.torch_run_process_wrapper",
            "--nnodes=4",
            "--node_rank=3",
            "--rdzv_endpoint=localhost:9350",
            "--rdzv_id=0",
            "--nproc_per_node=1",
            "train.py",
            "1",
            "3",
        ]
        self.assertEqual(
            TorchDistributor._create_torchrun_command(
                distributed_mode_input_params, train_path, *args_number
            ),
            expected_distributed_mode_output,
        )
        self.delete_env_vars(input_env_vars)


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorLocalUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        class_name = self.__class__.__name__
        self.gpu_discovery_script_file = tempfile.NamedTemporaryFile(delete=False)
        self.gpu_discovery_script_file.write(
            b'echo {\\"name\\": \\"gpu\\", \\"addresses\\": [\\"0\\",\\"1\\",\\"2\\"]}'
        )
        self.gpu_discovery_script_file.close()
        # create temporary directory for Worker resources coordination
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)
        os.chmod(
            self.gpu_discovery_script_file.name,
            stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP | stat.S_IROTH | stat.S_IXOTH,
        )
        conf = SparkConf().set("spark.test.home", SPARK_HOME)

        conf = conf.set("spark.driver.resource.gpu.amount", "3")
        conf = conf.set(
            "spark.driver.resource.gpu.discoveryScript", self.gpu_discovery_script_file.name
        )

        self.sc = SparkContext("local-cluster[2,2,1024]", class_name, conf=conf)
        self.spark = SparkSession(self.sc)
        self.mnist_dir_path = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.mnist_dir_path)
        os.unlink(self.gpu_discovery_script_file.name)
        self.spark.stop()

    def setup_env_vars(self, input_map: Dict[str, str]) -> None:
        for key, value in input_map.items():
            os.environ[key] = value

    def delete_env_vars(self, input_map: Dict[str, str]) -> None:
        for key in input_map.keys():
            del os.environ[key]

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
                with self.assertLogs("TorchDistributor", level="WARNING") as log:
                    distributor = TorchDistributor(num_processes, True, True)
                    self.assertEqual(len(log.records), 1)
                    self.assertEqual(distributor.num_processes, 3)

    def test_get_gpus_owned_local(self) -> None:
        addresses = ["0", "1", "2"]
        self.assertEqual(get_gpus_owned(self.sc), addresses)

        env_vars = {"CUDA_VISIBLE_DEVICES": "3,4,5"}
        self.setup_env_vars(env_vars)
        self.assertEqual(get_gpus_owned(self.sc), ["3", "4", "5"])
        self.delete_env_vars(env_vars)

    def test_local_training_succeeds(self) -> None:
        CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES"
        inputs = [
            ("0,1,2", 1, True, "1"),
            ("0,1,2", 3, True, "1,2,0"),
            ("0,1,2", 2, False, "0,1,2"),
            (None, 3, False, "NONE"),
        ]

        for i, (cuda_env_var, num_processes, use_gpu, expected) in enumerate(inputs):
            with self.subTest(f"subtest: {i + 1}"):
                # setup
                if cuda_env_var:
                    self.setup_env_vars({CUDA_VISIBLE_DEVICES: cuda_env_var})

                dist = TorchDistributor(num_processes, True, use_gpu)
                dist._run_training_on_pytorch_file = lambda *args: os.environ.get(
                    CUDA_VISIBLE_DEVICES, "NONE"
                )
                self.assertEqual(
                    expected,
                    dist._run_local_training(dist._run_training_on_pytorch_file, "train.py"),
                )
                # cleanup
                if cuda_env_var:
                    self.delete_env_vars({CUDA_VISIBLE_DEVICES: cuda_env_var})

    def test_local_file_with_pytorch(self) -> None:
        test_file_path = "python/test_support/test_pytorch_training_file.py"
        learning_rate_str = "0.01"
        TorchDistributor(num_processes=2, local_mode=True, use_gpu=False).run(
            test_file_path, learning_rate_str
        )

    def test_end_to_end_run_locally(self) -> None:
        train_fn = create_training_function(self.mnist_dir_path)
        output = TorchDistributor(num_processes=2, local_mode=True, use_gpu=False).run(
            train_fn, 0.001
        )
        self.assertEqual(output, "success")


@unittest.skipIf(not have_torch, "torch is required")
class TorchDistributorDistributedUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        class_name = self.__class__.__name__
        self.gpu_discovery_script_file = tempfile.NamedTemporaryFile(delete=False)
        self.gpu_discovery_script_file.write(
            b'echo {\\"name\\": \\"gpu\\", \\"addresses\\": [\\"0\\",\\"1\\",\\"2\\"]}'
        )
        self.gpu_discovery_script_file.close()
        # create temporary directory for Worker resources coordination
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)
        os.chmod(
            self.gpu_discovery_script_file.name,
            stat.S_IRWXU | stat.S_IXGRP | stat.S_IRGRP | stat.S_IROTH | stat.S_IXOTH,
        )
        conf = SparkConf().set("spark.test.home", SPARK_HOME)

        conf = conf.set(
            "spark.worker.resource.gpu.discoveryScript", self.gpu_discovery_script_file.name
        )
        conf = conf.set("spark.worker.resource.gpu.amount", "3")
        conf = conf.set("spark.task.cpus", "2")
        conf = conf.set("spark.task.resource.gpu.amount", "1")
        conf = conf.set("spark.executor.resource.gpu.amount", "1")

        self.sc = SparkContext("local-cluster[2,2,1024]", class_name, conf=conf)
        self.spark = SparkSession(self.sc)
        self.mnist_dir_path = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.mnist_dir_path)
        os.unlink(self.gpu_discovery_script_file.name)
        self.spark.stop()

    def test_dist_training_succeeds(self) -> None:
        CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES"
        inputs = [
            ("0,1,2", 2, True, "0"),
        ]

        for i, (_, num_processes, use_gpu, expected) in enumerate(inputs):
            with self.subTest(f"subtest: {i + 1}"):
                dist = TorchDistributor(num_processes, False, use_gpu)
                dist._run_training_on_pytorch_file = lambda *args: os.environ.get(
                    CUDA_VISIBLE_DEVICES, "NONE"
                )
                self.assertEqual(
                    expected,
                    dist._run_distributed_training(dist._run_training_on_pytorch_file, "..."),
                )

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

    def test_distributed_file_with_pytorch(self) -> None:
        test_file_path = "python/test_support/test_pytorch_training_file.py"
        learning_rate_str = "0.01"
        TorchDistributor(num_processes=2, local_mode=False, use_gpu=False).run(
            test_file_path, learning_rate_str
        )

    def test_end_to_end_run_distributedly(self) -> None:
        train_fn = create_training_function(self.mnist_dir_path)
        output = TorchDistributor(num_processes=2, local_mode=False, use_gpu=False).run(
            train_fn, 0.001
        )
        self.assertEqual(output, "success")


@unittest.skipIf(not have_torch, "torch is required")
class TorchWrapperUnitTests(unittest.TestCase):
    def test_clean_and_terminate(self) -> None:
        def kill_task(task: "subprocess.Popen") -> None:
            time.sleep(1)
            clean_and_terminate(task)

        command = [sys.executable, "-c", '"import time; time.sleep(20)"']
        task = subprocess.Popen(command)
        t = threading.Thread(target=kill_task, args=(task,))
        t.start()
        time.sleep(2)
        self.assertEqual(task.poll(), 0)  # implies task ended

    @patch("pyspark.ml.torch.torch_run_process_wrapper.clean_and_terminate")
    def test_check_parent_alive(self, mock_clean_and_terminate: Callable) -> None:
        command = [sys.executable, "-c", '"import time; time.sleep(2)"']
        task = subprocess.Popen(command)
        t = threading.Thread(target=check_parent_alive, args=(task,), daemon=True)
        t.start()
        time.sleep(2)
        self.assertEqual(mock_clean_and_terminate.call_count, 0)


if __name__ == "__main__":
    from pyspark.ml.torch.tests.test_distributor import *  # noqa: F401,F403

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
