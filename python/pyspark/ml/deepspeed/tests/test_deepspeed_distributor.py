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
import sys
from typing import Any, Tuple, Dict
import unittest

from pyspark.ml.deepspeed.deepspeed_distributor import DeepspeedTorchDistributor


class DeepspeedTorchDistributorUnitTests(unittest.TestCase):
    def _get_env_var(self, var_name: str, default_value: Any) -> Any:
        value = os.getenv(var_name)
        if value:
            return value
        os.environ[var_name] = str(default_value)
        return default_value

    def _get_env_variables_distributed(self) -> Tuple[Any, Any, Any]:
        MASTER_ADDR = self._get_env_var("MASTER_ADDR", "127.0.0.1")
        MASTER_PORT = self._get_env_var("MASTER_PORT", 2000)
        RANK = self._get_env_var("RANK", 0)
        return MASTER_ADDR, MASTER_PORT, RANK

    def test_get_torchrun_args_local(self) -> None:
        number_of_processes = 5
        EXPECTED_TORCHRUN_ARGS_LOCAL = ["--standalone", "--nnodes=1"]
        EXPECTED_PROCESSES_PER_NODE_LOCAL = number_of_processes
        (
            get_local_mode_torchrun_args,
            process_per_node,
        ) = DeepspeedTorchDistributor._get_torchrun_args(True, number_of_processes)
        self.assertEqual(get_local_mode_torchrun_args, EXPECTED_TORCHRUN_ARGS_LOCAL)
        self.assertEqual(EXPECTED_PROCESSES_PER_NODE_LOCAL, process_per_node)

    def test_get_torchrun_args_distributed(self) -> None:
        number_of_processes = 5
        MASTER_ADDR, MASTER_PORT, RANK = self._get_env_variables_distributed()
        EXPECTED_TORCHRUN_ARGS_DISTRIBUTED = [
            f"--nnodes={number_of_processes}",
            f"--node_rank={RANK}",
            f"--rdzv_endpoint={MASTER_ADDR}:{MASTER_PORT}",
            "--rdzv_id=0",
        ]
        torchrun_args_distributed, process_per_node = DeepspeedTorchDistributor._get_torchrun_args(
            False, number_of_processes
        )
        self.assertEqual(torchrun_args_distributed, EXPECTED_TORCHRUN_ARGS_DISTRIBUTED)
        self.assertEqual(process_per_node, 1)

    def test_create_torchrun_command_local(self) -> None:
        DEEPSPEED_CONF = "path/to/deepspeed"
        TRAIN_FILE_PATH = "path/to/exec"
        NUM_PROCS = 10
        input_params: Dict[str, Any] = {}
        input_params["local_mode"] = True
        input_params["num_processes"] = NUM_PROCS
        input_params["deepspeed_config"] = DEEPSPEED_CONF

        torchrun_local_args_expected = ["--standalone", "--nnodes=1"]
        with self.subTest(msg="Testing local training with no extra args"):
            LOCAL_CMD_NO_ARGS_EXPECTED = [
                sys.executable,
                "-m",
                "torch.distributed.run",
                *torchrun_local_args_expected,
                f"--nproc_per_node={NUM_PROCS}",
                TRAIN_FILE_PATH,
                "-deepspeed",
                "--deepspeed_config",
                DEEPSPEED_CONF,
            ]
            local_cmd = DeepspeedTorchDistributor._create_torchrun_command(
                input_params, TRAIN_FILE_PATH
            )
            self.assertEqual(local_cmd, LOCAL_CMD_NO_ARGS_EXPECTED)
        with self.subTest(msg="Testing local training with extra args for the training script"):
            local_mode_version_args = ["--arg1", "--arg2"]
            LOCAL_CMD_ARGS_EXPECTED = [
                sys.executable,
                "-m",
                "torch.distributed.run",
                *torchrun_local_args_expected,
                f"--nproc_per_node={NUM_PROCS}",
                TRAIN_FILE_PATH,
                *local_mode_version_args,
                "-deepspeed",
                "--deepspeed_config",
                DEEPSPEED_CONF,
            ]

            local_cmd_with_args = DeepspeedTorchDistributor._create_torchrun_command(
                input_params, TRAIN_FILE_PATH, *local_mode_version_args
            )
            self.assertEqual(local_cmd_with_args, LOCAL_CMD_ARGS_EXPECTED)

    def test_create_torchrun_command_distributed(self) -> None:
        DEEPSPEED_CONF = "path/to/deepspeed"
        TRAIN_FILE_PATH = "path/to/exec"
        NUM_PROCS = 10
        input_params: Dict[str, Any] = {}
        input_params["local_mode"] = True
        input_params["num_processes"] = NUM_PROCS
        input_params["deepspeed_config"] = DEEPSPEED_CONF
        (
            distributed_master_address,
            distributed_master_port,
            distributed_rank,
        ) = self._get_env_variables_distributed()
        distributed_torchrun_args = [
            f"--nnodes={NUM_PROCS}",
            f"--node_rank={distributed_rank}",
            f"--rdzv_endpoint={distributed_master_address}:{distributed_master_port}",
            "--rdzv_id=0",
        ]
        with self.subTest(msg="Distributed training command verification with no extra args"):
            DISTRIBUTED_CMD_NO_ARGS_EXPECTED = [
                sys.executable,
                "-m",
                "torch.distributed.run",
                *distributed_torchrun_args,
                "--nproc_per_node=1",
                TRAIN_FILE_PATH,
                "-deepspeed",
                "--deepspeed_config",
                DEEPSPEED_CONF,
            ]
            input_params["local_mode"] = False
            distributed_command = DeepspeedTorchDistributor._create_torchrun_command(
                input_params, TRAIN_FILE_PATH
            )
            self.assertEqual(DISTRIBUTED_CMD_NO_ARGS_EXPECTED, distributed_command)
        with self.subTest(msg="Distributed training command verification with extra arguments"):
            distributed_extra_args = ["-args1", "--args2"]
            DISTRIBUTED_CMD_ARGS_EXPECTED = [
                sys.executable,
                "-m",
                "torch.distributed.run",
                *distributed_torchrun_args,
                "--nproc_per_node=1",
                TRAIN_FILE_PATH,
                *distributed_extra_args,
                "-deepspeed",
                "--deepspeed_config",
                DEEPSPEED_CONF,
            ]
            distributed_command_with_args = DeepspeedTorchDistributor._create_torchrun_command(
                input_params, TRAIN_FILE_PATH, *distributed_extra_args
            )
            self.assertEqual(DISTRIBUTED_CMD_ARGS_EXPECTED, distributed_command_with_args)


if __name__ == "__main__":
    from pyspark.ml.deepspeed.tests.test_deepspeed_distributor import *  # noqa: F401,F403

    try:
        import xmlrunner  # type:ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
