import os
import sys
from typing import List, Any, Callable, Dict
import unittest

from pyspark.ml.torch.deepspeed.deepspeed_distributor import DeepspeedTorchDistributor

class DeepspeedTorchDistributorUnitTests(unittest.TestCase):
    
    def _get_env_var(self, var_name: str, default_value: Any) -> Any:
        value = os.getenv(var_name)
        if value:
            return value
        os.environ[var_name] = str(default_value)
        value = default_value
        return value

    def _get_env_variables_distributed(self):
        MASTER_ADDR = self._get_env_var("MASTER_ADDR", "127.0.0.1")
        MASTER_PORT = self._get_env_var("MASTER_PORT", 2000)
        RANK = self._get_env_var("RANK", 0)
        return MASTER_ADDR, MASTER_PORT, RANK

    def test_get_torchrun_args_local(self):
        number_of_processes = 5
        EXPECTED_TORCHRUN_ARGS_LOCAL= [
                "--standalone", "--nnodes=1"
                ]
        EXPECTED_PROCESSES_PER_NODE_LOCAL = number_of_processes
        get_local_mode_torchrun_args, process_per_node= DeepspeedTorchDistributor._get_torchrun_args(True, number_of_processes)
        self.assertEqual(get_local_mode_torchrun_args,EXPECTED_TORCHRUN_ARGS_LOCAL)
        self.assertEqual(EXPECTED_PROCESSES_PER_NODE_LOCAL,process_per_node)

    def test_get_torchrun_args_distributed(self):
        number_of_processes = 5
        MASTER_ADDR, MASTER_PORT, RANK = self._get_env_variables_distributed()
        EXPECTED_TORCHRUN_ARGS_DISTRIBUTED = [
                        f"--nnodes={number_of_processes}",
                        f"--node_rank={RANK}",
                        f"--rdzv_endpoint={MASTER_ADDR}:{MASTER_PORT}",
                        "--rdzv_id=0"
                    ]
        torchrun_args_distributed, process_per_node  = DeepspeedTorchDistributor._get_torchrun_args(False, number_of_processes)
        self.assertEqual(torchrun_args_distributed,EXPECTED_TORCHRUN_ARGS_DISTRIBUTED)
        self.assertEqual(process_per_node,1)

    def test_create_torchrun_command_local(self):
        DEEPSPEED_CONF = "path/to/deepspeed"
        TRAIN_FILE_PATH = "path/to/exec"
        NUM_PROCS = 10
        input_params = {}
        input_params["local_mode"] = True
        input_params['num_processes'] = NUM_PROCS
        input_params["deepspeed_config"] = DEEPSPEED_CONF

        # get the arguments for no argument, local run
        torchrun_local_args_expected = ["--standalone", "--nnodes=1"]

        LOCAL_CMD_NO_ARGS_EXPECTED= [
                     sys.executable,
                    "-m",
                    "torch.distributed.run",
                    *torchrun_local_args_expected,
                    f"--nproc_per_node={NUM_PROCS}",
                    TRAIN_FILE_PATH,
                    "-deepspeed",
                    "--deepspeed_config",
                    DEEPSPEED_CONF
                    ]
        local_cmd = DeepspeedTorchDistributor._create_torchrun_command(input_params, TRAIN_FILE_PATH)
        self.assertEqual(local_cmd,LOCAL_CMD_NO_ARGS_EXPECTED)
        local_mode_version_args = ["--arg1", "--arg2"]
        LOCAL_CMD_ARGS_EXPECTED= [
                     sys.executable,
                    "-m",
                    "torch.distributed.run",
                    *torchrun_local_args_expected,
                    f"--nproc_per_node={NUM_PROCS}",
                    TRAIN_FILE_PATH,
                    *local_mode_version_args,
                    "-deepspeed",
                    "--deepspeed_config",
                    DEEPSPEED_CONF
                    ]

        local_cmd_with_args = DeepspeedTorchDistributor._create_torchrun_command(input_params, TRAIN_FILE_PATH, *local_mode_version_args)
        self.assertEqual(local_cmd_with_args,LOCAL_CMD_ARGS_EXPECTED)
    
    def test_create_torchrun_command_distributed(self):
        DEEPSPEED_CONF = "path/to/deepspeed"
        TRAIN_FILE_PATH = "path/to/exec"
        NUM_PROCS = 10
        input_params = {}
        input_params["local_mode"] = True
        input_params['num_processes'] = NUM_PROCS
        input_params["deepspeed_config"] = DEEPSPEED_CONF
        # distributed training environment
        distributed_master_address, distributed_master_port, distributed_rank = self._get_env_variables_distributed()
        distributed_torchrun_args = [
                f"--nnodes={NUM_PROCS}",
                f"--node_rank={distributed_rank}",
                f"--rdzv_endpoint={distributed_master_address}:{distributed_master_port}",
                "--rdzv_id=0",
            ]
        DISTRIBUTED_CMD_NO_ARGS_EXPECTED = [sys.executable,
                                            "-m",
                                            "torch.distributed.run",
                                            *distributed_torchrun_args,
                                            "--nproc_per_node=1",
                                            TRAIN_FILE_PATH,
                                            "-deepspeed",
                                            "--deepspeed_config",
                                            DEEPSPEED_CONF
                                            ]
        # test distributed training without arguments
        input_params["local_mode"] = False
        distributed_command = DeepspeedTorchDistributor._create_torchrun_command(input_params, TRAIN_FILE_PATH)
        self.assertEqual(DISTRIBUTED_CMD_NO_ARGS_EXPECTED,distributed_command)
        # test distributed training with random arguments
        distributed_extra_args = ["-args1", "--args2"]
        DISTRIBUTED_CMD_ARGS_EXPECTED = [sys.executable,
                                            "-m",
                                            "torch.distributed.run",
                                            *distributed_torchrun_args,
                                            "--nproc_per_node=1",
                                            TRAIN_FILE_PATH,
                                            *distributed_extra_args,
                                            "-deepspeed",
                                            "--deepspeed_config",
                                            DEEPSPEED_CONF
                                            ]
        print("The distributed training command: ", DISTRIBUTED_CMD_ARGS_EXPECTED)
        distributed_command_with_args = DeepspeedTorchDistributor._create_torchrun_command(input_params, TRAIN_FILE_PATH,*distributed_extra_args)
        self.assertEqual(DISTRIBUTED_CMD_ARGS_EXPECTED,distributed_command_with_args)

if __name__ == "__main__":
    from pyspark.ml.torch.deepspeed.tests.test_deepspeed_distributor import *  # noqa: F401,F403

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
