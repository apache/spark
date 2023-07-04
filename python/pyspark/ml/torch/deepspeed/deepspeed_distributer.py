import json
from contextlib import contextmanager
import collections
import copy
from copy import deepcopy
import os
from queue import Queue
import random
from re import A
import shutil
import subprocess
import tempfile
from typing import (
    Union,
    Callable,
    List,
    Dict,
    Optional,
    Any,
)
from pyspark.ml.torch.distributor import Distributor, TorchDistributor
from pyspark.ml.torch.deepspeed.utils import SSHEnvManager, write_to_location
from pyspark.sql.functions import exists


class DeepspeedDistributor(Distributor):
    KNOWN_HOSTS = "/root/.ssh/known_hosts"
    KNOWN_HOSTS_TEMP = "/root/.ssh/known_hosts_temp"
    AUTHORIZED_LOCATION = "/root/.ssh/authorized_keys"
    HOSTFILE = "/root/hostfile"

    def __init__(
        self,
        num_processes: int = 1,
        local_mode: bool = True,
        use_gpu: bool = True,
        deepspeed_config=None,
    ):
        super().__init__(num_processes, local_mode, use_gpu)
        self.deepspeed_config = deepspeed_config
        self.temp_deepspeed_fname = None
        self.ssh_env_manager = SSHEnvManager()
        self.input_params = self._create_input_params()
        self.worker_hosts = self._setup_hostfile_info()
        self.setup_env()

    def _get_gpus_on_node(self, executor_ip: str):
        # TODO: ask Ricky, Lu, or Maddie if this is the best way to get the GPU information of a particular worker node
        command = f"ssh {executor_ip} nvidia-smi -L | grep GPU | wc -l"  # pyspark doesn't support this out of the box for some reason, so sadge
        proc_res = subprocess.run(command, capture_output=True, text=True, shell=True)
        if proc_res.returncode:
            raise RuntimeError(
                f"something went wrong when running the command {command}. Is nvidia-smi installed?"
            )
        num_gpus_on_worker = proc_res.stdout
        return int(num_gpus_on_worker)

    def _assign_procs_to_worker(self, gpu_node_map: Dict[str, int]) -> Dict[str, int]:
        procs_left = self.num_processes
        workers_left_to_serve = len(gpu_node_map)
        average_procs_per_node = procs_left // workers_left_to_serve
        gpu_mapped_to_node = {}
        # sorting allows us to just do a single pass, as filling the smallest capacity nodes first will allow for a single pass
        sorted_buckets = sorted(gpu_node_map.items(), key=lambda x: x[1])
        for worker, capacity in sorted_buckets:
            average_procs_per_node = procs_left // workers_left_to_serve
            gpu_mapped_to_node[worker] = min(average_procs_per_node, capacity)
            procs_left -= gpu_mapped_to_node[worker]
            workers_left_to_serve -= 1
        if procs_left != 0:
            print(
                f"There are not enough GPUS to fully assign processes to nodes; there are {procs_left} processes left over"
            )
        return gpu_mapped_to_node

    def _setup_hostfile_info(self):
        ssh_path = "/root/.ssh/id_rsa"
        ssh_pub_path = f"{ssh_path}.pub"
        self.ssh_env_manager.create_ssh_key(ssh_path)
        public_key = self.ssh_env_manager.get_ssh_key(ssh_pub_path)
        write_to_location(DeepspeedDistributor.AUTHORIZED_LOCATION, public_key)

        worker_hosts = [
            executor.host()
            for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
        ]  # we should check if this returns the driver or not
        worker_count = len(worker_hosts)  # find out if this number includes the driver or not
        rdd = spark.sparkContext.parallelize(range(worker_count), numSlices=worker_count)
        auth_key = (
            DeepspeedDistributor.AUTHORIZED_LOCATION
        )  # have to make this a separate variable to avoid a pickling error on next line

        # makes every worker write the driver public key to their own directory
        _ = rdd.mapPartitions(lambda _: [write_to_location(auth_key, public_key)]).collect()
        # make sure there is no terminal prompt when deepspeed launcher uses ssh to coordinate
        self.ssh_env_manager.ssh_keyscan(worker_hosts)
        # what do I do if the use_gpu flag is false?
        slots_on_workers = {}
        if self.use_gpu:
            for worker_host in worker_hosts:
                slots_on_workers[worker_host] = self._get_gpus_on_node(worker_host)
        else:
            raise RuntimeError("Deepspeed doesn't work with non-GPU clusters at this time")

        assigned_slots = self._assign_procs_to_worker(slots_on_workers)
        print(f"Writing to {DeepspeedDistributor.HOSTFILE}")
        for worker_host in worker_hosts:
            line = f"{worker_host} slots={assigned_slots[worker_host]}\n"
            write_to_location(DeepspeedDistributor.HOSTFILE, line)
        return worker_hosts

    def setup_env(self):
        try:
            subprocess.run("deepspeed --version".split())
            subprocess.run("ninja --version".split())
            with open("/root/.deepspeed_env", "w") as f:
                # if this is open; don't add that to path if they're not running on databricks
                # TODO: figure out what paths to add to this, because if this is OSS we don't want to constantly add a databricks filepath
                f.write(
                    "PATH=/databricks/python3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
                )
        except:
            raise ImportError("Install deepspeed and ninja on the cluster using PyPi")

    def _create_deepspeed_command(
        self, input_params: Dict[str, Any], path_to_train_file: str, *args: Any
    ):
        local_mode = input_params["local_mode"]
        num_processes = input_params["num_processes"]
        deepspeed_config = input_params["deepspeed_config"]
        if isinstance(deepspeed_config, dict):
            with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json") as f:
                json.dump(deepspeed_config, f)
                deepspeed_config_path = f.name
                self.temp_deepspeed_fname = f.name
        else:
            deepspeed_config_path = deepspeed_config
        if local_mode:
            deepspeed_args = [
                "--num_gpus",
                str(num_processes),
            ]  # no need for num nodes, the host file, or any port stuff (no communiation)
        else:
            deepspeed_args = [
                "--num_gpus",
                str(input_params["num_processes"]),
                "--num_nodes",
                str(len(self.worker_hosts)),
                "--hostfile",
                str(DeepspeedDistributor.HOSTFILE),
                "--master_addr",
                str(self.worker_hosts[0]),
                "--master_port=9902",
            ]
        return [
            "deepspeed",
            *deepspeed_args,
            path_to_train_file,
            *args,
            "--deepspeed",
            "--deepspeed_config",
            deepspeed_config_path,
        ]

    def _run_training_on_pytorch_file(
        self, input_params: Dict[str, Any], train_path: str, *args: Any, **kwargs: Any
    ) -> None:
        if kwargs:
            raise ValueError("Running pytorch file does not support key-word type arguments.")
        training_command = self._create_deepspeed_command(input_params, train_path, *args)
        TorchDistributor._execute_command(
            training_command
        )  # should we include some form of logging here

    def run(self, train_object: Union[Callable, str], *args: Any, **kwargs: Any) -> Optional[Any]:
        if isinstance(train_object, str):
            self._run_training_on_pytorch_file(self.input_params, train_object, *args, **kwargs)  # type: ignore
        else:
            raise RuntimeError("Using functions isn't implemented yet. Next iteration.")
        return "Finished"

    def _clean_up(self):
        self.ssh_env_manager.cleanup_ssh_env()
        if self.temp_deepspeed_fname:
            os.remove(self.temp_deepspeed_fname)
        os.remove(DeepspeedDistributor.HOSTFILE)
