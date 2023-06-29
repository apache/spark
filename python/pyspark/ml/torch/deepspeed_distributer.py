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
from pyspark.sql.functions import exists


def _write_to_location(location: str, content: str) -> None:
    os.makedirs(os.path.dirname(location), exist_ok=True)
    with open(location, "a") as f:
        f.write(content)

class DeepspeedDistributor(Distributor):
    KNOWN_HOSTS = "/root/.ssh/known_hosts"
    KNOWN_HOSTS_TEMP = "/root/.ssh/known_hosts_temp"
    AUTHORIZED_LOCATION = "/root/.ssh/authorized_keys"
    HOSTFILE = "/root/hostfile"
    def __init__(self, num_processes: int = 1, local_mode: bool = True, use_gpu : bool = True, deepspeed_config = None):
        super().__init__(num_processes, local_mode, use_gpu)
        self.deepspeed_config = deepspeed_config
        self.temp_deepspeed_fname = None
        self.known_hosts_exists = os.path.exists(DeepspeedDistributor.KNOWN_HOSTS)
        self.input_params = self._create_input_params()
        self.worker_hosts = self._setup_hostfile_info()
        self.setup_env()
   
    def _create_ssh_key(self, ssh_key_path: str):
        if not os.path.exists(ssh_key_path):
            print(f"Creating the ssh key to {ssh_key_path}")
            cmd_status = subprocess.run(["ssh-keygen", "-t", "rsa", "-f", ssh_key_path, "-q", "-N", ""])
            if cmd_status.returncode:
                raise RuntimeError(f"Was unabled to create ssh-key to {ssh_key_path}")
        else:
            print(f"{ssh_key_path} already exists")

    def _get_ssh_key(self, ssh_pub_key: str):
        with open(ssh_pub_key) as f:
            ssh_key = f.read()
        return ssh_key

    def _ssh_keyscan(self, ip_list: List[str]):
        """Is used to allow ssh to not prompt us `Are you sure you want to connect`, thus removing user need to use the terminal"""
        # if there is a known_hosts file, we need to preserve old one as we modify it
        # otherwise, just write to it
        print("Trying to add the worker node public ssh keys to the ssh known_hosts file")
        if self.known_hosts_exists:
            shutil.copyfile(DeepspeedDistributor.KNOWN_HOSTS, DeepspeedDistributor.KNOWN_HOSTS_TEMP)
        for ip in ip_list:
            cmd_args = ["ssh-keyscan", ip]
            error_code = subprocess.run(cmd_args, capture_output=True)
            if error_code.returncode:
                raise RuntimeError(f"Something went wrong when running ssh_keyscan {ip}. Command tried to run: ", cmd_args)
            cmd_output = error_code.stdout.decode('utf-8') # get the output from the command so we can write to right location
            _write_to_location(DeepspeedDistributor.KNOWN_HOSTS, cmd_output)
        print("Successfully finished writing worker ssh public keys to known_hosts on driver")
    

    def _cleanup_ssh_keyscan(self):
        try:
            os.remove(DeepspeedDistributor.KNOWN_HOSTS)
            os.rename(DeepspeedDistributor.KNOWN_HOSTS_TEMP, DeepspeedDistributor.KNOWN_HOSTS)
        except OSError:
            print("Wow something went wrong when cleaning up after myself.")
    
    def _get_gpus_on_node(self, executor_ip: str):
        # TODO: ask Ricky, Lu, or Maddie if this is the best way to get the GPU information of a particular worker node
        command = f"ssh {executor_ip} nvidia-smi -L | grep GPU | wc -l" # pyspark doesn't support this out of the box for some reason, so sadge
        proc_res = subprocess.run(command, capture_output=True, text=True, shell=True)
        if proc_res.returncode:
            raise RuntimeError(f'something went wrong when running the command {command}. Is nvidia-smi installed?')
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
           print(f"There are not enough GPUS to fully assign processes to nodes; there are {procs_left} processes left over")
       return gpu_mapped_to_node
        
    def _setup_hostfile_info(self):
        ssh_path = "/root/.ssh/id_rsa"
        ssh_pub_path = f"{ssh_path}.pub"
        self._create_ssh_key(ssh_path)
        public_key = self._get_ssh_key(ssh_pub_path)
        _write_to_location(DeepspeedDistributor.AUTHORIZED_LOCATION, public_key)

        worker_hosts = [executor.host() for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()] # we should check if this returns the driver or not
        worker_count = len(worker_hosts) # find out if this number includes the driver or not
        rdd = spark.sparkContext.parallelize(range(worker_count), numSlices=worker_count)
        auth_key = DeepspeedDistributor.AUTHORIZED_LOCATION # have to make this a separate variable to avoid a pickling error on next line

        # makes every worker write the driver public key to their own directory
        _ = rdd.mapPartitions(lambda _: [_write_to_location(auth_key, public_key)]).collect()
        # make sure there is no terminal prompt when deepspeed launcher uses ssh to coordinate
        self._ssh_keyscan(worker_hosts)
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
            _write_to_location(DeepspeedDistributor.HOSTFILE, line)
        return worker_hosts 

    def setup_env(self):
        try:
            subprocess.run('deepspeed --version'.split())
            subprocess.run('ninja --version'.split())
            with open("/root/.deepspeed_env", "w") as f:
                # if this is open; don't add that to path if they're not running on databricks
                # TODO: figure out what paths to add to this, because if this is OSS we don't want to constantly add a databricks filepath
                f.write("PATH=/databricks/python3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
        except:
            raise ImportError("Install deepspeed and ninja on the cluster using PyPi")

    def _create_deepspeed_command(self, input_params: Dict[str, Any], path_to_train_file: str, *args: Any):
            local_mode = input_params["local_mode"]
            num_processes = input_params["num_processes"]
            deepspeed_config = input_params["deepspeed_config"]
            if isinstance(deepspeed_config, dict):
                with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as f:
                    json.dump(deepspeed_config, f)
                    deepspeed_config_path = f.name
                    self.temp_deepspeed_fname = f.name
            else:
                deepspeed_config_path = deepspeed_config
            if local_mode:
                deepspeed_args = ["--num_gpus", str(num_processes)] # no need for num nodes, the host file, or any port stuff (no communiation)
            else:
                deepspeed_args = ["--num_gpus", str(input_params['num_processes']), "--num_nodes", str(len(self.worker_hosts)), "--hostfile", str(DeepspeedDistributor.HOSTFILE), "--master_addr", str(self.worker_hosts[0]), "--master_port=9902"]
            return [
                "deepspeed",
                *deepspeed_args,
                path_to_train_file,
                *args,
                "--deepspeed",
                "--deepspeed_config",
                deepspeed_config_path
            ]

    def _run_training_on_pytorch_file(self, input_params: Dict[str, Any], train_path: str, *args: Any, **kwargs: Any) -> None:
        if kwargs:
            raise ValueError("Running pytorch file does not support key-word type arguments.")
        training_command = self._create_deepspeed_command(
            input_params, train_path, *args
        )
        TorchDistributor._execute_command(training_command) # should we include some form of logging here
    
    def run(self, train_object: Union[Callable, str], *args: Any, **kwargs: Any) -> Optional[Any]:
        if isinstance(train_object, str):
            self._run_training_on_pytorch_file(self.input_params, train_object, *args, **kwargs)  # type: ignore
        else:
            raise RuntimeError("Using functions isn't implemented yet. Next iteration.")
        return "Finished"

    def _clean_up(self):
        self._cleanup_ssh_keyscan()
        if self.temp_deepspeed_fname:
            os.remove(self.temp_deepspeed_fname)
        os.remove(DeepspeedDistributor.HOSTFILE)

