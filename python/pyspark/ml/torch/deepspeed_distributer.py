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
from typing import (
        Union,
        Callable,
        List,
        Dict,
        Optional,
        Any,
        )
from pyspark.ml.torch.distributor import Distributor
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
        self.known_hosts_exists = os.path.exists(DeepspeedDistributor.KNOWN_HOSTS)
        self._setup_hostfile_info()
   
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
        # ask Ricky, Lu, or Maddie if this is the best way to get the GPU information of a particular worker node
        command = f"ssh {executor_ip} nvidia-smi -L | grep GPU | wc -l" # pyspark doesn't support this out of the box for some reason, so sadge
        proc_res = subprocess.run(command, capture_output=True, text=True, shell=True)
        if proc_res.returncode:
            raise RuntimeError(f'something went wrong when running the command {command}. Is nvidia-smi installed?')
        num_gpus_on_worker = proc_res.stdout
        return int(num_gpus_on_worker) 

    def _assign_gpu_to_worker(self, gpu_node_map: Dict[str, int]) -> Dict[str, int]:
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

        worker_hosts = [executor.host() for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()]
        worker_count = len(worker_hosts) # find out if this number includes the driver or not
        rdd = spark.sparkContext.parallelize(range(worker_count), numSlices=worker_count)
        auth_key = DeepspeedDistributor.AUTHORIZED_LOCATION # have to make this a separate variable to avoid a pickling error on next line

        # makes every worker write the driver public key to their own directory
        _ = rdd.mapPartitions(lambda _: [_write_to_location(auth_key, public_key)]).collect()
        # make sure there is no terminal prompt when deepspeed launcher uses ssh to coordinate
        self._ssh_keyscan(worker_hosts)
        # what do I do if the use_gpu flag is false?
        gpus_on_workers = {}
        for worker_host in worker_hosts:
            gpus_on_workers[worker_host] = self._get_gpus_on_node(worker_host)
        assigned_slots = self._assign_gpu_to_worker(gpus_on_workers)
        print(f"Writing to {DeepspeedDistributor.HOSTFILE}")
        for worker_host in worker_hosts:
            line = f"{worker_host} slots={assigned_slots[worker_host]}\n"
            _write_to_location(DeepspeedDistributor.HOSTFILE, line)
        

    


        
