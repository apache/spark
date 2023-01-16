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

import collections
import ctypes
import math
import os
import random
import re
import signal
import sys
import subprocess
import time
from typing import Union, Callable, List, Dict, Optional, Any
import warnings

from pyspark.sql import SparkSession
from pyspark.context import SparkContext


# TODO(SPARK-41589): will move the functions and tests to an external file
#       once we are in agreement about which functions should be in utils.py
def get_conf_boolean(sc: SparkContext, key: str, default_value: str) -> bool:
    """Get the conf "key" from the given spark context,
    or return the default value if the conf is not set.
    This expects the conf value to be a boolean or string;
    if the value is a string, this checks for all capitalization
    patterns of "true" and "false" to match Scala.

    Parameters
    ----------
    sc : :class:`SparkContext`
        The :class:`SparkContext` for the distributor.
    key : str
        string for conf name
    default_value : str
        default value for the conf value for the given key

    Returns
    -------
    bool
        Returns the boolean value that corresponds to the conf

    Raises
    ------
    ValueError
        Thrown when the conf value is not a valid boolean
    """
    val = sc.getConf().get(key, default_value)
    lowercase_val = val.lower()
    if lowercase_val == "true":
        return True
    if lowercase_val == "false":
        return False
    raise ValueError(
        f"The conf value for '{key}' was expected to be a boolean "
        f"value but found value of type {type(val)} "
        f"with value: {val}"
    )


def get_gpus_owned(sc: SparkContext) -> List[str]:
    """Gets the number of GPUs that Spark scheduled to the calling task.

    Parameters
    ----------
    sc : :class:`SparkContext`
        The :class:`SparkContext` that has GPUs available.

    Returns
    -------
    list
        The correct mapping of addresses to workers.

    Raises
    ------
    ValueError
        Raised if the input addresses were not found.
    """
    CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES"
    pattern = re.compile("^[1-9][0-9]*|0$")
    addresses = sc.resources["gpu"].addresses
    if any(not pattern.match(address) for address in addresses):
        raise ValueError(
            f"Found GPU addresses {addresses} which "
            "are not all in the correct format "
            "for CUDA_VISIBLE_DEVICES, which requires "
            "integers with no zero padding."
        )
    if CUDA_VISIBLE_DEVICES in os.environ:
        gpu_indices = list(map(int, addresses))
        gpu_list = os.environ[CUDA_VISIBLE_DEVICES].split(",")
        gpu_owned = [gpu_list[i] for i in gpu_indices]
        return gpu_owned
    return addresses


class Distributor:
    """
    The parent class for TorchDistributor. This class shouldn't be instantiated directly.
    """

    def __init__(
        self,
        num_processes: int = 1,
        local_mode: bool = True,
        use_gpu: bool = True,
    ):
        self.num_processes = num_processes
        self.local_mode = local_mode
        self.use_gpu = use_gpu
        self.spark = SparkSession.getActiveSession()
        if not self.spark:
            raise RuntimeError("An active SparkSession is required for the distributor.")
        self.sc = self.spark.sparkContext
        self.num_tasks = self._get_num_tasks()
        self.ssl_conf = None

    def _create_input_params(self) -> Dict[str, Any]:
        input_params = self.__dict__.copy()
        for unneeded_param in ["spark", "sc", "ssl_conf"]:
            del input_params[unneeded_param]
        return input_params

    def _get_num_tasks(self) -> int:
        """
        Returns the number of Spark tasks to use for distributed training

        Returns
        -------
        int
            The number of Spark tasks to use for distributed training

        Raises
        ------
        RuntimeError
            Raised when the SparkConf was misconfigured.
        """

        if self.use_gpu:
            if not self.local_mode:
                key = "spark.task.resource.gpu.amount"
                task_gpu_amount = int(self.sc.getConf().get(key, "0"))
                if task_gpu_amount < 1:
                    raise RuntimeError(f"'{key}' was unset, so gpu usage is unavailable.")
                # TODO(SPARK-41916): Address situation when spark.task.resource.gpu.amount > 1
                return math.ceil(self.num_processes / task_gpu_amount)
            else:
                key = "spark.driver.resource.gpu.amount"
                if "gpu" not in self.sc.resources:
                    raise RuntimeError("GPUs were unable to be found on the driver.")
                num_available_gpus = int(self.sc.getConf().get(key, "0"))
                if num_available_gpus == 0:
                    raise RuntimeError("GPU resources were not configured properly on the driver.")
                if self.num_processes > num_available_gpus:
                    warnings.warn(
                        f"'num_processes' cannot be set to a value greater than the number of "
                        f"available GPUs on the driver, which is {num_available_gpus}. "
                        f"'num_processes' was reset to be equal to the number of available GPUs.",
                        RuntimeWarning,
                    )
                    self.num_processes = num_available_gpus
        return self.num_processes

    def _validate_input_params(self) -> None:
        if self.num_processes <= 0:
            raise ValueError("num_proccesses has to be a positive integer")

    def _check_encryption(self) -> None:
        """Checks to see if the user requires encrpytion of data.
        If required, throw an exception since we don't support that.

        Raises
        ------
        RuntimeError
            Thrown when the user requires ssl encryption or when the user initializes
            the Distributor parent class.
        """
        if not "ssl_conf":
            raise RuntimeError(
                "Distributor doesn't have this functionality. Use TorchDistributor instead."
            )
        is_ssl_enabled = get_conf_boolean(self.sc, "spark.ssl.enabled", "false")
        ignore_ssl = get_conf_boolean(self.sc, self.ssl_conf, "false")  # type: ignore
        if is_ssl_enabled:
            name = self.__class__.__name__
            if ignore_ssl:
                warnings.warn(
                    f"""
                    This cluster has TLS encryption enabled;
                    however, {name} does not
                    support data encryption in transit.
                    The Spark configuration
                    '{self.ssl_conf}' has been set to
                    'true' to override this
                    configuration and use {name} anyway. Please
                    note this will cause model
                    parameters and possibly training data to
                    be sent between nodes unencrypted.
                    """,
                    RuntimeWarning,
                )
                return
            raise RuntimeError(
                f"""
                This cluster has TLS encryption enabled;
                however, {name} does not support
                data encryption in transit. To override
                this configuration and use {name}
                anyway, you may set '{self.ssl_conf}'
                to 'true' in the Spark configuration. Please note this
                will cause model parameters and possibly training
                data to be sent between nodes unencrypted.
                """
            )


class TorchDistributor(Distributor):
    """
    A class to support distributed training on PyTorch and PyTorch Lightning using PySpark.

    .. versionadded:: 3.4.0

    Examples
    --------

    Run PyTorch Training locally on GPU (using a PyTorch native function)

    >>> def train(learning_rate):
    ...     import torch.distributed
    ...     torch.distributed.init_process_group(backend="nccl")
    ...     # ...
    ...     torch.destroy_process_group()
    ...     return model # or anything else
    >>> distributor = TorchDistributor(
    ...     num_processes=2,
    ...     local_mode=True,
    ...     use_gpu=True)
    >>> model = distributor.run(train, 1e-3)

    Run PyTorch Training on GPU (using a file with PyTorch code)

    >>> distributor = TorchDistributor(
    ...     num_processes=2,
    ...     local_mode=False,
    ...     use_gpu=True)
    >>> distributor.run("/path/to/train.py", "--learning-rate=1e-3")

    Run PyTorch Lightning Training on GPU

    >>> num_proc = 2
    >>> def train():
    ...     from pytorch_lightning import Trainer
    ...     # ...
    ...     # required to set devices = 1 and num_nodes == num_processes for multi node
    ...     # required to set devices = num_processes and num_nodes = 1 for single node multi GPU
    ...     trainer = Trainer(accelerator="gpu", devices=1, num_nodes=num_proc, strategy="ddp")
    ...     trainer.fit()
    ...     # ...
    ...     return trainer
    >>> distributor = TorchDistributor(
    ...     num_processes=num_proc,
    ...     local_mode=True,
    ...     use_gpu=True)
    >>> trainer = distributor.run(train)
    """

    def __init__(
        self,
        num_processes: int = 1,
        local_mode: bool = True,
        use_gpu: bool = True,
    ):
        """Initializes the distributor.

        Parameters
        ----------
        num_processes : int, optional
            An integer that determines how many different concurrent
            tasks are allowed. We expect spark.task.gpus = 1 for GPU-enabled training. Default
            should be 1; we don't want to invoke multiple cores/gpus without explicit mention.
        local_mode : bool, optional
            A boolean that determines whether we are using the driver
            node for training. Default should be false; we don't want to invoke executors without
            explicit mention.
        use_gpu : bool, optional
            A boolean that indicates whether or not we are doing training
            on the GPU. Note that there are differences in how GPU-enabled code looks like and
            how CPU-specific code looks like.

        Raises
        ------
        ValueError
            If any of the parameters are incorrect.
        RuntimeError
            If an active SparkSession is unavailable.
        """
        super().__init__(num_processes, local_mode, use_gpu)
        self.ssl_conf = "pytorch.spark.distributor.ignoreSsl"  # type: ignore
        self._validate_input_params()
        self.input_params = self._create_input_params()

    @staticmethod
    def _create_torchrun_command(
        input_params: Dict[str, Any], path_to_train_file: str, *args: Any
    ) -> List[str]:
        local_mode = input_params["local_mode"]
        num_processes = input_params["num_processes"]

        if local_mode:
            torchrun_args = ["--standalone", "--nnodes=1"]
            processes_per_node = num_processes
        else:
            pass
            # TODO(SPARK-41592): Handle distributed training

        args_string = list(map(str, args))  # converting all args to strings

        return (
            [sys.executable, "-m", "pyspark.ml.torch.distributor.torch_run_process_wrapper"]
            + torchrun_args
            + [f"--nproc_per_node={processes_per_node}"]
            + [path_to_train_file, *args_string]
        )

    @staticmethod
    def _execute_command(
        cmd: List[str], _prctl: bool = True, redirect_to_stdout: bool = True
    ) -> None:
        _TAIL_LINES_TO_KEEP = 100

        def sigterm_on_parent_death() -> None:
            """
            Uses prctl to automatically send SIGTERM to the command process when its parent is dead.
            This handles the case when the parent is a PySpark worker process.
            If a user cancels the PySpark job, the worker process gets killed, regardless of
            PySpark daemon and worker reuse settings.
            """
            if _prctl:
                try:
                    libc = ctypes.CDLL("libc.so.6")
                    # Set the parent process death signal of the command process to SIGTERM.
                    libc.prctl(1, signal.SIGTERM)
                except OSError:
                    pass

        task = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            env=os.environ,
            preexec_fn=sigterm_on_parent_death,
        )
        task.stdin.close()  # type: ignore
        tail: collections.deque = collections.deque(maxlen=_TAIL_LINES_TO_KEEP)
        try:
            for line in task.stdout:  # type: ignore
                decoded = line.decode()
                tail.append(decoded)
                if redirect_to_stdout:
                    sys.stdout.write(decoded)
            task.wait()
        finally:
            if task.poll() is None:
                try:
                    task.terminate()  # SIGTERM
                    time.sleep(0.5)
                    if task.poll() is None:
                        task.kill()  # SIGKILL
                except OSError:
                    pass
        if task.returncode != os.EX_OK:
            if len(tail) == _TAIL_LINES_TO_KEEP:
                last_n_msg = f"last {_TAIL_LINES_TO_KEEP} lines of the task output are"
            else:
                last_n_msg = "task output is"
            task_output = "".join(tail)
            raise RuntimeError(
                f"Command {cmd} failed with return code {task.returncode}."
                f"The {last_n_msg} included below: {task_output}"
            )

    def _run_local_training(
        self,
        framework_wrapper_fn: Optional[Callable],
        train_object: Union[Callable, str],
        *args: Any,
    ) -> Optional[Any]:
        CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES"
        cuda_state_was_set = CUDA_VISIBLE_DEVICES in os.environ
        old_cuda_visible_devices = os.environ.get(CUDA_VISIBLE_DEVICES, "")
        try:
            if self.use_gpu:
                gpus_owned = get_gpus_owned(self.sc)

                if self.num_processes > len(gpus_owned):
                    raise ValueError(
                        f"""{self.num_processes} processes were requested
                        for local training with GPU training but only
                        {len(gpus_owned)} GPUs were available."""
                    )
                random.seed(hash(train_object))
                selected_gpus = [str(e) for e in random.sample(gpus_owned, self.num_processes)]
                os.environ[CUDA_VISIBLE_DEVICES] = ",".join(selected_gpus)

            output = framework_wrapper_fn(self.input_params, train_object, *args)  # type: ignore
        finally:
            if cuda_state_was_set:
                os.environ[CUDA_VISIBLE_DEVICES] = old_cuda_visible_devices
            else:
                if CUDA_VISIBLE_DEVICES in os.environ:
                    del os.environ[CUDA_VISIBLE_DEVICES]

        return output

    @staticmethod
    def _run_training_on_pytorch_file(
        input_params: Dict[str, Any], train_path: str, *args: Any
    ) -> None:
        training_command = TorchDistributor._create_torchrun_command(
            input_params, train_path, *args
        )
        TorchDistributor._execute_command(training_command)

    def run(self, train_object: Union[Callable, str], *args: Any) -> Optional[Any]:
        """Runs distributed training.

        Parameters
        ----------
        train_object : callable object or str
            Either a PyTorch/PyTorch Lightning training function or the path to a python file
            that launches distributed training.
        args :
            The arguments for train_object

        Returns
        -------
            Returns the output of train_object called with args if train_object is a
            Callable with an expected output.
        """
        framework_wrapper_fn = None
        if isinstance(train_object, str):
            framework_wrapper_fn = TorchDistributor._run_training_on_pytorch_file
        if self.local_mode:
            output = self._run_local_training(framework_wrapper_fn, train_object, *args)
        return output
