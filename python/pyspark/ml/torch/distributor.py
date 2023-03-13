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

from contextlib import contextmanager
import collections
import logging
import math
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from typing import Union, Callable, List, Dict, Optional, Any, Tuple, Generator

from pyspark import cloudpickle
from pyspark.sql import SparkSession
from pyspark.ml.torch.log_communication import (  # type: ignore
    get_driver_host,
    LogStreamingClient,
    LogStreamingServer,
)
from pyspark.context import SparkContext
from pyspark.taskcontext import BarrierTaskContext


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


def get_logger(name: str) -> logging.Logger:
    """
    Gets a logger by name, or creates and configures it for the first time.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # If the logger is configured, skip the configure
    if not logger.handlers and not logging.getLogger().handlers:
        handler = logging.StreamHandler(sys.stderr)
        logger.addHandler(handler)
    return logger


def get_gpus_owned(context: Union[SparkContext, BarrierTaskContext]) -> List[str]:
    """Gets the number of GPUs that Spark scheduled to the calling task.

    Parameters
    ----------
    context : :class:`SparkContext` or :class:`BarrierTaskContext`
        The :class:`SparkContext` or :class:`BarrierTaskContext` that has GPUs available.

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
    if isinstance(context, SparkContext):
        addresses = context.resources["gpu"].addresses
    else:
        addresses = context.resources()["gpu"].addresses
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
        self.logger = get_logger(self.__class__.__name__)
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
        for unneeded_param in ["spark", "sc", "ssl_conf", "logger"]:
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
                    self.logger.warning(
                        "'num_processes' cannot be set to a value greater than the number of "
                        f"available GPUs on the driver, which is {num_available_gpus}. "
                        "'num_processes' was reset to be equal to the number of available GPUs.",
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
                self.logger.warning(
                    textwrap.dedent(
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
                    )
                )
                return
            raise RuntimeError(
                textwrap.dedent(
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
            )


class TorchDistributor(Distributor):
    """
    A class to support distributed training on PyTorch and PyTorch Lightning using PySpark.

    .. versionadded:: 3.4.0

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
    ...     # required to set devices = 1 and num_nodes = num_processes for multi node
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

    _PICKLED_FUNC_FILE = "func.pickle"
    _TRAIN_FILE = "train.py"
    _PICKLED_OUTPUT_FILE = "output.pickle"

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
            master_addr, master_port = os.environ["MASTER_ADDR"], os.environ["MASTER_PORT"]
            node_rank = os.environ["RANK"]
            torchrun_args = [
                f"--nnodes={num_processes}",
                f"--node_rank={node_rank}",
                f"--rdzv_endpoint={master_addr}:{master_port}",
                "--rdzv_id=0",
            ]  # TODO: setup random ID that is gleaned from env variables
            processes_per_node = 1

        args_string = list(map(str, args))  # converting all args to strings

        return [
            sys.executable,
            "-m",
            "pyspark.ml.torch.torch_run_process_wrapper",
            *torchrun_args,
            f"--nproc_per_node={processes_per_node}",
            path_to_train_file,
            *args_string,
        ]

    @staticmethod
    def _execute_command(
        cmd: List[str],
        _prctl: bool = True,
        redirect_to_stdout: bool = True,
        log_streaming_client: Optional[LogStreamingClient] = None,
    ) -> None:
        _TAIL_LINES_TO_KEEP = 100

        task = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            env=os.environ,
        )
        task.stdin.close()  # type: ignore
        tail: collections.deque = collections.deque(maxlen=_TAIL_LINES_TO_KEEP)
        try:
            for line in task.stdout:  # type: ignore
                decoded = line.decode()
                tail.append(decoded)
                if redirect_to_stdout:
                    sys.stdout.write(decoded)
                if log_streaming_client:
                    log_streaming_client.send(decoded.rstrip())
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
                f"Command {cmd} failed with return code {task.returncode}. "
                f"The {last_n_msg} included below: {task_output}"
            )

    def _run_local_training(
        self,
        framework_wrapper_fn: Callable,
        train_object: Union[Callable, str],
        *args: Any,
    ) -> Optional[Any]:
        CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES"
        cuda_state_was_set = CUDA_VISIBLE_DEVICES in os.environ
        old_cuda_visible_devices = os.environ.get(CUDA_VISIBLE_DEVICES, "")
        try:
            if self.use_gpu:
                gpus_owned = get_gpus_owned(self.sc)
                random.seed(hash(train_object))
                selected_gpus = [str(e) for e in random.sample(gpus_owned, self.num_processes)]
                os.environ[CUDA_VISIBLE_DEVICES] = ",".join(selected_gpus)

            self.logger.info(f"Started local training with {self.num_processes} processes")
            output = framework_wrapper_fn(self.input_params, train_object, *args)
            self.logger.info(f"Finished local training with {self.num_processes} processes")

        finally:
            if cuda_state_was_set:
                os.environ[CUDA_VISIBLE_DEVICES] = old_cuda_visible_devices
            else:
                if CUDA_VISIBLE_DEVICES in os.environ:
                    del os.environ[CUDA_VISIBLE_DEVICES]

        return output

    def _get_spark_task_function(
        self,
        framework_wrapper_fn: Optional[Callable],
        train_object: Union[Callable, str],
        *args: Any,
    ) -> Callable:
        """Creates a spark task function that is used inside `mapPartitions`.

        Parameters
        ----------
        framework_wrapper_fn : Optional[Callable]
            The function that determines whether we are running training
            on a PyTorch file or a PyTorch function.
        train_object : Union[Callable, str]
            The actual train function/file.

        Returns
        -------
        Callable
            The wrapped function ready for use with `mapPartitions`
        """
        num_processes = self.num_processes
        use_gpu = self.use_gpu
        input_params = self.input_params
        driver_address = self.driver_address
        log_streaming_server_port = self.log_streaming_server_port

        # Spark task program
        def wrapped_train_fn(_):  # type: ignore[no-untyped-def]
            import os
            from pyspark import BarrierTaskContext

            CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES"

            def get_free_port(address: str, context: "BarrierTaskContext") -> int:
                port = ""
                if context.partitionId() == 0:
                    try:
                        import socket

                        sock = socket.socket()
                        sock.bind((address, 0))
                        port = sock.getsockname()[1]
                    except socket.error:
                        pass
                available_port = context.allGather(str(port))[0]
                if not available_port:
                    raise RuntimeError("Failed to find free port for distributed training.")
                return int(available_port)

            def set_torch_config(context: "BarrierTaskContext") -> None:
                addrs = [e.address.split(":")[0] for e in context.getTaskInfos()]

                os.environ["MASTER_ADDR"] = str(addrs[0])
                os.environ["MASTER_PORT"] = str(get_free_port(addrs[0], context))
                os.environ["WORLD_SIZE"] = str(num_processes)
                os.environ["NODE_RANK"] = str(context.partitionId())
                os.environ["RANK"] = str(context.partitionId())

            def set_gpus(context: "BarrierTaskContext") -> None:
                if CUDA_VISIBLE_DEVICES in os.environ:
                    return

                gpus_owned = get_gpus_owned(context)
                os.environ[CUDA_VISIBLE_DEVICES] = ",".join(gpus_owned)

            context = BarrierTaskContext.get()

            if use_gpu:
                set_gpus(context)
            else:
                os.environ[CUDA_VISIBLE_DEVICES] = ""
            set_torch_config(context)

            log_streaming_client = LogStreamingClient(driver_address, log_streaming_server_port)
            input_params["log_streaming_client"] = log_streaming_client
            try:
                output = framework_wrapper_fn(input_params, train_object, *args)
            finally:
                try:
                    LogStreamingClient._destroy()
                except BaseException:
                    pass

            if context.partitionId() == 0:
                yield output

        return wrapped_train_fn

    def _run_distributed_training(
        self,
        framework_wrapper_fn: Callable,
        train_object: Union[Callable, str],
        *args: Any,
    ) -> Optional[Any]:
        if not framework_wrapper_fn:
            raise RuntimeError("Unknown combination of parameters")

        log_streaming_server = LogStreamingServer()
        self.driver_address = get_driver_host(self.sc)
        log_streaming_server.start(spark_host_address=self.driver_address)
        time.sleep(1)  # wait for the server to start
        self.log_streaming_server_port = log_streaming_server.port

        spark_task_function = self._get_spark_task_function(
            framework_wrapper_fn, train_object, *args
        )
        self._check_encryption()
        self.logger.info(
            f"Started distributed training with {self.num_processes} executor proceses"
        )
        try:
            result = (
                self.sc.parallelize(range(self.num_tasks), self.num_tasks)
                .barrier()
                .mapPartitions(spark_task_function)
                .collect()[0]
            )
        finally:
            log_streaming_server.shutdown()
        self.logger.info(
            f"Finished distributed training with {self.num_processes} executor proceses"
        )
        return result

    @staticmethod
    def _run_training_on_pytorch_file(
        input_params: Dict[str, Any], train_path: str, *args: Any
    ) -> None:
        log_streaming_client = input_params.get("log_streaming_client", None)
        training_command = TorchDistributor._create_torchrun_command(
            input_params, train_path, *args
        )
        TorchDistributor._execute_command(
            training_command, log_streaming_client=log_streaming_client
        )

    @staticmethod
    @contextmanager
    def _setup_files(train_fn: Callable, *args: Any) -> Generator[Tuple[str, str], None, None]:
        save_dir = TorchDistributor._create_save_dir()
        pickle_file_path = TorchDistributor._save_pickled_function(save_dir, train_fn, *args)
        output_file_path = os.path.join(save_dir, TorchDistributor._PICKLED_OUTPUT_FILE)
        train_file_path = TorchDistributor._create_torchrun_train_file(
            save_dir, pickle_file_path, output_file_path
        )
        try:
            yield (train_file_path, output_file_path)
        finally:
            TorchDistributor._cleanup_files(save_dir)

    @staticmethod
    def _run_training_on_pytorch_function(
        input_params: Dict[str, Any], train_fn: Callable, *args: Any
    ) -> Any:
        with TorchDistributor._setup_files(train_fn, *args) as (train_file_path, output_file_path):
            args = []  # type: ignore
            TorchDistributor._run_training_on_pytorch_file(input_params, train_file_path, *args)
            if not os.path.exists(output_file_path):
                raise RuntimeError(
                    "TorchDistributor failed during training. "
                    "View stdout logs for detailed error message."
                )
            try:
                output = TorchDistributor._get_pickled_output(output_file_path)
            except Exception as e:
                raise RuntimeError(
                    "TorchDistributor failed due to a pickling error. "
                    "View stdout logs for detailed error message."
                ) from e
        return output

    @staticmethod
    def _create_save_dir() -> str:
        # TODO: need to do this in a safe way to avoid issues during concurrent runs
        return tempfile.mkdtemp()

    @staticmethod
    def _cleanup_files(save_dir: str) -> None:
        shutil.rmtree(save_dir, ignore_errors=True)

    @staticmethod
    def _save_pickled_function(save_dir: str, train_fn: Union[str, Callable], *args: Any) -> str:
        saved_pickle_path = os.path.join(save_dir, TorchDistributor._PICKLED_FUNC_FILE)
        with open(saved_pickle_path, "wb") as f:
            cloudpickle.dump((train_fn, args), f)
        return saved_pickle_path

    @staticmethod
    def _create_torchrun_train_file(
        save_dir_path: str, pickle_file_path: str, output_file_path: str
    ) -> str:
        code = textwrap.dedent(
            f"""
                    import cloudpickle
                    import os

                    if __name__ == "__main__":
                        with open("{pickle_file_path}", "rb") as f:
                            train_fn, args = cloudpickle.load(f)
                        output = train_fn(*args)
                        with open("{output_file_path}", "wb") as f:
                            cloudpickle.dump(output, f)
                    """
        )
        saved_file_path = os.path.join(save_dir_path, TorchDistributor._TRAIN_FILE)
        with open(saved_file_path, "w") as f:
            f.write(code)
        return saved_file_path

    @staticmethod
    def _get_pickled_output(output_file_path: str) -> Any:
        with open(output_file_path, "rb") as f:
            output = cloudpickle.load(f)
        return output

    def run(self, train_object: Union[Callable, str], *args: Any) -> Optional[Any]:
        """Runs distributed training.

        Parameters
        ----------
        train_object : callable object or str
            Either a PyTorch function, PyTorch Lightning function, or the path to a python file
            that launches distributed training.
        args :
            If train_object is a python function and not a path to a python file, args need
            to be the input parameters to that function. It would look like

            >>> model = distributor.run(train, 1e-3, 64)

            where train is a function and 1e-3 is a regular numeric input to the function.

            If train_object is a python file, then args would be the command-line arguments for
            that python file which are all in the form of strings. An example would be

            >>> distributor.run("/path/to/train.py", "--learning-rate=1e-3", "--batch-size=64")

            where since the input is a path, all of the parameters are strings that can be
            handled by argparse in that python file.

        Returns
        -------
            Returns the output of train_object called with args if the train_object is a
            Callable with an expected output. Returns None if train_object is a file.
        """
        if isinstance(train_object, str):
            framework_wrapper_fn = TorchDistributor._run_training_on_pytorch_file
        else:
            framework_wrapper_fn = (
                TorchDistributor._run_training_on_pytorch_function  # type: ignore
            )
        if self.local_mode:
            output = self._run_local_training(framework_wrapper_fn, train_object, *args)
        else:
            output = self._run_distributed_training(framework_wrapper_fn, train_object, *args)
        return output
