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
import json
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
from typing import (
    Union,
    Callable,
    List,
    Dict,
    Optional,
    Any,
    Tuple,
    Generator,
    Iterator,
)

from pyspark import cloudpickle
from pyspark.resource.information import ResourceInformation
from pyspark.sql import DataFrame, SparkSession
from pyspark.taskcontext import BarrierTaskContext
from pyspark.ml.torch.log_communication import (  # type: ignore
    LogStreamingClient,
    LogStreamingServer,
)
from pyspark.ml.dl_util import FunctionPickler


def _get_resources(session: SparkSession) -> Dict[str, ResourceInformation]:
    resources: Dict[str, ResourceInformation] = {}
    try:
        resources = session.sparkContext.resources
    except Exception:
        resources = session._client._resources()  # type: ignore[attr-defined]
    return resources


def _get_conf(spark: SparkSession, key: str, default_value: str) -> str:
    """Get the conf "key" from the given spark session,
    or return the default value if the conf is not set.

    Parameters
    ----------
    spark : :class:`SparkSession`
        The :class:`SparkSession` for the distributor.
    key : str
        string for conf name
    default_value : str
        default value for the conf value for the given key

    Returns
    -------
    str
        Returns the string value that corresponds to the conf
    """
    value = spark.conf.get(key, default_value)
    assert value is not None
    return value


# TODO(SPARK-41589): will move the functions and tests to an external file
#       once we are in agreement about which functions should be in utils.py
def _get_conf_boolean(spark: SparkSession, key: str, default_value: str) -> bool:
    value = _get_conf(spark=spark, key=key, default_value=default_value)
    value = value.lower()
    assert value in ["true", "false"]
    return value == "true"


def _get_logger(name: str) -> logging.Logger:
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


def _get_gpus_owned(context: Union[SparkSession, BarrierTaskContext]) -> List[str]:
    """Gets the number of GPUs that Spark scheduled to the calling task.

    Parameters
    ----------
    context : :class:`SparkSession` or :class:`BarrierTaskContext`
        The :class:`SparkSession` or :class:`BarrierTaskContext` that has GPUs available.

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
    if isinstance(context, BarrierTaskContext):
        addresses = context.resources()["gpu"].addresses
    else:
        addresses = _get_resources(context)["gpu"].addresses

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


SPARK_PARTITION_ARROW_DATA_FILE = "SPARK_PARTITION_ARROW_DATA_FILE"
SPARK_DATAFRAME_SCHEMA_FILE = "SPARK_DATAFRAME_SCHEMA_FILE"


class Distributor:
    """
    The parent class for TorchDistributor. This class shouldn't be instantiated directly.
    """

    def __init__(
        self,
        num_processes: int = 1,
        local_mode: bool = True,
        use_gpu: bool = True,
        ssl_conf: Optional[str] = None,
    ):
        from pyspark.sql.utils import is_remote

        self.is_remote = is_remote()
        self.spark = SparkSession.active()

        # indicate whether the server side is local mode
        self.is_spark_local_master = False
        # Refer to 'org.apache.spark.util.Utils#isLocalMaster'
        master = _get_conf(self.spark, "spark.master", "")
        if master == "local" or master.startswith("local["):
            self.is_spark_local_master = True

        self.logger = _get_logger(self.__class__.__name__)
        self.num_processes = num_processes
        self.local_mode = local_mode
        self.use_gpu = use_gpu
        self.num_tasks = self._get_num_tasks()
        self.ssl_conf = ssl_conf

    def _create_input_params(self) -> Dict[str, Any]:
        input_params = self.__dict__.copy()
        for unneeded_param in [
            "spark",
            "ssl_conf",
            "logger",
            "is_remote",
            "is_spark_local_master",
        ]:
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
                task_gpu_amount = int(_get_conf(self.spark, key, "0"))
                if task_gpu_amount < 1:
                    raise RuntimeError(f"'{key}' was unset, so gpu usage is unavailable.")
                # TODO(SPARK-41916): Address situation when spark.task.resource.gpu.amount > 1
                return math.ceil(self.num_processes / task_gpu_amount)
            else:
                key = "spark.driver.resource.gpu.amount"
                if "gpu" not in _get_resources(self.spark):
                    raise RuntimeError("GPUs were unable to be found on the driver.")
                num_available_gpus = int(_get_conf(self.spark, key, "0"))
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
            raise ValueError("num_processes has to be a positive integer")

    def _check_encryption(self) -> None:
        """Checks to see if the user requires encryption of data.
        If required, throw an exception since we don't support that.

        Raises
        ------
        RuntimeError
            Thrown when the user requires ssl encryption or when the user initializes
            the Distributor parent class.
        """
        if not hasattr(self, "ssl_conf"):
            raise RuntimeError(
                "Distributor doesn't have this functionality. Use TorchDistributor instead."
            )
        is_ssl_enabled = _get_conf_boolean(self.spark, "spark.ssl.enabled", "false")
        ignore_ssl = _get_conf_boolean(self.spark, self.ssl_conf, "false")  # type: ignore
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

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

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
    ...
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
    ...
    >>> distributor = TorchDistributor(
    ...     num_processes=num_proc,
    ...     local_mode=True,
    ...     use_gpu=True)
    >>> trainer = distributor.run(train)
    """

    _PICKLED_FUNC_FILE = "func.pickle"
    _TRAIN_FILE = "train.py"
    _PICKLED_OUTPUT_FILE = "output.pickle"
    _TORCH_SSL_CONF = "pytorch.spark.distributor.ignoreSsl"

    def __init__(
        self,
        num_processes: int = 1,
        local_mode: bool = True,
        use_gpu: bool = True,
        _ssl_conf: str = _TORCH_SSL_CONF,
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
        super().__init__(num_processes, local_mode, use_gpu, ssl_conf=_ssl_conf)
        self._validate_input_params()
        self.input_params = self._create_input_params()

    @staticmethod
    def _get_torchrun_args(local_mode: bool, num_processes: int) -> Tuple[List[Any], int]:
        """
        Given the mode and the number of processes, create the arguments to be given to for torch

        Parameters
        ---------
        local_mode: bool
            Whether or not we are running training locally or in a distributed fashion

        num_processes: int
            The number of processes that we are going to use

        Returns
        ------
        Tuple[List[Any], int]
            A tuple containing a list of arguments to pass as pytorch args,
            as well as the number of processes per node
        """
        if local_mode:
            torchrun_args = ["--standalone", "--nnodes=1"]
            processes_per_node = num_processes
            return torchrun_args, processes_per_node

        master_addr = os.environ["MASTER_ADDR"]
        master_port = os.environ["MASTER_PORT"]
        node_rank = os.environ["RANK"]
        torchrun_args = [
            f"--nnodes={num_processes}",
            f"--node_rank={node_rank}",
            f"--rdzv_endpoint={master_addr}:{master_port}",
            "--rdzv_id=0",  # TODO: setup random ID that is gleaned from env variables
        ]
        processes_per_node = 1
        return torchrun_args, processes_per_node

    @staticmethod
    def _create_torchrun_command(
        input_params: Dict[str, Any], path_to_train_file: str, *args: Any
    ) -> List[str]:
        local_mode = input_params["local_mode"]
        num_processes = input_params["num_processes"]

        torchrun_args, processes_per_node = TorchDistributor._get_torchrun_args(
            local_mode=local_mode, num_processes=num_processes
        )
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
                    if (
                        log_streaming_client
                        and not log_streaming_client.failed
                        and (
                            log_streaming_client.sock.getsockname()[0]
                            == log_streaming_client.sock.getpeername()[0]
                        )
                    ):
                        # If log_streaming_client and log_stream_server are in the same
                        # node (typical case is spark local mode),
                        # server side will redirect the log to STDOUT,
                        # to avoid STDOUT outputs duplication, skip redirecting
                        # logs to STDOUT in client side.
                        pass
                    else:
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

    @staticmethod
    def _get_output_from_framework_wrapper(
        framework_wrapper: Optional[Callable],
        input_params: Dict,
        train_object: Union[Callable, str],
        run_pytorch_file_fn: Optional[Callable],
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Any]:
        """
        This function is meant to get the output from framework wrapper function by passing in the
        correct arguments, depending on the type of train_object.

        Parameters
        ----------
        framework_wrapper: Optional[Callable]
            Function pointer that will be invoked. Can either be the function that runs distributed
            training on files if train_object is a string. Otherwise, it will be the function that
            runs distributed training for functions if the train_object is a Callable
        input_params: Dict
            A dictionary that maps parameter to arguments for the command to be created.
        train_object: Union[Callable, str]
            This input comes from the user. If the user inputs a string, then this means
            it's a filepath. Otherwise, if the input is a function, then this means that
            the user wants to run this function in a distributed manner.
        run_pytorch_file_fn: Optional[Callable]
            The function that will be used to run distributed training of a file;
            mainly used for the distributed training using a function.
        *args: Any
            Extra arguments to be used by framework wrapper.
        **kwargs: Any
            Extra keyword args to be used. Not currently supported but kept for
            future improvement.

        Returns
        -------
        Optional[Any]
            Returns the result of the framework_wrapper
        """
        if not framework_wrapper:
            raise RuntimeError("`framework_wrapper` is not set. ...")
        # The object to train is a file path, so framework_wrapper is some
        # run_training_on_pytorch_file function.
        if type(train_object) is str:
            return framework_wrapper(input_params, train_object, *args, **kwargs)
        else:
            # We are doing training with a function, will call run_training_on_pytorch_function
            if not run_pytorch_file_fn:
                run_pytorch_file_fn = TorchDistributor._run_training_on_pytorch_file
            return framework_wrapper(
                input_params, train_object, run_pytorch_file_fn, *args, **kwargs
            )

    def _run_local_training(
        self,
        framework_wrapper_fn: Callable,
        train_object: Union[Callable, str],
        run_pytorch_file_fn: Optional[Callable],
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Any]:
        CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES"
        cuda_state_was_set = CUDA_VISIBLE_DEVICES in os.environ
        old_cuda_visible_devices = os.environ.get(CUDA_VISIBLE_DEVICES, "")
        try:
            # Only replace the GPUs with 'SparkContext.resources' in legacy mode.
            # In connect mode, this replacement is skipped since only GPUs on the client side
            # can be used.
            if self.use_gpu and not self.is_remote:
                gpus_owned = _get_gpus_owned(self.spark)
                random.seed(hash(train_object))
                selected_gpus = [str(e) for e in random.sample(gpus_owned, self.num_processes)]
                os.environ[CUDA_VISIBLE_DEVICES] = ",".join(selected_gpus)

            self.logger.info(f"Started local training with {self.num_processes} processes")
            output = TorchDistributor._get_output_from_framework_wrapper(
                framework_wrapper_fn,
                self.input_params,
                train_object,
                run_pytorch_file_fn,
                *args,
                **kwargs,
            )
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
        run_pytorch_file_fn: Optional[Callable],
        input_dataframe: Optional["DataFrame"],
        *args: Any,
        **kwargs: Any,
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
        is_spark_local_master = self.is_spark_local_master
        driver_owned_gpus: List[str] = []
        if is_spark_local_master and use_gpu:
            driver_owned_gpus = _get_gpus_owned(self.spark)

        if input_dataframe is not None:
            schema_json = input_dataframe.schema.jsonValue()
        else:
            schema_json = None

        # Spark task program
        def wrapped_train_fn(iterator):  # type: ignore[no-untyped-def]
            import os
            import pandas as pd
            import pyarrow
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

                if context.partitionId() >= num_processes:
                    raise ValueError(
                        "TorchDistributor._train_on_dataframe requires setting num_processes "
                        "equal to input spark dataframe partition number."
                    )

            if is_spark_local_master:
                # distributed training on a local mode spark cluster
                def set_gpus(context: "BarrierTaskContext") -> None:
                    if CUDA_VISIBLE_DEVICES in os.environ:
                        return

                    gpu_owned = driver_owned_gpus[context.partitionId()]
                    os.environ[CUDA_VISIBLE_DEVICES] = gpu_owned

            else:

                def set_gpus(context: "BarrierTaskContext") -> None:
                    if CUDA_VISIBLE_DEVICES in os.environ:
                        return

                    gpus_owned = _get_gpus_owned(context)
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
                with TorchDistributor._setup_spark_partition_data(iterator, schema_json):
                    output = TorchDistributor._get_output_from_framework_wrapper(
                        framework_wrapper_fn,
                        input_params,
                        train_object,
                        run_pytorch_file_fn,
                        *args,
                        **kwargs,
                    )
            finally:
                try:
                    LogStreamingClient._destroy()
                except BaseException:
                    pass

            if context.partitionId() == 0:
                output_bytes = cloudpickle.dumps(output)
                output_size = len(output_bytes)

                # In Spark Connect, DataFrame.collect stacks rows to size
                # 'spark.connect.grpc.arrow.maxBatchSize' (default 4MiB),
                # here use 4KiB for each chunk, which mean each arrow batch
                # may contain about 1000 chunks.
                chunks = []
                chunk_size = 4096
                index = 0
                while index < output_size:
                    chunks.append(output_bytes[index : index + chunk_size])
                    index += chunk_size

                yield pyarrow.RecordBatch.from_pandas(pd.DataFrame(data={"chunk": chunks}))

        return wrapped_train_fn

    def _run_distributed_training(
        self,
        framework_wrapper_fn: Callable,
        train_object: Union[Callable, str],
        run_pytorch_file_fn: Optional[Callable],
        spark_dataframe: Optional["DataFrame"],
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Any]:
        log_streaming_server = LogStreamingServer()
        self.driver_address = _get_conf(self.spark, "spark.driver.host", "")
        assert self.driver_address != ""
        try:
            log_streaming_server.start(spark_host_address=self.driver_address)
            time.sleep(1)  # wait for the server to start
            self.log_streaming_server_port = log_streaming_server.port
        except Exception as e:
            # If starting log streaming server failed, we don't need to break
            # the distributor training but emit a warning instead.
            self.log_streaming_server_port = -1
            self.logger.warning(
                "Start torch distributor log streaming server failed, "
                "You cannot receive logs sent from distributor workers, ",
                f"error: {repr(e)}.",
            )

        try:
            spark_task_function = self._get_spark_task_function(
                framework_wrapper_fn,
                train_object,
                run_pytorch_file_fn,
                spark_dataframe,
                *args,
                **kwargs,
            )
            self._check_encryption()
            self.logger.info(
                f"Started distributed training with {self.num_processes} executor processes"
            )
            if spark_dataframe is not None:
                input_df = spark_dataframe
            else:
                input_df = self.spark.range(
                    start=0, end=self.num_tasks, step=1, numPartitions=self.num_tasks
                )
            rows = input_df.mapInArrow(
                func=spark_task_function, schema="chunk binary", barrier=True
            ).collect()
            output_bytes = b"".join([row.chunk for row in rows])
            result = cloudpickle.loads(output_bytes)
        finally:
            log_streaming_server.shutdown()
        self.logger.info(
            f"Finished distributed training with {self.num_processes} executor processes"
        )
        return result

    @staticmethod
    def _run_training_on_pytorch_file(
        input_params: Dict[str, Any], train_path: str, *args: Any, **kwargs: Any
    ) -> None:
        if kwargs:
            raise ValueError("Running pytorch file does not support key-word type arguments.")
        log_streaming_client = input_params.get("log_streaming_client", None)
        training_command = TorchDistributor._create_torchrun_command(
            input_params, train_path, *args
        )
        TorchDistributor._execute_command(
            training_command, log_streaming_client=log_streaming_client
        )

    @staticmethod
    @contextmanager
    def _setup_files(
        train_fn: Callable, *args: Any, **kwargs: Any
    ) -> Generator[Tuple[str, str], None, None]:
        save_dir = TorchDistributor._create_save_dir()
        pickle_file_path = FunctionPickler.pickle_fn_and_save(
            train_fn, "", save_dir, *args, **kwargs
        )
        output_file_path = os.path.join(save_dir, TorchDistributor._PICKLED_OUTPUT_FILE)
        script_path = os.path.join(save_dir, TorchDistributor._TRAIN_FILE)
        train_file_path = FunctionPickler.create_fn_run_script(
            pickle_file_path, output_file_path, script_path
        )
        try:
            yield (train_file_path, output_file_path)
        finally:
            TorchDistributor._cleanup_files(save_dir)

    @staticmethod
    @contextmanager
    def _setup_spark_partition_data(
        partition_data_iterator: Iterator[Any], input_schema_json: Dict[str, Any]
    ) -> Iterator[Any]:
        from pyspark.sql.pandas.serializers import ArrowStreamSerializer
        from pyspark.core.files import SparkFiles
        import json

        if input_schema_json is None:
            yield
            return

        # We need to temporarily write partition data into a temp dir,
        # partition data might be huge, so we need to write it under
        # configured `SPARK_LOCAL_DIRS`.
        save_dir = TorchDistributor._create_save_dir(root_dir=SparkFiles.getRootDirectory())

        try:
            serializer = ArrowStreamSerializer()
            arrow_file_path = os.path.join(save_dir, "data.arrow")
            with open(arrow_file_path, "wb") as f:
                serializer.dump_stream(partition_data_iterator, f)
                if f.tell() == 0:
                    # Nothing is written to file, this partition is empty
                    raise ValueError(
                        "Empty Spark partition is not allowed in "
                        "TorchDistributor.train_on_dataframe."
                    )

            schema_file_path = os.path.join(save_dir, "schema.json")
            schema_json_string = json.dumps(input_schema_json)

            with open(schema_file_path, "w") as f:
                f.write(schema_json_string)

            os.environ[SPARK_PARTITION_ARROW_DATA_FILE] = arrow_file_path
            os.environ[SPARK_DATAFRAME_SCHEMA_FILE] = schema_file_path
            yield
        finally:
            os.environ.pop(SPARK_PARTITION_ARROW_DATA_FILE)
            os.environ.pop(SPARK_DATAFRAME_SCHEMA_FILE)
            TorchDistributor._cleanup_files(save_dir)

    @staticmethod
    def _run_training_on_pytorch_function(
        input_params: Dict[str, Any],
        train_fn: Callable,
        run_pytorch_file_fn: Optional[Callable],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if not run_pytorch_file_fn:
            run_pytorch_file_fn = TorchDistributor._run_training_on_pytorch_file

        with TorchDistributor._setup_files(train_fn, *args, **kwargs) as (
            train_file_path,
            output_file_path,
        ):
            run_pytorch_file_fn(input_params, train_file_path)
            if not os.path.exists(output_file_path):
                raise RuntimeError(
                    "TorchDistributor failed during training."
                    "View stdout logs for detailed error message."
                )
            try:
                output = FunctionPickler.get_fn_output(output_file_path)
            except Exception as e:
                raise RuntimeError(
                    "TorchDistributor failed due to a pickling error. "
                    "View stdout logs for detailed error message."
                ) from e
        return output

    @staticmethod
    def _create_save_dir(root_dir: Optional[str] = None) -> str:
        # TODO: need to do this in a safe way to avoid issues during concurrent runs
        return tempfile.mkdtemp(dir=root_dir)

    @staticmethod
    def _cleanup_files(save_dir: str) -> None:
        shutil.rmtree(save_dir, ignore_errors=True)

    def run(self, train_object: Union[Callable, str], *args: Any, **kwargs: Any) -> Optional[Any]:
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

            where train is a function and 1e-3 and 64 are regular numeric inputs to the function.

            If train_object is a python file, then args would be the command-line arguments for
            that python file which are all in the form of strings. An example would be

            >>> distributor.run("/path/to/train.py", "--learning-rate=1e-3", "--batch-size=64")

            where since the input is a path, all of the parameters are strings that can be
            handled by argparse in that python file.
        kwargs :
            If train_object is a python function and not a path to a python file, kwargs need
            to be the key-word input parameters to that function. It would look like

            >>> model = distributor.run(train, tol=1e-3, max_iter=64)

            where train is a function of 2 arguments `tol` and `max_iter`.

            If train_object is a python file, then you should not set kwargs arguments.

        Returns
        -------
            Returns the output of train_object called with args inside spark rank 0 task if the
            train_object is a Callable with an expected output. Returns None if train_object is
            a file.
        """
        return self._run(
            train_object, TorchDistributor._run_training_on_pytorch_file, *args, **kwargs
        )

    def _run(
        self,
        train_object: Union[Callable, str],
        run_pytorch_file_fn: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Any]:
        if isinstance(train_object, str):
            framework_wrapper_fn = run_pytorch_file_fn
        else:
            framework_wrapper_fn = TorchDistributor._run_training_on_pytorch_function
        if self.local_mode:
            output = self._run_local_training(
                framework_wrapper_fn, train_object, run_pytorch_file_fn, *args, **kwargs
            )
        else:
            output = self._run_distributed_training(
                framework_wrapper_fn, train_object, run_pytorch_file_fn, None, *args, **kwargs
            )
        return output

    def _train_on_dataframe(
        self,
        train_function: Callable,
        spark_dataframe: "DataFrame",
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Runs distributed training using provided Spark DataFrame as input data.
        You should ensure the input Spark DataFrame have evenly distributed partitions,
        and this method starts a barrier Spark job that each Spark task in the job
        process one partition of the input Spark DataFrame.

        Parameters
        ----------
        train_function :
            Either a PyTorch function, PyTorch Lightning function that launches distributed
            training. Note that inside the function, you can call
            `pyspark.ml.torch.distributor.get_spark_partition_data_loader` API to get a torch
            data loader, the data loader loads data from the corresponding partition of the
            input Spark DataFrame.
        spark_dataframe :
            An input Spark DataFrame that can be used in PyTorch `train_function` function.
            See `train_function` argument doc for details.
        args :
            `args` need to be the input parameters to `train_function` function. It would look like

            >>> model = distributor.run(train, 1e-3, 64)

            where train is a function and 1e-3 and 64 are regular numeric inputs to the function.
        kwargs :
            `kwargs` need to be the key-word input parameters to `train_function` function.
            It would look like

            >>> model = distributor.run(train, tol=1e-3, max_iter=64)

            where train is a function of 2 arguments `tol` and `max_iter`.

        Returns
        -------
            Returns the output of `train_function` called with args inside Spark rank 0 task.
        """

        if self.local_mode:
            raise ValueError(
                "TorchDistributor.train_on_dataframe requires setting "
                "TorchDistributor.local_mode to False."
            )

        return self._run_distributed_training(
            TorchDistributor._run_training_on_pytorch_function,
            train_function,
            TorchDistributor._run_training_on_pytorch_file,
            spark_dataframe,
            *args,
            **kwargs,
        )


def _get_spark_partition_data_loader(
    num_samples: int, batch_size: int, num_workers: int = 1, prefetch_factor: int = 2
) -> Any:
    """
    This function must be called inside the `train_function` where `train_function`
    is the input argument of `TorchDistributor.train_on_dataframe`.
    The function returns a pytorch data loader that loads data from
    the corresponding spark partition data.

    Parameters
    ----------
    num_samples :
        Number of samples to generate per epoch. If `num_samples` is less than the number of
        rows in the spark partition, it generate the first `num_samples` rows of
        the spark partition, if `num_samples` is greater than the number of
        rows in the spark partition, then after the iterator loaded all rows from the partition,
        it wraps round back to the first row.
    batch_size:
        How many samples per batch to load.
    num_workers:
        How many subprocesses to use for data loading.
        0 means that the data will be loaded in the main process.
    prefetch_factor:
        Number of batches loaded in advance by each worker
    """
    from pyspark.sql.types import StructType
    from pyspark.ml.torch.data import _SparkPartitionTorchDataset
    from torch.utils.data import DataLoader

    arrow_file = os.environ[SPARK_PARTITION_ARROW_DATA_FILE]
    schema_file = os.environ[SPARK_DATAFRAME_SCHEMA_FILE]

    with open(schema_file, "r") as fp:
        schema = StructType.fromJson(json.load(fp))

    dataset = _SparkPartitionTorchDataset(arrow_file, schema, num_samples)

    if num_workers > 0:
        return DataLoader(
            dataset, batch_size, num_workers=num_workers, prefetch_factor=prefetch_factor
        )
    else:
        # if num_workers is zero, we cannot set `prefetch_factor` otherwise
        # torch will raise error.
        return DataLoader(dataset, batch_size, num_workers=num_workers)
