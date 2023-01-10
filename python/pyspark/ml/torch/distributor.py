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

import math
from typing import Union, Callable, Optional, Any
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
    sc : SparkContext
        The SparkContext for the distributor.
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
        pass
