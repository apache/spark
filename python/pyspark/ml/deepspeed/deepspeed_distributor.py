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
import os
import sys
import tempfile
from typing import (
    Union,
    Callable,
    List,
    Dict,
    Optional,
    Any,
)

from pyspark.ml.torch.distributor import TorchDistributor


class DeepspeedTorchDistributor(TorchDistributor):

    _DEEPSPEED_SSL_CONF = "deepspeed.spark.distributor.ignoreSsl"

    def __init__(
        self,
        num_gpus: int = 1,
        nnodes: int = 1,
        local_mode: bool = True,
        use_gpu: bool = True,
        deepspeed_config: Optional[Union[str, Dict[str, Any]]] = None,
    ):
        """
        This class is used to run deepspeed training workloads with spark clusters.
        The user has the option to specify the number of gpus per node
        and the number of nodes (the same as if running from terminal),
        as well as specify a deepspeed configuration file.

        Parameters
        ----------
        num_gpus: int
            The number of GPUs to use per node (analagous to num_gpus in deepspeed command).

        nnodes: int
            The number of nodes that should be used for the run.

        local_mode: bool
            Whether or not to run the training in a distributed fashion or just locally.

        use_gpu: bool
            Boolean flag to determine whether to utilize gpus.

        deepspeed_config: Union[Dict[str,Any], str] or None:
            The configuration file to be used for launching the deepspeed application.
            If it's a dictionary containing the parameters, then we will create the file.
            If None, deepspeed will fall back to default parameters.
        """
        num_processes = num_gpus * nnodes
        self.deepspeed_config = deepspeed_config
        super().__init__(
            num_processes,
            local_mode,
            use_gpu,
            _ssl_conf=DeepspeedTorchDistributor._DEEPSPEED_SSL_CONF,
        )
        self.cleanup_deepspeed_conf = False

    @staticmethod
    def _get_deepspeed_config_path(deepspeed_config: Union[str, Dict[str, Any]]) -> str:
        if isinstance(deepspeed_config, dict):
            with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json") as file:
                json.dump(deepspeed_config, file)
                return file.name
        deepspeed_config_path = deepspeed_config
        # Empty value means the deepspeed will fall back to default settings.
        if deepspeed_config is None:
            return ""
        return deepspeed_config_path

    @staticmethod
    def _create_torchrun_command(
        input_params: Dict[str, Any], train_path: str, *args: Any
    ) -> List[str]:
        local_mode = input_params["local_mode"]
        num_processes = input_params["num_processes"]
        deepspeed_config = input_params["deepspeed_config"]
        deepspeed_config_path = DeepspeedTorchDistributor._get_deepspeed_config_path(
            deepspeed_config
        )
        torchrun_args, processes_per_node = TorchDistributor._get_torchrun_args(
            local_mode, num_processes
        )
        args_string = list(map(str, args))
        command_to_run = [
            sys.executable,
            "-m",
            "torch.distributed.run",
            *torchrun_args,
            f"--nproc_per_node={processes_per_node}",
            train_path,
            *args_string,
            "-deepspeed",
        ]

        # Don't have the deepspeed_config argument if no path is provided or no parameters set
        if deepspeed_config_path == "":
            return command_to_run
        return command_to_run + ["--deepspeed_config", deepspeed_config_path]

    @staticmethod
    def _run_training_on_pytorch_file(
        input_params: Dict[str, Any], train_path: str, *args: Any, **kwargs: Any
    ) -> None:
        if kwargs:
            raise ValueError(
                "DeepspeedTorchDistributor with pytorch file doesn't support keyword arguments"
            )

        log_streaming_client = input_params.get("log_streaming_client", None)
        training_command = DeepspeedTorchDistributor._create_torchrun_command(
            input_params, train_path, *args
        )
        DeepspeedTorchDistributor._execute_command(
            training_command, log_streaming_client=log_streaming_client
        )

    def run(self, train_object: Union[Callable, str], *args: Any, **kwargs: Any) -> Optional[Any]:
        # If the "train_object" is a string, then we assume it's a filepath.
        # Otherwise, we assume it's a function.
        if isinstance(train_object, str):
            if os.path.exists(train_object) is False:
                raise FileNotFoundError(f"The path to training file {train_object} does not exist.")
            framework_wrapper_fn = DeepspeedTorchDistributor._run_training_on_pytorch_file
        else:
            raise RuntimeError("Python training functions aren't supported as inputs at this time")

        if self.local_mode:
            return self._run_local_training(framework_wrapper_fn, train_object, *args, **kwargs)
        return self._run_distributed_training(
            framework_wrapper_fn,
            train_object,
            spark_dataframe=None,
            *args,
            **kwargs,  # type:ignore[misc]
        )
