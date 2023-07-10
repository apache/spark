
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
    Tuple,
)

from pyspark.ml.torch.distributor import TorchDistributor

class DeepspeedTorchDistributor(TorchDistributor):
    
    def __init__(self, num_gpus: int = 1, nnodes: int = 1, local_mode: bool = True, use_gpu: bool = True, deepspeed_config = None):
        """
            This class is used to run deepspeed training workloads with spark clusters. The user has the option to 
            specify the number of gpus per node and the number of nodes (the same as if running from terminal), 
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
                If it is a dictionary mapping parameters to values, then we will create the file.
                If None, deepspeed will fall back to default parameters.
        """
        num_processes = num_gpus * nnodes
        super().__init__(num_processes, local_mode, use_gpu)
        self.deepspeed_config = deepspeed_config 
        self.ssl_conf = "deepspeed.spark.distributor.ignoreSsl"
        self._validate_input_params()
        self.input_params = self._create_input_params()
        self.cleanup_deepspeed_conf = False

    @staticmethod
    def _get_deepspeed_config_path(deepspeed_config):
        if isinstance(deepspeed_config, dict):
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as fil:
                json.dump(deepspeed_config, fil)
                return fil.name
        deepspeed_config_path = deepspeed_config
        # Empty value means the deepspeed will fall back to default settings.
        if deepspeed_config == None:
            return "" 
        return deepspeed_config_path


    @staticmethod 
    def _get_torchrun_args(local_mode: bool, num_processes: int) -> Tuple[List[Any], int]:
        """
        Given the mode and the number of processes, create the arguments to be given to deepspeed
        
        Parameters
        ---------
        local_mode: bool
            Whether or not we are running training locally or in a distributed fashion

        num_processes: int
            The number of processes that we are going to use (number of gpus per node * number of nodes)

        Returns
        ------
        Tuple[List[Any], int]
            A tuple containing a list of arguments to pass as pytorch args to deepspeed, as well as the number of processes per node
        """
        if local_mode:
            torchrun_args = ["--standalone","--nnodes=1"]
            processes_per_node = num_processes
            return torchrun_args, processes_per_node

        master_addr = os.environ["MASTER_ADDR"]
        master_port = os.environ["MASTER_PORT"]
        node_rank = os.environ["RANK"]
        torchrun_args = [
            f"--nnodes={num_processes}",
            f"--node_rank={node_rank}",
            f"--rdzv_endpoint={master_addr}:{master_port}",
            "--rdzv_id=0",
        ]
        processes_per_node = 1
        return torchrun_args, processes_per_node

    @staticmethod
    def _create_torchrun_command(
            input_params: Dict[str, Any], train_path: str, *args: Any) -> List[str]:
        local_mode = input_params["local_mode"]
        num_processes = input_params["num_processes"]
        deepspeed_config = input_params["deepspeed_config"]
        deepspeed_config_path = DeepspeedTorchDistributor._get_deepspeed_config_path(deepspeed_config)
        torchrun_args, processes_per_node = DeepspeedTorchDistributor._get_torchrun_args(local_mode, num_processes)
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
                          "--deepspeed_config",
                          deepspeed_config_path
                        ]
        return command_to_run

    @staticmethod
    def _run_training_on_pytorch_file(input_params: Dict[str, Any], train_path: str, *args: Any, **kwargs : Any) -> None :
        if kwargs:
            raise ValueError("DeepspeedTorchDistributor with pytorch file doesn't support key-word type arguments")

        log_streaming_client = input_params.get("log_streaming_client", None)
        training_command = DeepspeedTorchDistributor._create_torchrun_command(input_params, train_path, *args)
        DeepspeedTorchDistributor._execute_command(training_command, log_streaming_client=log_streaming_client)

    def run(self, train_object: Union[Callable, str], *args : Any, **kwargs: Any) -> Optional[Any]:
        # If the "train_object" is a string, then we assume it's a filepath. Otherwise, we assume it's a function.
        if isinstance(train_object, str):
            framework_wrapper_fn = DeepspeedTorchDistributor._run_training_on_pytorch_file
        else:
            raise RuntimeError("Work in progress; not supported atm")
            framework_wrapper_fn = TorchDistributor._run_training_on_pytorch_file
        if self.local_mode:
            output = self._run_local_training(framework_wrapper_fn, train_object, *args, **kwargs)
            return output
        output = self._run_distributed_training(framework_wrapper_fn, train_object, spark_dataframe=None, *args, **kwargs)
        return output
