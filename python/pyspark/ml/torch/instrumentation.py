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

from dataclasses import dataclass
import functools
import json
import logging
from typing import Callable, Any, Dict, Union

from pyspark.sql import SparkSession

_logger = logging.getLogger(__name__)


@dataclass
class TorchDistributorInputParams:
    num_processes: int
    local_mode: bool
    use_gpu: bool
    num_tasks: int


@dataclass
class TorchDistributorSuccess:
    output: Any


@dataclass
class TorchDistributorFailure:
    error_type: str
    error_msg: str


def instrumented(func: Callable) -> Callable:
    """
    This decorator works with the TorchDistributor and wraps it to instrument the relevant
    information.

    Parameters
    ----------
    func : Callable
        The original function

    Returns
    -------
    Callable
        The wrapped function.
    """

    def wrapper(*args: Any, **kwargs: Dict[Any, Any]) -> Any:
        self = args[0]
        spark = self.spark
        input_params = TorchDistributorInputParams(
            self.num_processes, self.local_mode, self.use_gpu, self.num_tasks
        )

        _flush_event(spark=spark, event=input_params)
        functools.wraps(func)
        try:
            output = func(*args, **kwargs)
        except Exception as e:
            failure = TorchDistributorFailure(type(e).__name__, str(e))
            _flush_event(spark=spark, event=failure)
            raise e
        else:
            success = TorchDistributorSuccess(output)
            _flush_event(spark=spark, event=success)
            return output

    return wrapper


def _to_dict(obj: Any) -> Dict[Any, Any]:
    """
    Helper function to handle nested objects to dict
    """
    if not hasattr(obj, "__dict__"):
        return obj
    return {key: _to_dict(val) for key, val in obj.__dict__.items()}


def _flush_event(
    spark: SparkSession,
    event: Union[TorchDistributorInputParams, TorchDistributorSuccess, TorchDistributorFailure],
) -> None:
    """This fn creates a dictionary from the event fields, and logs this to the spark
    instrumentation pipeline.

    Parameters
    ----------
    spark : SparkSession
        The SparkSession used for logging this event.
    event : Union[TorchDistributorInputParams, TorchDistributorSuccess, TorchDistributorFailure]
        The type of event to be logged.
    """
    ev_dict = _to_dict(event)
    ev_dict = {k: v for k, v in ev_dict.items() if v is not None}

    blob = json.dumps(ev_dict)
    try:
        spark.sparkContext._gateway.entry_point.getLogger().logUsage(  # type: ignore
            "TorchDistributorRun",
            {"eventType": event.__class__.__name__},
            blob,
        )
    except Exception as e:
        _logger.warning(f"Unable to log instrumentation for TorchDistributor: {e}")
