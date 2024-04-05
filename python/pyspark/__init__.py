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

"""
PySpark is the Python API for Spark.

Public classes:

  - :class:`SparkContext`:
      Main entry point for Spark functionality.
  - :class:`RDD`:
      A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
  - :class:`Broadcast`:
      A broadcast variable that gets reused across tasks.
  - :class:`Accumulator`:
      An "add-only" shared variable that tasks can only add values to.
  - :class:`SparkConf`:
      For configuring Spark.
  - :class:`SparkFiles`:
      Access files shipped with jobs.
  - :class:`StorageLevel`:
      Finer-grained cache persistence levels.
  - :class:`TaskContext`:
      Information about the current running task, available on the workers and experimental.
  - :class:`RDDBarrier`:
      Wraps an RDD under a barrier stage for barrier execution.
  - :class:`BarrierTaskContext`:
      A :class:`TaskContext` that provides extra info and tooling for barrier execution.
  - :class:`BarrierTaskInfo`:
      Information about a barrier task.
  - :class:`InheritableThread`:
      A inheritable thread to use in Spark when the pinned thread mode is on.
"""

import sys
from functools import wraps
from typing import cast, Any, Callable, TypeVar, Union

from pyspark.util import is_remote_only

if not is_remote_only():
    from pyspark.core.conf import SparkConf
    from pyspark.core.rdd import RDD, RDDBarrier
    from pyspark.core.files import SparkFiles
    from pyspark.core.status import StatusTracker, SparkJobInfo, SparkStageInfo
    from pyspark.core.broadcast import Broadcast
    from pyspark.core import conf, rdd, files, status, broadcast

    # for backward compatibility references.
    sys.modules["pyspark.conf"] = conf
    sys.modules["pyspark.rdd"] = rdd
    sys.modules["pyspark.files"] = files
    sys.modules["pyspark.status"] = status
    sys.modules["pyspark.broadcast"] = broadcast

from pyspark.util import InheritableThread, inheritable_thread_target
from pyspark.storagelevel import StorageLevel
from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.serializers import MarshalSerializer, CPickleSerializer
from pyspark.taskcontext import TaskContext, BarrierTaskContext, BarrierTaskInfo
from pyspark.profiler import Profiler, BasicProfiler
from pyspark.version import __version__
from pyspark._globals import _NoValue  # noqa: F401

_F = TypeVar("_F", bound=Callable)


def since(version: Union[str, float]) -> Callable[[_F], _F]:
    """
    A decorator that annotates a function to append the version of Spark the function was added.
    """
    import re

    indent_p = re.compile(r"\n( +)")

    def deco(f: _F) -> _F:
        assert f.__doc__ is not None

        indents = indent_p.findall(f.__doc__)
        indent = " " * (min(len(m) for m in indents) if indents else 0)
        f.__doc__ = f.__doc__.rstrip() + "\n\n%s.. versionadded:: %s" % (indent, version)
        return f

    return deco


def keyword_only(func: _F) -> _F:
    """
    A decorator that forces keyword arguments in the wrapped method
    and saves actual input keyword arguments in `_input_kwargs`.

    Notes
    -----
    Should only be used to wrap a method where first arg is `self`
    """

    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if len(args) > 0:
            raise TypeError("Method %s forces keyword arguments." % func.__name__)
        self._input_kwargs = kwargs
        return func(self, **kwargs)

    return cast(_F, wrapper)


# To avoid circular dependencies
if not is_remote_only():
    from pyspark.core.context import SparkContext
    from pyspark.core import context

    # for backward compatibility references.
    sys.modules["pyspark.context"] = context

# for back compatibility
from pyspark.sql import SQLContext, HiveContext, Row  # noqa: F401

__all__ = [
    "SparkConf",
    "SparkContext",
    "SparkFiles",
    "RDD",
    "StorageLevel",
    "Broadcast",
    "Accumulator",
    "AccumulatorParam",
    "MarshalSerializer",
    "CPickleSerializer",
    "StatusTracker",
    "SparkJobInfo",
    "SparkStageInfo",
    "Profiler",
    "BasicProfiler",
    "TaskContext",
    "RDDBarrier",
    "BarrierTaskContext",
    "BarrierTaskInfo",
    "InheritableThread",
    "inheritable_thread_target",
    "__version__",
]
