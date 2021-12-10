#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Callable, Optional, TypeVar, Union

from pyspark.accumulators import (  # noqa: F401
    Accumulator as Accumulator,
    AccumulatorParam as AccumulatorParam,
)
from pyspark.broadcast import Broadcast as Broadcast  # noqa: F401
from pyspark.conf import SparkConf as SparkConf  # noqa: F401
from pyspark.context import SparkContext as SparkContext  # noqa: F401
from pyspark.files import SparkFiles as SparkFiles  # noqa: F401
from pyspark.status import (
    StatusTracker as StatusTracker,
    SparkJobInfo as SparkJobInfo,
    SparkStageInfo as SparkStageInfo,
)  # noqa: F401
from pyspark.profiler import (  # noqa: F401
    BasicProfiler as BasicProfiler,
    Profiler as Profiler,
)
from pyspark.rdd import RDD as RDD, RDDBarrier as RDDBarrier  # noqa: F401
from pyspark.serializers import (  # noqa: F401
    MarshalSerializer as MarshalSerializer,
    CPickleSerializer as CPickleSerializer,
)
from pyspark.status import (  # noqa: F401
    SparkJobInfo as SparkJobInfo,
    SparkStageInfo as SparkStageInfo,
    StatusTracker as StatusTracker,
)
from pyspark.storagelevel import StorageLevel as StorageLevel  # noqa: F401
from pyspark.taskcontext import (  # noqa: F401
    BarrierTaskContext as BarrierTaskContext,
    BarrierTaskInfo as BarrierTaskInfo,
    TaskContext as TaskContext,
)
from pyspark.util import (
    InheritableThread as InheritableThread,  # noqa: F401
    inheritable_thread_target as inheritable_thread_target,  # noqa: F401
)
from pyspark.version import __version__ as __version__

# Compatibility imports
from pyspark.sql import (  # noqa: F401
    SQLContext as SQLContext,
    HiveContext as HiveContext,
    Row as Row,
)

T = TypeVar("T")
F = TypeVar("F", bound=Callable)

def since(version: Union[str, float]) -> Callable[[T], T]: ...
def copy_func(
    f: F,
    name: Optional[str] = ...,
    sinceversion: Optional[Union[str, float]] = ...,
    doc: Optional[str] = ...,
) -> F: ...
def keyword_only(func: F) -> F: ...
