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

"""Type aliases and variables for the eval type handlers.

The Arrow element types are forward refs so pyarrow stays a type-checking-only
import at runtime, as elsewhere in the worker.
"""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.sql.types import StructType

# Grouped-category input element, matching what the serializer yields: a group
# serializer yields a lazy iterator; a co-group serializer materializes each side.
GroupedBatch = Iterator["pa.RecordBatch"]
CoGroupedBatch = tuple[list["pa.RecordBatch"], list["pa.RecordBatch"]]

# Handler input and output stream element types.
InputBatch = TypeVar("InputBatch")
OutputBatch = TypeVar("OutputBatch")


@dataclass(frozen=True)
class UDF:
    """One UDF as built by ``EvalTypeHandler.create_udf``; each handler takes the fields it
    needs."""

    # The chained function wrapped for profiling and, for most eval types, in
    # fail_on_stopiteration.
    func: Callable[..., Any]
    # The chained function before any wrapping, for handlers that need the pickled object
    # itself (an incremental Aggregator) or its signature.
    raw_func: Any
    args_offsets: list[int]
    kwargs_offsets: dict[str, int]
    # The DataType of the last function in the chain; a StructType for the eval types that
    # return rows.
    return_type: Any


class UDTFInstance(Protocol):
    """An instance of the user's UDTF class, or the PartitionedUDTF wrapping one: an object whose
    methods are called, not a function. ``terminate`` and ``cleanup`` are optional."""

    def eval(self, *args: Any, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class UDTF:
    """The UDTF as built by ``UDTFHandlerBase.create_udtf``."""

    # Wrapped in the handler's partition_wrapper when its TABLE argument has PARTITION BY
    # expressions.
    udtf: UDTFInstance
    args_offsets: list[int]
    kwargs_offsets: dict[str, int]
    return_type: "StructType"
