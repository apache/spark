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

from collections.abc import Iterator
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import pyarrow as pa

# Input stream element type for the grouped categories (the batch category's
# element is a plain ``pa.RecordBatch``).
GroupedBatch = Iterator["pa.RecordBatch"]  # one group of batches
CoGroupedBatch = tuple[Iterator["pa.RecordBatch"], Iterator["pa.RecordBatch"]]  # a co-group pair

# Handler input and output stream element types.
InputBatch = TypeVar("InputBatch")
OutputBatch = TypeVar("OutputBatch")
