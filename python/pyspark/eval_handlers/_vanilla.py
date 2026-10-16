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

"""Handler for the plain Python (pickle) UDF eval type, SQL_BATCHED_UDF.

Input rows arrive as pickled batches and are evaluated one row at a time.
"""

import os
from collections.abc import Iterator
from typing import Any

from pyspark.eval_handlers._base import EvalTypeHandler
from pyspark.serializers import BatchedSerializer, CPickleSerializer, Serializer
from pyspark.util import PythonEvalType


class BatchedUDFHandler(EvalTypeHandler[Any, Any]):
    """SQL_BATCHED_UDF. ``read_single_udf`` prepares each UDF as an ``(arg_offsets, eval_func)``
    pair. Apply every one to each input row: a single UDF yields its bare result, multiple UDFs
    yield a tuple of results, which is the shape the JVM side expects. The number of UDFs is
    fixed, so the single-result case is handled once here rather than by unwrapping a one-element
    tuple per row."""

    eval_type = PythonEvalType.SQL_BATCHED_UDF

    @property
    def serializer(self) -> Serializer:
        batch_size = int(os.environ.get("PYTHON_UDF_BATCH_SIZE", "100"))
        return BatchedSerializer(CPickleSerializer(), batch_size)

    def run(self, split_index: int, data: Iterator[Any]) -> Iterator[Any]:
        udfs = self._udfs
        if len(udfs) == 1:
            arg_offsets, f = udfs[0]
            return (f(*[row[offset] for offset in arg_offsets]) for row in data)
        return (
            tuple(f(*[row[offset] for offset in arg_offsets]) for arg_offsets, f in udfs)
            for row in data
        )
