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
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

from pyspark.eval_handlers._base import EvalTypeHandler
from pyspark.eval_handlers.utils import wrap_kwargs_support
from pyspark.serializers import BatchedSerializer, CPickleSerializer, Serializer
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    from pyspark.eval_handlers._typing import UDF


class BatchedUDFHandler(EvalTypeHandler[Any, Any]):
    """SQL_BATCHED_UDF. Prepare each UDF as an ``(arg_offsets, eval_func)`` pair and apply every
    one to each input row: a single UDF yields its bare result, multiple UDFs yield a tuple of
    results, which is the shape the JVM side expects. The number of UDFs is fixed, so the
    single-result case is handled once here rather than by unwrapping a one-element tuple per
    row."""

    eval_type = PythonEvalType.SQL_BATCHED_UDF

    @staticmethod
    def _prepare_udf(udf: "UDF") -> tuple[list[int], Callable[..., Any]]:
        """Apply kwargs binding, and convert each result to the internal representation only
        when the return type requires it."""
        func, args_kwargs_offsets = wrap_kwargs_support(
            udf.func, udf.args_offsets, udf.kwargs_offsets
        )
        if udf.return_type.needConversion():
            toInternal = udf.return_type.toInternal
            return args_kwargs_offsets, lambda *a: toInternal(func(*a))
        return args_kwargs_offsets, func

    @property
    def serializer(self) -> Serializer:
        batch_size = int(os.environ.get("PYTHON_UDF_BATCH_SIZE", "100"))
        return BatchedSerializer(CPickleSerializer(), batch_size)

    def run(self, split_index: int, data: Iterator[Any]) -> Iterator[Any]:
        self._udf_infos = [self._prepare_udf(udf) for udf in self._udfs]

        if len(self._udf_infos) == 1:
            arg_offsets, f = self._udf_infos[0]
            return (f(*[row[offset] for offset in arg_offsets]) for row in data)
        return (
            tuple(f(*[row[offset] for offset in arg_offsets]) for arg_offsets, f in self._udf_infos)
            for row in data
        )
