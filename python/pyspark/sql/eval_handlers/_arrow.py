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

"""Handlers for the Arrow-native UDF eval types (the UDF exchanges ``pa.Array`` /
``pa.RecordBatch`` values directly, without a pandas conversion)."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Tuple

from pyspark.sql.conversion import ArrowBatchTransformer
from pyspark.sql.eval_handlers import BatchEvalTypeHandler
from pyspark.sql.eval_handlers.verification import verify_scalar_result
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import StructField, StructType
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pyarrow as pa


class ArrowScalarUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_SCALAR_ARROW_UDF: one user invocation per input RecordBatch.

    The columns each UDF consumes are read straight off the RecordBatch, so
    there is no up-front argument extraction; ``pre_process`` streams the input
    batches through unchanged and carries each batch's row count forward for the
    ``post_process`` row-count check.
    """

    eval_type = PythonEvalType.SQL_SCALAR_ARROW_UDF

    def __init__(self, udfs: list, runner_conf: Any, eval_conf: Any) -> None:
        super().__init__(udfs, runner_conf, eval_conf)
        self._col_names = ["_%d" % i for i in range(len(udfs))]
        self._combined_arrow_schema = to_arrow_schema(
            StructType([StructField(n, rt) for n, (_, _, _, rt) in zip(self._col_names, udfs)]),
            timezone="UTC",
            prefers_large_types=runner_conf.use_large_var_types,
        )

    def pre_process(self, data: "Iterator[pa.RecordBatch]") -> "Iterator[pa.RecordBatch]":
        return data

    def process(
        self, work_items: "Iterator[pa.RecordBatch]"
    ) -> "Iterator[Tuple[pa.RecordBatch, int]]":
        import pyarrow as pa

        for batch in work_items:
            output_batch = pa.RecordBatch.from_arrays(
                [
                    udf_func(
                        *[batch.column(o) for o in args_offsets],
                        **{k: batch.column(v) for k, v in kwargs_offsets.items()},
                    )
                    for udf_func, args_offsets, kwargs_offsets, _ in self._udfs
                ],
                self._col_names,
            )
            yield output_batch, batch.num_rows

    def post_process(
        self, results: "Iterator[Tuple[pa.RecordBatch, int]]"
    ) -> "Iterator[pa.RecordBatch]":
        for output_batch, num_rows in results:
            output_batch = ArrowBatchTransformer.enforce_schema(
                output_batch, self._combined_arrow_schema
            )
            verify_scalar_result(output_batch, num_rows)
            yield output_batch
