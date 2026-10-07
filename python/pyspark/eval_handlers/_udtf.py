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

"""Handlers for the user-defined table function (UDTF) eval types.

Each handler receives a single ``(udtf, args_offsets, kwargs_offsets, return_type)`` tuple
from ``read_single_udtf``, where ``udtf`` is the instantiated UDTF.

pyarrow is imported lazily (inside the handlers and the type-checking block) so the module
stays importable and its handlers register without pyarrow installed.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Callable

from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import BatchEvalTypeHandler
from pyspark.eval_handlers.utils import wrap_kwargs_support
from pyspark.sql.conversion import ArrowBatchTransformer
from pyspark.sql.functions import SkipRestOfInputTableException
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.worker_util import EvalConf, RunnerConf


class ArrowUDTFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_ARROW_UDTF: the UDTF's ``eval`` receives ``pa.Array`` arguments, with TABLE
    arguments flattened into ``pa.RecordBatch``, and returns an iterable of
    ``pa.Table``/``pa.RecordBatch``, coerced to the declared schema."""

    eval_type = PythonEvalType.SQL_ARROW_UDTF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        import pyarrow as pa

        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One ARROW_UDTF expected here."
        udtf, args_offsets, kwargs_offsets, return_type = udfs[0]

        arrow_return_type = to_arrow_type(
            return_type, timezone="UTC", prefers_large_types=runner_conf.use_large_var_types
        )
        self._return_type_size = len(return_type)
        self._target_schema = pa.schema(list(arrow_return_type))

        self._eval_method, self._args_kwargs_offsets = wrap_kwargs_support(
            getattr(udtf, "eval"), args_offsets, kwargs_offsets
        )
        self._terminate = getattr(udtf, "terminate", None)
        self._cleanup = getattr(udtf, "cleanup", None)

        self._table_arg_offsets = (
            set(eval_conf.table_arg_offsets) if eval_conf.table_arg_offsets else set()
        )

    def _verify_result(self, result: pa.RecordBatch, method_name: str) -> pa.RecordBatch:
        # Validate the output schema when the result has columns
        if result.num_columns != self._return_type_size:
            raise PySparkRuntimeError(
                errorClass="UDTF_RETURN_SCHEMA_MISMATCH",
                messageParameters={
                    "expected": str(self._return_type_size),
                    "actual": str(result.num_columns),
                    "func": method_name,
                },
            )
        return result

    def _convert_to_arrow(self, res: Any, method_name: str) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        # Check whether the result of a PyArrow UDTF is iterable before processing
        if res is None:
            res = iter([])
        elif not isinstance(res, Iterable):
            raise PySparkRuntimeError(
                errorClass="UDTF_RETURN_NOT_ITERABLE",
                messageParameters={
                    "type": type(res).__name__,
                    "func": method_name,
                },
            )

        # Handle PyArrow Tables/RecordBatches directly
        is_empty = True
        for item in res:
            is_empty = False
            if isinstance(item, pa.Table):
                yield from item.to_batches()
            elif isinstance(item, pa.RecordBatch):
                yield item
            else:
                # Arrow UDTF should only return Arrow types (RecordBatch/Table)
                raise PySparkRuntimeError(
                    errorClass="UDTF_ARROW_TYPE_CONVERSION_ERROR",
                    messageParameters={},
                )

        if is_empty:
            yield pa.RecordBatch.from_pylist([], schema=self._target_schema)

    def _evaluate(self, method: Callable, *args: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        # Wrap the exception thrown from the UDTF in a PySparkRuntimeError.
        try:
            res = method(*args)
        except SkipRestOfInputTableException:
            raise
        except Exception as e:
            raise PySparkRuntimeError(
                errorClass="UDTF_EXEC_ERROR",
                messageParameters={"method_name": method.__name__, "error": str(e)},
            )

        for batch in self._convert_to_arrow(res, method.__name__):
            coerced = ArrowBatchTransformer.enforce_schema(
                self._verify_result(batch, method.__name__), self._target_schema, safecheck=True
            )
            yield ArrowBatchTransformer.wrap_struct(coerced)

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Apply Arrow UDTF"""
        table_arg_offsets = self._table_arg_offsets
        args_kwargs_offsets = self._args_kwargs_offsets
        terminate = self._terminate
        cleanup = self._cleanup
        try:
            for batch in data:
                # Pre-processing: for each column, flatten struct columns at
                # table_arg_offsets into RecordBatch, keep other columns as Array.
                columns = [
                    (
                        ArrowBatchTransformer.flatten_struct(batch, column_index=i)
                        if i in table_arg_offsets
                        else batch.column(i)
                    )
                    for i in range(batch.num_columns)
                ]
                # For PyArrow UDTFs, pass RecordBatches directly (no row conversion needed)
                yield from self._evaluate(
                    self._eval_method, *[columns[o] for o in args_kwargs_offsets]
                )
            if terminate is not None:
                yield from self._evaluate(terminate)
        except SkipRestOfInputTableException:
            if terminate is not None:
                yield from self._evaluate(terminate)
        finally:
            if cleanup is not None:
                cleanup()
