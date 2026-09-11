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

# ---------------------------------------------------------------------------
# Eval type handlers
#
# Every Arrow/Pandas UDF eval type runs the same three-stage pipeline inside the
# worker: prepare the input (argument extraction, conversions), invoke the user
# code, then validate and normalize the result (schema enforcement, row-count
# checks). Historically each eval type spelled this out inline in the worker's
# ``read_udfs``, growing a single central if/elif dispatcher. ``EvalTypeHandler``
# makes the lifecycle explicit and lets a new eval type attach itself as a
# self-contained subclass -- no edit to a central branch is required.
#
# A handler declares its ``eval_type`` and is registered automatically via
# ``__init_subclass__``; the worker's ``read_udfs`` looks it up in
# ``_EVAL_TYPE_HANDLERS`` and, when present, delegates to ``handler.run`` instead
# of walking the if/elif chain.
# ---------------------------------------------------------------------------
from abc import ABCMeta, abstractmethod
from collections.abc import Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from pyspark.sql.conversion import ArrowBatchTransformer
from pyspark.sql.pandas.serializers import (
    ArrowStreamCoGroupSerializer,
    ArrowStreamGroupSerializer,
    ArrowStreamSerializer,
)
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import StructField, StructType
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.sql.pandas._typing import CoGroupedBatch, GroupedBatch

# Registry of concrete handlers keyed by PythonEvalType. Populated at class
# definition time by ``EvalTypeHandler.__init_subclass__``.
_EVAL_TYPE_HANDLERS: "Dict[int, Type[EvalTypeHandler]]" = {}

# Input/output stream element types. ``InputBatch`` is the element type of the
# stream the JVM sends (a plain ``pa.RecordBatch`` for batch handlers, a
# ``GroupedBatch`` for grouped handlers, ...); ``OutputBatch`` is the element type
# the worker writes back (a ``pa.RecordBatch`` for all Arrow eval types).
InputBatch = TypeVar("InputBatch")
OutputBatch = TypeVar("OutputBatch")


class EvalTypeHandler(Generic[InputBatch, OutputBatch], metaclass=ABCMeta):
    """Base class for the Arrow/Pandas UDF execution pipeline.

    A handler splits one eval type's work into three explicit stages that
    ``run`` chains lazily:

    - ``pre_process``: turn the input stream into a stream of work items
      (argument extraction, offset handling, Arrow/pandas conversions).
    - ``process``: invoke the user function only. This maps to the ``func`` /
      ``grouped_func`` / ``cogrouped_func`` closures the eval types used before.
    - ``post_process``: validate and normalize the raw results (schema
      enforcement, row-count checks) into the output stream.

    Concrete handlers subclass one of the typed category bases
    (``BatchEvalTypeHandler``, ``GroupedEvalTypeHandler``,
    ``CoGroupedEvalTypeHandler``) rather than this class directly, so the input
    stream type and the default serializer are fixed by the category.
    """

    # Set by a concrete subclass to the PythonEvalType it handles. Category
    # bases leave it ``None`` so they are not registered and stay abstract.
    eval_type: ClassVar[Optional[int]] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Register only concrete handlers that declare an eval type. This is the
        # single, automatic registration step: a new eval type is picked up as
        # soon as its subclass is defined, with no central table to edit.
        eval_type = cls.__dict__.get("eval_type")
        if eval_type is not None:
            if eval_type in _EVAL_TYPE_HANDLERS:
                raise AssertionError(
                    "Duplicate eval type handler for {}: {} and {}".format(
                        eval_type, _EVAL_TYPE_HANDLERS[eval_type].__name__, cls.__name__
                    )
                )
            _EVAL_TYPE_HANDLERS[eval_type] = cls

    def __init__(self, udfs: list, runner_conf: Any, eval_conf: Any) -> None:
        self._udfs = udfs
        self._runner_conf = runner_conf
        self._eval_conf = eval_conf

    @abstractmethod
    def select_serializer(self) -> Any:
        """Return the serializer used for both the input and output streams."""

    @abstractmethod
    def pre_process(self, data: "Iterator[InputBatch]") -> Iterator[Any]:
        """Prepare a stream of work items from the input stream."""

    @abstractmethod
    def process(self, work_items: Iterator[Any]) -> Iterator[Any]:
        """Invoke the user function over the work items, yielding raw results."""

    @abstractmethod
    def post_process(self, results: Iterator[Any]) -> "Iterator[OutputBatch]":
        """Validate and normalize the raw results into the output stream."""

    def run(self, split_index: int, data: "Iterator[InputBatch]") -> "Iterator[OutputBatch]":
        """Chain the three stages. Matches the ``func(split_index, data)`` shape
        ``read_udfs`` returns; the stages are generators, so the pipeline stays
        lazy and streams one work item at a time."""
        return self.post_process(self.process(self.pre_process(data)))


class BatchEvalTypeHandler(EvalTypeHandler["pa.RecordBatch", OutputBatch], metaclass=ABCMeta):
    """Category base for eval types whose input stream is
    ``Iterator[pa.RecordBatch]`` -- one flat RecordBatch at a time."""

    def select_serializer(self) -> Any:
        return ArrowStreamSerializer(write_start_stream=True)

    @abstractmethod
    def pre_process(self, data: "Iterator[pa.RecordBatch]") -> Iterator[Any]:
        """Prepare a stream of work items from the RecordBatch stream."""


class GroupedEvalTypeHandler(EvalTypeHandler["GroupedBatch", OutputBatch], metaclass=ABCMeta):
    """Category base for eval types whose input stream is
    ``Iterator[GroupedBatch]`` -- one Arrow stream (group) at a time."""

    def select_serializer(self) -> Any:
        return ArrowStreamGroupSerializer(write_start_stream=True)

    @abstractmethod
    def pre_process(self, data: "Iterator[GroupedBatch]") -> Iterator[Any]:
        """Prepare a stream of work items from the per-group Arrow streams."""


class CoGroupedEvalTypeHandler(EvalTypeHandler["CoGroupedBatch", OutputBatch], metaclass=ABCMeta):
    """Category base for eval types whose input stream is
    ``Iterator[CoGroupedBatch]`` -- a pair of Arrow streams per co-group."""

    def select_serializer(self) -> Any:
        return ArrowStreamCoGroupSerializer(write_start_stream=True)

    @abstractmethod
    def pre_process(self, data: "Iterator[CoGroupedBatch]") -> Iterator[Any]:
        """Prepare a stream of work items from the per-co-group Arrow streams."""


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
        # Imported lazily to avoid a circular import: the worker module imports
        # this module to build the handler registry.
        from pyspark.worker import verify_scalar_result

        for output_batch, num_rows in results:
            output_batch = ArrowBatchTransformer.enforce_schema(
                output_batch, self._combined_arrow_schema
            )
            verify_scalar_result(output_batch, num_rows)
            yield output_batch
