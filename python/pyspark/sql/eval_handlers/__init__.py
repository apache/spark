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
# Every Arrow/Pandas UDF eval type runs the same shape of work inside the worker:
# prepare the input (argument extraction, conversions), invoke the user code, then
# validate and normalize the result (schema enforcement, row-count checks).
# Historically each eval type spelled this out inline in the worker's
# ``read_udfs``, growing a single central if/elif dispatcher. ``EvalTypeHandler``
# turns each eval type into a self-contained subclass that owns its ``run`` and
# ``serializer`` -- a new eval type attaches without editing a central branch.
#
# A handler declares its ``eval_type`` and is registered automatically via
# ``__init_subclass__``; the worker's ``read_udfs`` looks it up in
# ``EVAL_TYPE_HANDLERS`` and, when present, delegates to ``handler.run`` instead
# of walking the if/elif chain.
#
# This package holds the base classes here in ``__init__`` and the concrete
# handlers in private per-family submodules (``_arrow``; a ``_pandas`` module
# will follow as pandas eval types are migrated), imported at the bottom so
# their handlers self-register. Import handlers from the package, not the
# submodules.
# ---------------------------------------------------------------------------
from abc import ABCMeta, abstractmethod
from collections.abc import Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
    TypeVar,
)

from pyspark.serializers import Serializer
from pyspark.sql.pandas.serializers import (
    ArrowStreamCoGroupSerializer,
    ArrowStreamGroupSerializer,
    ArrowStreamSerializer,
)

if TYPE_CHECKING:
    # Referenced only as string forward refs in the category bases' generic
    # subscripts (e.g. ``EvalTypeHandler["pa.RecordBatch", ...]``); kept for the
    # type checker to resolve them.
    import pyarrow as pa  # noqa: F401

    from pyspark.sql.pandas._typing import CoGroupedBatch, GroupedBatch  # noqa: F401
    from pyspark.worker import EvalConf, RunnerConf

# Registry of concrete handlers keyed by PythonEvalType. Populated at class
# definition time by ``EvalTypeHandler.__init_subclass__``.
EVAL_TYPE_HANDLERS: "dict[int, type[EvalTypeHandler]]" = {}

# Input/output stream element types. ``InputBatch`` is the element type of the
# stream the JVM sends (a plain ``pa.RecordBatch`` for batch handlers, a
# ``GroupedBatch`` for grouped handlers, ...); ``OutputBatch`` is the element type
# the worker writes back (a ``pa.RecordBatch`` for all Arrow eval types).
InputBatch = TypeVar("InputBatch")
OutputBatch = TypeVar("OutputBatch")


class EvalTypeHandler(Generic[InputBatch, OutputBatch], metaclass=ABCMeta):
    """Base class for the Arrow/Pandas UDF execution model.

    A handler owns one eval type's end-to-end execution: it declares the
    ``serializer`` for the input/output streams and implements ``run``, which
    consumes the input stream and yields the output stream.

    Concrete handlers subclass one of the typed category bases
    (``BatchEvalTypeHandler``, ``GroupedEvalTypeHandler``,
    ``CoGroupedEvalTypeHandler``) rather than this class directly, so the input
    stream type and the default serializer are fixed by the category.

    ``run`` typically does three things -- prepare the input (argument
    extraction, conversions), invoke the user code, then validate and normalize
    the result. That structure is a convention, not a requirement: a handler may
    write ``run`` as one generator, or factor the steps into private helpers, and
    a family base (e.g. for pandas) may supply shared prepare/verify helpers that
    its handlers reuse. The base only requires ``run``.
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
            if eval_type in EVAL_TYPE_HANDLERS:
                raise AssertionError(
                    "Duplicate eval type handler for {}: {} and {}".format(
                        eval_type, EVAL_TYPE_HANDLERS[eval_type].__name__, cls.__name__
                    )
                )
            EVAL_TYPE_HANDLERS[eval_type] = cls

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        self._udfs = udfs
        self._runner_conf = runner_conf
        self._eval_conf = eval_conf

    @property
    @abstractmethod
    def serializer(self) -> Serializer:
        """The serializer used for both the input and output streams."""

    @abstractmethod
    def run(self, split_index: int, data: Iterator[InputBatch]) -> Iterator[OutputBatch]:
        """Run the eval type end to end: consume the input stream and yield the
        output stream. Matches the ``func(split_index, data)`` shape ``read_udfs``
        returns. Implementations are generators, so the pipeline stays lazy and
        streams one batch at a time."""


class BatchEvalTypeHandler(EvalTypeHandler["pa.RecordBatch", OutputBatch], metaclass=ABCMeta):
    """Category base for eval types whose input stream is
    ``Iterator[pa.RecordBatch]`` -- one flat RecordBatch at a time."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamSerializer(write_start_stream=True)


class GroupedEvalTypeHandler(EvalTypeHandler["GroupedBatch", OutputBatch], metaclass=ABCMeta):
    """Category base for eval types whose input stream is
    ``Iterator[GroupedBatch]`` -- one Arrow stream (group) at a time."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamGroupSerializer(write_start_stream=True)


class CoGroupedEvalTypeHandler(EvalTypeHandler["CoGroupedBatch", OutputBatch], metaclass=ABCMeta):
    """Category base for eval types whose input stream is
    ``Iterator[CoGroupedBatch]`` -- a pair of Arrow streams per co-group."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamCoGroupSerializer(write_start_stream=True)


# Import the per-family handler submodules so their concrete handlers register
# in ``EVAL_TYPE_HANDLERS`` and are re-exported from the package (see __all__).
# Kept at the bottom to avoid a circular import: the submodules import the base
# classes defined above.
from pyspark.sql.eval_handlers._arrow import ArrowScalarUDFHandler

__all__ = [
    "EVAL_TYPE_HANDLERS",
    "EvalTypeHandler",
    "BatchEvalTypeHandler",
    "GroupedEvalTypeHandler",
    "CoGroupedEvalTypeHandler",
    "ArrowScalarUDFHandler",
]
