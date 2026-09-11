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

"""Handlers for the Arrow/Pandas UDF eval types.

Each eval type is an ``EvalTypeHandler`` subclass that declares its ``eval_type``
and self-registers in ``EVAL_TYPE_HANDLERS`` via ``__init_subclass__``; the
worker's ``read_udfs`` dispatches on the registry. Concrete handlers live in
private per-family submodules (``_arrow``), imported at the bottom so importing
this package registers them.
"""

from abc import ABCMeta, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional

from pyspark.serializers import Serializer
from pyspark.sql.eval_handlers._typing import (
    CoGroupedBatch,
    GroupedBatch,
    InputBatch,
    OutputBatch,
)
from pyspark.sql.pandas.serializers import (
    ArrowStreamCoGroupSerializer,
    ArrowStreamGroupSerializer,
    ArrowStreamSerializer,
)

if TYPE_CHECKING:
    import pyarrow as pa  # noqa: F401  # only in the batch category's forward-ref subscript

    from pyspark.worker_util import EvalConf, RunnerConf

# eval type -> handler class, populated by EvalTypeHandler.__init_subclass__.
EVAL_TYPE_HANDLERS: "dict[int, type[EvalTypeHandler]]" = {}


class EvalTypeHandler(Generic[InputBatch, OutputBatch], metaclass=ABCMeta):
    """Base class for the Arrow/Pandas UDF execution model.

    A handler declares the ``serializer`` for the input/output streams and
    implements ``run``, which consumes the input stream and yields the output
    stream. Concrete handlers subclass a typed category base
    (``BatchEvalTypeHandler``, ``GroupedEvalTypeHandler``,
    ``CoGroupedEvalTypeHandler``), which fixes the input type and serializer.
    """

    # PythonEvalType this handler serves; None on the abstract bases.
    eval_type: ClassVar[Optional[int]] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Register concrete handlers, i.e. those that declare an eval type.
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
        """Serializer for both the input and output streams."""

    @abstractmethod
    def run(self, split_index: int, data: Iterator[InputBatch]) -> Iterator[OutputBatch]:
        """Consume the input stream and yield the output stream, matching the
        ``func(split_index, data)`` shape ``read_udfs`` returns."""


class BatchEvalTypeHandler(EvalTypeHandler["pa.RecordBatch", OutputBatch], metaclass=ABCMeta):
    """Handler category whose input stream is ``Iterator[pa.RecordBatch]``."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamSerializer(write_start_stream=True)


class GroupedEvalTypeHandler(EvalTypeHandler[GroupedBatch, OutputBatch], metaclass=ABCMeta):
    """Handler category whose input stream is ``Iterator[GroupedBatch]``."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamGroupSerializer(write_start_stream=True)


class CoGroupedEvalTypeHandler(EvalTypeHandler[CoGroupedBatch, OutputBatch], metaclass=ABCMeta):
    """Handler category whose input stream is ``Iterator[CoGroupedBatch]``."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamCoGroupSerializer(write_start_stream=True)


# Re-export the concrete handlers from their private submodules so callers reach
# them from this package (or via EVAL_TYPE_HANDLERS), never from the ``_``
# submodules. The import also registers them. Kept last so the submodules can
# import the base classes above.
from pyspark.sql.eval_handlers._arrow import ArrowScalarUDFHandler

__all__ = [
    "EVAL_TYPE_HANDLERS",
    "EvalTypeHandler",
    "BatchEvalTypeHandler",
    "GroupedEvalTypeHandler",
    "CoGroupedEvalTypeHandler",
    "ArrowScalarUDFHandler",
]
