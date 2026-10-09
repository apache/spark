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

"""Base classes and registry for the eval type handlers.

This leaf module imports only ``_typing`` and the serializers, so the package
``__init__`` and the concrete-handler submodules can both import it.
"""

from abc import ABCMeta, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional

from pyspark.eval_handlers._typing import (
    CoGroupedBatch,
    GroupedBatch,
    InputBatch,
    OutputBatch,
)
from pyspark.serializers import Serializer
from pyspark.sql.pandas.serializers import (
    ArrowStreamCoGroupSerializer,
    ArrowStreamGroupSerializer,
    ArrowStreamSerializer,
)

if TYPE_CHECKING:
    import pyarrow as pa  # noqa: F401  # only in the batch category's forward-ref subscript

    from pyspark.worker_util import EvalConf, RunnerConf

# eval type -> handler class, populated by _EvalTypeHandlerMeta at class definition.
_eval_type_handlers: "dict[int, type[EvalTypeHandler]]" = {}


def get_eval_type_handler(eval_type: int) -> "Optional[type[EvalTypeHandler]]":
    """Return the handler class registered for ``eval_type``, or ``None``."""
    return _eval_type_handlers.get(eval_type)


class _EvalTypeHandlerMeta(ABCMeta):
    """Registers a concrete handler under its ``eval_type`` at class definition.

    Runs after ``ABCMeta`` sets ``__abstractmethods__``, so a class that declares
    an ``eval_type`` while leaving ``run``/``serializer`` abstract is rejected at
    definition.
    """

    def __new__(mcs, name: str, bases: tuple, namespace: dict, **kwargs: Any) -> type:
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        eval_type = namespace.get("eval_type")
        if eval_type is not None:
            assert issubclass(cls, EvalTypeHandler)
            assert not cls.__abstractmethods__, (
                "Handler {} declares eval_type {} but is abstract: {}".format(
                    name, eval_type, sorted(cls.__abstractmethods__)
                )
            )
            assert eval_type not in _eval_type_handlers, (
                "Duplicate eval type handler for {}: {} and {}".format(
                    eval_type, _eval_type_handlers[eval_type].__name__, name
                )
            )
            _eval_type_handlers[eval_type] = cls
        return cls


class EvalTypeHandler(Generic[InputBatch, OutputBatch], metaclass=_EvalTypeHandlerMeta):
    """Base class for the Arrow/Pandas UDF execution model.

    A handler declares the ``serializer`` for the input/output streams and
    implements ``run``, which consumes the input stream and yields the output
    stream. Concrete handlers subclass a typed category base
    (``BatchEvalTypeHandler``, ``GroupedEvalTypeHandler``,
    ``CoGroupedEvalTypeHandler``), which fixes the input type and serializer.
    """

    # PythonEvalType this handler serves; None on the abstract bases.
    eval_type: ClassVar[Optional[int]] = None

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


class BatchEvalTypeHandler(EvalTypeHandler["pa.RecordBatch", OutputBatch]):
    """Handler category whose input stream is ``Iterator[pa.RecordBatch]``."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamSerializer(write_start_stream=True)


class GroupedEvalTypeHandler(EvalTypeHandler[GroupedBatch, OutputBatch]):
    """Handler category whose input stream is ``Iterator[GroupedBatch]``."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamGroupSerializer(write_start_stream=True)


class CoGroupedEvalTypeHandler(EvalTypeHandler[CoGroupedBatch, OutputBatch]):
    """Handler category whose input stream is ``Iterator[CoGroupedBatch]``."""

    @property
    def serializer(self) -> Serializer:
        return ArrowStreamCoGroupSerializer(write_start_stream=True)
