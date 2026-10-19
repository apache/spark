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

# eval type -> (variant_conf or None, {variant_value: handler class}), populated by
# _EvalTypeHandlerMeta at class definition. Handlers that declare no variant_conf register
# under the conf name None and the value None.
_eval_type_handlers: "dict[int, tuple[Optional[str], dict[Any, type[EvalTypeHandler]]]]" = {}


def get_eval_type_handler(
    eval_type: int, runner_conf: "RunnerConf"
) -> "Optional[type[EvalTypeHandler]]":
    """Return the handler class registered for ``eval_type``, or ``None``.

    When the eval type's handlers declare a ``variant_conf``, the variant is selected by the
    value of that ``RunnerConf`` property. This happens per lookup rather than at import, as
    the conf is per task and Python workers are reused across tasks.
    """
    entry = _eval_type_handlers.get(eval_type)
    if entry is None:
        return None
    conf_name, variants = entry
    key = getattr(runner_conf, conf_name) if conf_name is not None else None
    handler_cls = variants.get(key)
    if handler_cls is None:
        raise RuntimeError(
            "No eval type handler for {} with {}={!r}; registered values: {}".format(
                eval_type, conf_name, key, sorted(map(repr, variants))
            )
        )
    return handler_cls


class _EvalTypeHandlerMeta(ABCMeta):
    """Registers a concrete handler under its ``eval_type`` (and ``variant_value``, if it
    declares a ``variant_conf``) at class definition.

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
            conf_name = namespace.get("variant_conf")
            assert conf_name is not None or "variant_value" not in namespace, (
                "Handler {} declares variant_value without variant_conf".format(name)
            )
            assert conf_name is None or "variant_value" in namespace, (
                "Handler {} declares variant_conf {} without variant_value".format(name, conf_name)
            )
            value = namespace.get("variant_value")
            registered_conf_name, variants = _eval_type_handlers.get(eval_type, (conf_name, {}))
            assert registered_conf_name == conf_name, (
                "Handlers for eval type {} must share one variant_conf: {} keys on {}, "
                "but {} keys on {}".format(
                    eval_type,
                    next(iter(variants.values())).__name__,
                    registered_conf_name,
                    name,
                    conf_name,
                )
            )
            assert value not in variants, (
                "Duplicate eval type handler for {} with {}={!r}: {} and {}".format(
                    eval_type, conf_name, value, variants[value].__name__, name
                )
            )
            variants[value] = cls
            _eval_type_handlers[eval_type] = (conf_name, variants)
        return cls


class EvalTypeHandler(Generic[InputBatch, OutputBatch], metaclass=_EvalTypeHandlerMeta):
    """Base class for the UDF execution model.

    A handler declares the ``serializer`` for the input/output streams and
    implements ``run``, which consumes the input stream and yields the output
    stream. Arrow handlers subclass a typed category base
    (``BatchEvalTypeHandler``, ``GroupedEvalTypeHandler``,
    ``CoGroupedEvalTypeHandler``), which fixes the input type and serializer;
    the pickled-row handlers subclass this class directly.
    """

    # PythonEvalType this handler serves; None on the abstract bases.
    eval_type: ClassVar[Optional[int]] = None
    # Name of the RunnerConf property that selects between the handlers of one eval type, and
    # the value of it this handler serves; None when the eval type has a single handler.
    variant_conf: ClassVar[Optional[str]] = None
    variant_value: ClassVar[Any] = None

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
        """Consume the input stream and yield the output stream; the worker calls it as
        ``func(split_index, data)``."""


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
