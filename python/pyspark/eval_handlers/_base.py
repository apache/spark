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

This leaf module imports only ``_typing``, the serializers and ``pyspark.util``, so the
package ``__init__`` and the concrete-handler submodules can both import it.
"""

from abc import ABCMeta, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Generic, Optional

from pyspark.eval_handlers._typing import (
    UDF,
    UDTF,
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
from pyspark.util import fail_on_stopiteration

if TYPE_CHECKING:
    import pyarrow as pa  # noqa: F401  # only in the batch category's forward-ref subscript

    from pyspark.sql.types import DataType
    from pyspark.worker_message import UDFInfo
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

    _iter_based: bool

    @property
    def is_iter_based(cls) -> bool:
        """Whether the handler's UDFs return an iterator."""
        return cls._iter_based


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
    # Whether the handler's UDFs return an iterator.
    _iter_based: ClassVar[bool] = False
    # Whether create_udf wraps the UDF in fail_on_stopiteration, so a StopIteration raised in
    # user code fails the task instead of silently ending the loop that calls the UDF.
    _fail_on_stopiteration: ClassVar[bool] = True

    @classmethod
    def create_udf(
        cls,
        chained_func: Callable[..., Any],
        return_type: "DataType",
        udf_info: "UDFInfo",
        runner_conf: "RunnerConf",
    ) -> UDF:
        """Build the ``UDF`` record of ``udf_info`` from ``chained_func``, its unpickled (and
        chained) function, wrapping it for profiling and in ``fail_on_stopiteration``."""
        profiling_func = cls._with_profiler(chained_func, udf_info.result_id, runner_conf)
        return UDF(
            func=(
                fail_on_stopiteration(profiling_func)
                if cls._fail_on_stopiteration
                else profiling_func
            ),
            raw_func=chained_func,
            args_offsets=udf_info.args,
            kwargs_offsets=udf_info.kwargs,
            return_type=return_type,
        )

    @classmethod
    def _with_profiler(
        cls, f: Callable[..., Any], result_id: int, runner_conf: "RunnerConf"
    ) -> Callable[..., Any]:
        """Return ``f`` profiled by the profiler its config selects, or ``f`` itself when none is
        set."""
        # If f is from pyspark.sql.worker, it is to read/write data source.
        # In this case, we check the data_source_profiler config.
        module = getattr(f, "__module__", "")
        if isinstance(module, str) and module.startswith("pyspark.sql.worker."):
            profiler = runner_conf.data_source_profiler
        else:
            profiler = runner_conf.udf_profiler
        if profiler == "perf":
            return cls._with_perf_profiler(f, result_id)
        elif profiler == "memory":
            return cls._with_memory_profiler(f, result_id)
        else:
            return f

    @classmethod
    def _with_perf_profiler(cls, f: Callable[..., Any], result_id: int) -> Callable[..., Any]:
        from pyspark.accumulators import SpecialAccumulatorIds, _deserialize_accumulator
        from pyspark.sql.profiler import (
            ProfileResultsParam,
            ProfileResultsParamV2,
            WorkerPerfProfiler,
        )

        accumulator = _deserialize_accumulator(
            SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
        )
        accumulator_v2 = _deserialize_accumulator(
            SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
        )

        if cls.is_iter_based:

            def profiling_func(*args: Any, **kwargs: Any) -> Any:
                iterator = iter(f(*args, **kwargs))
                while True:
                    try:
                        with WorkerPerfProfiler(accumulator, accumulator_v2, result_id):
                            item = next(iterator)
                        yield item
                    except StopIteration:
                        break

        else:

            def profiling_func(*args: Any, **kwargs: Any) -> Any:
                with WorkerPerfProfiler(accumulator, accumulator_v2, result_id):
                    ret = f(*args, **kwargs)
                return ret

        return profiling_func

    @classmethod
    def _with_memory_profiler(cls, f: Callable[..., Any], result_id: int) -> Callable[..., Any]:
        import pyspark.memory_profiler_ext
        from pyspark.accumulators import SpecialAccumulatorIds, _deserialize_accumulator
        from pyspark.sql.profiler import (
            ProfileResultsParam,
            ProfileResultsParamV2,
            WorkerMemoryProfiler,
        )

        if not pyspark.memory_profiler_ext.has_memory_profiler:
            return f

        accumulator = _deserialize_accumulator(
            SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
        )

        accumulator_v2 = _deserialize_accumulator(
            SpecialAccumulatorIds.SQL_UDF_PROFIER_V2, {}, ProfileResultsParamV2
        )

        if cls.is_iter_based:

            def profiling_func(*args: Any, **kwargs: Any) -> Any:
                g = f(*args, **kwargs)
                iterator = iter(g)

                while True:
                    try:
                        with WorkerMemoryProfiler(
                            accumulator, accumulator_v2, result_id, g.gi_code
                        ):
                            item = next(iterator)
                        yield item
                    except StopIteration:
                        break

        else:

            def profiling_func(*args: Any, **kwargs: Any) -> Any:
                with WorkerMemoryProfiler(accumulator, accumulator_v2, result_id, f):
                    ret = f(*args, **kwargs)
                return ret

        return profiling_func

    def __init__(
        self, udfs: "list[UDF] | list[UDTF]", runner_conf: "RunnerConf", eval_conf: "EvalConf"
    ) -> None:
        self._udfs: list[Any] = udfs
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
