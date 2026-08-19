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
"""
Incremental user-defined aggregators for PySpark, the Python analog of Scala's
``org.apache.spark.sql.expressions.Aggregator``.
"""

from abc import ABC, abstractmethod
from typing import Any, Tuple

from pyspark.errors import PySparkNotImplementedError, PySparkTypeError, PySparkValueError
from pyspark.sql.types import DataType, StructType
from pyspark.util import PythonEvalType

__all__ = ["Aggregator", "udaf"]


class Aggregator(ABC):
    """
    Base class for a user-defined *incremental* aggregator, the Python analog of Scala's
    :class:`org.apache.spark.sql.expressions.Aggregator`.

    Unlike a grouped-aggregate ``pandas_udf`` (which materializes the whole group and is invoked
    once), an :class:`Aggregator` is executed as a genuine two-stage aggregation with map-side
    combine: :meth:`reduce` folds input rows into a per-group *buffer* on the map side, the buffers
    are shuffled by the grouping key, :meth:`merge` combines the partial buffers of each group, and
    :meth:`finish` produces the final output value.

    The buffer is represented as a Python :class:`tuple` whose elements correspond, in order, to the
    fields of :attr:`bufferSchema`. An input row is likewise a tuple of the argument values passed
    to the aggregator call. :meth:`merge` must be associative and commutative (the framework may
    combine partial buffers in any order), and :meth:`zero` must be its identity element -- see
    :meth:`zero` for the identity law that makes the result independent of the partition count.

    .. versionadded:: 4.4.0

    Examples
    --------
    A mean aggregator::

        from pyspark.sql.aggregator import Aggregator, udaf
        from pyspark.sql.types import StructType, StructField, DoubleType, LongType

        class Mean(Aggregator):
            @property
            def bufferSchema(self):
                return StructType([
                    StructField("sum", DoubleType()),
                    StructField("count", LongType()),
                ])

            @property
            def outputType(self):
                return DoubleType()

            def zero(self):
                return (0.0, 0)

            def reduce(self, buffer, value):
                (v,) = value
                if v is None:  # ignore null inputs, like SQL aggregates do
                    return buffer
                return (buffer[0] + v, buffer[1] + 1)

            def merge(self, b1, b2):
                return (b1[0] + b2[0], b1[1] + b2[1])

            def finish(self, buffer):
                return buffer[0] / buffer[1] if buffer[1] else None

        mean = udaf(Mean())
        df.groupBy("k").agg(mean(df.v)).show()
    """

    @property
    @abstractmethod
    def bufferSchema(self) -> StructType:
        """The schema of the intermediate buffer that crosses the shuffle."""
        ...

    @property
    @abstractmethod
    def outputType(self) -> DataType:
        """The data type of the aggregator's output value."""
        ...

    @abstractmethod
    def zero(self) -> Tuple[Any, ...]:
        """The initial (identity) buffer value, as a tuple matching :attr:`bufferSchema`.

        This must be the identity element for :meth:`merge`::

            merge(buffer, zero()) == buffer
            merge(zero(), buffer) == buffer

        A fresh ``zero()`` seeds every partition -- and every early-flushed chunk of the map-side
        combine -- so associativity and commutativity of :meth:`merge` alone do not guarantee a
        partition-independent result; the identity law above is what makes the aggregate value
        independent of how the input is split across partitions and batches.
        """
        ...

    @abstractmethod
    def reduce(self, buffer: Tuple[Any, ...], value: Tuple[Any, ...]) -> Tuple[Any, ...]:
        """Fold a single input row ``value`` into ``buffer`` and return the updated buffer."""
        ...

    @abstractmethod
    def merge(self, buffer1: Tuple[Any, ...], buffer2: Tuple[Any, ...]) -> Tuple[Any, ...]:
        """Merge two partial buffers into one. Must be associative and commutative."""
        ...

    @abstractmethod
    def finish(self, buffer: Tuple[Any, ...]) -> Any:
        """Produce the output value from the final merged buffer."""
        ...

    # The aggregator instance is shipped to the worker as the UDF "function"; making it callable
    # lets it satisfy ``UserDefinedFunction``'s ``callable`` check. It is never actually invoked as
    # a function -- the worker calls :meth:`zero`/:meth:`reduce`/:meth:`merge`/:meth:`finish`.
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={"feature": "calling an Aggregator directly; wrap it with udaf(...)"},
        )


def udaf(agg: "Aggregator") -> Any:
    """
    Turn an :class:`Aggregator` instance into a callable usable in ``groupBy().agg(...)``, the
    Python counterpart of Scala's ``functions.udaf``.

    The aggregator is executed with true incremental (partial) aggregation and transfers its
    intermediate buffer as Arrow; PyArrow is therefore required.

    .. versionadded:: 4.4.0

    Parameters
    ----------
    agg : :class:`Aggregator`
        The aggregator instance.

    Returns
    -------
    function
        A callable that, applied to input columns, produces an aggregate :class:`Column`.

    Raises
    ------
    :class:`PySparkImportError`
        If a supported version of PyArrow is not installed.
    :class:`PySparkTypeError`
        If ``agg`` is not an :class:`Aggregator`, or its ``bufferSchema`` is not a
        :class:`StructType`.
    """
    from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
    from pyspark.sql.utils import is_remote

    require_minimum_pyarrow_version()

    if is_remote():
        from pyspark.sql.connect.udf import UserDefinedFunction
    else:
        # The classic UserDefinedFunction is a distinct class from the Connect one above;
        # both provide the same interface used below, so silence mypy's reassignment check.
        from pyspark.sql.udf import UserDefinedFunction  # type: ignore[assignment]

    if not isinstance(agg, Aggregator):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "agg",
                "expected_type": "Aggregator",
                "arg_type": type(agg).__name__,
            },
        )
    if not isinstance(agg.bufferSchema, StructType):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "bufferSchema",
                "expected_type": "StructType",
                "arg_type": type(agg.bufferSchema).__name__,
            },
        )
    # The buffer crosses the shuffle as an Arrow struct whose children are matched by name, and the
    # worker keys the buffer tuple back by field name. Duplicate names would silently collapse
    # fields on the map side and then fail with an opaque Arrow error post-shuffle, so reject them
    # up front where the aggregator is created.
    field_names = [field.name for field in agg.bufferSchema.fields]
    if len(field_names) != len(set(field_names)):
        duplicates = sorted({name for name in field_names if field_names.count(name) > 1})
        raise PySparkValueError(
            errorClass="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
            messageParameters={"field_names": ", ".join(duplicates)},
        )

    # ``bufferSchema`` is a first-class ``UserDefinedFunction`` field (threaded to the JVM in
    # ``_create_judf`` so ``PythonAggregate`` can plan the two-stage aggregation), so it survives
    # ``_wrapped()`` and ``spark.udf.register`` without being re-attached.
    udf_obj = UserDefinedFunction(
        agg,
        returnType=agg.outputType,
        name=agg.__class__.__name__,
        evalType=PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF,
        deterministic=True,
        bufferSchema=agg.bufferSchema,
    )
    return udf_obj._wrapped()
