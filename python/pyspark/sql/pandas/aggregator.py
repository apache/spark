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

from pyspark.errors import PySparkTypeError
from pyspark.sql.types import DataType, StructType
from pyspark.util import PythonEvalType

__all__ = ["Aggregator", "arrow_udaf"]


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
    to the aggregator call. :meth:`merge` must be associative and commutative, since the framework
    may combine partial buffers in any order.

    .. versionadded:: 4.2.0

    Examples
    --------
    A mean aggregator::

        from pyspark.sql.pandas.aggregator import Aggregator, arrow_udaf
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
                return (buffer[0] + v, buffer[1] + 1)

            def merge(self, b1, b2):
                return (b1[0] + b2[0], b1[1] + b2[1])

            def finish(self, buffer):
                return buffer[0] / buffer[1] if buffer[1] else None

        mean = arrow_udaf(Mean())
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
        """The initial (identity) buffer value, as a tuple matching :attr:`bufferSchema`."""
        ...

    @abstractmethod
    def reduce(self, buffer: Tuple[Any, ...], value: Tuple[Any, ...]) -> Tuple[Any, ...]:
        """Fold a single input row ``value`` into ``buffer`` and return the updated buffer."""
        ...

    @abstractmethod
    def merge(
        self, buffer1: Tuple[Any, ...], buffer2: Tuple[Any, ...]
    ) -> Tuple[Any, ...]:
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
        raise NotImplementedError(
            "An Aggregator is not directly callable; wrap it with arrow_udaf(...)."
        )


def arrow_udaf(agg: "Aggregator") -> Any:
    """
    Turn an :class:`Aggregator` instance into a callable usable in ``groupBy().agg(...)``.

    .. versionadded:: 4.2.0

    Parameters
    ----------
    agg : :class:`Aggregator`
        The aggregator instance.

    Returns
    -------
    function
        A callable that, applied to input columns, produces an aggregate :class:`Column`.
    """
    from pyspark.sql.utils import is_remote

    if is_remote():
        from pyspark.sql.connect.udf import UserDefinedFunction
    else:
        from pyspark.sql.udf import UserDefinedFunction

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

    udf_obj = UserDefinedFunction(
        agg,
        returnType=agg.outputType,
        name=agg.__class__.__name__,
        evalType=PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF,
        deterministic=True,
    )
    # Threaded to the JVM in UserDefinedFunction._create_judf so PythonAggregate can plan the
    # two-stage aggregation.
    udf_obj.bufferSchema = agg.bufferSchema  # type: ignore[attr-defined]
    return udf_obj._wrapped()
