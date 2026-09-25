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
Python API for in-process UDF registration.

Usage::

    import pyarrow.compute as pc
    from pyspark.inprocess import inprocess_udf
    from pyspark.sql.types import LongType

    @inprocess_udf(return_type=LongType())
    def double(x):
        # x is a pa.Array; return a pa.Array
        return pc.multiply(x, 2)

    df.select(double(df.value)).show()
"""

import io
import sys
from functools import update_wrapper
from inspect import getfullargspec
from typing import Any, Callable, Optional, Union

from pyspark import Accumulator, Broadcast, cloudpickle
from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.sql.column import Column
from pyspark.sql.types import DataType, _parse_datatype_string
from pyspark.util import PythonEvalType


class _InProcessPickler(cloudpickle.CloudPickler):
    def reducer_override(self, obj: Any) -> Any:
        if isinstance(obj, (Broadcast, Accumulator)):
            raise TypeError("In-process UDFs do not support Spark broadcasts or accumulators")
        return super().reducer_override(obj)


def _serialize_udf(func: Callable) -> bytes:
    buffer = io.BytesIO()
    _InProcessPickler(buffer).dump(func)
    return buffer.getvalue()


class InProcessUDFWrapper:
    """
    Wraps a Python function as an in-process UDF.

    Returned by ``@inprocess_udf``. Calling an instance with Spark ``Column``
    arguments creates a ``Column`` expression backed by ``PythonUDF``
    on the JVM side.
    """

    def __init__(
        self, func: Callable, return_type: Union[DataType, str], deterministic: bool = True
    ) -> None:
        if not isinstance(return_type, (DataType, str)):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "expected_type": "DataType or str",
                    "arg_name": "return_type",
                    "arg_type": type(return_type).__name__,
                },
            )
        self._return_type = return_type
        self._parsed_return_type: Optional[DataType] = None
        self.evalType = PythonEvalType.SQL_SCALAR_ARROW_INPROCESS_UDF
        self._deterministic: bool = deterministic
        self._name: str = getattr(func, "__name__", "inprocess_udf")

        argspec = getfullargspec(func)
        if not argspec.args and argspec.varargs is None and not argspec.kwonlyargs:
            raise PySparkValueError(
                errorClass="INVALID_PANDAS_UDF",
                messageParameters={"detail": "0-arg inprocess_udfs are not supported."},
            )
        self._func = func
        self._serialized: Optional[bytes] = None
        update_wrapper(self, func, updated=())

    @property
    def func(self) -> Callable:
        return self._func

    @property
    def returnType(self) -> DataType:
        if self._parsed_return_type is None:
            self._parsed_return_type = (
                _parse_datatype_string(self._return_type)
                if isinstance(self._return_type, str)
                else self._return_type
            )
        return self._parsed_return_type

    @property
    def deterministic(self) -> bool:
        return self._deterministic

    def asNondeterministic(self) -> "InProcessUDFWrapper":
        self._deterministic = False
        return self

    def _serialize(self) -> bytes:
        if self._serialized is None:
            self._serialized = _serialize_udf(self._func)
        return self._serialized

    def __call__(self, *cols: Union[Column, str], **kwargs: Union[Column, str]) -> Column:
        """
        Create a ``Column`` expression invoking this UDF with the given columns.

        Args:
            *cols: Spark ``Column`` objects (e.g. ``df.value``, ``col("x")``)

        Returns:
            pyspark.sql.Column
        """
        from pyspark import SparkContext
        from pyspark.sql.classic.column import _to_java_column

        sc = SparkContext._active_spark_context
        if sc is None:
            raise RuntimeError(
                "No active SparkContext. Start a SparkSession before calling an inprocess_udf."
            )

        jvm = sc._jvm
        assert jvm is not None

        # Convert Python Column objects to JVM Column objects
        if not cols and not kwargs:
            raise PySparkValueError(
                errorClass="INVALID_PANDAS_UDF",
                messageParameters={"detail": "An inprocess_udf requires at least one argument."},
            )
        jcols = [_to_java_column(c) for c in cols]
        jcols.extend(
            jvm.PythonSQLUtils.namedArgumentExpression(name, _to_java_column(value))
            for name, value in kwargs.items()
        )

        # Build a Java ArrayList (py4j vararg spread doesn't work with Arrays.asList)
        jlist = jvm.java.util.ArrayList()
        for jcol in jcols:
            jlist.add(jcol)

        # Use the existing PythonUDF planning contracts with an in-process eval type.
        jcol = jvm.org.apache.spark.sql.execution.python.InProcessPythonUDFBuilder.build(
            self._name,
            self._serialize(),
            self.returnType.json(),
            jlist,
            self._deterministic,
            "%d.%d" % sys.version_info[:2],
        )

        return Column(jcol)


def inprocess_udf(return_type: Union[DataType, str], deterministic: bool = True) -> Callable:
    """
    Decorator to register a Python function as an in-process UDF.

    The decorated function receives one ``pa.Array`` per input column and must
    return a single ``pa.Array`` of the declared ``return_type``.

    The result must have the same length as the input batch and its Arrow type
    must match the declared Spark type, including nested fields and timestamp
    timezone. Nested nullability may be widened, but actual nulls cannot be returned
    in non-nullable fields. Value types must match exactly; use an explicit PyArrow
    cast in the function when conversion is intended. Sliced results are copied when
    required by Arrow Java.

    Spark broadcasts, accumulators, and ``SparkContext.addPyFile`` are unsupported.
    Install dependencies on executors before starting Spark. The driver's Python
    major.minor version must match the embedded interpreter.

    Args:
        return_type:   Spark SQL DataType or DDL string for the UDF return value
        deterministic: Whether this UDF produces the same output for the same input.
                       Set to ``False`` for UDFs that use randomness, external state,
                       or other sources of non-determinism so the optimizer does not
                       deduplicate or reorder calls to this UDF.  Default: ``True``.

    Returns:
        Decorator that wraps the function as an ``InProcessUDFWrapper``

    Example::

        @inprocess_udf(return_type=LongType())
        def double(x):
            import pyarrow.compute as pc
            return pc.multiply(x, 2)

        @inprocess_udf(return_type=LongType(), deterministic=False)
        def random_noise(x):
            import pyarrow as pa, numpy as np
            return pa.array(np.random.randint(0, 100, len(x)), type=pa.int64())
    """

    def decorator(func: Callable) -> InProcessUDFWrapper:
        return InProcessUDFWrapper(func, return_type, deterministic)

    return decorator
