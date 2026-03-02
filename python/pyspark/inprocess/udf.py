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

from typing import Callable

import cloudpickle
import pyarrow as pa

from pyspark.sql.types import (
    DataType, LongType, IntegerType, DoubleType, FloatType,
    BooleanType, ShortType, ByteType,
)

# Map from Spark SQL DataType to PyArrow type for output type enforcement.
_SPARK_TO_ARROW: dict = {
    LongType(): pa.int64(),
    IntegerType(): pa.int32(),
    DoubleType(): pa.float64(),
    FloatType(): pa.float32(),
    BooleanType(): pa.bool_(),
    ShortType(): pa.int16(),
    ByteType(): pa.int8(),
}


class InProcessUDFWrapper:
    """
    Wraps a Python function as an in-process UDF.

    Returned by ``@inprocess_udf``. Calling an instance with Spark ``Column``
    arguments creates a ``Column`` expression backed by ``InProcessPythonUDF``
    on the JVM side.
    """

    def __init__(self, func: Callable, return_type: DataType, deterministic: bool = True) -> None:
        self._return_type: DataType = return_type
        self._deterministic: bool = deterministic
        self._name: str = getattr(func, "__name__", "inprocess_udf")

        # Wrap the function to cast its output to the declared return type.
        # This handles the case where the UDF's input column type differs from
        # the declared return type (e.g. input is int64, return_type is IntegerType).
        arrow_type = _SPARK_TO_ARROW.get(return_type)
        if arrow_type is not None:
            def _wrapped(*args, _fn=func, _atype=arrow_type):
                result = _fn(*args)
                if result.type != _atype:
                    result = result.cast(_atype)
                return result
            self._serialized: bytes = cloudpickle.dumps(_wrapped)
        else:
            self._serialized = cloudpickle.dumps(func)

    def __call__(self, *cols):
        """
        Create a ``Column`` expression invoking this UDF with the given columns.

        Args:
            *cols: Spark ``Column`` objects (e.g. ``df.value``, ``col("x")``)

        Returns:
            pyspark.sql.Column
        """
        from pyspark import SparkContext
        from pyspark.sql.column import Column
        from pyspark.sql.classic.column import _to_java_column

        sc = SparkContext._active_spark_context
        if sc is None:
            raise RuntimeError(
                "No active SparkContext. Start a SparkSession before calling an inprocess_udf.")

        jvm = sc._jvm

        # Convert Python Column objects to JVM Column objects
        jcols = [_to_java_column(c) for c in cols]

        # Build a Java ArrayList (py4j vararg spread doesn't work with Arrays.asList)
        jlist = jvm.java.util.ArrayList()
        for jcol in jcols:
            jlist.add(jcol)

        # Delegate to JVM builder which returns a JVM Column backed by InProcessPythonUDF
        jcol = jvm.org.apache.spark.sql.execution.python.InProcessPythonUDFBuilder.build(
            self._name,
            self._serialized,
            self._return_type.json(),
            jlist,
            self._deterministic,
        )

        return Column(jcol)


def inprocess_udf(return_type: DataType, deterministic: bool = True) -> Callable:
    """
    Decorator to register a Python function as an in-process UDF.

    The decorated function receives one ``pa.Array`` per input column and must
    return a single ``pa.Array`` of the declared ``return_type``.

    All Arrow types are supported via the Arrow C Data Interface (CDI), including
    primitives, strings, binary, temporal types, arrays, maps, and structs.

    Args:
        return_type:   Spark SQL DataType for the UDF return value
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
