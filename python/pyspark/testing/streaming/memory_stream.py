#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""A controllable in-memory streaming source for PySpark tests.

``MemoryStream`` wraps the JVM ``MemoryStream[Row]`` so Python tests can
add rows to a streaming source and obtain the corresponding streaming
DataFrame for use in ``StreamTest`` flows.
"""

from __future__ import annotations

from typing import Any, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.testing.streaming._conversions import resolve_schema, to_rows


class MemoryStream:
    """A controllable streaming source backed by the JVM ``MemoryStream``.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession. Spark Connect sessions are not supported;
        the Scala bridge requires JVM access.
    schema : str or StructType, default ``"int"``
        Either a primitive name (``"int"``, ``"long"``, ``"string"``,
        ``"double"``, ``"float"``, ``"boolean"``) or a full ``StructType``.
        Primitive names produce a single-column schema with a column named
        ``value``, mirroring Scala's ``MemoryStream[Int]``.

    Examples
    --------
    >>> source = MemoryStream(spark, "int")  # doctest: +SKIP
    >>> source.add_data(1, 2, 3)             # doctest: +SKIP
    0
    >>> df = source.to_df()                  # doctest: +SKIP
    """

    def __init__(
        self,
        spark: SparkSession,
        schema: Union[str, StructType] = "int",
    ) -> None:
        self._spark = spark
        self._schema = resolve_schema(schema)
        jvm = self._jvm()
        jspark = spark._jsparkSession  # type: ignore[attr-defined]
        jschema = jspark.parseDataType(self._schema.json())
        py_utils = jvm.org.apache.spark.sql.api.python.PythonSQLUtils
        self._jstream = py_utils.createMemoryStream(jspark, jschema)
        self._current_offset = -1

    def _jvm(self) -> Any:
        jvm = getattr(self._spark, "_jvm", None)
        if jvm is None:
            raise RuntimeError(
                "MemoryStream requires a JVM-backed SparkSession; "
                "Spark Connect sessions are not supported."
            )
        return jvm

    @property
    def schema(self) -> StructType:
        """The output schema of the stream."""
        return self._schema

    @property
    def current_offset(self) -> int:
        """The most recent offset returned by ``add_data``, or ``-1``."""
        return self._current_offset

    def add_data(self, *rows: Any) -> int:
        """Append rows to the underlying JVM memory stream.

        Each call corresponds to a single addition and produces a single
        new offset on the JVM side. Calling with no arguments is a no-op
        and returns the current offset.

        Parameters
        ----------
        *rows
            Values to add. For a single-column schema, scalars are accepted.
            For multi-column schemas, pass ``Row`` objects, dicts, tuples,
            or objects whose attributes match the schema field names.

        Returns
        -------
        int
            The new current offset after the addition.
        """
        if not rows:
            return self._current_offset
        converted = to_rows(list(rows), self._schema)
        df = self._spark.createDataFrame(converted, schema=self._schema)
        py_utils = self._jvm().org.apache.spark.sql.api.python.PythonSQLUtils
        offset = py_utils.memoryStreamAddData(self._jstream, df._jdf)
        self._current_offset = int(offset)
        return self._current_offset

    def to_df(self) -> DataFrame:
        """Return a streaming ``DataFrame`` reading from this memory stream."""
        py_utils = self._jvm().org.apache.spark.sql.api.python.PythonSQLUtils
        jdf = py_utils.memoryStreamToDF(self._jstream)
        return DataFrame(jdf, self._spark)

    def __repr__(self) -> str:
        return f"MemoryStream(schema={self._schema.simpleString()})"
