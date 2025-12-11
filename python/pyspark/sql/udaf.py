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
User-defined aggregate function related classes and functions
"""
from typing import Any, TYPE_CHECKING, Optional, List, Iterator, Tuple

from pyspark.sql.column import Column
from pyspark.sql.types import (
    DataType,
    _parse_datatype_string,
)
from pyspark.errors import PySparkTypeError, PySparkNotImplementedError

if TYPE_CHECKING:
    from pyspark.sql._typing import DataTypeOrString, ColumnOrName
    from pyspark.sql.dataframe import DataFrame

__all__ = [
    "Aggregator",
    "UserDefinedAggregateFunction",
    "udaf",
]


class Aggregator:
    """
    Base class for user-defined aggregations.

    This class defines the interface for implementing user-defined aggregate functions (UDAFs)
    in Python. Users should subclass this class and implement the required methods.

    .. versionadded:: 4.2.0

    Examples
    --------
    >>> class MySum(Aggregator):
    ...     def zero(self):
    ...         return 0
    ...     def reduce(self, buffer, value):
    ...         return buffer + value
    ...     def merge(self, buffer1, buffer2):
    ...         return buffer1 + buffer2
    ...     def finish(self, reduction):
    ...         return reduction
    """

    def zero(self) -> Any:
        """
        A zero value for this aggregation. Should satisfy the property that any b + zero = b.

        Returns
        -------
        Any
            The zero value for the aggregation buffer.
        """
        raise NotImplementedError

    def reduce(self, buffer: Any, value: Any) -> Any:
        """
        Combine an input value into the current intermediate value.

        For performance, the function may modify `buffer` and return it instead of
        constructing a new object.

        Parameters
        ----------
        buffer : Any
            The current intermediate value (buffer).
        value : Any
            The input value to aggregate.

        Returns
        -------
        Any
            The updated buffer.
        """
        raise NotImplementedError

    def merge(self, buffer1: Any, buffer2: Any) -> Any:
        """
        Merge two intermediate values.

        Parameters
        ----------
        buffer1 : Any
            The first intermediate value.
        buffer2 : Any
            The second intermediate value.

        Returns
        -------
        Any
            The merged intermediate value.
        """
        raise NotImplementedError

    def finish(self, reduction: Any) -> Any:
        """
        Transform the output of the reduction.

        Parameters
        ----------
        reduction : Any
            The final reduction result.

        Returns
        -------
        Any
            The final output value.
        """
        raise NotImplementedError


class UserDefinedAggregateFunction:
    """
    User-defined aggregate function wrapper for Python Aggregator.

    This class wraps an Aggregator instance and provides the functionality to use it
    as an aggregate function in Spark SQL. The implementation uses mapInArrow and
    applyInArrow to perform partial aggregation and final aggregation.

    .. versionadded:: 4.2.0
    """

    def __init__(
        self,
        aggregator: Aggregator,
        returnType: "DataTypeOrString",
        name: Optional[str] = None,
    ):
        if not isinstance(aggregator, Aggregator):
            raise PySparkTypeError(
                errorClass="NOT_CALLABLE",
                messageParameters={
                    "arg_name": "aggregator",
                    "arg_type": type(aggregator).__name__,
                },
            )

        if not isinstance(returnType, (DataType, str)):
            raise PySparkTypeError(
                errorClass="NOT_DATATYPE_OR_STR",
                messageParameters={
                    "arg_name": "returnType",
                    "arg_type": type(returnType).__name__,
                },
            )

        self.aggregator = aggregator
        self._returnType = returnType
        self._name = name or (
            aggregator.__class__.__name__
            if hasattr(aggregator, "__class__")
            else "UserDefinedAggregateFunction"
        )
        # Serialize aggregator for use in Arrow functions
        # Use cloudpickle to ensure proper serialization of classes
        try:
            import cloudpickle
        except ImportError:
            import pickle as cloudpickle
        self._serialized_aggregator = cloudpickle.dumps(aggregator)

    @property
    def returnType(self) -> DataType:
        """Get the return type of this UDAF."""
        if isinstance(self._returnType, DataType):
            return self._returnType
        else:
            return _parse_datatype_string(self._returnType)

    def __call__(self, *args: "ColumnOrName") -> Column:
        """
        Apply this UDAF to the given columns.

        This creates a Column expression that can be used in DataFrame operations.
        The actual aggregation is performed using mapInArrow and applyInArrow.

        Parameters
        ----------
        *args : ColumnOrName
            The columns to aggregate. Currently supports a single column.

        Returns
        -------
        Column
            A Column representing the aggregation result.

        Notes
        -----
        This implementation uses mapInArrow and applyInArrow internally to perform
        the aggregation. The approach follows:
        1. mapInArrow: Performs partial aggregation (reduce) on each partition
        2. groupBy: Groups partial results by a random key (range based on
           spark.sql.shuffle.partitions config or DataFrame partition count)
        3. applyInArrow: Merges partial results and produces final result

        Examples
        --------
        >>> class MySum(Aggregator):
        ...     def zero(self):
        ...         return 0
        ...     def reduce(self, buffer, value):
        ...         return buffer + value
        ...     def merge(self, buffer1, buffer2):
        ...         return buffer1 + buffer2
        ...     def finish(self, reduction):
        ...         return reduction
        ...
        >>> sum_udaf = udaf(MySum(), "bigint")
        >>> df = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
        >>> df.agg(sum_udaf(df.value)).show()
        +------------+
        |MySum(value)|
        +------------+
        |           6|
        +------------+
        """
        # Return a Column with UDAF metadata attached as an attribute
        # This allows GroupedData.agg() to detect and handle UDAF columns
        # without introducing a special Column type
        from pyspark.sql.classic.column import Column as ClassicColumn
        from pyspark.sql.functions import col as spark_col

        col_expr = args[0]
        if isinstance(col_expr, str):
            col_expr = spark_col(col_expr)

        # Create a Column and attach UDAF information as an attribute
        # This is similar to how pandas UDF works - the Column contains metadata
        # that can be checked in agg() without requiring a special Column type
        result_col = ClassicColumn(col_expr._jc)  # type: ignore[attr-defined]
        # Attach UDAF metadata as attributes (not a special type)
        result_col._udaf_func = self  # type: ignore[attr-defined]
        result_col._udaf_col = col_expr  # type: ignore[attr-defined]
        return result_col


def _extract_column_name(col_expr: "ColumnOrName") -> tuple[Column, str]:
    """Extract column name from Column or string, return (Column, name)."""
    from pyspark.sql.functions import col as spark_col

    if isinstance(col_expr, str):
        return spark_col(col_expr), col_expr
    else:
        # Extract column name from expression string (e.g., "value" from "Column<'value'>")
        col_name_str = col_expr._jc.toString() if hasattr(col_expr, "_jc") else str(col_expr)
        col_name = col_name_str.split("'")[1] if "'" in col_name_str else "value"
        return col_expr, col_name


def _extract_grouping_column_names(grouping_cols: List[Column]) -> List[str]:
    """Extract grouping column names from Column objects."""
    grouping_col_names = []
    for gc in grouping_cols:
        gc_str = gc._jc.toString() if hasattr(gc, "_jc") else str(gc)
        if "'" in gc_str:
            gc_name = gc_str.split("'")[1]
        else:
            # Fallback: use the string representation
            gc_name = gc_str.split("(")[0].strip() if "(" in gc_str else gc_str.strip()
        grouping_col_names.append(gc_name)
    return grouping_col_names


def _extract_grouping_columns_from_jvm(jgd: Any) -> List[Column]:
    """
    Extract grouping columns from GroupedData's JVM representation.

    Parameters
    ----------
    jgd : JavaObject
        The JVM GroupedData object.

    Returns
    -------
    List[Column]
        List of grouping column expressions, empty if no grouping or parsing fails.
    """
    from pyspark.sql.functions import col as spark_col
    import re

    try:
        jvm_string = jgd.toString()
        # Format: "RelationalGroupedDataset: [grouping expressions: [col1, col2], ...]"
        match = re.search(r"grouping expressions:\s*\[([^\]]+)\]", jvm_string)
        if match:
            grouping_exprs_str = match.group(1)
            grouping_col_names = [name.strip() for name in grouping_exprs_str.split(",")]
            return [spark_col(name.strip()) for name in grouping_col_names]
    except Exception:
        # If parsing fails, assume no grouping
        pass
    return []


def _apply_udaf_via_catalyst(
    df: "DataFrame",
    jgd: Any,
    udaf_func: "UserDefinedAggregateFunction",
    udaf_col_expr: Column,
    grouping_cols: Optional[List[Column]],
) -> "DataFrame":
    """
    Apply UDAF via the Catalyst optimizer path (Scala).

    This creates three Arrow UDFs and passes them to the Scala
    pythonAggregatorUDAF method, which uses the Catalyst optimizer
    to execute the aggregation.

    Parameters
    ----------
    df : DataFrame
        The original DataFrame.
    jgd : JavaObject
        The JVM GroupedData object.
    udaf_func : UserDefinedAggregateFunction
        The UDAF function.
    udaf_col_expr : Column
        The column expression to aggregate.
    grouping_cols : Optional[List[Column]]
        The grouping columns if this is a grouped aggregation.

    Returns
    -------
    DataFrame
        Aggregated result DataFrame
    """
    from pyspark.sql import DataFrame
    from pyspark.sql.pandas.functions import pandas_udf
    from pyspark.util import PythonEvalType
    from pyspark.sql.types import StructType, StructField, LongType, BinaryType

    # Get aggregator info
    serialized_aggregator = udaf_func._serialized_aggregator
    return_type = udaf_func.returnType
    has_grouping = grouping_cols is not None and len(grouping_cols) > 0
    grouping_col_names = _extract_grouping_column_names(grouping_cols) if has_grouping else []
    col_expr, col_name = _extract_column_name(udaf_col_expr)

    # Get max key for random grouping
    max_key = _get_max_key_for_random_grouping(df)

    # Create the three phase functions
    reduce_func = _create_reduce_func(serialized_aggregator, max_key, len(grouping_col_names))
    merge_func = _create_merge_func(serialized_aggregator)

    return_type_str = (
        return_type.simpleString() if hasattr(return_type, "simpleString") else str(return_type)
    )
    result_col_name_safe = f"{udaf_func._name}_{col_name}".replace("(", "_").replace(")", "_")
    final_merge_func = _create_final_merge_func(
        serialized_aggregator,
        return_type,
        has_grouping,
        grouping_col_names,
        udaf_func._name,
        col_name,
    )

    # Create Arrow UDFs for each phase
    reduce_schema = StructType(
        [
            StructField("key", LongType(), False),
            StructField("buffer", BinaryType(), True),
        ]
    )
    reduce_udf = pandas_udf(
        reduce_func,
        returnType=reduce_schema,
        functionType=PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
    )

    merge_schema = StructType([StructField("buffer", BinaryType(), True)])
    merge_udf = pandas_udf(
        merge_func,
        returnType=merge_schema,
        functionType=PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
    )

    final_schema_str = _build_result_schema(
        has_grouping, grouping_col_names, result_col_name_safe, return_type_str
    )
    final_udf = pandas_udf(
        final_merge_func,
        returnType=final_schema_str,
        functionType=PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
    )

    # Apply UDFs to the original DataFrame's columns
    # This ensures attribute IDs match between Python UDF expressions and Scala logical plan
    all_cols = [df[c] for c in df.columns]
    reduce_udf_col = reduce_udf(*all_cols)
    merge_udf_col = merge_udf(*all_cols)
    final_udf_col = final_udf(*all_cols)

    # Get result type as JSON string
    spark_session = df.sparkSession
    result_type_json = return_type.json()

    # Call the Scala pythonAggregatorUDAF method
    jdf = jgd.pythonAggregatorUDAF(
        reduce_udf_col._jc,
        merge_udf_col._jc,
        final_udf_col._jc,
        result_type_json,
    )

    result_df = DataFrame(jdf, spark_session)

    # Rename result column to match expected format
    from pyspark.sql.functions import col as spark_col

    result_col_name = f"{udaf_func._name}({col_name})"
    if has_grouping:
        select_exprs = [spark_col(gc_name) for gc_name in grouping_col_names]
        select_exprs.append(spark_col("result").alias(result_col_name))
        return result_df.select(*select_exprs)
    else:
        return result_df.select(spark_col("result").alias(result_col_name))


def _handle_udaf_aggregation_in_grouped_data(
    df: "DataFrame",
    jgd: Any,
    exprs: Tuple[Column, ...],
    udaf_cols: List[Column],
) -> "DataFrame":
    """
    Handle UDAF aggregation in GroupedData.agg() method.

    Parameters
    ----------
    df : DataFrame
        The original DataFrame.
    jgd : JavaObject
        The JVM GroupedData object.
    exprs : Tuple[Column, ...]
        All expression columns passed to agg()
    udaf_cols : List[Column]
        Columns that have _udaf_func attribute

    Returns
    -------
    DataFrame
        Aggregated result DataFrame
    """
    from pyspark.errors import PySparkNotImplementedError

    # Validate UDAF usage constraints
    if len(udaf_cols) > 1:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": "Multiple UDAFs in a single agg() call. Currently only one UDAF is supported."
            },
        )
    if len(exprs) > 1:
        raise PySparkNotImplementedError(
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": "Mixing UDAF with other aggregate functions. Currently only single UDAF is supported."
            },
        )

    # Extract UDAF information
    udaf_col = udaf_cols[0]
    udaf_func = udaf_col._udaf_func  # type: ignore[attr-defined]
    udaf_col_expr = udaf_col._udaf_col  # type: ignore[attr-defined]

    # Get grouping columns
    grouping_cols = _extract_grouping_columns_from_jvm(jgd)

    # Use Catalyst optimizer path (via Scala pythonAggregatorUDAF)
    return _apply_udaf_via_catalyst(
        df,
        jgd,
        udaf_func,
        udaf_col_expr,
        grouping_cols if grouping_cols else None,
    )


def _get_max_key_for_random_grouping(df: "DataFrame") -> int:
    """Get max key for random grouping based on Spark config or partition count."""
    try:
        spark_session = df.sparkSession
        shuffle_partitions = int(spark_session.conf.get("spark.sql.shuffle.partitions", "200"))
        num_partitions = df.rdd.getNumPartitions()
        return max(shuffle_partitions, num_partitions, 1)
    except Exception:
        return 200


def _convert_results_to_arrow(results: List[Any], return_type: DataType) -> Any:
    """Convert a list of result values to Arrow array based on return type."""
    import pyarrow as pa
    from pyspark.sql.pandas.types import to_arrow_type
    from pyspark.sql.conversion import LocalDataToArrowConversion

    # Use existing conversion utilities for accurate type handling
    arrow_type = to_arrow_type(return_type)
    converter = LocalDataToArrowConversion._create_converter(return_type, nullable=True)

    if converter is not None:
        converted_results = [converter(r) for r in results]
    else:
        converted_results = results

    return pa.array(converted_results, type=arrow_type)


def _create_reduce_func(
    serialized_aggregator: bytes,
    max_key: int,
    num_grouping_cols: int,
):
    """Create reduce function for mapInArrow."""

    def reduce_func(iterator):
        import pyarrow as pa
        import cloudpickle
        import random

        agg = cloudpickle.loads(serialized_aggregator)
        group_buffers = {}
        value_col_idx = num_grouping_cols

        for batch in iterator:
            if (
                isinstance(batch, pa.RecordBatch)
                and batch.num_columns > value_col_idx
                and batch.num_rows > 0
            ):
                value_col = batch.column(value_col_idx)

                for row_idx in range(batch.num_rows):
                    # Extract grouping key (None for non-grouped case, tuple for grouped case)
                    grouping_key = (
                        tuple([batch.column(i)[row_idx].as_py() for i in range(num_grouping_cols)])
                        if num_grouping_cols > 0
                        else None
                    )

                    value = value_col[row_idx].as_py()

                    if grouping_key not in group_buffers:
                        group_buffers[grouping_key] = agg.zero()

                    if value is not None:
                        group_buffers[grouping_key] = agg.reduce(group_buffers[grouping_key], value)

        # Handle empty DataFrame case for non-grouped aggregation
        if not group_buffers and num_grouping_cols == 0:
            group_buffers[None] = agg.zero()

        # Yield one record per group with random key (always serialize as (grouping_key, buffer))
        for grouping_key, buffer in group_buffers.items():
            key = random.randint(0, max_key)
            grouping_key_bytes = cloudpickle.dumps((grouping_key, buffer))
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([key], type=pa.int64()),
                    pa.array([grouping_key_bytes], type=pa.binary()),
                ],
                ["key", "buffer"],
            )

    return reduce_func


def _create_merge_func(serialized_aggregator: bytes):
    """Create merge function for applyInArrow using iterator API."""
    import pyarrow as pa

    def merge_func(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Iterator-based merge function that processes batches one by one.

        Note: When called via FlatMapGroupsInArrow, batches contain only the value columns
        (key is handled separately by the executor). The buffer column is at index 0.
        When called via Python-only path (applyInArrow), batches contain all columns
        (key at 0, buffer at 1).
        """
        import cloudpickle

        agg = cloudpickle.loads(serialized_aggregator)
        group_buffers = {}

        for batch in batches:
            if isinstance(batch, pa.RecordBatch) and batch.num_columns > 0 and batch.num_rows > 0:
                # Buffer is at last column position (handles both 1-column and 2-column cases)
                buffer_col = batch.column(batch.num_columns - 1)
                for i in range(batch.num_rows):
                    buffer_bytes = buffer_col[i].as_py()
                    grouping_key, buffer_value = cloudpickle.loads(buffer_bytes)

                    if grouping_key not in group_buffers:
                        group_buffers[grouping_key] = buffer_value
                    else:
                        group_buffers[grouping_key] = agg.merge(
                            group_buffers[grouping_key], buffer_value
                        )

        # Yield merged buffers (always serialize as (grouping_key, buffer))
        for grouping_key, buffer in group_buffers.items():
            grouping_key_bytes = cloudpickle.dumps((grouping_key, buffer))
            yield pa.RecordBatch.from_arrays(
                [pa.array([grouping_key_bytes], type=pa.binary())], ["buffer"]
            )

    return merge_func


def _create_final_merge_func(
    serialized_aggregator: bytes,
    return_type: DataType,
    has_grouping: bool,
    grouping_col_names: List[str],
    udaf_func_name: str,
    col_name: str,
):
    """Create final merge function for applyInArrow using iterator API."""
    import pyarrow as pa
    import cloudpickle

    # Serialize return_type for use in worker
    serialized_return_type = cloudpickle.dumps(return_type)

    def _convert_results_to_arrow_local(results: List[Any], serialized_dt: bytes) -> Any:
        """Convert a list of result values to Arrow array based on return type."""
        from pyspark.sql.pandas.types import to_arrow_type
        from pyspark.sql.conversion import LocalDataToArrowConversion

        # Use DataType object for accurate conversion
        dt = cloudpickle.loads(serialized_dt)
        arrow_type = to_arrow_type(dt)
        converter = LocalDataToArrowConversion._create_converter(dt, nullable=True)

        if converter is not None:
            converted_results = [converter(r) for r in results]
        else:
            converted_results = results

        return pa.array(converted_results, type=arrow_type)

    def final_merge_func(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Iterator-based final merge function that processes batches one by one.

        Note: When called via FlatMapGroupsInArrow, batches contain only the value columns
        (key is handled separately by the executor). The buffer column is at index 0.
        When called via Python-only path (applyInArrow), batches contain all columns
        (final_key at 0, buffer at 1).
        """
        import cloudpickle

        agg = cloudpickle.loads(serialized_aggregator)

        # Unified logic: always deserialize as (grouping_key, buffer)
        # For non-grouped case, grouping_key is None
        group_results = {}
        has_data = False

        for batch in batches:
            if isinstance(batch, pa.RecordBatch) and batch.num_columns > 0:
                if batch.num_rows > 0:
                    has_data = True
                    # Buffer is at last column position (handles both 1-column and 2-column cases)
                    buffer_col = batch.column(batch.num_columns - 1)
                    for i in range(batch.num_rows):
                        buffer_bytes = buffer_col[i].as_py()
                        # Always deserialize as (grouping_key, buffer)
                        grouping_key, buffer_value = cloudpickle.loads(buffer_bytes)

                        if grouping_key not in group_results:
                            group_results[grouping_key] = buffer_value
                        else:
                            group_results[grouping_key] = agg.merge(
                                group_results[grouping_key], buffer_value
                            )

        # Finish each group and collect all results
        all_grouping_vals = []
        all_results = []

        for grouping_key, buffer in group_results.items():
            all_grouping_vals.append(grouping_key)
            all_results.append(agg.finish(buffer))

        # Handle empty case: for non-grouped, use zero() result; for grouped, keep empty
        # Unified handling: both cases go through the same array building logic
        if not has_data and not has_grouping:
            # Non-grouped empty case: return zero() result
            all_results = [agg.finish(agg.zero())]

        # Build result arrays - unified for both grouped and non-grouped cases
        result_arrays = []
        result_names = []

        # Add grouping columns (empty list for non-grouped case, so loop won't execute)
        for i in range(len(grouping_col_names)):
            col_values = [grouping_vals[i] for grouping_vals in all_grouping_vals]
            if col_values and isinstance(col_values[0], str):
                result_arrays.append(pa.array(col_values, type=pa.string()))
            else:
                result_arrays.append(pa.array(col_values, type=pa.int64()))
            result_names.append(grouping_col_names[i])

        # Add result column (always add, even if empty for grouped case)
        result_col_name_safe = f"{udaf_func_name}_{col_name}".replace("(", "_").replace(")", "_")
        result_arrays.append(_convert_results_to_arrow_local(all_results, serialized_return_type))
        result_names.append(result_col_name_safe if has_grouping else "result")

        yield pa.RecordBatch.from_arrays(result_arrays, result_names)

    return final_merge_func


def _build_result_schema(
    has_grouping: bool,
    grouping_col_names: List[str],
    result_col_name_safe: str,
    return_type_str: str,
) -> str:
    """Build schema string for final merge result."""
    if has_grouping:
        schema_parts = [f"{gc_name} string" for gc_name in grouping_col_names]
        schema_parts.append(f"{result_col_name_safe} {return_type_str}")
        return ", ".join(schema_parts)
    else:
        return f"result {return_type_str}"


def udaf(
    aggregator: Aggregator,
    returnType: "DataTypeOrString",
    name: Optional[str] = None,
) -> UserDefinedAggregateFunction:
    """
    Creates a user-defined aggregate function (UDAF) from an Aggregator instance.

    .. versionadded:: 4.2.0

    Parameters
    ----------
    aggregator : Aggregator
        An instance of Aggregator that implements the aggregation logic.
    returnType : :class:`pyspark.sql.types.DataType` or str
        The return type of the UDAF. Can be either a DataType object or a DDL-formatted string.
    name : str, optional
        Optional name for the UDAF. If not provided, uses the aggregator class name.

    Returns
    -------
    UserDefinedAggregateFunction
        A UserDefinedAggregateFunction that can be used in DataFrame operations.

    Examples
    --------
    >>> class MySum(Aggregator):
    ...     def zero(self):
    ...         return 0
    ...     def reduce(self, buffer, value):
    ...         return buffer + value
    ...     def merge(self, buffer1, buffer2):
    ...         return buffer1 + buffer2
    ...     def finish(self, reduction):
    ...         return reduction
    ...
    >>> sum_udaf = udaf(MySum(), "bigint")
    >>> df = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
    >>> df.agg(sum_udaf(df.value)).show()
    +------------+
    |MySum(value)|
    +------------+
    |           6|
    +------------+
    """
    return UserDefinedAggregateFunction(aggregator, returnType, name)
