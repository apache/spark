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

"""Handlers for the user-defined table function (UDTF) eval types, and the wrappers that
implement TABLE argument PARTITION BY semantics on top of a UDTF instance.

Each handler is assigned the single ``UDTF`` record ``assign_udtf`` builds, which holds
the instantiated UDTF.

pyarrow is imported lazily (inside the handlers and the type-checking block) so the module
stays importable and its handlers register without pyarrow installed.
"""

from __future__ import annotations

import dataclasses
import inspect
import itertools
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, Tuple

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import BatchEvalTypeHandler, EvalTypeHandler
from pyspark.eval_handlers._typing import UDTF, InputBatch, OutputBatch, UDTFInstance
from pyspark.eval_handlers.utils import wrap_kwargs_support
from pyspark.eval_handlers.verification import verify_arrow_result, verify_pandas_result
from pyspark.serializers import (
    BatchedSerializer,
    CPickleSerializer,
    Serializer,
)
from pyspark.sql.conversion import (
    ArrowBatchTransformer,
    ArrowTableToRowsConversion,
    ArrowToPandasConversion,
    LocalDataToArrowConversion,
    PandasToArrowConversion,
)
from pyspark.sql.functions import SkipRestOfInputTableException
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    Row,
    StructField,
    StructType,
    _create_row,
)
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


class PartitionedUDTF(metaclass=ABCMeta):
    """
    Base for the wrappers that implement TABLE argument PARTITION BY semantics on top of a UDTF.

    A wrapper owns one UDTF instance per partition: when ``eval`` sees a new partition it calls
    ``terminate`` on the current instance and replaces it with a fresh one. A
    ``SkipRestOfInputTableException`` from ``eval`` skips the rest of the current partition only.
    """

    def __init__(self, create_udtf: Callable, partition_child_indexes: list):
        self._create_udtf: Callable = create_udtf
        self._udtf = create_udtf()
        self._partition_child_indexes: list = partition_child_indexes
        self._eval_raised_skip_rest_of_input_table: bool = False

    @abstractmethod
    def eval(self, *args: Any, **kwargs: Any) -> Iterator:
        """Evaluate the input, calling the per-partition UDTF once per partition it covers."""

    def terminate(self) -> Iterator:
        if hasattr(self._udtf, "terminate"):
            return self._udtf.terminate()
        return iter(())

    def cleanup(self) -> None:
        if hasattr(self._udtf, "cleanup"):
            self._udtf.cleanup()

    def _start_next_partition(self) -> Iterator:
        """Terminate the current partition's UDTF, yielding its results, and start a new one."""
        if hasattr(self._udtf, "terminate"):
            result = self._udtf.terminate()
            if result is not None:
                yield from result
        self._udtf = self._create_udtf()
        self._eval_raised_skip_rest_of_input_table = False

    def _eval_partition(self, *args: Any, **kwargs: Any) -> Iterator:
        """Call the current partition's UDTF ``eval``, yielding its results."""
        try:
            result = self._udtf.eval(*args, **kwargs)
            if result is not None:
                yield from result
        except SkipRestOfInputTableException:
            # Skip the rest of the rows in the current partition: callers stop calling 'eval'
            # until they see a change in the partition boundaries.
            self._eval_raised_skip_rest_of_input_table = True


class UDTFWithPartitions(PartitionedUDTF):
    """
    This implements the logic of a UDTF that accepts an input TABLE argument with one or more
    PARTITION BY expressions.

    For example, let's assume we have a table like:
        CREATE TABLE t (c1 INT, c2 INT) USING delta;
    Then for the following queries:
        SELECT * FROM my_udtf(TABLE (t) PARTITION BY c1, c2);
        The partition_child_indexes will be: 0, 1.
        SELECT * FROM my_udtf(TABLE (t) PARTITION BY c1, c2 + 4);
        The partition_child_indexes will be: 0, 2 (where we add a projection for "c2 + 4").
    """

    def __init__(self, create_udtf: Callable, partition_child_indexes: list):
        """
        Creates a new instance of this class to wrap the provided UDTF with another one that
        checks the values of projected partitioning expressions on consecutive rows to figure
        out when the partition boundaries change.

        Parameters
        ----------
        create_udtf: function
            Function to create a new instance of the UDTF to be invoked.
        partition_child_indexes: list
            List of integers identifying zero-based indexes of the columns of the input table
            that contain projected partitioning expressions. This class will inspect these
            values for each pair of consecutive input rows. When they change, this indicates
            the boundary between two partitions, and we will invoke the 'terminate' method on
            the UDTF class instance and then destroy it and create a new one to implement the
            desired partitioning semantics.
        """
        super().__init__(create_udtf, partition_child_indexes)
        self._prev_arguments: list = list()

    def eval(self, *args: Any, **kwargs: Any) -> Iterator:
        changed_partitions = self._check_partition_boundaries(list(args) + list(kwargs.values()))
        if changed_partitions:
            yield from self._start_next_partition()
        if self._udtf.eval is not None and not self._eval_raised_skip_rest_of_input_table:
            # Filter the arguments to exclude projected PARTITION BY values added by Catalyst.
            filtered_args = [self._remove_partition_by_exprs(arg) for arg in args]
            filtered_kwargs = {
                key: self._remove_partition_by_exprs(value) for (key, value) in kwargs.items()
            }
            yield from self._eval_partition(*filtered_args, **filtered_kwargs)

    def _check_partition_boundaries(self, arguments: list) -> bool:
        result = False
        if len(self._prev_arguments) > 0:
            cur_table_arg = self._get_table_arg(arguments)
            prev_table_arg = self._get_table_arg(self._prev_arguments)
            cur_partitions_args = []
            prev_partitions_args = []
            for i in self._partition_child_indexes:
                cur_partitions_args.append(cur_table_arg[i])
                prev_partitions_args.append(prev_table_arg[i])
            result = any(k != v for k, v in zip(cur_partitions_args, prev_partitions_args))
        self._prev_arguments = arguments
        return result

    def _get_table_arg(self, inputs: list) -> Row:
        return [x for x in inputs if type(x) is Row][0]

    def _remove_partition_by_exprs(self, arg: Any) -> Any:
        if isinstance(arg, Row):
            new_row_keys = []
            new_row_values = []
            for i, (key, value) in enumerate(zip(arg.__fields__, arg)):
                if i not in self._partition_child_indexes:
                    new_row_keys.append(key)
                    new_row_values.append(value)
            return _create_row(new_row_keys, new_row_values)
        else:
            return arg


class ArrowUDTFWithPartition(PartitionedUDTF):
    """
    Implements logic for an Arrow UDTF (SQL_ARROW_UDTF) that accepts a TABLE argument
    with one or more PARTITION BY expressions.

    Arrow UDTFs receive data as PyArrow RecordBatch objects instead of individual Row
    objects. This wrapper ensures the UDTF's eval() method is called separately for each
    unique partition key value combination.

    How Catalyst handles PARTITION BY and ORDER BY:
    ------------------------------------------------
    When a UDTF is called with PARTITION BY and/or ORDER BY clauses, Catalyst adds
    operations to the physical plan to ensure correct data organization:

    Example SQL:
        SELECT * FROM my_udtf(TABLE(t) PARTITION BY key1, key2 ORDER BY value DESC)

    Physical Plan generated by Catalyst:
        1. Project: Adds partition_by_0 = key1, partition_by_1 = key2 columns
        2. Exchange: hashpartitioning(partition_by_0, partition_by_1, 200)
           - Shuffles data so rows with same partition keys go to same worker
        3. Sort: [partition_by_0 ASC, partition_by_1 ASC, value DESC], local=true
           - First sorts by partition keys to group them together
           - Then sorts by ORDER BY expressions within each partition
           - Local sort (not global) within each worker's data
        4. Project: Creates struct with all columns including partition_by_* columns
        5. ArrowEvalPythonUDTF: Executes this Python UDTF wrapper

    Key guarantee: After the Sort operation, all rows with the same partition key
    values are contiguous within each RecordBatch, allowing efficient boundary detection.

    Example queries:
        SELECT * FROM my_udtf(TABLE (t) PARTITION BY c1);
        partition_child_indexes: [2] (refers to partition_by_0 column at index 2)

        SELECT * FROM my_udtf(TABLE (t) PARTITION BY c1, c2);
        partition_child_indexes: [2, 3] (partition_by_0 and partition_by_1 columns)

        SELECT * FROM my_udtf(TABLE (t) PARTITION BY c1, c2 + 4);
        partition_child_indexes: 0, 2 (adds a projection for "c2 + 4").
    """

    def __init__(self, create_udtf: Callable, partition_child_indexes: list):
        """
        Create a new instance that wraps the provided Arrow UDTF with partitioning
        logic.

        Parameters
        ----------
        create_udtf: function
            Function that creates a new instance of the Arrow UDTF to invoke.
        partition_child_indexes: list
            Zero-based indexes of input-table columns that contain projected
            partitioning expressions.
        """
        super().__init__(create_udtf, partition_child_indexes)
        # Track last partition key from previous batch
        self._last_partition_key: Optional[Tuple[Any, ...]] = None

    def eval(self, *args: Any, **kwargs: Any) -> Iterator:
        """Handle partitioning logic for Arrow UDTFs that receive RecordBatch objects."""
        import pyarrow as pa

        # Get the original batch with partition columns
        original_batch = self._get_table_arg(list(args) + list(kwargs.values()))
        if not isinstance(original_batch, pa.RecordBatch):
            # Arrow UDTFs with PARTITION BY must have a TABLE argument that
            # results in a PyArrow RecordBatch
            raise PySparkRuntimeError(
                errorClass="INVALID_ARROW_UDTF_TABLE_ARGUMENT",
                messageParameters={
                    "actual_type": (
                        str(type(original_batch)) if original_batch is not None else "None"
                    )
                },
            )

        # Remove partition columns to get the filtered arguments
        filtered_args = [self._remove_partition_by_exprs(arg) for arg in args]
        filtered_kwargs = {
            key: self._remove_partition_by_exprs(value) for (key, value) in kwargs.items()
        }

        # Get the filtered RecordBatch (without partition columns)
        filtered_batch = self._get_table_arg(filtered_args + list(filtered_kwargs.values()))

        # Process the RecordBatch by partitions
        yield from self._process_arrow_batch_by_partitions(
            original_batch, filtered_batch, filtered_args, filtered_kwargs
        )

    def _process_arrow_batch_by_partitions(
        self,
        original_batch: pa.RecordBatch,
        filtered_batch: pa.RecordBatch,
        filtered_args: list,
        filtered_kwargs: dict,
    ) -> Iterator:
        """Process an Arrow RecordBatch that may contain multiple partition key values.

        When using PARTITION BY with Arrow UDTFs, a single RecordBatch from Spark may contain
        rows with different partition key values. For example, with 10 distinct partition keys
        and 2 workers, each worker might receive a batch containing 5 different partition key
        values.

        According to UDTF PARTITION BY semantics, the UDTF's eval() method must be called
        separately for each unique partition key value, not for the entire batch. This method
        handles splitting the batch by partition boundaries and calling the UDTF appropriately.

        The implementation leverages two key properties:
        1. Catalyst guarantees rows with the same partition key are contiguous (pre-sorted)
        2. Arrow's columnar format allows efficient boundary detection

        Parameters:
        -----------
        original_batch : pa.RecordBatch
            The original batch including partition columns, used for detecting boundaries
        filtered_batch : pa.RecordBatch
            The batch with partition columns removed, to be passed to the UDTF
        filtered_args : list
            Arguments with partition columns filtered out
        filtered_kwargs : dict
            Keyword arguments with partition columns filtered out

        Yields:
        -------
        Iterator of pa.Table objects returned by the UDTF's eval() method
        """
        import pyarrow as pa

        # This class should only be used when partition_child_indexes is non-empty
        assert self._partition_child_indexes, (
            "ArrowUDTFWithPartition should only be instantiated when "
            "len(partition_child_indexes) > 0"
        )

        # Detect partition boundaries.
        boundaries = self._detect_partition_boundaries(original_batch)

        # Process each contiguous partition
        for i in range(len(boundaries) - 1):
            start_idx = boundaries[i]
            end_idx = boundaries[i + 1]

            # Get the partition key for this segment
            partition_key = tuple(
                original_batch.column(idx)[start_idx].as_py()
                for idx in self._partition_child_indexes
            )

            # Check if this is a continuation of the previous batch's partition
            # TODO: This check is only necessary for the first boundary in each batch.
            # The following boundaries are always for new partitions within the same batch.
            # This could be optimized by only checking i == 0.
            is_new_partition = (
                self._last_partition_key is not None and partition_key != self._last_partition_key
            )

            if is_new_partition:
                # Previous partition ended: terminate it and start a new UDTF instance
                yield from self._start_next_partition()

            # Slice the filtered batch for this partition
            partition_batch = filtered_batch.slice(start_idx, end_idx - start_idx)

            # Update the last partition key
            self._last_partition_key = partition_key

            # Update filtered args to use the partition batch
            partition_filtered_args = []
            for arg in filtered_args:
                if isinstance(arg, pa.RecordBatch):
                    partition_filtered_args.append(partition_batch)
                else:
                    partition_filtered_args.append(arg)

            partition_filtered_kwargs = {}
            for key, value in filtered_kwargs.items():
                if isinstance(value, pa.RecordBatch):
                    partition_filtered_kwargs[key] = partition_batch
                else:
                    partition_filtered_kwargs[key] = value

            # Call the UDTF with this partition's data
            if not self._eval_raised_skip_rest_of_input_table:
                yield from self._eval_partition(
                    *partition_filtered_args, **partition_filtered_kwargs
                )

        # Don't terminate here - let the next batch or final terminate handle it

    def _get_table_arg(self, inputs: list) -> Optional[pa.RecordBatch]:
        """Get the table argument (RecordBatch) from the inputs list.

        For Arrow UDTFs with TABLE arguments, we can guarantee the table argument
        will be a pa.RecordBatch, not a Row.
        """
        import pyarrow as pa

        # Find all RecordBatch arguments
        batches = [arg for arg in inputs if isinstance(arg, pa.RecordBatch)]

        if len(batches) == 0:
            # No RecordBatch found - this shouldn't happen for Arrow UDTFs with TABLE arguments
            return None
        elif len(batches) == 1:
            return batches[0]
        else:
            # Multiple RecordBatch arguments found - this is unexpected
            raise RuntimeError(
                f"Expected exactly one pa.RecordBatch argument for TABLE parameter, "
                f"but found {len(batches)}. Received types: "
                f"{[type(arg).__name__ for arg in inputs]}"
            )

    def _detect_partition_boundaries(self, batch: pa.RecordBatch) -> list:
        """
        Efficiently detect partition boundaries in a batch with contiguous partitions.

        Since Catalyst ensures rows with the same partition key are contiguous,
        we only need to find where partition values change.

        Returns:
            List of indices where each partition starts, plus the total row count.
            For example: [0, 3, 8, 10] means partitions are rows [0:3), [3:8), [8:10)
        """
        boundaries = [0]  # First partition starts at index 0

        if batch.num_rows <= 1:
            boundaries.append(batch.num_rows)
            return boundaries

        # Get partition column arrays
        partition_arrays = [batch.column(i) for i in self._partition_child_indexes]

        # Find boundaries by comparing consecutive rows
        for row_idx in range(1, batch.num_rows):
            # Check if any partition column changed from previous row
            partition_changed = False
            for col_array in partition_arrays:
                if col_array[row_idx].as_py() != col_array[row_idx - 1].as_py():
                    partition_changed = True
                    break

            if partition_changed:
                boundaries.append(row_idx)

        boundaries.append(batch.num_rows)  # Last boundary at end
        return boundaries

    def _remove_partition_by_exprs(self, arg: Any) -> Any:
        """
        Remove partition columns from the RecordBatch argument.

        Why this is needed:
        When a UDTF is called with TABLE(t) PARTITION BY expressions, Catalyst transforms
        the data:
        1. Adds complex partition expressions as new columns
           (e.g., "c2 + 4" becomes a new column)
        2. Repartitions data by partition columns using hash partitioning
        3. Sends ALL columns (including partition columns) to the Python worker

        Partition columns serve two purposes:
        - Routing: decide which worker processes which partition
        - Boundary detection: know when one partition ends and another begins

        However, the user's UDTF should only receive the actual table data, not the
        partition columns. This method filters out partition columns before passing
        data to the user's UDTF eval() method.

        Example:
        - User writes: SELECT * FROM udtf(TABLE(t) PARTITION BY c1, c2)
        - Catalyst sends: RecordBatch with [c1, c2, c3, c4],
          partition_child_indexes=[0, 1]
        - This method removes columns at indexes 0, 1 if they are pure partition columns
        - UDTF.eval() receives: RecordBatch with only the non-partition columns
        """
        import pyarrow as pa

        if isinstance(arg, pa.RecordBatch):
            # Remove partition columns from the RecordBatch
            keep_indices = [
                i for i in range(len(arg.schema.names)) if i not in self._partition_child_indexes
            ]
            if keep_indices:
                # Select only the columns we want to keep
                keep_arrays = [arg.column(i) for i in keep_indices]
                keep_names = [arg.schema.names[i] for i in keep_indices]
                return pa.RecordBatch.from_arrays(keep_arrays, names=keep_names)
            else:
                # If no columns remain, return an empty RecordBatch with the same number of rows
                return pa.RecordBatch.from_arrays([], schema=pa.schema([]), num_rows=arg.num_rows)

        # For non-RecordBatch arguments (like scalar pa.Arrays), return unchanged
        return arg


def build_null_checker(return_type: StructType) -> Optional[Callable[[Any], None]]:
    def raise_(result_column_index: int) -> None:
        raise PySparkRuntimeError(
            errorClass="UDTF_EXEC_ERROR",
            messageParameters={
                "method_name": "eval' or 'terminate",
                "error": f"Column {result_column_index} within a returned row had a "
                + "value of None, either directly or within array/struct/map "
                + "subfields, but the corresponding column type was declared as "
                + "non-nullable; please update the UDTF to return a non-None value at "
                + "this location or otherwise declare the column type as nullable.",
            },
        )

    def checker(data_type: DataType, result_column_index: int) -> Optional[Callable[[Any], None]]:
        if isinstance(data_type, ArrayType):
            element_checker = checker(data_type.elementType, result_column_index)
            contains_null = data_type.containsNull

            if element_checker is None and contains_null:
                return None

            def check_array(arr: Any) -> None:
                if isinstance(arr, list):
                    for e in arr:
                        if e is None:
                            if not contains_null:
                                raise_(result_column_index)
                        elif element_checker is not None:
                            element_checker(e)

            return check_array

        elif isinstance(data_type, MapType):
            key_checker = checker(data_type.keyType, result_column_index)
            value_checker = checker(data_type.valueType, result_column_index)
            value_contains_null = data_type.valueContainsNull

            if value_checker is None and value_contains_null:

                def check_map(map: Any) -> None:
                    if isinstance(map, dict):
                        for k, v in map.items():
                            if k is None:
                                raise_(result_column_index)
                            elif key_checker is not None:
                                key_checker(k)

            else:

                def check_map(map: Any) -> None:
                    if isinstance(map, dict):
                        for k, v in map.items():
                            if k is None:
                                raise_(result_column_index)
                            elif key_checker is not None:
                                key_checker(k)
                            if v is None:
                                if not value_contains_null:
                                    raise_(result_column_index)
                            elif value_checker is not None:
                                value_checker(v)

            return check_map

        elif isinstance(data_type, StructType):
            field_checkers = [checker(f.dataType, result_column_index) for f in data_type]
            nullables = [f.nullable for f in data_type]

            if all(c is None for c in field_checkers) and all(nullables):
                return None

            def check_struct(struct: Any) -> None:
                if isinstance(struct, tuple):
                    for value, checker, nullable in zip(struct, field_checkers, nullables):
                        if value is None:
                            if not nullable:
                                raise_(result_column_index)
                        elif checker is not None:
                            checker(value)

            return check_struct

        else:
            return None

    field_checkers = [checker(f.dataType, result_column_index=i) for i, f in enumerate(return_type)]
    nullables = [f.nullable for f in return_type]

    if all(c is None for c in field_checkers) and all(nullables):
        return None

    def check(row: Any) -> None:
        if isinstance(row, tuple):
            for i, (value, checker, nullable) in enumerate(zip(row, field_checkers, nullables)):
                if value is None:
                    if not nullable:
                        raise_(i)
                elif checker is not None:
                    checker(value)

    return check


class UDTFHandlerBase(EvalTypeHandler[InputBatch, OutputBatch]):
    """Base for every UDTF eval type handler; ``assign_udtf`` assigns the UDTF to the handler."""

    # Wraps the UDTF when its TABLE argument has PARTITION BY expressions.
    partition_wrapper: ClassVar[type[PartitionedUDTF]] = UDTFWithPartitions

    def assign_udtf(self, udtf: UDTF) -> None:
        """Assign the UDTF to this handler: keep the record as ``_udfs[0]`` and, as ``_udtf``, the
        instance of ``udtf.udtf_cls`` (wrapped in ``partition_wrapper`` when its TABLE argument
        has PARTITION BY expressions, its constructor given the AnalyzeResult when it takes one)
        after checking the 'eval' signature against the call arguments, then set the handler up
        for it."""
        udtf_cls = udtf.udtf_cls
        if not isinstance(udtf_cls, type):
            raise PySparkRuntimeError(
                f"Invalid UDTF handler type. Expected a class (type 'type'), but "
                f"got an instance of {type(udtf_cls).__name__}."
            )
        analyze_result = udtf.analyze_result

        # Update the function that creates a new UDTF instance to first try calling the UDTF
        # constructor with one argument containing the previous AnalyzeResult. If that fails, then
        # try a constructor with no arguments. In this way each UDTF class instance can decide if
        # it wants to inspect the AnalyzeResult.
        handler: Callable[[], Any] = udtf_cls
        udtf_init_args = inspect.getfullargspec(udtf_cls)
        if analyze_result is not None:
            if len(udtf_init_args.args) > 2:
                raise PySparkRuntimeError(
                    errorClass="UDTF_CONSTRUCTOR_INVALID_IMPLEMENTS_ANALYZE_METHOD",
                    messageParameters={"name": udtf.name},
                )
            elif len(udtf_init_args.args) == 2:

                def construct_udtf() -> Any:
                    # Here we pass the AnalyzeResult to the UDTF's __init__ method.
                    return udtf_cls(dataclasses.replace(analyze_result))

                handler = construct_udtf
        elif len(udtf_init_args.args) > 1:
            raise PySparkRuntimeError(
                errorClass="UDTF_CONSTRUCTOR_INVALID_NO_ANALYZE_METHOD",
                messageParameters={"name": udtf.name},
            )

        # Instantiate the UDTF class.
        try:
            if len(udtf.partition_child_indexes) > 0:
                instance = self.partition_wrapper(handler, udtf.partition_child_indexes)
            else:
                instance = handler()
        except Exception as e:
            raise PySparkRuntimeError(
                errorClass="UDTF_EXEC_ERROR",
                messageParameters={"method_name": "__init__", "error": str(e)},
            )

        # Validate the UDTF
        if not hasattr(instance, "eval"):
            raise PySparkRuntimeError(
                "Failed to execute the user defined table function because it has not "
                "implemented the 'eval' method. Please add the 'eval' method and try "
                "the query again."
            )

        # Check that the arguments provided to the UDTF call match the expected parameters
        # defined in the 'eval' method signature.
        try:
            inspect.signature(instance.eval).bind(*udtf.args_offsets, **udtf.kwargs_offsets)
        except TypeError as e:
            raise PySparkRuntimeError(
                errorClass="UDTF_EVAL_METHOD_ARGUMENTS_DO_NOT_MATCH_SIGNATURE",
                messageParameters={"name": udtf.name, "reason": str(e)},
            ) from None

        self._udfs = [udtf]
        self._udtf: UDTFInstance = instance
        self._setup_udfs(self._udfs)


class UDTFEvalTypeHandler(UDTFHandlerBase[InputBatch, OutputBatch]):
    """Base for the Arrow UDTF eval types.

    ``run`` calls ``_eval_batch`` per input batch, then ``_eval_terminate`` once at the end (also
    when ``eval`` raised ``SkipRestOfInputTableException``), and always calls the UDTF's
    ``cleanup``. Subclasses implement the two hooks and pick the serializer.
    """

    def _setup_udfs(self, udfs: list[UDTF]) -> None:
        assert len(udfs) == 1, "One UDTF expected here."
        udtf = self._udtf
        self._return_type = udfs[0].return_type
        self._eval_method, self._args_kwargs_offsets = wrap_kwargs_support(
            udtf.eval, udfs[0].args_offsets, udfs[0].kwargs_offsets
        )
        self._terminate_method = getattr(udtf, "terminate", None)
        self._cleanup_method = getattr(udtf, "cleanup", None)

    @abstractmethod
    def _eval_batch(self, batch: InputBatch) -> Iterator[OutputBatch]:
        """Call the UDTF's ``eval`` on one input batch and yield the converted results."""

    @abstractmethod
    def _eval_terminate(self) -> Iterator[OutputBatch]:
        """Call the UDTF's ``terminate`` and yield the converted results."""

    def run(self, split_index: int, data: Iterator[InputBatch]) -> Iterator[OutputBatch]:
        terminate = self._terminate_method
        cleanup = self._cleanup_method
        try:
            for batch in data:
                yield from self._eval_batch(batch)
            if terminate is not None:
                yield from self._eval_terminate()
        except SkipRestOfInputTableException:
            if terminate is not None:
                yield from self._eval_terminate()
        finally:
            if cleanup is not None:
                cleanup()


class TableUDTFHandler(UDTFHandlerBase[Any, tuple]):
    """SQL_TABLE_UDF, the Python UDTF: input rows arrive pickled one at a time and the UDTF's
    ``eval`` is called once per row; the results of each call are converted to the internal
    representation and returned to the executor as one tuple."""

    eval_type = PythonEvalType.SQL_TABLE_UDF

    def _setup_udfs(self, udfs: list[UDTF]) -> None:
        assert len(udfs) == 1, "One UDTF expected here."
        udtf, return_type = self._udtf, udfs[0].return_type
        self._check_output_row_against_schema = build_null_checker(return_type)

        eval_func_kwargs_support, self._args_kwargs_offsets = wrap_kwargs_support(
            udtf.eval, udfs[0].args_offsets, udfs[0].kwargs_offsets
        )
        self._eval = self._wrap_udtf(eval_func_kwargs_support, return_type)

        self._terminate: Optional[Callable[..., tuple]]
        if hasattr(udtf, "terminate"):
            self._terminate = self._wrap_udtf(getattr(udtf, "terminate"), return_type)
        else:
            self._terminate = None

        self._cleanup = getattr(udtf, "cleanup") if hasattr(udtf, "cleanup") else None

    @property
    def serializer(self) -> Serializer:
        # Each row is a group so do not batch but send one by one.
        return BatchedSerializer(CPickleSerializer(), 1)

    def _wrap_udtf(self, f: Callable, return_type: StructType) -> Callable[..., tuple]:
        assert return_type.needConversion()
        toInternal = return_type.toInternal
        return_type_size = len(return_type)
        check_output_row_against_schema = self._check_output_row_against_schema

        def verify_and_convert_result(result: Any) -> Any:
            if result is not None:
                if hasattr(result, "__UDT__"):
                    # UDT object should not be returned directly.
                    raise PySparkRuntimeError(
                        errorClass="UDTF_INVALID_OUTPUT_ROW_TYPE",
                        messageParameters={
                            "type": type(result).__name__,
                            "func": f.__name__,
                        },
                    )

                if hasattr(result, "__len__") and len(result) != return_type_size:
                    raise PySparkRuntimeError(
                        errorClass="UDTF_RETURN_SCHEMA_MISMATCH",
                        messageParameters={
                            "expected": str(return_type_size),
                            "actual": str(len(result)),
                            "func": f.__name__,
                        },
                    )

                if not (isinstance(result, (list, dict, tuple)) or hasattr(result, "__dict__")):
                    raise PySparkRuntimeError(
                        errorClass="UDTF_INVALID_OUTPUT_ROW_TYPE",
                        messageParameters={
                            "type": type(result).__name__,
                            "func": f.__name__,
                        },
                    )
                if check_output_row_against_schema is not None:
                    check_output_row_against_schema(result)
            return toInternal(result)

        # Evaluate the function and return a tuple back to the executor.
        def evaluate(*a: Any) -> tuple:
            try:
                res = f(*a)
            except SkipRestOfInputTableException:
                raise
            except Exception as e:
                raise PySparkRuntimeError(
                    errorClass="UDTF_EXEC_ERROR",
                    messageParameters={"method_name": f.__name__, "error": str(e)},
                )

            if res is None:
                # If the function returns None or does not have an explicit return statement,
                # an empty tuple is returned to the executor.
                # This is because directly constructing tuple(None) results in an exception.
                return tuple()

            if not isinstance(res, Iterable):
                raise PySparkRuntimeError(
                    errorClass="UDTF_RETURN_NOT_ITERABLE",
                    messageParameters={
                        "type": type(res).__name__,
                        "func": f.__name__,
                    },
                )

            # If the function returns a result, we map it to the internal representation and
            # returns the results as a tuple.
            return tuple(map(verify_and_convert_result, res))

        return evaluate

    def run(self, split_index: int, data: Iterator[Any]) -> Iterator[tuple]:
        eval = self._eval
        terminate = self._terminate
        cleanup = self._cleanup
        args_kwargs_offsets = self._args_kwargs_offsets
        try:
            for a in data:
                yield eval(*[a[o] for o in args_kwargs_offsets])
            if terminate is not None:
                yield terminate()
        except SkipRestOfInputTableException:
            if terminate is not None:
                yield terminate()
        finally:
            if cleanup is not None:
                cleanup()


class ArrowUDTFHandler(
    BatchEvalTypeHandler["pa.RecordBatch"],
    UDTFEvalTypeHandler["pa.RecordBatch", "pa.RecordBatch"],
):
    """SQL_ARROW_UDTF: the UDTF's ``eval`` receives ``pa.Array`` arguments, with TABLE
    arguments flattened into ``pa.RecordBatch``, and returns an iterable of
    ``pa.Table``/``pa.RecordBatch``, coerced to the declared schema."""

    eval_type = PythonEvalType.SQL_ARROW_UDTF
    partition_wrapper = ArrowUDTFWithPartition

    def _setup_udfs(self, udfs: list[UDTF]) -> None:
        import pyarrow as pa

        super()._setup_udfs(udfs)
        arrow_return_type = to_arrow_type(
            self._return_type,
            timezone="UTC",
            prefers_large_types=self._runner_conf.use_large_var_types,
        )
        self._return_type_size = len(self._return_type)
        self._target_schema = pa.schema(list(arrow_return_type))

        self._table_arg_offsets = (
            set(self._eval_conf.table_arg_offsets) if self._eval_conf.table_arg_offsets else set()
        )

    def _verify_result(self, result: pa.RecordBatch, method_name: str) -> pa.RecordBatch:
        # Validate the output schema when the result has columns
        if result.num_columns != self._return_type_size:
            raise PySparkRuntimeError(
                errorClass="UDTF_RETURN_SCHEMA_MISMATCH",
                messageParameters={
                    "expected": str(self._return_type_size),
                    "actual": str(result.num_columns),
                    "func": method_name,
                },
            )
        return result

    def _convert_to_arrow(self, res: Any, method_name: str) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        # Check whether the result of a PyArrow UDTF is iterable before processing
        if res is None:
            res = iter([])
        elif not isinstance(res, Iterable):
            raise PySparkRuntimeError(
                errorClass="UDTF_RETURN_NOT_ITERABLE",
                messageParameters={
                    "type": type(res).__name__,
                    "func": method_name,
                },
            )

        # Handle PyArrow Tables/RecordBatches directly
        is_empty = True
        for item in res:
            is_empty = False
            if isinstance(item, pa.Table):
                yield from item.to_batches()
            elif isinstance(item, pa.RecordBatch):
                yield item
            else:
                # Arrow UDTF should only return Arrow types (RecordBatch/Table)
                raise PySparkRuntimeError(
                    errorClass="UDTF_ARROW_TYPE_CONVERSION_ERROR",
                    messageParameters={},
                )

        if is_empty:
            yield pa.RecordBatch.from_pylist([], schema=self._target_schema)

    def _evaluate(self, method: Callable, *args: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        # Wrap the exception thrown from the UDTF in a PySparkRuntimeError.
        try:
            res = method(*args)
        except SkipRestOfInputTableException:
            raise
        except Exception as e:
            raise PySparkRuntimeError(
                errorClass="UDTF_EXEC_ERROR",
                messageParameters={"method_name": method.__name__, "error": str(e)},
            )

        for batch in self._convert_to_arrow(res, method.__name__):
            coerced = ArrowBatchTransformer.enforce_schema(
                self._verify_result(batch, method.__name__), self._target_schema, safecheck=True
            )
            yield ArrowBatchTransformer.wrap_struct(coerced)

    def _eval_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        # Pre-processing: for each column, flatten struct columns at
        # table_arg_offsets into RecordBatch, keep other columns as Array.
        table_arg_offsets = self._table_arg_offsets
        columns = [
            (
                ArrowBatchTransformer.flatten_struct(batch, column_index=i)
                if i in table_arg_offsets
                else batch.column(i)
            )
            for i in range(batch.num_columns)
        ]
        # For PyArrow UDTFs, pass RecordBatches directly (no row conversion needed)
        yield from self._evaluate(
            self._eval_method, *[columns[o] for o in self._args_kwargs_offsets]
        )

    def _eval_terminate(self) -> Iterator[pa.RecordBatch]:
        assert self._terminate_method is not None
        yield from self._evaluate(self._terminate_method)


class ArrowTableUDTFHandler(
    BatchEvalTypeHandler["pa.RecordBatch"],
    UDTFEvalTypeHandler["pa.RecordBatch", "pa.RecordBatch"],
):
    """Base for SQL_ARROW_TABLE_UDF, the Arrow-optimized Python UDTF: each input RecordBatch is
    converted to per-column values and the UDTF's ``eval`` is called once per input row. The
    variants differ in how the values are converted, selected by
    ``spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled``. Subclasses implement
    ``_to_columns`` and ``_convert_result``."""

    def _setup_udfs(self, udfs: list[UDTF]) -> None:
        super()._setup_udfs(udfs)
        self._return_type_size = len(self._return_type)
        self._check_output_row_against_schema = build_null_checker(self._return_type)

    @abstractmethod
    def _to_columns(self, batch: pa.RecordBatch) -> list:
        """Convert the input batch into one sequence of per-row values per column."""

    @abstractmethod
    def _convert_result(self, res: Any, method_name: str) -> Iterator[pa.RecordBatch]:
        """Convert the result of one ``eval``/``terminate`` call into output batches."""

    def _evaluate_rows(
        self, method: Callable, *args: list, num_rows: int = 1
    ) -> Iterator[pa.RecordBatch]:
        rows = itertools.repeat((), num_rows) if len(args) == 0 else zip(*args)
        for row in rows:
            # Wrap the exception thrown from the UDTF in a PySparkRuntimeError.
            try:
                res = method(*row)
            except SkipRestOfInputTableException:
                raise
            except Exception as e:
                raise PySparkRuntimeError(
                    errorClass="UDTF_EXEC_ERROR",
                    messageParameters={"method_name": method.__name__, "error": str(e)},
                )
            yield from self._convert_result(res, method.__name__)

    def _eval_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        columns = self._to_columns(batch)
        yield from self._evaluate_rows(
            self._eval_method,
            *[columns[o] for o in self._args_kwargs_offsets],
            num_rows=batch.num_rows,
        )

    def _eval_terminate(self) -> Iterator[pa.RecordBatch]:
        assert self._terminate_method is not None
        yield from self._evaluate_rows(self._terminate_method)


class ArrowTableUDTFLegacyPandasHandler(ArrowTableUDTFHandler):
    """SQL_ARROW_TABLE_UDF with the legacy pandas conversion: the input is converted to pandas
    Series, and each ``eval`` result is collected into a ``pd.DataFrame`` and converted back to
    Arrow as a single ``_0`` struct column."""

    eval_type = PythonEvalType.SQL_ARROW_TABLE_UDF
    variant_conf = "use_legacy_pandas_udtf_conversion"
    variant_value = True

    def _setup_udfs(self, udfs: list[UDTF]) -> None:
        import pandas  # noqa: F401

        super()._setup_udfs(udfs)
        # The output pandas DataFrame is converted as a single struct column named
        # "_0" against this schema.
        self._output_schema = StructType([StructField("_0", self._return_type)])

    def _verify_result(self, result: Any, method_name: str) -> Any:
        import pandas as pd

        if not isinstance(result, pd.DataFrame):
            raise PySparkTypeError(
                errorClass="INVALID_ARROW_UDTF_RETURN_TYPE",
                messageParameters={
                    "return_type": type(result).__name__,
                    "value": str(result),
                    "func": method_name,
                },
            )

        # Validate the output schema when the result dataframe has either output
        # rows or columns. Note that we avoid using `df.empty` here because the
        # result dataframe may contain an empty row. For example, when a UDTF is
        # defined as follows: def eval(self): yield tuple().
        if len(result) > 0 or len(result.columns) > 0:
            if len(result.columns) != self._return_type_size:
                raise PySparkRuntimeError(
                    errorClass="UDTF_RETURN_SCHEMA_MISMATCH",
                    messageParameters={
                        "expected": str(self._return_type_size),
                        "actual": str(len(result.columns)),
                        "func": method_name,
                    },
                )

        # Verify the type and the schema of the result.
        verify_pandas_result(
            result, self._return_type, assign_cols_by_name=False, truncate_return_schema=False
        )
        return result

    def _check_return_value(self, res: Any, method_name: str) -> Iterator:
        # Check whether the result of an arrow UDTF is iterable before
        # using it to construct a pandas DataFrame.
        if res is not None:
            if not isinstance(res, Iterable):
                raise PySparkRuntimeError(
                    errorClass="UDTF_RETURN_NOT_ITERABLE",
                    messageParameters={
                        "type": type(res).__name__,
                        "func": method_name,
                    },
                )
            check_output_row_against_schema = self._check_output_row_against_schema
            if check_output_row_against_schema is not None:
                for row in res:
                    if row is not None:
                        check_output_row_against_schema(row)
                    yield row
            else:
                yield from res

    def _convert_df_to_arrow(self, result: pd.DataFrame) -> pa.RecordBatch:
        # Convert the output pandas DataFrame into a single "_0" struct column,
        # applying the legacy pandas-to-Arrow coercions.
        runner_conf = self._runner_conf
        return PandasToArrowConversion.from_pandas(
            [result],
            self._output_schema,
            timezone=runner_conf.timezone,
            safecheck=runner_conf.safecheck,
            arrow_cast=True,
            assign_cols_by_name=False,
            int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
            ignore_unexpected_complex_type_values=True,
            use_legacy_error_handling=True,
        )

    def _to_columns(self, batch: pa.RecordBatch) -> list:
        # Deserialize the Arrow batch into a list of pandas Series (one per
        # input column).
        return ArrowToPandasConversion.to_pandas(
            batch,
            timezone=self._runner_conf.timezone,
            schema=self._eval_conf.input_type,  # type: ignore[arg-type]
            struct_in_pandas="row",
            ndarray_as_list=True,
            prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            df_for_struct=False,
        )

    def _convert_result(self, res: Any, method_name: str) -> Iterator[pa.RecordBatch]:
        import pandas as pd

        result = self._verify_result(
            pd.DataFrame(list(self._check_return_value(res, method_name))), method_name
        )
        yield self._convert_df_to_arrow(result)


class ArrowTableUDTFArrowHandler(ArrowTableUDTFHandler):
    """SQL_ARROW_TABLE_UDF with the Arrow conversion: each input column is converted to Python
    values, and each ``eval`` result's rows are converted to Arrow against the declared
    schema and wrapped as a single struct column."""

    eval_type = PythonEvalType.SQL_ARROW_TABLE_UDF
    variant_conf = "use_legacy_pandas_udtf_conversion"
    variant_value = False

    def _setup_udfs(self, udfs: list[UDTF]) -> None:
        import pyarrow  # noqa: F401

        super()._setup_udfs(udfs)
        self._arrow_return_type = to_arrow_type(
            self._return_type,
            timezone="UTC",
            prefers_large_types=self._runner_conf.use_large_var_types,
        )
        self._converters = [
            ArrowTableToRowsConversion._create_converter(
                f.dataType,
                none_on_identity=True,
                binary_as_bytes=self._runner_conf.binary_as_bytes,
            )
            for f in self._eval_conf.input_type  # type: ignore[union-attr]
        ]

    def _verify_result(self, result: pa.Table, method_name: str) -> pa.Table:
        import pyarrow as pa

        if not isinstance(result, pa.Table):
            raise PySparkTypeError(
                errorClass="INVALID_ARROW_UDTF_RETURN_TYPE",
                messageParameters={
                    "return_type": type(result).__name__,
                    "value": str(result),
                    "func": method_name,
                },
            )

        # Validate the output schema when the result dataframe has either output
        # rows or columns. Note that we avoid using `df.empty` here because the
        # result dataframe may contain an empty row. For example, when a UDTF is
        # defined as follows: def eval(self): yield tuple().
        if result.num_rows > 0 or result.num_columns > 0:
            if result.num_columns != self._return_type_size:
                raise PySparkRuntimeError(
                    errorClass="UDTF_RETURN_SCHEMA_MISMATCH",
                    messageParameters={
                        "expected": str(self._return_type_size),
                        "actual": str(result.num_columns),
                        "func": method_name,
                    },
                )

        # Verify the type and the schema of the result.
        verify_arrow_result(
            result,
            assign_cols_by_name=False,
            expected_cols_and_types=[(field.name, field.type) for field in self._arrow_return_type],
        )
        return result

    def _check_return_value(self, res: Any, method_name: str) -> Iterator:
        # Check whether the result of an arrow UDTF is iterable before
        # using it to construct a pandas DataFrame.
        if res is not None:
            if not isinstance(res, Iterable):
                raise PySparkRuntimeError(
                    errorClass="UDTF_RETURN_NOT_ITERABLE",
                    messageParameters={
                        "type": type(res).__name__,
                        "func": method_name,
                    },
                )
            return_type_size = self._return_type_size
            check_output_row_against_schema = self._check_output_row_against_schema
            for row in res:
                if not isinstance(row, tuple) and return_type_size == 1:
                    row = (row,)
                if check_output_row_against_schema is not None:
                    if row is not None:
                        check_output_row_against_schema(row)
                yield row

    def _convert_rows_to_arrow(self, data: Iterable, method_name: str) -> list[pa.RecordBatch]:
        import pyarrow as pa

        arrow_return_type = self._arrow_return_type
        data = list(self._check_return_value(data, method_name))
        if len(data) == 0:
            # Return one empty RecordBatch to match the left side of the lateral join
            return [pa.RecordBatch.from_pylist(data, schema=pa.schema(list(arrow_return_type)))]

        def raise_conversion_error(original_exception: Exception) -> None:
            raise PySparkRuntimeError(
                errorClass="UDTF_ARROW_DATA_CONVERSION_ERROR",
                messageParameters={
                    "data": str(data),
                    "schema": self._return_type.simpleString(),
                    "arrow_schema": str(arrow_return_type),
                },
            ) from original_exception

        try:
            table = LocalDataToArrowConversion.convert(
                data, self._return_type, self._runner_conf.use_large_var_types
            )
        except PySparkValueError as e:
            if e.getErrorClass() == "AXIS_LENGTH_MISMATCH":
                raise PySparkRuntimeError(
                    errorClass="UDTF_RETURN_SCHEMA_MISMATCH",
                    messageParameters={
                        "expected": e.getMessageParameters()["expected_length"],  # type: ignore[index]
                        "actual": e.getMessageParameters()["actual_length"],  # type: ignore[index]
                        "func": method_name,
                    },
                ) from e
            # Fall through to general conversion error
            raise_conversion_error(e)
        except Exception as e:
            raise_conversion_error(e)

        return self._verify_result(table, method_name).to_batches()

    def _to_columns(self, batch: pa.RecordBatch) -> list:
        # Convert each input column to a list of Python values per row.
        return [
            (
                [conv(v) for v in ArrowTableToRowsConversion._to_pylist(column)]
                if conv is not None
                else ArrowTableToRowsConversion._to_pylist(column)
            )
            for column, conv in zip(batch.columns, self._converters)
        ]

    def _convert_result(self, res: Any, method_name: str) -> Iterator[pa.RecordBatch]:
        for batch in self._convert_rows_to_arrow(res, method_name):
            yield ArrowBatchTransformer.wrap_struct(batch)
