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

Each handler receives a single ``(udtf, args_offsets, kwargs_offsets, return_type)`` tuple
from ``read_single_udtf``, where ``udtf`` is the instantiated UDTF.

pyarrow is imported lazily (inside the handlers and the type-checking block) so the module
stays importable and its handlers register without pyarrow installed.
"""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, Tuple

from pyspark.errors import PySparkRuntimeError
from pyspark.eval_handlers._base import BatchEvalTypeHandler, EvalTypeHandler
from pyspark.eval_handlers._typing import InputBatch, OutputBatch
from pyspark.eval_handlers.utils import wrap_kwargs_support
from pyspark.sql.conversion import ArrowBatchTransformer
from pyspark.sql.functions import SkipRestOfInputTableException
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import Row, _create_row
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.worker_util import EvalConf, RunnerConf


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


class UDTFEvalTypeHandler(EvalTypeHandler[InputBatch, OutputBatch]):
    """Base for the UDTF eval types.

    ``run`` calls ``_eval_batch`` per input batch, then ``_eval_terminate`` once at the end (also
    when ``eval`` raised ``SkipRestOfInputTableException``), and always calls the UDTF's
    ``cleanup``. Subclasses implement the two hooks and pick the serializer.
    """

    # Wraps the UDTF when its TABLE argument has PARTITION BY expressions; see read_single_udtf.
    partition_wrapper: ClassVar[type[PartitionedUDTF]] = UDTFWithPartitions

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        super().__init__(udfs, runner_conf, eval_conf)
        assert len(udfs) == 1, "One UDTF expected here."
        udtf, args_offsets, kwargs_offsets, self._return_type = udfs[0]
        self._eval_method, self._args_kwargs_offsets = wrap_kwargs_support(
            getattr(udtf, "eval"), args_offsets, kwargs_offsets
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


class ArrowUDTFHandler(
    BatchEvalTypeHandler["pa.RecordBatch"],
    UDTFEvalTypeHandler["pa.RecordBatch", "pa.RecordBatch"],
):
    """SQL_ARROW_UDTF: the UDTF's ``eval`` receives ``pa.Array`` arguments, with TABLE
    arguments flattened into ``pa.RecordBatch``, and returns an iterable of
    ``pa.Table``/``pa.RecordBatch``, coerced to the declared schema."""

    eval_type = PythonEvalType.SQL_ARROW_UDTF
    partition_wrapper = ArrowUDTFWithPartition

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        import pyarrow as pa

        super().__init__(udfs, runner_conf, eval_conf)
        arrow_return_type = to_arrow_type(
            self._return_type, timezone="UTC", prefers_large_types=runner_conf.use_large_var_types
        )
        self._return_type_size = len(self._return_type)
        self._target_schema = pa.schema(list(arrow_return_type))

        self._table_arg_offsets = (
            set(eval_conf.table_arg_offsets) if eval_conf.table_arg_offsets else set()
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
