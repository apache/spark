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

"""Handlers for the element-wise UDF eval types: Python UDFs lifted out of higher-order function
lambdas such as ``transform(arr, x -> udf(x))`` by ``ExtractPythonUDFFromLambda``.

Each UDF argument arrives as ``array^depth<T>`` (one ``array`` level per enclosing lambda, see
``EvalConf.elementwise_nesting``). The handlers flatten the arguments to their leaves, evaluate the
UDF over the flat elements, and re-nest the flat result using the first argument's list shape. The
row-at-a-time handler converts the leaves to Python values. The scalar and scalar iterator
handlers pass whole flat columns to the UDF: each family shares its flatten / re-nest logic in an
abstract base, whose Arrow subclass uses the base's Arrow conversions and whose pandas subclass
takes the pandas conversions from ``_PandasElementwiseUDFHandler``.

pyarrow (and pandas) are imported lazily so the module stays importable and its handlers register
without them installed.
"""

from __future__ import annotations

import collections
from abc import abstractmethod
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Optional

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import BatchEvalTypeHandler
from pyspark.eval_handlers.utils import wrap_kwargs_support
from pyspark.eval_handlers.verification import (
    verify_iterator_exhausted,
    verify_pandas_result,
    verify_result_row_count,
    verify_return_type,
    verify_scalar_result,
)
from pyspark.sql.conversion import (
    ArrowBatchTransformer,
    ArrowTableToRowsConversion,
    ArrowToPandasConversion,
    LocalDataToArrowConversion,
    PandasToArrowConversion,
)
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.sql.types import ArrayType, DataType, StructField, StructType
from pyspark.util import PythonEvalType, fail_on_stopiteration

if TYPE_CHECKING:
    import pyarrow as pa

    from pyspark.worker_util import EvalConf, RunnerConf


def _elementwise_input_schema(
    input_type: Optional[DataType], offsets: Sequence[int], depth: int
) -> StructType:
    """The leaf schema of a UDF's arguments, one positional ``_N`` field per argument.

    A lifted UDF's input arrives as ``array^depth<T>`` (one ``array`` level per enclosing higher-
    order function lambda); this peels them off to the scalar leaf ``T`` the user function sees. See
    ``ExtractPythonUDFFromLambda``.
    """
    assert isinstance(input_type, StructType)
    fields = []
    for i, offset in enumerate(offsets):
        data_type = input_type.fields[offset].dataType
        for _ in range(depth):
            assert isinstance(data_type, ArrayType)
            data_type = data_type.elementType
        fields.append(StructField(f"_{i}", data_type))
    return StructType(fields)


def _elementwise_flatten_inputs(
    batch: "pa.RecordBatch", input_column_indices: Sequence[int], depth: int
) -> tuple["pa.RecordBatch", list[list[Optional[int]]], list[bool]]:
    """Flatten element-wise UDF inputs and capture the first input's list shape.

    Returns ``(flat_batch, list_lengths_by_level, large_list_by_level)``. ``flat_batch`` contains
    one fully flattened leaf Array per selected input under a positional ``_N`` name.
    ``list_lengths_by_level[k]`` contains the per-slot length at level ``k`` (0 is outermost), using
    ``None`` for a null list. ``large_list_by_level[k]`` records whether that level uses
    ``LargeListArray``.

    Only the first selected input supplies shape metadata because
    ``ExtractPythonUDFFromLambda`` aligns the other inputs to it. ``depth`` is 1 for a UDF in a
    single lambda and greater for one lifted out of nested lambdas.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    assert input_column_indices
    assert depth > 0

    flat_inputs = []
    list_lengths_by_level = []
    large_list_by_level = []
    for input_index, column_index in enumerate(input_column_indices):
        current = batch.column(column_index)
        for _ in range(depth):
            if input_index == 0:
                list_lengths_by_level.append(pc.list_value_length(current).to_pylist())
                large_list_by_level.append(pa.types.is_large_list(current.type))
            current = current.flatten()
        flat_inputs.append(current)

    return (
        pa.RecordBatch.from_arrays(
            flat_inputs, names=[f"_{index}" for index in range(len(flat_inputs))]
        ),
        list_lengths_by_level,
        large_list_by_level,
    )


def _elementwise_renest_output(
    flat_values: "pa.Array",
    list_lengths_by_level: Sequence[Sequence[Optional[int]]],
    large_list_by_level: Sequence[bool],
) -> "pa.Array":
    """Re-nest one element-wise UDF output using its first input's recorded shape.

    Levels are rebuilt from innermost to outermost. A ``None`` length creates a null list and
    consumes no flat values; zero creates an empty, non-null list. ``large_list_by_level`` preserves
    each input level's int32 ``ListArray`` versus int64 ``LargeListArray`` offset width.
    """
    import pyarrow as pa

    result = flat_values
    for list_lengths, use_large_list in zip(
        reversed(list_lengths_by_level), reversed(large_list_by_level)
    ):
        offsets = [0]
        running = 0
        nulls = []
        for length in list_lengths:
            nulls.append(length is None)
            if length is not None:
                running += length
            offsets.append(running)
        list_type = pa.LargeListArray if use_large_list else pa.ListArray
        result = list_type.from_arrays(
            pa.array(offsets, type=pa.int64() if use_large_list else pa.int32()),
            result,
            mask=pa.array(nulls, type=pa.bool_()),
        )
    return result


def _elementwise_output_schema(return_type: DataType, runner_conf: RunnerConf) -> pa.Schema:
    """The UTC-typed one-column schema of a UDF's flat result. The UDF returns one value per
    element, so its declared return type is the element type of the ``array<R>`` produced."""
    import pyarrow as pa

    arrow_type = to_arrow_type(
        return_type, timezone="UTC", prefers_large_types=runner_conf.use_large_var_types
    )
    return pa.schema([pa.field("_0", arrow_type)])


class ArrowElementwiseUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_ARROW_ELEMENTWISE_UDF: a row-at-a-time ``@udf`` lifted out of a higher-order function
    lambda. Flatten each argument to its leaves, convert them to Python values, invoke the UDF once
    per element tuple, convert the results to the declared Arrow type and re-nest them."""

    eval_type = PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        # This path exchanges data with the JVM over Arrow, so PyArrow is required. Fail with a
        # clear message rather than a bare ImportError from `import pyarrow`.
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

        # Element-wise UDFs back higher-order lambdas like transform(arr, x -> udf(x)).
        # ExtractPythonUDFFromLambda rewrites them so the UDF receives *all* array elements
        # at once (as ``array<T>``) rather than per-element. Flatten each argument down to its
        # leaves, evaluate once over the batch, then re-nest with the input offsets. A UDF lifted
        # out of nested lambdas (e.g. transform(arr, i -> transform(i, x -> udf(x)))) flattens more
        # than one ``array`` level - its per-UDF depth comes from ``elementwise_nesting``.
        # Example: array<array<int>> -> udf(depth 2) -> array<array<int>>.

        # UDF preparation
        nesting = eval_conf.elementwise_nesting
        self._udf_infos = []
        for udf_index, udf in enumerate(udfs):
            udf_func, udf_args_offsets, udf_kwargs_offsets, udf_return_type = udf
            wrapped_func, args_kwargs_offsets = wrap_kwargs_support(
                udf_func, udf_args_offsets, udf_kwargs_offsets
            )
            depth = nesting[udf_index] if nesting is not None else 1
            # Each argument arrives as ``array^depth<T>``; convert its leaves with the element type
            # ``T`` reached by peeling ``depth`` array levels.
            arg_converters = [
                ArrowTableToRowsConversion._create_converter(
                    field.dataType,
                    none_on_identity=True,
                    binary_as_bytes=runner_conf.binary_as_bytes,
                )
                for field in _elementwise_input_schema(
                    eval_conf.input_type, args_kwargs_offsets, depth
                )
            ]
            self._udf_infos.append(
                (
                    wrapped_func,
                    args_kwargs_offsets,
                    depth,
                    arg_converters,
                    # UDF returns one value per element; return type was pickled, unchanged. This is
                    # per-element, so element type equals the declared return type.
                    to_arrow_type(
                        udf_return_type,
                        timezone="UTC",
                        prefers_large_types=runner_conf.use_large_var_types,
                    ),
                    LocalDataToArrowConversion._create_converter(
                        udf_return_type,
                        none_on_identity=True,
                        int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
                    ),
                )
            )
        self._col_names = [f"_{i}" for i in range(len(udfs))]

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        @fail_on_stopiteration
        def _evaluate_elementwise_udf(udf_func: Any, rows: Iterator[tuple]) -> list[Any]:
            if self._runner_conf.arrow_concurrency_level <= 0:
                return [udf_func(*row) for row in rows]
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=self._runner_conf.arrow_concurrency_level) as pool:
                return list(pool.map(lambda row: udf_func(*row), rows))

        for input_batch in data:
            # Each UDF is re-nested by *its own* first argument's shape. ExtractPythonUDFs can
            # fuse UDFs over differently shaped/nested arrays into one batch, so a single shared
            # shape would misalign every UDF but the first. The rewrite always passes at least
            # one array argument, so `offsets` is non-empty.
            output_arrays = []
            for (
                wrapped_func,
                offsets,
                depth,
                arg_converters,
                arrow_element_type,
                result_conv,
            ) in self._udf_infos:
                # Flatten each argument `depth` list levels to its leaves; the first argument's
                # per-level shapes drive the re-nest.
                flat_batch, list_lengths_by_level, large_list_by_level = (
                    _elementwise_flatten_inputs(input_batch, offsets, depth)
                )
                columns = []
                for column, conv in zip(flat_batch.columns, arg_converters):
                    values = ArrowTableToRowsConversion._to_pylist(column)
                    if conv is not None:
                        values = [conv(v) for v in values]
                    columns.append(values)

                total_elements = flat_batch.num_rows
                # Stream the argument tuples rather than materializing a batch-sized list.
                rows = zip(*columns)
                results = _evaluate_elementwise_udf(wrapped_func, rows)
                verify_result_row_count(len(results), total_elements)

                # Convert results and re-nest to array<R> using that UDF's offsets.
                converted = (
                    [result_conv(r) for r in results] if result_conv is not None else results
                )
                try:
                    flat_arr = pa.array(converted, type=arrow_element_type)
                # Catch ArrowTypeError as well as ArrowInvalid: the element-wise wrapper
                # commonly returns list/struct-typed elements, whose type mismatches surface
                # as ArrowTypeError, so both are caught before falling back to an explicit
                # cast.
                except (pa.lib.ArrowInvalid, pa.lib.ArrowTypeError):
                    flat_arr = pa.array(converted).cast(
                        target_type=arrow_element_type, safe=self._runner_conf.safecheck
                    )
                output_arrays.append(
                    _elementwise_renest_output(flat_arr, list_lengths_by_level, large_list_by_level)
                )

            yield pa.RecordBatch.from_arrays(output_arrays, self._col_names)


class _VectorizedElementwiseUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """Base of the scalar and scalar iterator element-wise handlers, whose UDF receives and returns
    whole flat columns. The conversions below are the Arrow flavor (``pa.Array`` in and out);
    ``_PandasElementwiseUDFHandler`` overrides them for pandas."""

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

    def _to_udf_inputs(self, flat_batch: pa.RecordBatch, input_schema: StructType) -> list[Any]:
        """Adapt one flattened input batch (one aligned leaf Array per argument) to the UDF's
        inputs, using the leaf ``input_schema``: the flat leaf Arrays, unchanged."""
        return flat_batch.columns

    def _to_flat_batch(
        self, output: Any, return_type: DataType, output_schema: pa.Schema
    ) -> pa.RecordBatch:
        """Convert one UDF result over flat elements to a one-column batch, one row per element:
        coerce it to the UTC-typed ``output_schema``."""
        import pyarrow as pa

        return ArrowBatchTransformer.enforce_schema(
            pa.RecordBatch.from_arrays([output], names=output_schema.names),
            output_schema,
            safecheck=self._runner_conf.safecheck,
        )


class _PandasElementwiseUDFHandler(_VectorizedElementwiseUDFHandler):
    """pandas flavor of the scalar and scalar iterator element-wise handlers: the UDF receives and
    returns pandas Series (DataFrames for struct types)."""

    def _to_udf_inputs(self, flat_batch: pa.RecordBatch, input_schema: StructType) -> list[Any]:
        """Convert the flat batch to Series / DataFrames using the leaf ``input_schema`` and the
        same options as the ordinary scalar pandas UDF path.

        Positional ``_N`` field names exist only to form the RecordBatch. Clear them from Series
        results so this batch wrapper does not expose new names to user code; struct inputs remain
        DataFrames with their real child-field names.
        """
        import pandas as pd

        timezone = self._runner_conf.timezone
        assert timezone is not None
        results = ArrowToPandasConversion.to_pandas(
            flat_batch,
            timezone=timezone,
            schema=input_schema,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            df_for_struct=True,
        )
        for result in results:
            if isinstance(result, pd.Series):
                # Do not expose synthetic flattened-batch field names to the UDF.
                result.name = None
        return results

    def _to_flat_batch(
        self, output: Any, return_type: DataType, output_schema: pa.Schema
    ) -> pa.RecordBatch:
        """Convert one UDF result over flat elements to a one-column batch. It is typed by
        ``PandasToArrowConversion`` using the session timezone, so its timestamp type may differ
        from the UTC-typed ``output_schema``."""
        return PandasToArrowConversion.from_pandas(
            [output],
            StructType([StructField("_0", return_type)]),
            timezone=self._runner_conf.timezone,
            safecheck=self._runner_conf.safecheck,
            arrow_cast=True,
            prefers_large_types=self._runner_conf.use_large_var_types,
            assign_cols_by_name=self._runner_conf.assign_cols_by_name,
            int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
        )


class _ScalarElementwiseUDFHandler(_VectorizedElementwiseUDFHandler):
    """Shared logic of the scalar pandas / Arrow UDFs lifted out of a higher-order function's
    lambda. Each argument arrives as ``array<T>`` aligned with the iterated array. Flatten each
    list column to its element column, run the function once over that flat column (so it still
    receives its native pandas Series / DataFrame or pa.Array), then re-nest the flat result to
    ``array<R>`` using the input's offsets - one row in, one row out, one Python round trip per
    batch. Subclasses supply the flavor's conversions and result check."""

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        super().__init__(udfs, runner_conf, eval_conf)

        nesting = eval_conf.elementwise_nesting
        self._udf_infos = []
        for udf_index, udf in enumerate(udfs):
            udf_func, udf_args_offsets, udf_kwargs_offsets, udf_return_type = udf
            wrapped_func, args_kwargs_offsets = wrap_kwargs_support(
                udf_func, udf_args_offsets, udf_kwargs_offsets
            )
            udf_output_schema = _elementwise_output_schema(udf_return_type, runner_conf)
            depth = nesting[udf_index] if nesting is not None else 1
            # Each argument arrives as ``array^depth<T>``; the vectorized function must see the leaf
            # element type ``T`` reached by peeling ``depth`` array levels.
            udf_input_schema = _elementwise_input_schema(
                eval_conf.input_type, args_kwargs_offsets, depth
            )
            self._udf_infos.append(
                (
                    wrapped_func,
                    args_kwargs_offsets,
                    udf_return_type,
                    udf_output_schema,
                    depth,
                    udf_input_schema,
                )
            )
        self._col_names = [f"_{i}" for i in range(len(udfs))]

    @abstractmethod
    def _verify_result(self, result: Any, return_type: DataType, num_elements: int) -> None:
        """Check one UDF result over ``num_elements`` flat elements before it is converted."""

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        for input_batch in data:
            output_arrays = []
            for (
                wrapped_func,
                offsets,
                return_type,
                udf_output_schema,
                depth,
                udf_input_schema,
            ) in self._udf_infos:
                # Flatten each argument `depth` list levels to its leaves and adapt to the
                # pandas or Arrow function's input. Different UDFs in one operator may iterate
                # differently shaped or differently nested arrays, so each flattens and
                # re-nests by its own argument (the first argument's shapes drive the re-nest).
                flat_batch, list_lengths_by_level, large_list_by_level = (
                    _elementwise_flatten_inputs(input_batch, offsets, depth)
                )
                total_elements = flat_batch.num_rows
                udf_inputs = self._to_udf_inputs(flat_batch, udf_input_schema)

                result = wrapped_func(*udf_inputs)
                self._verify_result(result, return_type, total_elements)

                flat_output_batch = self._to_flat_batch(result, return_type, udf_output_schema)
                output_arrays.append(
                    _elementwise_renest_output(
                        flat_output_batch.column(0), list_lengths_by_level, large_list_by_level
                    )
                )

            yield pa.RecordBatch.from_arrays(output_arrays, self._col_names)


class ArrowScalarElementwiseUDFHandler(_ScalarElementwiseUDFHandler):
    """SQL_SCALAR_ARROW_ELEMENTWISE_UDF: a scalar Arrow UDF lifted out of a higher-order function
    lambda; it receives and returns ``pa.Array`` over the flat elements."""

    eval_type = PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF

    def _verify_result(self, result: Any, return_type: DataType, num_elements: int) -> None:
        # A non-array-like result (e.g. a bare int) raises the friendly UDF_RETURN_TYPE rather
        # than a bare TypeError from len(), matching the base SQL_SCALAR_ARROW_UDF path, and also
        # checks the flat length.
        verify_scalar_result(result, num_elements)


class PandasScalarElementwiseUDFHandler(_PandasElementwiseUDFHandler, _ScalarElementwiseUDFHandler):
    """SQL_SCALAR_PANDAS_ELEMENTWISE_UDF: a scalar pandas UDF lifted out of a higher-order function
    lambda; it receives and returns pandas Series (DataFrames for struct types) over the flat
    elements."""

    eval_type = PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF

    def _verify_result(self, result: Any, return_type: DataType, num_elements: int) -> None:
        import pandas as pd

        if not hasattr(result, "__len__"):
            pd_type = "pandas.DataFrame" if isinstance(return_type, StructType) else "pandas.Series"
            raise PySparkTypeError(
                errorClass="UDF_RETURN_TYPE",
                messageParameters={
                    "expected": pd_type,
                    "actual": type(result).__name__,
                },
            )
        # struct return type must be a DataFrame (matches the base pandas path).
        if isinstance(return_type, StructType) and not isinstance(result, pd.DataFrame):
            raise PySparkValueError(
                "Invalid return type. Please make sure that the UDF returns a "
                "pandas.DataFrame when the specified return type is StructType."
            )
        # Verify the flat length before re-nesting so a wrong-length result raises
        # the friendly RESULT_ROWS_MISMATCH rather than an opaque pyarrow error.
        verify_result_row_count(len(result), num_elements)


class _ScalarIterElementwiseUDFHandler(_VectorizedElementwiseUDFHandler):
    """Shared logic of the scalar iterator pandas / Arrow UDFs lifted out of a higher-order
    function's lambda.

    The user function keeps its iterator contract: it consumes an iterator of batches and yields
    an iterator of batches, one output value per input value. Feed it the *flattened* elements of
    each input batch and, since the JVM joins UDF output to input positionally by row (one
    ``array<R>`` per input ``array<T>`` row, in order), buffer a FIFO of the per-row element counts
    to re-group the streamed flat results back into arrays. Output batch boundaries need not match
    input ones. Subclasses supply the flavor's conversions and result checks.
    """

    iter_based_udf = True

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        super().__init__(udfs, runner_conf, eval_conf)

        assert len(udfs) == 1, "One SCALAR_*_ITER_ELEMENTWISE UDF expected here."
        self._udf_func, self._args_offsets, kwargs_offsets, self._return_type = udfs[0]
        assert not kwargs_offsets, "Iterator UDFs do not take keyword arguments."

        # Each argument arrives as ``array^depth<T>``; the vectorized function must see the leaf
        # element type ``T`` (arguments may differ, e.g. an outer column repeated into an aligned
        # array). ``depth`` > 1 for a UDF lifted out of nested lambdas.
        nesting = eval_conf.elementwise_nesting
        self._depth = nesting[0] if nesting is not None else 1
        self._udf_input_schema = _elementwise_input_schema(
            eval_conf.input_type, self._args_offsets, self._depth
        )
        self._udf_output_schema = _elementwise_output_schema(self._return_type, runner_conf)

    @abstractmethod
    def _verify_return_type(self, results: Any) -> Iterator[Any]:
        """Check that the UDF returned an iterator of the flavor's batch type, returning the
        verifying iterator."""

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa

        # FIFO of per-input-batch nesting shapes awaiting their flat leaf results. Each entry
        # (per-level shapes, per-level list width, leaf count) re-nests one input batch's worth
        # of rows back to ``array^depth<R>`` once that many leaf results have streamed in.
        pending_shapes: collections.deque = collections.deque()
        num_input_elements = 0

        def extract_flat(batch: pa.RecordBatch) -> Any:
            nonlocal num_input_elements
            # Flatten each argument `depth` levels to its leaves; the first argument's per-level
            # shapes re-nest this batch's rows. The user function sees the flat leaves as a
            # pandas Series / DataFrame (pandas) or a pa.Array (Arrow), each with its leaf type.
            flat_batch, list_lengths_by_level, large_list_by_level = _elementwise_flatten_inputs(
                batch, self._args_offsets, self._depth
            )
            pending_shapes.append((list_lengths_by_level, large_list_by_level, flat_batch.num_rows))
            num_input_elements += flat_batch.num_rows
            udf_inputs = self._to_udf_inputs(flat_batch, self._udf_input_schema)
            return udf_inputs[0] if len(udf_inputs) == 1 else tuple(udf_inputs)

        flat_args_iter = map(extract_flat, data)

        verified_iter = self._verify_return_type(self._udf_func(flat_args_iter))

        # Buffer the streamed flat element results and emit an ``array<R>`` row as soon as the
        # shape at the head of the FIFO is fully covered. A row whose length is 0 (an empty
        # array) or None (a null array) needs no elements, so it is emitted immediately even
        # before any chunk arrives - this matters when a whole partition is empty/null arrays
        # and the UDF yields nothing, otherwise those rows would be dropped by the positional
        # JVM join. Chunks are held in a list and concatenated only when a shape spans more than
        # one, so a UDF that yields once per input batch (the common case) never re-copies the
        # buffer. ``empty_schema`` supplies the type for a zero-length emit; it tracks the most
        # recent chunk's schema (even a zero-length chunk carries the flavor's type - the
        # pandas flavor types timestamps with the session timezone), falling back to the
        # UTC-typed ``_udf_output_schema`` only before any chunk arrives, so all emitted batches
        # use one schema.
        pending_chunks: list = []
        pending_len = 0
        empty_schema = self._udf_output_schema
        num_output_elements = 0

        def emit_ready() -> Iterator[pa.RecordBatch]:
            nonlocal pending_chunks, pending_len
            while pending_shapes:
                list_lengths_by_level, large_list_by_level, needed = pending_shapes[0]
                if needed > pending_len:
                    break
                pending_shapes.popleft()
                if needed == 0:
                    flat_batch = pa.RecordBatch.from_pylist([], schema=empty_schema)
                else:
                    combined = ArrowBatchTransformer.concat_batches(pending_chunks, empty_schema)
                    flat_batch = combined.slice(0, needed)
                    remainder = combined.slice(needed)
                    pending_chunks = [remainder] if remainder.num_rows else []
                    pending_len -= needed
                nested = _elementwise_renest_output(
                    flat_batch.column(0), list_lengths_by_level, large_list_by_level
                )
                yield pa.RecordBatch.from_arrays([nested], ["_0"])

        def process_results() -> Iterator[pa.RecordBatch]:
            nonlocal pending_chunks, pending_len, empty_schema, num_output_elements
            for result in verified_iter:
                chunk = self._to_flat_batch(result, self._return_type, self._udf_output_schema)
                num_output_elements += chunk.num_rows
                # Fail fast if the UDF over-produces, before the buffer grows unbounded (the
                # base iterator paths do the same via verify_output_row_limit).
                if num_output_elements > num_input_elements:
                    raise PySparkRuntimeError(
                        errorClass="OUTPUT_EXCEEDS_INPUT_ROWS", messageParameters={}
                    )
                # Even a zero-length chunk carries the flavor's element type (the pandas flavor
                # types timestamps with the session timezone), so always take it: otherwise
                # rows emitted for an all-empty batch before the first non-empty chunk would use
                # the UTC-typed default and disagree with later batches, breaking the output
                # stream's single-schema contract.
                empty_schema = chunk.schema
                if chunk.num_rows:
                    pending_chunks.append(chunk)
                    pending_len += chunk.num_rows
                yield from emit_ready()

            # The iterator is exhausted: every input row's flat elements must have arrived.
            verify_result_row_count(num_output_elements, num_input_elements)
            # Flush any residual all-empty / all-null rows (they consume no elements).
            if pending_shapes:
                yield from emit_ready()
            verify_iterator_exhausted(flat_args_iter)

        yield from process_results()


class ArrowScalarIterElementwiseUDFHandler(_ScalarIterElementwiseUDFHandler):
    """SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF: a scalar iterator Arrow UDF lifted out of a
    higher-order function lambda; it maps an iterator of flat ``pa.Array`` inputs to an iterator
    of ``pa.Array`` results."""

    eval_type = PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF

    def _verify_return_type(self, results: Any) -> Iterator[Any]:
        import pyarrow as pa

        return verify_return_type(results, Iterator[pa.Array])


class PandasScalarIterElementwiseUDFHandler(
    _PandasElementwiseUDFHandler, _ScalarIterElementwiseUDFHandler
):
    """SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF: a scalar iterator pandas UDF lifted out of a
    higher-order function lambda; it maps an iterator of flat pandas Series / DataFrame inputs to
    an iterator of pandas Series (DataFrames for struct types)."""

    eval_type = PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF

    def _verify_return_type(self, results: Any) -> Iterator[Any]:
        import pandas as pd

        pandas_iter_type = (
            Iterator[pd.DataFrame]
            if isinstance(self._return_type, StructType)
            else Iterator[pd.Series]
        )
        return verify_return_type(results, pandas_iter_type)

    def _to_flat_batch(
        self, output: Any, return_type: DataType, output_schema: pa.Schema
    ) -> pa.RecordBatch:
        """Check the chunk's pandas type and columns, then convert it."""
        verify_pandas_result(
            output, return_type, assign_cols_by_name=True, truncate_return_schema=True
        )
        return super()._to_flat_batch(output, return_type, output_schema)
