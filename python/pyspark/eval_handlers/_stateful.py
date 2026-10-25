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

"""Handlers for the stateful streaming UDF eval types (``applyInPandasWithState``,
``transformWithStateInPandas`` and ``transformWithState`` over ``Row``).

The input stream is a flat ``Iterator[pa.RecordBatch]`` in which the rows of one grouping
key arrive sequentially, possibly spanning batch boundaries, so each handler regroups the
rows itself rather than relying on a grouped serializer.

pandas and pyarrow are imported lazily (inside ``run`` and the type-checking block) so the
module stays importable and its handlers register without either installed. Each handler
calls the matching ``require_minimum_*_version`` in ``__init__`` so a missing or too-old
dependency surfaces a clear error when the handler runs.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Optional

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.eval_handlers._base import BatchEvalTypeHandler
from pyspark.eval_handlers.utils import extract_key_value_indexes
from pyspark.serializers import CPickleSerializer
from pyspark.sql.conversion import (
    ArrowBatchTransformer,
    ArrowToPandasConversion,
    PandasToArrowConversion,
)
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.pandas.utils import (
    require_minimum_pandas_version,
    require_minimum_pyarrow_version,
)
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.streaming.stateful_processor_util import TransformWithStateInPandasFuncMode
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    Row,
    StringType,
    StructField,
    StructType,
)
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

    from pyspark.eval_handlers._typing import UDF
    from pyspark.worker_util import EvalConf, RunnerConf


class PandasGroupedMapWithStateUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE (``applyInPandasWithState``): regroup the input
    chunks by grouping key, invoke the UDF once per key with a lazy iterator of pandas
    DataFrames and its ``GroupState``, and batch the output DataFrames plus the updated state
    into count/data/state RecordBatches."""

    eval_type = PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE
    _iter_based = True

    # The output RecordBatch has three struct fields, accessed by position
    # (_0/_1/_2, not by name): a count column indicating how many data and
    # state rows are present, the UDF output data, and the serialized state.
    _RESULT_COUNT_DF_TYPE = StructType(
        [
            StructField("dataCount", IntegerType()),
            StructField("stateCount", IntegerType()),
        ]
    )
    _RESULT_STATE_DF_TYPE = StructType(
        [
            StructField("properties", StringType()),
            StructField("keyRowAsUnsafe", BinaryType()),
            StructField("object", BinaryType()),
            StructField("oldTimeoutTimestamp", LongType()),
        ]
    )

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

        # See FlatMapGroupsInPandasWithStateExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes.
        parsed_offsets = extract_key_value_indexes(self._udf_desc.args_offsets)
        self._key_offsets = parsed_offsets[0][0]
        self._value_offsets = parsed_offsets[0][1]

        self._state_object_schema = eval_conf.state_value_schema

        arrow_max_records_per_batch = runner_conf.arrow_max_records_per_batch
        self._arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )

        self._pickle_ser = CPickleSerializer()

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Apply applyInPandasWithState UDF.

        The input batches carry the data columns plus a trailing state-info
        column. Chunks are regrouped by grouping key (via the reused state
        object), the UDF is invoked once per key with a lazy iterator of
        data DataFrames and its state, and the output DataFrames plus updated
        state are batched into count/data/state RecordBatches bounded by
        arrow_max_records_per_batch.
        """
        import pandas as pd
        import pyarrow as pa

        def to_pandas(batch: pa.RecordBatch) -> list:
            return ArrowToPandasConversion.to_pandas(
                batch,
                timezone=self._runner_conf.timezone,
                struct_in_pandas="dict",
                ndarray_as_list=False,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
                df_for_struct=False,
            )

        def construct_state(state_info_col: dict) -> GroupState:
            """Construct a state instance from the value of the state info column."""
            state_properties = json.loads(state_info_col["properties"])
            state_info_col_object = state_info_col["object"]
            if state_info_col_object:
                state_object = self._pickle_ser.loads(state_info_col_object)
            else:
                state_object = None
            state_properties["optionalValue"] = state_object
            return GroupState(
                keyAsUnsafe=state_info_col["keyRowAsUnsafe"],
                valueSchema=self._state_object_schema,  # type: ignore[arg-type]
                **state_properties,
            )

        def gen_data_and_state(
            batches: Iterator[pa.RecordBatch],
        ) -> Iterator[tuple]:
            """Deserialize ArrowRecordBatches into (list of pandas.Series, state) chunks.

            Each batch carries the data columns plus a trailing state-info
            column. For every state-info row, the matching data slice is cut
            out via its (startOffset, numRows) and converted to pandas. A single
            state instance is reused across all chunks of one grouping key (the
            key appears sequentially and its last chunk is flagged), so grouping
            the output by the state object is equivalent to grouping by key.
            This must not materialize multiple Arrow RecordBatches at once.
            """
            state_for_current_group = None

            for batch in batches:
                batch_schema = batch.schema
                data_schema = pa.schema([batch_schema[i] for i in range(0, len(batch_schema) - 1)])
                state_schema = pa.schema([batch_schema[-1]])

                batch_columns = batch.columns
                data_columns = batch_columns[0:-1]
                state_column = batch_columns[-1]

                data_batch = pa.RecordBatch.from_arrays(data_columns, schema=data_schema)
                state_batch = pa.RecordBatch.from_arrays([state_column], schema=state_schema)

                state_pandas = to_pandas(state_batch)[0]

                for state_idx in range(0, len(state_pandas)):
                    state_info_col = state_pandas.iloc[state_idx]

                    if not state_info_col:
                        # no more data with grouping key + state
                        break

                    data_start_offset = state_info_col["startOffset"]
                    num_data_rows = state_info_col["numRows"]
                    is_last_chunk = state_info_col["isLastChunk"]

                    if state_for_current_group:
                        # reuse the state already built for this group
                        state = state_for_current_group
                    else:
                        # first occurrence of this group, construct a new state
                        state = construct_state(state_info_col)

                    if is_last_chunk:
                        # last chunk for this group, drop the cached state
                        state_for_current_group = None
                    elif not state_for_current_group:
                        # more chunks expected for this group, cache the state
                        state_for_current_group = state

                    data_batch_for_group = data_batch.slice(data_start_offset, num_data_rows)
                    yield (to_pandas(data_batch_for_group), state)

        def verify_element(result: pd.DataFrame) -> pd.DataFrame:
            if not isinstance(result, pd.DataFrame):
                raise PySparkTypeError(
                    errorClass="UDF_RETURN_TYPE",
                    messageParameters={
                        "expected": "iterator of pandas.DataFrame",
                        "actual": "iterator of {}".format(type(result).__name__),
                    },
                )
            # The number of columns of the result must match the return type,
            # but an empty result with no columns at all is acceptable.
            if not (
                len(result.columns) == len(self._udf_desc.return_type)
                or (len(result.columns) == 0 and result.empty)
            ):
                raise PySparkRuntimeError(
                    errorClass="RESULT_COLUMN_SCHEMA_MISMATCH",
                    messageParameters={
                        "expected": str(len(self._udf_desc.return_type)),
                        "actual": str(len(result.columns)),
                    },
                )
            return result

        def apply_udf_to_group(
            key_series: list, value_series_gen: Iterator[list], state: GroupState
        ) -> Iterator[pd.DataFrame]:
            """Adapt the deserialized chunks to the user function signature.

            Extract the scalar grouping key, convert each chunk of value Series
            into a pandas DataFrame (lazily), invoke the UDF, and validate that
            every returned element is a pandas.DataFrame conforming to the
            return type.
            """
            key = tuple(s[0] for s in key_series)

            values: Iterable
            if state.hasTimedOut:
                # On timeout the UDF is called with an empty DataFrame instead
                # of an empty iterator.
                values = [
                    pd.DataFrame(columns=pd.concat(next(value_series_gen), axis=1).columns),
                ]
            else:
                values = (pd.concat(x, axis=1) for x in value_series_gen)

            result_iter = self._func(key, values, state)

            if isinstance(result_iter, pd.DataFrame):
                raise PySparkTypeError(
                    errorClass="UDF_RETURN_TYPE",
                    messageParameters={
                        "expected": "iterable of pandas.DataFrame",
                        "actual": type(result_iter).__name__,
                    },
                )
            try:
                iter(result_iter)
            except TypeError:
                raise PySparkTypeError(
                    errorClass="UDF_RETURN_TYPE",
                    messageParameters={
                        "expected": "iterable",
                        "actual": type(result_iter).__name__,
                    },
                )

            return (verify_element(x) for x in result_iter)

        def construct_state_pdf(state: GroupState) -> pd.DataFrame:
            """Construct a single-row pandas DataFrame from the state instance."""
            state_properties = state.json().encode("utf-8")
            state_key_row_as_binary = state._keyAsUnsafe
            if state.exists:
                state_object = self._pickle_ser.dumps(state._value_schema.toInternal(state._value))
            else:
                state_object = None
            state_old_timeout_timestamp = state.oldTimeoutTimestamp

            state_dict = {
                "properties": [state_properties],
                "keyRowAsUnsafe": [state_key_row_as_binary],
                "object": [state_object],
                "oldTimeoutTimestamp": [state_old_timeout_timestamp],
            }
            return pd.DataFrame.from_dict(state_dict)

        def construct_record_batch(
            pdfs: list,
            pdf_data_cnt: int,
            pdf_schema: StructType,
            state_pdfs: list,
            state_data_cnt: int,
        ) -> pa.RecordBatch:
            """Construct a count/data/state RecordBatch from output DataFrames and states.

            Arrow RecordBatch requires all columns to have the same number of
            rows, so data and state are padded with empty rows to the max of the
            two counts; the count column records the real (unpadded) sizes.
            """
            max_data_cnt = max(1, max(pdf_data_cnt, state_data_cnt))

            # Only the first row of the count column is meaningful; the rest
            # repeat the same values for friendlier compression.
            count_dict = {
                "dataCount": [pdf_data_cnt] * max_data_cnt,
                "stateCount": [state_data_cnt] * max_data_cnt,
            }
            count_pdf = pd.DataFrame.from_dict(count_dict)

            empty_row_cnt_in_data = max_data_cnt - pdf_data_cnt
            empty_row_cnt_in_state = max_data_cnt - state_data_cnt

            empty_rows_pdf = pd.DataFrame(
                dict.fromkeys(pdf_schema.names),
                index=[x for x in range(0, empty_row_cnt_in_data)],
            )
            empty_rows_state = pd.DataFrame(
                columns=["properties", "keyRowAsUnsafe", "object", "oldTimeoutTimestamp"],
                index=[x for x in range(0, empty_row_cnt_in_state)],
            )

            pdfs.append(empty_rows_pdf)
            state_pdfs.append(empty_rows_state)

            merged_pdf = pd.concat(pdfs, ignore_index=True)
            merged_state_pdf = pd.concat(state_pdfs, ignore_index=True)

            # Fields map to _0=count, _1=output data, _2=state data.
            data = [count_pdf, merged_pdf, merged_state_pdf]
            schema = StructType(
                [
                    StructField("_0", self._RESULT_COUNT_DF_TYPE),
                    StructField("_1", pdf_schema),
                    StructField("_2", self._RESULT_STATE_DF_TYPE),
                ]
            )
            return PandasToArrowConversion.from_pandas(
                data,
                schema,
                timezone=self._runner_conf.timezone,
                safecheck=self._runner_conf.safecheck,
                arrow_cast=True,
                assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._runner_conf.int_to_decimal_coercion_enabled,
            )

        def result_and_state_stream() -> Iterator[tuple]:
            # The same state object is reused across all chunks of a group,
            # so grouping by it is equivalent to grouping by key.
            for state, group in itertools.groupby(gen_data_and_state(data), key=lambda x: x[1]):
                # These must stay lazy - do not materialize the data chunks.
                data_gen = (data_pandas for data_pandas, _ in group)
                # Consume the first chunk to extract the grouping key series.
                first_elem = next(data_gen)
                key_series = [first_elem[o] for o in self._key_offsets]
                value_series_gen = (
                    [x[o] for o in self._value_offsets]
                    for x in itertools.chain([first_elem], data_gen)
                )
                yield (apply_udf_to_group(key_series, value_series_gen, state), state)

        pdfs: list = []
        state_pdfs: list = []
        pdf_data_cnt = 0
        state_data_cnt = 0

        for result_iter, state in result_and_state_stream():
            for pdf in result_iter:
                # Ignore empty pandas DataFrames.
                if len(pdf) > 0:
                    pdf_data_cnt += len(pdf)
                    pdfs.append(pdf)

                    # Flush a batch once the record threshold is exceeded.
                    if pdf_data_cnt > self._arrow_max_records_per_batch:
                        yield construct_record_batch(
                            pdfs,
                            pdf_data_cnt,
                            self._udf_desc.return_type,
                            state_pdfs,
                            state_data_cnt,
                        )
                        pdfs = []
                        state_pdfs = []
                        pdf_data_cnt = 0
                        state_data_cnt = 0

            # The state must be captured after the result iterator is fully
            # consumed, so the UDF has run and the state is up to date.
            state_pdfs.append(construct_state_pdf(state))
            state_data_cnt += 1

        # Flush the trailing batch if it has any data or state left.
        if pdf_data_cnt > 0 or state_data_cnt > 0:
            yield construct_record_batch(
                pdfs, pdf_data_cnt, self._udf_desc.return_type, state_pdfs, state_data_cnt
            )


class PandasTransformWithStateUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_TRANSFORM_WITH_STATE_PANDAS_UDF (``transformWithStateInPandas``): regroup the input
    rows by grouping key into bounded pandas DataFrame chunks, invoke the UDF once per key
    (PROCESS_DATA), then once for PROCESS_TIMER and once for COMPLETE, talking to the JVM state
    server through a ``StatefulProcessorApiClient``."""

    eval_type = PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

        # See TransformWithStateInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes
        parsed_offsets = extract_key_value_indexes(self._udf_desc.args_offsets)
        assert len(parsed_offsets) == 1, (
            "Expected one pair of offsets for TRANSFORM_WITH_STATE_PANDAS UDF."
        )

        self._key_offsets = parsed_offsets[0][0]
        self._value_offsets = parsed_offsets[0][1]
        self._output_schema = StructType([StructField("_0", self._udf_desc.return_type)])

        self._stateful_processor_api_client = StatefulProcessorApiClient(
            eval_conf.state_server_socket_port,  # type: ignore[arg-type]
            eval_conf.grouping_key_schema,  # type: ignore[arg-type]
            auth_secret=eval_conf.state_server_auth_secret,
        )

        arrow_max_records_per_batch = runner_conf.arrow_max_records_per_batch
        self._arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )
        self._arrow_max_bytes_per_batch = runner_conf.arrow_max_bytes_per_batch

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Apply transformWithStateInPandas UDF.

        Data chunks for the same grouping key appear sequentially in the
        input batches but may span batch boundaries, so rows are regrouped
        by key and re-chunked into pandas DataFrames bounded by
        arrow_max_records_per_batch and arrow_max_bytes_per_batch. The UDF
        is invoked once per grouping key with a lazy iterator of chunks,
        then once for PROCESS_TIMER and once for COMPLETE.
        """
        import pandas as pd

        total_bytes = 0
        total_rows = 0
        average_arrow_row_size = 0.0

        def row_stream() -> Iterator[tuple]:
            nonlocal total_bytes, total_rows, average_arrow_row_size
            for batch in data:
                # Short circuit batch size stats if the batch size is
                # unlimited as computing batch size is computationally
                # expensive.
                if self._arrow_max_bytes_per_batch != 2**31 - 1 and batch.num_rows > 0:
                    total_bytes += sum(
                        buf.size
                        for col in batch.columns
                        for buf in col.buffers()
                        if buf is not None
                    )
                    total_rows += batch.num_rows
                    average_arrow_row_size = total_bytes / total_rows
                data_pandas = ArrowToPandasConversion.to_pandas(
                    batch,
                    timezone=self._runner_conf.timezone,
                    prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
                )
                for row in pd.concat(data_pandas, axis=1).itertuples(index=False):
                    batch_key = tuple(row[o] for o in self._key_offsets)
                    yield (batch_key, row)

        def generate_data_batches() -> Iterator[tuple]:
            """
            Deserialize ArrowRecordBatches and return a generator of
            (grouping key, pandas.DataFrame) chunks.

            This function must avoid materializing multiple Arrow
            RecordBatches into memory at the same time, and data chunks
            from the same grouping key should appear sequentially.
            """
            for batch_key, group_rows in itertools.groupby(row_stream(), key=lambda x: x[0]):
                rows = []
                for _, row in group_rows:
                    rows.append(row)
                    if (
                        len(rows) >= self._arrow_max_records_per_batch
                        or len(rows) * average_arrow_row_size >= self._arrow_max_bytes_per_batch
                    ):
                        yield (batch_key, pd.DataFrame(rows))
                        rows = []
                if rows:
                    yield (batch_key, pd.DataFrame(rows))

        def convert_results(result_iter: Iterable[pd.DataFrame]) -> Iterator[pa.RecordBatch]:
            for result in result_iter:
                if isinstance(self._udf_desc.return_type, StructType) and not isinstance(
                    result, pd.DataFrame
                ):
                    raise PySparkValueError(
                        "Invalid return type. Please make sure that the UDF returns a "
                        "pandas.DataFrame when the specified return type is StructType."
                    )
                yield PandasToArrowConversion.from_pandas(
                    [result],
                    self._output_schema,
                    timezone=self._runner_conf.timezone,
                    safecheck=self._runner_conf.safecheck,
                    arrow_cast=True,
                    assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                    int_to_decimal_coercion_enabled=(
                        self._runner_conf.int_to_decimal_coercion_enabled
                    ),
                )

        for key, group in itertools.groupby(generate_data_batches(), key=lambda x: x[0]):
            # This must be a generator expression - do not materialize.
            values_gen = (df.iloc[:, self._value_offsets] for _, df in group)
            yield from convert_results(
                self._func(
                    self._stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.PROCESS_DATA,
                    key,
                    values_gen,
                )
            )

        yield from convert_results(
            self._func(
                self._stateful_processor_api_client,
                TransformWithStateInPandasFuncMode.PROCESS_TIMER,
                None,
                iter([]),
            )
        )

        yield from convert_results(
            self._func(
                self._stateful_processor_api_client,
                TransformWithStateInPandasFuncMode.COMPLETE,
                None,
                iter([]),
            )
        )


class PandasTransformWithStateInitStateUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF (``transformWithStateInPandas`` with an
    initial state): like ``PandasTransformWithStateUDFHandler``, but each input batch carries
    either input data or initial state, and the UDF receives the two as separate lazy
    iterators of pandas DataFrames."""

    eval_type = PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

        # See TransformWithStateInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes.
        # parsed offsets:
        # [
        #     [groupingKeyOffsets, dedupDataOffsets],
        #     [initStateGroupingOffsets, dedupInitDataOffsets]
        # ]
        parsed_offsets = extract_key_value_indexes(self._udf_desc.args_offsets)
        self._key_offsets = parsed_offsets[0][0]
        self._init_key_offsets = parsed_offsets[1][0]
        self._output_schema = StructType([StructField("_0", self._udf_desc.return_type)])

        self._stateful_processor_api_client = StatefulProcessorApiClient(
            eval_conf.state_server_socket_port,  # type: ignore[arg-type]
            eval_conf.grouping_key_schema,  # type: ignore[arg-type]
            auth_secret=eval_conf.state_server_auth_secret,
        )

        arrow_max_records_per_batch = runner_conf.arrow_max_records_per_batch
        self._arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )
        self._arrow_max_bytes_per_batch = runner_conf.arrow_max_bytes_per_batch

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Apply transformWithStateInPandas UDF with initial state.

        The input batches carry two struct columns, ``inputData`` and
        ``initState``; each batch holds one or the other but never both.
        Rows are flattened out of whichever struct is present, regrouped by
        grouping key, and re-chunked into pandas DataFrames bounded by
        arrow_max_records_per_batch and arrow_max_bytes_per_batch. The UDF
        is invoked once per grouping key with two separate lazy iterators
        (data DataFrames and init-state DataFrames), then once for
        PROCESS_TIMER and once for COMPLETE.
        """
        import pandas as pd
        import pyarrow as pa

        total_bytes = 0
        total_rows = 0
        average_arrow_row_size = 0.0

        def flatten_columns(cur_batch: pa.RecordBatch, col_name: str) -> pa.Table:
            struct_column = cur_batch.column(cur_batch.schema.get_field_index(col_name))
            # Check if the entire column is null: an empty table (no columns)
            # signals the struct is absent from this batch.
            if struct_column.null_count == len(struct_column):
                return pa.Table.from_arrays([], names=[])
            field_names = [struct_column.type[i].name for i in range(struct_column.type.num_fields)]
            field_arrays = [struct_column.field(i) for i in range(struct_column.type.num_fields)]
            return pa.Table.from_arrays(field_arrays, names=field_names)

        def to_pandas(table: pa.Table) -> list:
            return ArrowToPandasConversion.to_pandas(
                table,
                timezone=self._runner_conf.timezone,
                prefer_int_ext_dtype=self._runner_conf.prefer_int_ext_dtype,
            )

        def row_stream() -> Iterator[tuple]:
            nonlocal total_bytes, total_rows, average_arrow_row_size
            for batch in data:
                # Short circuit batch size stats if the batch size is
                # unlimited as computing batch size is computationally
                # expensive.
                if self._arrow_max_bytes_per_batch != 2**31 - 1 and batch.num_rows > 0:
                    total_bytes += sum(
                        buf.size
                        for col in batch.columns
                        for buf in col.buffers()
                        if buf is not None
                    )
                    total_rows += batch.num_rows
                    average_arrow_row_size = total_bytes / total_rows

                data_table = flatten_columns(batch, "inputData")
                init_table = flatten_columns(batch, "initState")

                # Empty table has no columns. Each batch carries either
                # input data or init state, never both.
                has_data = data_table.num_columns > 0
                has_init = init_table.num_columns > 0
                assert not (has_data and has_init)

                if has_data:
                    for row in pd.concat(to_pandas(data_table), axis=1).itertuples(index=False):
                        batch_key = tuple(row[o] for o in self._key_offsets)
                        yield (batch_key, row, None)
                elif has_init:
                    for row in pd.concat(to_pandas(init_table), axis=1).itertuples(index=False):
                        batch_key = tuple(row[o] for o in self._init_key_offsets)
                        yield (batch_key, None, row)

        empty_dataframe = pd.DataFrame()

        def generate_data_batches() -> Iterator[tuple]:
            """
            Deserialize ArrowRecordBatches and return a generator of
            (grouping key, data DataFrame, init-state DataFrame) chunks.

            This function must avoid materializing multiple Arrow
            RecordBatches into memory at the same time, and data chunks
            from the same grouping key should appear sequentially.
            """
            for batch_key, group_rows in itertools.groupby(row_stream(), key=lambda x: x[0]):
                rows = []
                init_state_rows = []
                for _, row, init_state_row in group_rows:
                    if row is not None:
                        rows.append(row)
                    if init_state_row is not None:
                        init_state_rows.append(init_state_row)

                    total_len = len(rows) + len(init_state_rows)
                    if (
                        total_len >= self._arrow_max_records_per_batch
                        or total_len * average_arrow_row_size >= self._arrow_max_bytes_per_batch
                    ):
                        yield (
                            batch_key,
                            pd.DataFrame(rows) if rows else empty_dataframe.copy(),
                            (
                                pd.DataFrame(init_state_rows)
                                if init_state_rows
                                else empty_dataframe.copy()
                            ),
                        )
                        rows = []
                        init_state_rows = []
                if rows or init_state_rows:
                    yield (
                        batch_key,
                        pd.DataFrame(rows) if rows else empty_dataframe.copy(),
                        (
                            pd.DataFrame(init_state_rows)
                            if init_state_rows
                            else empty_dataframe.copy()
                        ),
                    )

        def convert_results(
            result_iter: Iterable[pd.DataFrame],
        ) -> Iterator[pa.RecordBatch]:
            # TODO(SPARK-49100): add verification that elements in result_iter are
            # indeed of type pd.DataFrame and conform to assigned cols
            for result in result_iter:
                if isinstance(self._udf_desc.return_type, StructType) and not isinstance(
                    result, pd.DataFrame
                ):
                    raise PySparkValueError(
                        "Invalid return type. Please make sure that the UDF returns a "
                        "pandas.DataFrame when the specified return type is StructType."
                    )
                yield PandasToArrowConversion.from_pandas(
                    [result],
                    self._output_schema,
                    timezone=self._runner_conf.timezone,
                    safecheck=self._runner_conf.safecheck,
                    arrow_cast=True,
                    assign_cols_by_name=self._runner_conf.assign_cols_by_name,
                    int_to_decimal_coercion_enabled=(
                        self._runner_conf.int_to_decimal_coercion_enabled
                    ),
                )

        for key, group in itertools.groupby(generate_data_batches(), key=lambda x: x[0]):
            # These must be generator expressions - do not materialize. The
            # UDF receives the data and init-state DataFrames as two
            # separate iterators, with empty chunks filtered out.
            group_data, group_init = itertools.tee(group, 2)
            state_values = (data_df for _, data_df, _ in group_data if not data_df.empty)
            init_states = (init_df for _, _, init_df in group_init if not init_df.empty)
            yield from convert_results(
                self._func(
                    self._stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.PROCESS_DATA,
                    key,
                    state_values,
                    init_states,
                )
            )

        yield from convert_results(
            self._func(
                self._stateful_processor_api_client,
                TransformWithStateInPandasFuncMode.PROCESS_TIMER,
                None,
                iter([]),
                iter([]),
            )
        )

        yield from convert_results(
            self._func(
                self._stateful_processor_api_client,
                TransformWithStateInPandasFuncMode.COMPLETE,
                None,
                iter([]),
                iter([]),
            )
        )


class RowTransformWithStateUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF (``transformWithState`` over ``Row``): regroup the
    input rows by grouping key, invoke the UDF once per key (PROCESS_DATA) with a lazy iterator
    of Rows, then once for PROCESS_TIMER and once for COMPLETE, talking to the JVM state server
    through a ``StatefulProcessorApiClient``."""

    eval_type = PythonEvalType.SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

        # See TransformWithStateInPySparkExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes.
        parsed_offsets = extract_key_value_indexes(self._udf_desc.args_offsets)
        self._key_offsets = parsed_offsets[0][0]

        self._stateful_processor_api_client = StatefulProcessorApiClient(
            eval_conf.state_server_socket_port,  # type: ignore[arg-type]
            eval_conf.grouping_key_schema,  # type: ignore[arg-type]
            auth_secret=eval_conf.state_server_auth_secret,
        )

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Apply transformWithStateInPySpark UDF over Row objects.

        Input batches are read row by row without materializing whole
        batches. Rows carrying the same grouping key appear sequentially, so
        they are regrouped by key and the UDF is invoked once per key with a
        lazy iterator of Row objects, then once for PROCESS_TIMER and once
        for COMPLETE. The UDF yields (iterator of Row, Spark type) pairs that
        are converted back into Arrow RecordBatches wrapped in a single
        struct column for the output stream.
        """
        import pyarrow as pa

        def generate_data_batches() -> Iterator[tuple[Any, Any]]:
            """
            Deserialize ArrowRecordBatches and return a generator of
            (grouping key, Row) tuples.

            This function must avoid materializing multiple Arrow
            RecordBatches into memory at the same time, and data chunks
            from the same grouping key should appear sequentially.
            """
            for batch in data:
                DataRow = Row(*batch.schema.names)

                # Iterate row by row without converting the whole batch.
                num_cols = batch.num_columns
                for row_idx in range(batch.num_rows):
                    row_key = tuple(batch[o][row_idx].as_py() for o in self._key_offsets)
                    row = DataRow(*(batch.column(i)[row_idx].as_py() for i in range(num_cols)))
                    yield row_key, row

        def convert_results(result_rows: Iterable[Any]) -> Iterator[pa.RecordBatch]:
            # TODO(SPARK-XXXXX): add verification that elements in result_rows
            # are indeed of type Row and conform to assigned cols

            # Convert spark type to arrow type
            # TODO: we need to make this configurable, currently using default values.
            arrow_type = to_arrow_type(
                self._udf_desc.return_type,
                timezone="UTC",
                prefers_large_types=False,
            )

            rows_as_dict = [row.asDict(True) for row in result_rows]

            pdf_schema = pa.schema(list(arrow_type))
            record_batch = pa.RecordBatch.from_pylist(rows_as_dict, schema=pdf_schema)
            yield ArrowBatchTransformer.wrap_struct(record_batch)

        for key, group in itertools.groupby(generate_data_batches(), key=lambda x: x[0]):
            # This must be a generator expression - do not materialize.
            values_gen = map(lambda x: x[1], group)
            yield from convert_results(
                self._func(
                    self._stateful_processor_api_client,
                    TransformWithStateInPandasFuncMode.PROCESS_DATA,
                    key,
                    values_gen,
                )
            )

        yield from convert_results(
            self._func(
                self._stateful_processor_api_client,
                TransformWithStateInPandasFuncMode.PROCESS_TIMER,
                None,
                iter([]),
            )
        )

        yield from convert_results(
            self._func(
                self._stateful_processor_api_client,
                TransformWithStateInPandasFuncMode.COMPLETE,
                None,
                iter([]),
            )
        )


class RowTransformWithStateInitStateUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF (``transformWithState`` over ``Row``
    with an initial state): like ``RowTransformWithStateUDFHandler``, but each input batch
    carries either input data or initial state, and the UDF receives the two as separate
    iterators of Rows per chunk."""

    eval_type = PythonEvalType.SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF

    def __init__(self, udfs: list[UDF], runner_conf: RunnerConf, eval_conf: EvalConf) -> None:
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

        # See TransformWithStateInPandasExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes.
        # parsed offsets:
        # [
        #     [groupingKeyOffsets, dedupDataOffsets],
        #     [initStateGroupingOffsets, dedupInitDataOffsets]
        # ]
        parsed_offsets = extract_key_value_indexes(self._udf_desc.args_offsets)
        self._key_offsets = parsed_offsets[0][0]
        self._init_key_offsets = parsed_offsets[1][0]

        self._stateful_processor_api_client = StatefulProcessorApiClient(
            eval_conf.state_server_socket_port,  # type: ignore[arg-type]
            eval_conf.grouping_key_schema,  # type: ignore[arg-type]
            auth_secret=eval_conf.state_server_auth_secret,
        )

        arrow_max_records_per_batch = runner_conf.arrow_max_records_per_batch
        self._arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )

    def run(self, split_index: int, data: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Apply transformWithStateInPySpark UDF with initial state over Rows.

        The input batches carry two struct columns, ``inputData`` and
        ``initState``; each batch holds one or the other but never both.
        Rows are flattened out of whichever struct is present, regrouped by
        grouping key, and re-chunked into Row lists bounded by
        arrow_max_records_per_batch. The UDF is invoked once per chunk with
        two separate iterators (data Rows and init-state Rows), then once for
        PROCESS_TIMER and once for COMPLETE. The UDF yields (iterator of Row,
        Spark type) pairs that are converted back into Arrow RecordBatches
        wrapped in a single struct column for the output stream.
        """
        import pyarrow as pa

        def extract_rows(
            cur_batch: pa.RecordBatch, col_name: str, offsets: list
        ) -> Optional[Iterator[tuple[Any, Any]]]:
            data_column = cur_batch.column(cur_batch.schema.get_field_index(col_name))

            # Check if the entire column is null.
            if data_column.null_count == len(data_column):
                return None

            data_field_names = [
                data_column.type[i].name for i in range(data_column.type.num_fields)
            ]
            data_field_arrays = [data_column.field(i) for i in range(data_column.type.num_fields)]

            DataRow = Row(*data_field_names)

            table = pa.Table.from_arrays(data_field_arrays, names=data_field_names)

            if table.num_rows == 0:
                return None

            def row_iterator() -> Iterator[tuple[Any, Any]]:
                for row_idx in range(table.num_rows):
                    key = tuple(table.column(o)[row_idx].as_py() for o in offsets)
                    row = DataRow(
                        *(table.column(i)[row_idx].as_py() for i in range(table.num_columns))
                    )
                    yield (key, row)

            return row_iterator()

        def row_stream() -> Iterator[tuple[Any, Optional[Any], Optional[Any]]]:
            # The arrow batch is written in the schema:
            # schema: StructType = new StructType()
            #     .add("inputData", dataSchema)
            #     .add("initState", initStateSchema)
            # We parse each batch into tuples of (key, inputData, initState).
            # Each batch will have either init_data or input_data, not both.
            for batch in data:
                input_result = extract_rows(batch, "inputData", self._key_offsets)
                init_result = extract_rows(batch, "initState", self._init_key_offsets)

                assert not (input_result is not None and init_result is not None)

                if input_result is not None:
                    for key, input_data_row in input_result:
                        yield (key, input_data_row, None)
                elif init_result is not None:
                    for key, init_state_row in init_result:
                        yield (key, None, init_state_row)

        def generate_data_batches() -> Iterator[tuple[Any, tuple[Any, Any]]]:
            """
            Deserialize ArrowRecordBatches and return a generator of
            (grouping key, (data Rows iterator, init-state Rows iterator))
            chunks bounded by arrow_max_records_per_batch.

            This function must avoid materializing multiple Arrow
            RecordBatches into memory at the same time, and data chunks
            from the same grouping key should appear sequentially.
            """
            for k, group_rows in itertools.groupby(row_stream(), key=lambda x: x[0]):
                input_rows: list = []
                init_rows: list = []

                for _, input_row, init_row in group_rows:
                    if input_row is not None:
                        input_rows.append(input_row)
                    if init_row is not None:
                        init_rows.append(init_row)

                    total_len = len(input_rows) + len(init_rows)
                    if total_len >= self._arrow_max_records_per_batch:
                        yield (k, (iter(input_rows), iter(init_rows)))
                        input_rows = []
                        init_rows = []

                if input_rows or init_rows:
                    yield (k, (iter(input_rows), iter(init_rows)))

        def convert_results(result_rows: Iterable[Any]) -> Iterator[pa.RecordBatch]:
            # TODO(SPARK-XXXXX): add verification that elements in result_rows
            # are indeed of type Row and conform to assigned cols

            # Convert spark type to arrow type
            # TODO: we need to make this configurable, currently using default values.
            arrow_type = to_arrow_type(
                self._udf_desc.return_type,
                timezone="UTC",
                prefers_large_types=False,
            )

            rows_as_dict = [row.asDict(True) for row in result_rows]

            pdf_schema = pa.schema(list(arrow_type))
            record_batch = pa.RecordBatch.from_pylist(rows_as_dict, schema=pdf_schema)
            yield ArrowBatchTransformer.wrap_struct(record_batch)

        for key, group in itertools.groupby(generate_data_batches(), key=lambda x: x[0]):
            # These must be generator expressions - do not materialize.
            for _, (values_gen, init_states_gen) in group:
                yield from convert_results(
                    self._func(
                        self._stateful_processor_api_client,
                        TransformWithStateInPandasFuncMode.PROCESS_DATA,
                        key,
                        values_gen,
                        init_states_gen,
                    )
                )

        yield from convert_results(
            self._func(
                self._stateful_processor_api_client,
                TransformWithStateInPandasFuncMode.PROCESS_TIMER,
                None,
                iter([]),
                iter([]),
            )
        )

        yield from convert_results(
            self._func(
                self._stateful_processor_api_client,
                TransformWithStateInPandasFuncMode.COMPLETE,
                None,
                iter([]),
                iter([]),
            )
        )
