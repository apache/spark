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

"""Handlers for the stateful streaming UDF eval types (``applyInPandasWithState``).

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
from typing import TYPE_CHECKING, Any

from pyspark.errors import PySparkRuntimeError, PySparkTypeError
from pyspark.eval_handlers._base import BatchEvalTypeHandler
from pyspark.eval_handlers.utils import extract_key_value_indexes
from pyspark.serializers import CPickleSerializer
from pyspark.sql.conversion import ArrowToPandasConversion, PandasToArrowConversion
from pyspark.sql.pandas.utils import (
    require_minimum_pandas_version,
    require_minimum_pyarrow_version,
)
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.util import PythonEvalType

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

    from pyspark.worker_util import EvalConf, RunnerConf


class PandasGroupedMapWithStateUDFHandler(BatchEvalTypeHandler["pa.RecordBatch"]):
    """SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE (``applyInPandasWithState``): regroup the input
    chunks by grouping key, invoke the UDF once per key with a lazy iterator of pandas
    DataFrames and its ``GroupState``, and batch the output DataFrames plus the updated state
    into count/data/state RecordBatches."""

    eval_type = PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE

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

    def __init__(
        self, udfs: list[tuple[Any, ...]], runner_conf: RunnerConf, eval_conf: EvalConf
    ) -> None:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
        super().__init__(udfs, runner_conf, eval_conf)

        assert len(udfs) == 1, "One GROUPED_MAP_PANDAS_UDF_WITH_STATE UDF expected here."

        # See FlatMapGroupsInPandasWithStateExec for how arg_offsets are used to
        # distinguish between grouping attributes and data attributes.
        self._func, arg_offsets, self._return_type = udfs[0]
        parsed_offsets = extract_key_value_indexes(arg_offsets)
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

        runner_conf = self._runner_conf
        f = self._func
        return_type = self._return_type
        key_offsets = self._key_offsets
        value_offsets = self._value_offsets
        state_object_schema = self._state_object_schema
        arrow_max_records_per_batch = self._arrow_max_records_per_batch
        pickle_ser = self._pickle_ser
        result_count_df_type = self._RESULT_COUNT_DF_TYPE
        result_state_df_type = self._RESULT_STATE_DF_TYPE

        def to_pandas(batch: pa.RecordBatch) -> list:
            return ArrowToPandasConversion.to_pandas(
                batch,
                timezone=runner_conf.timezone,
                struct_in_pandas="dict",
                ndarray_as_list=False,
                prefer_int_ext_dtype=runner_conf.prefer_int_ext_dtype,
                df_for_struct=False,
            )

        def construct_state(state_info_col: dict) -> GroupState:
            """Construct a state instance from the value of the state info column."""
            state_properties = json.loads(state_info_col["properties"])
            state_info_col_object = state_info_col["object"]
            if state_info_col_object:
                state_object = pickle_ser.loads(state_info_col_object)
            else:
                state_object = None
            state_properties["optionalValue"] = state_object
            return GroupState(
                keyAsUnsafe=state_info_col["keyRowAsUnsafe"],
                valueSchema=state_object_schema,  # type: ignore[arg-type]
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
                len(result.columns) == len(return_type)
                or (len(result.columns) == 0 and result.empty)
            ):
                raise PySparkRuntimeError(
                    errorClass="RESULT_COLUMN_SCHEMA_MISMATCH",
                    messageParameters={
                        "expected": str(len(return_type)),
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

            result_iter = f(key, values, state)

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
                state_object = pickle_ser.dumps(state._value_schema.toInternal(state._value))
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
                    StructField("_0", result_count_df_type),
                    StructField("_1", pdf_schema),
                    StructField("_2", result_state_df_type),
                ]
            )
            return PandasToArrowConversion.from_pandas(
                data,
                schema,
                timezone=runner_conf.timezone,
                safecheck=runner_conf.safecheck,
                arrow_cast=True,
                assign_cols_by_name=runner_conf.assign_cols_by_name,
                int_to_decimal_coercion_enabled=runner_conf.int_to_decimal_coercion_enabled,
            )

        def result_and_state_stream() -> Iterator[tuple]:
            # The same state object is reused across all chunks of a group,
            # so grouping by it is equivalent to grouping by key.
            for state, group in itertools.groupby(gen_data_and_state(data), key=lambda x: x[1]):
                # These must stay lazy - do not materialize the data chunks.
                data_gen = (data_pandas for data_pandas, _ in group)
                # Consume the first chunk to extract the grouping key series.
                first_elem = next(data_gen)
                key_series = [first_elem[o] for o in key_offsets]
                value_series_gen = (
                    [x[o] for o in value_offsets] for x in itertools.chain([first_elem], data_gen)
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
                    if pdf_data_cnt > arrow_max_records_per_batch:
                        yield construct_record_batch(
                            pdfs, pdf_data_cnt, return_type, state_pdfs, state_data_cnt
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
                pdfs, pdf_data_cnt, return_type, state_pdfs, state_data_cnt
            )
