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
Serializers for PyArrow and pandas conversions. See `pyspark.serializers` for more details.
"""

from itertools import groupby

import pyspark
from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.serializers import (
    Serializer,
    read_int,
    write_int,
    UTF8Deserializer,
    CPickleSerializer,
)
from pyspark.sql import Row
from pyspark.sql.conversion import (
    LocalDataToArrowConversion,
    ArrowTableToRowsConversion,
    ArrowBatchTransformer,
    PandasBatchTransformer,
)
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import (
    StringType,
    StructType,
    BinaryType,
    StructField,
    LongType,
    IntegerType,
)


class SpecialLengths:
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
    START_ARROW_STREAM = -6


class ArrowCollectSerializer(Serializer):
    """
    Deserialize a stream of batches followed by batch order information. Used in
    PandasConversionMixin._collect_as_arrow() after invoking Dataset.collectAsArrowToPython()
    in the JVM.
    """

    def __init__(self):
        self.serializer = ArrowStreamSerializer()

    def dump_stream(self, iterator, stream):
        return self.serializer.dump_stream(iterator, stream)

    def load_stream(self, stream):
        """
        Load a stream of un-ordered Arrow RecordBatches, where the last iteration yields
        a list of indices that can be used to put the RecordBatches in the correct order.
        """
        # load the batches
        for batch in self.serializer.load_stream(stream):
            yield batch

        # load the batch order indices or propagate any error that occurred in the JVM
        num = read_int(stream)
        if num == -1:
            error_msg = UTF8Deserializer().loads(stream)
            raise PySparkRuntimeError(
                errorClass="ERROR_OCCURRED_WHILE_CALLING",
                messageParameters={
                    "func_name": "ArrowCollectSerializer.load_stream",
                    "error_msg": error_msg,
                },
            )
        batch_order = []
        for i in range(num):
            index = read_int(stream)
            batch_order.append(index)
        yield batch_order

    def __repr__(self):
        return "ArrowCollectSerializer(%s)" % self.serializer


class ArrowStreamSerializer(Serializer):
    """
    Serializes Arrow record batches as a stream.
    """

    def dump_stream(self, iterator, stream):
        import pyarrow as pa

        writer = None
        try:
            for batch in iterator:
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()

    def load_stream(self, stream):
        import pyarrow as pa

        reader = pa.ipc.open_stream(stream)
        for batch in reader:
            yield batch

    def _load_group_dataframes(self, stream, num_dfs: int = 1):
        """
        Load groups with specified number of dataframes from stream.

        For num_dfs=1, yields a single-element tuple containing a lazy iterator.
        For num_dfs>1, yields a tuple of eagerly loaded lists to ensure correct
        stream position when reading multiple dataframes sequentially.

        Parameters
        ----------
        stream
            The input stream to read from
        num_dfs : int
            The expected number of dataframes in each group (e.g., 1 for grouped UDFs,
            2 for cogrouped UDFs)

        Yields
        ------
        tuple
            For num_dfs=1: tuple[Iterator[pa.RecordBatch]]
            For num_dfs>1: tuple[list[pa.RecordBatch], ...]
        """
        dataframes_in_group = None

        while dataframes_in_group is None or dataframes_in_group > 0:
            dataframes_in_group = read_int(stream)

            if dataframes_in_group == num_dfs:
                if num_dfs == 1:
                    # Single dataframe: can use lazy iterator
                    yield (ArrowStreamSerializer.load_stream(self, stream),)
                else:
                    # Multiple dataframes: must eagerly load sequentially
                    # to maintain correct stream position
                    yield tuple(
                        list(ArrowStreamSerializer.load_stream(self, stream))
                        for _ in range(num_dfs)
                    )
            elif dataframes_in_group > 0:
                raise PySparkValueError(
                    errorClass="INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP",
                    messageParameters={"dataframes_in_group": str(dataframes_in_group)},
                )

    def _write_stream_start(self, batch_iterator, stream):
        """
        Write START_ARROW_STREAM before the first batch, passing batches through unchanged.

        This marker signals the JVM that the Arrow stream is about to begin. It must be sent
        after the first batch is successfully created, so that if an error occurs during batch
        creation, the error can be sent back to the JVM before the Arrow stream starts.

        Parameters
        ----------
        batch_iterator : Iterator[pa.RecordBatch]
            Iterator of Arrow record batches to write
        stream
            The output stream to write to

        Yields
        ------
        pa.RecordBatch
            The same batches from the input iterator, unmodified
        """
        import itertools

        first = next(batch_iterator, None)
        if first is None:
            return

        write_int(SpecialLengths.START_ARROW_STREAM, stream)
        yield from itertools.chain([first], batch_iterator)

    def __repr__(self):
        return "ArrowStreamSerializer"


class ArrowStreamGroupSerializer(ArrowStreamSerializer):
    """
    Unified serializer for Arrow stream operations with optional grouping support.

    This serializer handles:
    - Non-grouped operations: SQL_MAP_ARROW_ITER_UDF (num_dfs=0)
    - Grouped operations: SQL_GROUPED_MAP_ARROW_UDF, SQL_GROUPED_MAP_PANDAS_UDF (num_dfs=1)
    - Cogrouped operations: SQL_COGROUPED_MAP_ARROW_UDF, SQL_COGROUPED_MAP_PANDAS_UDF (num_dfs=2)
    - Grouped aggregations: SQL_GROUPED_AGG_ARROW_UDF, SQL_GROUPED_AGG_PANDAS_UDF (num_dfs=1)

    The serializer handles Arrow stream I/O and START signal, while transformation logic
    (flatten/wrap struct, pandas conversion) is handled by worker wrappers.

    Parameters
    ----------
    num_dfs : int, optional
        Number of DataFrames per group:
        - 0: Non-grouped mode (default) - yields all batches as single stream
        - 1: Grouped mode - yields one iterator of batches per group
        - 2: Cogrouped mode - yields tuple of two iterators per group
    """

    def __init__(self, num_dfs: int = 0):
        super().__init__()
        self._num_dfs = num_dfs

    def load_stream(self, stream):
        """
        Deserialize Arrow record batches from stream.

        Returns
        -------
        Iterator
            - num_dfs=0: Iterator[pa.RecordBatch] - all batches in a single stream
            - num_dfs=1: Iterator[Iterator[pa.RecordBatch]] - one iterator per group
            - num_dfs=2: Iterator[Tuple[Iterator[pa.RecordBatch], Iterator[pa.RecordBatch]]] -
                         tuple of two iterators per cogrouped group
        """
        if self._num_dfs > 0:
            # Grouped mode: return raw Arrow batches per group
            for batch_iters in self._load_group_dataframes(stream, num_dfs=self._num_dfs):
                if self._num_dfs == 1:
                    yield batch_iters[0]
                    # Make sure the batches are fully iterated before getting the next group
                    for _ in batch_iters[0]:
                        pass
                else:
                    yield batch_iters
        else:
            # Non-grouped mode: return raw Arrow batches
            yield from super().load_stream(stream)

    def dump_stream(self, iterator, stream):
        """
        Serialize Arrow record batches to stream with START signal.

        The START_ARROW_STREAM marker is sent before the first batch to signal
        the JVM that Arrow data is about to be transmitted. This allows proper
        error handling during batch creation.

        Parameters
        ----------
        iterator : Iterator[pa.RecordBatch]
            Iterator of Arrow RecordBatches to serialize. For grouped/cogrouped UDFs,
            this iterator is already flattened by the worker's func layer.
        stream : file-like object
            Output stream to write the serialized batches
        """
        batches = self._write_stream_start(iterator, stream)
        return super().dump_stream(batches, stream)


class ArrowStreamArrowUDTFSerializer(ArrowStreamGroupSerializer):
    """
    Serializer for PyArrow-native UDTFs that work directly with PyArrow RecordBatches and Arrays.
    """

    def __init__(self, table_arg_offsets=None):
        super().__init__()
        self.table_arg_offsets = table_arg_offsets if table_arg_offsets else []

    def load_stream(self, stream):
        """
        Flatten the struct into Arrow's record batches.
        """
        for batch in super().load_stream(stream):
            # For each column: flatten struct columns at table_arg_offsets into RecordBatch,
            # keep other columns as Array
            yield [
                ArrowBatchTransformer.flatten_struct(batch, column_index=i)
                if i in self.table_arg_offsets
                else batch.column(i)
                for i in range(batch.num_columns)
            ]

    def dump_stream(self, iterator, stream):
        """
        Override to handle type coercion for ArrowUDTF outputs.
        ArrowUDTF returns iterator of (pa.RecordBatch, arrow_return_type) tuples.
        """
        import pyarrow as pa

        def apply_type_coercion():
            for batch, arrow_return_type in iterator:
                assert isinstance(
                    arrow_return_type, pa.StructType
                ), f"Expected pa.StructType, got {type(arrow_return_type)}"

                # Batch is already wrapped into a struct column by worker (wrap_arrow_udtf)
                # Unwrap it first to access individual columns
                if batch.num_columns == 1 and batch.column(0).type == pa.struct(
                    list(arrow_return_type)
                ):
                    # Batch is wrapped, unwrap it
                    unwrapped_batch = ArrowBatchTransformer.flatten_struct(batch, column_index=0)
                elif batch.num_columns == 0:
                    # Empty batch: wrap it back to struct column
                    coerced_batch = ArrowBatchTransformer.wrap_struct(batch)
                    yield coerced_batch
                    continue
                else:
                    # Batch is not wrapped (shouldn't happen, but handle it)
                    unwrapped_batch = batch

                # Handle empty struct case specially (no columns to coerce)
                if len(arrow_return_type) == 0:
                    # Empty struct: wrap unwrapped batch (which should also be empty) back to struct column
                    coerced_batch = ArrowBatchTransformer.wrap_struct(unwrapped_batch)
                    yield coerced_batch
                    continue

                # Check field names match
                expected_field_names = [field.name for field in arrow_return_type]
                actual_field_names = unwrapped_batch.schema.names
                if expected_field_names != actual_field_names:
                    raise PySparkTypeError(
                        "Target schema's field names are not matching the record batch's "
                        "field names. "
                        f"Expected: {expected_field_names}, but got: {actual_field_names}."
                    )

                # Use zip_batches for type coercion: create (array, type) tuples
                arrays_and_types = [
                    (unwrapped_batch.column(i), field.type)
                    for i, field in enumerate(arrow_return_type)
                ]
                try:
                    coerced_batch = ArrowBatchTransformer.zip_batches(
                        arrays_and_types, safecheck=True
                    )
                except PySparkTypeError as e:
                    # Re-raise with UDTF-specific error
                    # Find the first array that failed type coercion
                    # arrays_and_types contains (array, field_type) tuples
                    for array, expected_type in arrays_and_types:
                        if array.type != expected_type:
                            raise PySparkRuntimeError(
                                errorClass="RESULT_COLUMNS_MISMATCH_FOR_ARROW_UDTF",
                                messageParameters={
                                    "expected": str(expected_type),
                                    "actual": str(array.type),
                                },
                            ) from e
                    # If no type mismatch found, re-raise original error
                    raise

                # Rename columns to match expected field names
                coerced_batch = coerced_batch.rename_columns(expected_field_names)
                # Wrap into struct column for JVM
                coerced_batch = ArrowBatchTransformer.wrap_struct(coerced_batch)
                yield coerced_batch

        return super().dump_stream(apply_type_coercion(), stream)


class ArrowStreamUDFSerializer(ArrowStreamSerializer):
    """
    Serializer for UDFs that handles Arrow RecordBatch serialization.

    This is a thin wrapper around ArrowStreamSerializer kept for backward compatibility
    and future extensibility. Currently it doesn't override any methods - all conversion
    logic has been moved to wrappers/callers, and this serializer only handles pure
    Arrow stream serialization.

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values (for compatibility)
    safecheck : bool
        If True, conversion checks for overflow/truncation (for compatibility)
    int_to_decimal_coercion_enabled : bool
        If True, applies additional coercions (for compatibility)
    """

    def __init__(
        self,
        timezone,
        safecheck,
        int_to_decimal_coercion_enabled: bool = False,
    ):
        super().__init__()
        # Store parameters for backward compatibility
        self._timezone = timezone
        self._safecheck = safecheck
        self._int_to_decimal_coercion_enabled = int_to_decimal_coercion_enabled

    def __repr__(self):
        return "ArrowStreamUDFSerializer"


class ArrowBatchUDFSerializer(ArrowStreamGroupSerializer):
    """
    Serializer used by Python worker to evaluate Arrow Python UDFs
    when the legacy pandas conversion is disabled
    (instead of legacy ArrowStreamGroupSerializer with pandas conversion in serializer).

    Parameters
    ----------
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    input_type : spark data type
        input data type for the UDF, must be a StructType
    int_to_decimal_coercion_enabled : bool
        If True, applies additional coercions in Python before converting to Arrow
        This has performance penalties.
    binary_as_bytes : bool
        If True, binary type will be deserialized as bytes, otherwise as bytearray.
    """

    def __init__(
        self,
        safecheck: bool,
        input_type: StructType,
        int_to_decimal_coercion_enabled: bool,
        binary_as_bytes: bool,
    ):
        super().__init__(num_dfs=0)
        assert isinstance(input_type, StructType)
        self._safecheck = safecheck
        self._input_type = input_type
        self._int_to_decimal_coercion_enabled = int_to_decimal_coercion_enabled
        self._binary_as_bytes = binary_as_bytes

    def load_stream(self, stream):
        """
        Loads a stream of Arrow record batches and converts them to Python values.

        Parameters
        ----------
        stream : object
            Input stream containing Arrow record batches

        Yields
        ------
        list
            List of columns containing list of Python values.
        """
        converters = [
            ArrowTableToRowsConversion._create_converter(
                f.dataType, none_on_identity=True, binary_as_bytes=self._binary_as_bytes
            )
            for f in self._input_type
        ]

        for batch in super().load_stream(stream):
            if batch.num_columns == 0:
                yield [[pyspark._NoValue] * batch.num_rows]
            else:
                yield [
                    [conv(v) for v in col.to_pylist()] if conv else col.to_pylist()
                    for col, conv in zip(batch.itercolumns(), converters)
                ]

    def dump_stream(self, iterator, stream):
        """
        Dumps an iterator of Python values as a stream of Arrow record batches.

        Parameters
        ----------
        iterator : iterator
            Iterator yielding tuples of (data, arrow_type, spark_type) for single UDF
            or list of tuples for multiple UDFs in a projection
        stream : object
            Output stream to write the Arrow record batches

        Returns
        -------
        object
            Result of writing the Arrow stream via ArrowStreamGroupSerializer dump_stream
        """
        import pyarrow as pa

        def create_array(results, arrow_type, spark_type):
            return LocalDataToArrowConversion.create_array(
                results,
                arrow_type,
                spark_type,
                safecheck=self._safecheck,
                int_to_decimal_coercion_enabled=self._int_to_decimal_coercion_enabled,
            )

        def py_to_batch():
            for packed in iterator:
                if len(packed) == 3 and isinstance(packed[1], pa.DataType):
                    # single array UDF in a projection
                    yield ArrowBatchTransformer.zip_batches(
                        [(create_array(packed[0], packed[1], packed[2]), packed[1])],
                        safecheck=self._safecheck,
                    )
                else:
                    # multiple array UDFs in a projection
                    arrays_and_types = [(create_array(*t), t[1]) for t in packed]
                    yield ArrowBatchTransformer.zip_batches(
                        arrays_and_types,
                        safecheck=self._safecheck,
                    )

        return super().dump_stream(py_to_batch(), stream)


class ApplyInPandasWithStateSerializer(ArrowStreamUDFSerializer):
    """
    Serializer used by Python worker to evaluate UDF for applyInPandasWithState.

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    assign_cols_by_name : bool
        If True, then Pandas DataFrames will get columns by name
    state_object_schema : StructType
        The type of state object represented as Spark SQL type
    arrow_max_records_per_batch : int
        Limit of the number of records that can be written to a single ArrowRecordBatch in memory.
    """

    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        state_object_schema,
        arrow_max_records_per_batch,
        prefers_large_var_types,
        int_to_decimal_coercion_enabled,
    ):
        super().__init__(
            timezone=timezone,
            safecheck=safecheck,
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
        )
        self._df_for_struct = False
        self._struct_in_pandas = "dict"
        self._ndarray_as_list = False
        self._input_type = None
        self._assign_cols_by_name = assign_cols_by_name
        self._arrow_cast = True
        self.pickleSer = CPickleSerializer()
        self.utf8_deserializer = UTF8Deserializer()
        self.state_object_schema = state_object_schema

        self.result_count_df_type = StructType(
            [
                StructField("dataCount", IntegerType()),
                StructField("stateCount", IntegerType()),
            ]
        )

        self.result_count_pdf_arrow_type = to_arrow_type(
            self.result_count_df_type, timezone="UTC", prefers_large_types=prefers_large_var_types
        )

        self.result_state_df_type = StructType(
            [
                StructField("properties", StringType()),
                StructField("keyRowAsUnsafe", BinaryType()),
                StructField("object", BinaryType()),
                StructField("oldTimeoutTimestamp", LongType()),
            ]
        )

        self.result_state_pdf_arrow_type = to_arrow_type(
            self.result_state_df_type, timezone="UTC", prefers_large_types=prefers_large_var_types
        )
        self.arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )

    def load_stream(self, stream):
        """
        Read ArrowRecordBatches from stream, deserialize them to populate a list of pair
        (data chunk, state), and convert the data into a list of pandas.Series.

        Please refer the doc of inner function `gen_data_and_state` for more details how
        this function works in overall.

        In addition, this function further groups the return of `gen_data_and_state` by the state
        instance (same semantic as grouping by grouping key) and produces an iterator of data
        chunks for each group, so that the caller can lazily materialize the data chunk.
        """

        import pyarrow as pa
        import json
        from itertools import groupby
        from pyspark.sql.streaming.state import GroupState

        def construct_state(state_info_col):
            """
            Construct state instance from the value of state information column.
            """

            state_info_col_properties = state_info_col["properties"]
            state_info_col_key_row = state_info_col["keyRowAsUnsafe"]
            state_info_col_object = state_info_col["object"]

            state_properties = json.loads(state_info_col_properties)
            if state_info_col_object:
                state_object = self.pickleSer.loads(state_info_col_object)
            else:
                state_object = None
            state_properties["optionalValue"] = state_object

            return GroupState(
                keyAsUnsafe=state_info_col_key_row,
                valueSchema=self.state_object_schema,
                **state_properties,
            )

        def gen_data_and_state(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of
            `(a list of pandas.Series, state)`.

            The logic on deserialization is following:

            1. Read the entire data part from Arrow RecordBatch.
            2. Read the entire state information part from Arrow RecordBatch.
            3. Loop through each state information:
               3.A. Extract the data out from entire data via the information of data range.
               3.B. Construct a new state instance if the state information is the first occurrence
                    for the current grouping key.
               3.C. Leverage the existing state instance if it is already available for the current
                    grouping key. (Meaning it's not the first occurrence.)
               3.D. Remove the cache of state instance if the state information denotes the data is
                    the last chunk for current grouping key.

            This deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.

            This function must avoid materializing multiple Arrow RecordBatches into memory at the
            same time. And data chunks from the same grouping key should appear sequentially, to
            further group them based on state instance (same state instance will be produced for
            same grouping key).
            """

            state_for_current_group = None

            for batch in batches:
                batch_schema = batch.schema
                data_schema = pa.schema([batch_schema[i] for i in range(0, len(batch_schema) - 1)])
                state_schema = pa.schema(
                    [
                        batch_schema[-1],
                    ]
                )

                batch_columns = batch.columns
                data_columns = batch_columns[0:-1]
                state_column = batch_columns[-1]

                data_batch = pa.RecordBatch.from_arrays(data_columns, schema=data_schema)
                state_batch = pa.RecordBatch.from_arrays(
                    [
                        state_column,
                    ],
                    schema=state_schema,
                )

                state_pandas = ArrowBatchTransformer.to_pandas(
                    state_batch,
                    timezone=self._timezone,
                    schema=None,
                    struct_in_pandas=self._struct_in_pandas,
                    ndarray_as_list=self._ndarray_as_list,
                    df_for_struct=self._df_for_struct,
                )[0]

                for state_idx in range(0, len(state_pandas)):
                    state_info_col = state_pandas.iloc[state_idx]

                    if not state_info_col:
                        # no more data with grouping key + state
                        break

                    data_start_offset = state_info_col["startOffset"]
                    num_data_rows = state_info_col["numRows"]
                    is_last_chunk = state_info_col["isLastChunk"]

                    if state_for_current_group:
                        # use the state, we already have state for same group and there should be
                        # some data in same group being processed earlier
                        state = state_for_current_group
                    else:
                        # there is no state being stored for same group, construct one
                        state = construct_state(state_info_col)

                    if is_last_chunk:
                        # discard the state being cached for same group
                        state_for_current_group = None
                    elif not state_for_current_group:
                        # there's no cached state but expected to have additional data in same group
                        # cache the current state
                        state_for_current_group = state

                    data_batch_for_group = data_batch.slice(data_start_offset, num_data_rows)
                    data_pandas = ArrowBatchTransformer.to_pandas(
                        data_batch_for_group,
                        timezone=self._timezone,
                        schema=None,
                        struct_in_pandas=self._struct_in_pandas,
                        ndarray_as_list=self._ndarray_as_list,
                        df_for_struct=self._df_for_struct,
                    )

                    # state info
                    yield (
                        data_pandas,
                        state,
                    )

        _batches = super().load_stream(stream)

        data_state_generator = gen_data_and_state(_batches)

        # state will be same object for same grouping key
        for _state, _data in groupby(data_state_generator, key=lambda x: x[1]):
            yield (
                _data,
                _state,
            )

    def dump_stream(self, iterator, stream):
        """
        Read through an iterator of (iterator of pandas DataFrame, state), serialize them to Arrow
        RecordBatches, and write batches to stream.
        """

        import pandas as pd
        import pyarrow as pa

        def construct_state_pdf(state):
            """
            Construct a pandas DataFrame from the state instance.
            """

            state_properties = state.json().encode("utf-8")
            state_key_row_as_binary = state._keyAsUnsafe
            if state.exists:
                state_object = self.pickleSer.dumps(state._value_schema.toInternal(state._value))
            else:
                state_object = None
            state_old_timeout_timestamp = state.oldTimeoutTimestamp

            state_dict = {
                "properties": [
                    state_properties,
                ],
                "keyRowAsUnsafe": [
                    state_key_row_as_binary,
                ],
                "object": [
                    state_object,
                ],
                "oldTimeoutTimestamp": [
                    state_old_timeout_timestamp,
                ],
            }

            return pd.DataFrame.from_dict(state_dict)

        def construct_record_batch(pdfs, pdf_data_cnt, pdf_schema, state_pdfs, state_data_cnt):
            """
            Construct a new Arrow RecordBatch based on output pandas DataFrames and states. Each
            one matches to the single struct field for Arrow schema. We also need an extra one to
            indicate array length for data and state, so the return value of Arrow RecordBatch will
            have schema with three fields, in `count`, `data`, `state` order.
            (Readers are expected to access the field via position rather than the name. We do
            not guarantee the name of the field.)

            Note that Arrow RecordBatch requires all columns to have all same number of rows,
            hence this function inserts empty data for count/state/data with less elements to
            compensate.
            """

            max_data_cnt = max(1, max(pdf_data_cnt, state_data_cnt))

            # We only use the first row in the count column, and fill other rows to be the same
            # value, hoping it is more friendly for compression, in case it is needed.
            count_dict = {
                "dataCount": [pdf_data_cnt] * max_data_cnt,
                "stateCount": [state_data_cnt] * max_data_cnt,
            }
            count_pdf = pd.DataFrame.from_dict(count_dict)

            empty_row_cnt_in_data = max_data_cnt - pdf_data_cnt
            empty_row_cnt_in_state = max_data_cnt - state_data_cnt

            empty_rows_pdf = pd.DataFrame(
                dict.fromkeys(pa.schema(pdf_schema).names),
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

            return PandasBatchTransformer.to_arrow(
                [
                    (count_pdf, self.result_count_pdf_arrow_type),
                    (merged_pdf, pdf_schema),
                    (merged_state_pdf, self.result_state_pdf_arrow_type),
                ],
                timezone=self._timezone,
                safecheck=self._safecheck,
                int_to_decimal_coercion_enabled=self._int_to_decimal_coercion_enabled,
                struct_in_pandas=self._struct_in_pandas,
                assign_cols_by_name=self._assign_cols_by_name,
                arrow_cast=self._arrow_cast,
            )

        def serialize_batches():
            """
            Read through an iterator of (iterator of pandas DataFrame, state), and serialize them
            to Arrow RecordBatches.

            This function does batching on constructing the Arrow RecordBatch; a batch will be
            serialized to the Arrow RecordBatch when the total number of records exceeds the
            configured threshold.
            """
            # a set of variables for the state of current batch which will be converted to Arrow
            # RecordBatch.
            pdfs = []
            state_pdfs = []
            pdf_data_cnt = 0
            state_data_cnt = 0

            return_schema = None

            for data in iterator:
                # data represents the result of each call of user function
                packaged_result = data[0]

                # There are two results from the call of user function:
                # 1) iterator of pandas DataFrame (output)
                # 2) updated state instance
                pdf_iter = packaged_result[0][0]
                state = packaged_result[0][1]

                # This is static and won't change across batches.
                return_schema = packaged_result[1]

                for pdf in pdf_iter:
                    # We ignore empty pandas DataFrame.
                    if len(pdf) > 0:
                        pdf_data_cnt += len(pdf)
                        pdfs.append(pdf)

                        # If the total number of records in current batch exceeds the configured
                        # threshold, time to construct the Arrow RecordBatch from the batch.
                        if pdf_data_cnt > self.arrow_max_records_per_batch:
                            batch = construct_record_batch(
                                pdfs, pdf_data_cnt, return_schema, state_pdfs, state_data_cnt
                            )

                            # Reset the variables to start with new batch for further data.
                            pdfs = []
                            state_pdfs = []
                            pdf_data_cnt = 0
                            state_data_cnt = 0

                            yield batch

                # This has to be performed 'after' evaluating all elements in iterator, so that
                # the user function has been completed and the state is guaranteed to be updated.
                state_pdf = construct_state_pdf(state)

                state_pdfs.append(state_pdf)
                state_data_cnt += 1

            # processed all output, but current batch may not be flushed yet.
            if pdf_data_cnt > 0 or state_data_cnt > 0:
                batch = construct_record_batch(
                    pdfs, pdf_data_cnt, return_schema, state_pdfs, state_data_cnt
                )

                yield batch

        batches = self._write_stream_start(serialize_batches(), stream)
        return ArrowStreamSerializer.dump_stream(self, batches, stream)


class TransformWithStateInPandasSerializer(ArrowStreamUDFSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithStateInPandasSerializer`.

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    assign_cols_by_name : bool
        If True, then Pandas DataFrames will get columns by name
    arrow_max_records_per_batch : int
        Limit of the number of records that can be written to a single ArrowRecordBatch in memory.
    """

    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        arrow_max_records_per_batch,
        arrow_max_bytes_per_batch,
        int_to_decimal_coercion_enabled,
    ):
        super().__init__(
            timezone=timezone,
            safecheck=safecheck,
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
        )
        self._df_for_struct = False
        self._struct_in_pandas = "dict"
        self._ndarray_as_list = False
        self._input_type = None
        self._assign_cols_by_name = assign_cols_by_name
        self._arrow_cast = True
        self.arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )
        self.arrow_max_bytes_per_batch = arrow_max_bytes_per_batch
        self.key_offsets = None
        self.average_arrow_row_size = 0
        self.total_bytes = 0
        self.total_rows = 0

    def _update_batch_size_stats(self, batch):
        """
        Update batch size statistics for adaptive batching.
        """
        # Short circuit batch size calculation if the batch size is
        # unlimited as computing batch size is computationally expensive.
        if self.arrow_max_bytes_per_batch != 2**31 - 1 and batch.num_rows > 0:
            batch_bytes = sum(
                buf.size for col in batch.columns for buf in col.buffers() if buf is not None
            )
            self.total_bytes += batch_bytes
            self.total_rows += batch.num_rows
            self.average_arrow_row_size = self.total_bytes / self.total_rows

    def load_stream(self, stream):
        """
        Read ArrowRecordBatches from stream, deserialize them to populate a list of data chunk, and
        convert the data into Rows.

        Please refer the doc of inner function `generate_data_batches` for more details how
        this function works in overall.
        """
        import pandas as pd
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )

        def generate_data_batches(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of Rows.

            The deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.

            This function must avoid materializing multiple Arrow RecordBatches into memory at the
            same time. And data chunks from the same grouping key should appear sequentially.
            """

            def row_stream():
                for batch in batches:
                    self._update_batch_size_stats(batch)
                    data_pandas = ArrowBatchTransformer.to_pandas(
                        batch,
                        timezone=self._timezone,
                        schema=self._input_type,
                        struct_in_pandas=self._struct_in_pandas,
                        ndarray_as_list=self._ndarray_as_list,
                        df_for_struct=self._df_for_struct,
                    )
                    for row in pd.concat(data_pandas, axis=1).itertuples(index=False):
                        batch_key = tuple(row[s] for s in self.key_offsets)
                        yield (batch_key, row)

            for batch_key, group_rows in groupby(row_stream(), key=lambda x: x[0]):
                rows = []
                for _, row in group_rows:
                    rows.append(row)
                    if (
                        len(rows) >= self.arrow_max_records_per_batch
                        or len(rows) * self.average_arrow_row_size >= self.arrow_max_bytes_per_batch
                    ):
                        yield (batch_key, pd.DataFrame(rows))
                        rows = []
                if rows:
                    yield (batch_key, pd.DataFrame(rows))

        _batches = super().load_stream(stream)
        data_batches = generate_data_batches(_batches)

        for k, g in groupby(data_batches, key=lambda x: x[0]):
            yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, g)

        yield (TransformWithStateInPandasFuncMode.PROCESS_TIMER, None, None)

        yield (TransformWithStateInPandasFuncMode.COMPLETE, None, None)

    def dump_stream(self, iterator, stream):
        """
        Read through an iterator of (iterator of pandas DataFrame), serialize them to Arrow
        RecordBatches, and write batches to stream.
        """
        from pyspark.sql.conversion import ArrowBatchTransformer

        def create_batches():
            # iterator: iter[list[(iter[pandas.DataFrame], arrow_return_type)]]
            for packed in iterator:
                iter_pdf, arrow_return_type = packed[0]
                for pdf in iter_pdf:
                    # Extract columns from DataFrame and pair with Arrow types
                    # Similar to wrap_grouped_map_pandas_udf pattern
                    if self._assign_cols_by_name and any(
                        isinstance(name, str) for name in pdf.columns
                    ):
                        series_list = [(pdf[field.name], field.type) for field in arrow_return_type]
                    else:
                        series_list = [
                            (pdf[pdf.columns[i]].rename(field.name), field.type)
                            for i, field in enumerate(arrow_return_type)
                        ]
                    batch = PandasBatchTransformer.to_arrow(
                        series_list,
                        timezone=self._timezone,
                        safecheck=self._safecheck,
                        int_to_decimal_coercion_enabled=self._int_to_decimal_coercion_enabled,
                        arrow_cast=self._arrow_cast,
                    )
                    # Wrap columns as struct for JVM compatibility
                    yield ArrowBatchTransformer.wrap_struct(batch)

        # Write START_ARROW_STREAM marker before first batch
        batches = self._write_stream_start(create_batches(), stream)
        ArrowStreamSerializer.dump_stream(self, batches, stream)


class TransformWithStateInPandasInitStateSerializer(TransformWithStateInPandasSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithStateInPandasInitStateSerializer`.
    Parameters
    ----------
    Same as input parameters in TransformWithStateInPandasSerializer.
    """

    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        arrow_max_records_per_batch,
        arrow_max_bytes_per_batch,
        int_to_decimal_coercion_enabled,
    ):
        super().__init__(
            timezone,
            safecheck,
            assign_cols_by_name,
            arrow_max_records_per_batch,
            arrow_max_bytes_per_batch,
            int_to_decimal_coercion_enabled,
        )
        self.init_key_offsets = None

    def load_stream(self, stream):
        import pyarrow as pa
        import pandas as pd
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )

        def generate_data_batches(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of pandas.Series list.

            The deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.
            See `TransformWithStateInPandasPythonInitialStateRunner` for arrow batch schema sent
             from JVM.
            This function flatten the columns of input rows and initial state rows and feed them
             into the data generator.
            """

            def flatten_columns(cur_batch, col_name):
                state_column = cur_batch.column(cur_batch.schema.get_field_index(col_name))

                # Check if the entire column is null
                if state_column.null_count == len(state_column):
                    # Return empty table with no columns
                    return pa.Table.from_arrays([], names=[])

                state_field_names = [
                    state_column.type[i].name for i in range(state_column.type.num_fields)
                ]
                state_field_arrays = [
                    state_column.field(i) for i in range(state_column.type.num_fields)
                ]
                table_from_fields = pa.Table.from_arrays(
                    state_field_arrays, names=state_field_names
                )
                return table_from_fields

            """
            The arrow batch is written in the schema:
            schema: StructType = new StructType()
                .add("inputData", dataSchema)
                .add("initState", initStateSchema)
            We'll parse batch into Tuples of (key, inputData, initState) and pass into the Python
             data generator. Rows in the same batch may have different grouping keys,
             but each batch will have either init_data or input_data, not mix.
            """

            def to_pandas(table):
                return ArrowBatchTransformer.to_pandas(
                    table,
                    timezone=self._timezone,
                    schema=self._input_type,
                    struct_in_pandas=self._struct_in_pandas,
                    ndarray_as_list=self._ndarray_as_list,
                    df_for_struct=self._df_for_struct,
                )

            def row_stream():
                for batch in batches:
                    self._update_batch_size_stats(batch)

                    data_pandas = to_pandas(flatten_columns(batch, "inputData"))
                    init_data_pandas = to_pandas(flatten_columns(batch, "initState"))

                    assert not (bool(init_data_pandas) and bool(data_pandas))

                    if bool(data_pandas):
                        for row in pd.concat(data_pandas, axis=1).itertuples(index=False):
                            batch_key = tuple(row[s] for s in self.key_offsets)
                            yield (batch_key, row, None)
                    elif bool(init_data_pandas):
                        for row in pd.concat(init_data_pandas, axis=1).itertuples(index=False):
                            batch_key = tuple(row[s] for s in self.init_key_offsets)
                            yield (batch_key, None, row)

            EMPTY_DATAFRAME = pd.DataFrame()
            for batch_key, group_rows in groupby(row_stream(), key=lambda x: x[0]):
                rows = []
                init_state_rows = []
                for _, row, init_state_row in group_rows:
                    if row is not None:
                        rows.append(row)
                    if init_state_row is not None:
                        init_state_rows.append(init_state_row)

                    total_len = len(rows) + len(init_state_rows)
                    if (
                        total_len >= self.arrow_max_records_per_batch
                        or total_len * self.average_arrow_row_size >= self.arrow_max_bytes_per_batch
                    ):
                        yield (
                            batch_key,
                            pd.DataFrame(rows) if len(rows) > 0 else EMPTY_DATAFRAME.copy(),
                            pd.DataFrame(init_state_rows)
                            if len(init_state_rows) > 0
                            else EMPTY_DATAFRAME.copy(),
                        )
                        rows = []
                        init_state_rows = []
                if rows or init_state_rows:
                    yield (
                        batch_key,
                        pd.DataFrame(rows) if len(rows) > 0 else EMPTY_DATAFRAME.copy(),
                        pd.DataFrame(init_state_rows)
                        if len(init_state_rows) > 0
                        else EMPTY_DATAFRAME.copy(),
                    )

        # Call ArrowStreamSerializer.load_stream directly to get raw Arrow batches
        # (not the parent's load_stream which returns processed data with mode info)
        _batches = ArrowStreamSerializer.load_stream(self, stream)
        data_batches = generate_data_batches(_batches)

        for k, g in groupby(data_batches, key=lambda x: x[0]):
            yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, g)

        yield (TransformWithStateInPandasFuncMode.PROCESS_TIMER, None, None)

        yield (TransformWithStateInPandasFuncMode.COMPLETE, None, None)


class TransformWithStateInPySparkRowSerializer(ArrowStreamGroupSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithState`.

    Parameters
    ----------
    arrow_max_records_per_batch : int
        Limit of the number of records that can be written to a single ArrowRecordBatch in memory.
    """

    def __init__(self, arrow_max_records_per_batch):
        super().__init__()
        self.arrow_max_records_per_batch = (
            arrow_max_records_per_batch if arrow_max_records_per_batch > 0 else 2**31 - 1
        )
        self.key_offsets = None

    def load_stream(self, stream):
        """
        Read ArrowRecordBatches from stream, deserialize them to populate a list of data chunks,
        and convert the data into a list of pandas.Series.

        Please refer the doc of inner function `generate_data_batches` for more details how
        this function works in overall.
        """
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )
        import itertools

        def generate_data_batches(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of Row.

            The deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.

            This function must avoid materializing multiple Arrow RecordBatches into memory at the
            same time. And data chunks from the same grouping key should appear sequentially.
            """
            for batch in batches:
                DataRow = Row(*batch.schema.names)

                # Iterate row by row without converting the whole batch
                num_cols = batch.num_columns
                for row_idx in range(batch.num_rows):
                    # build the key for this row
                    row_key = tuple(batch[o][row_idx].as_py() for o in self.key_offsets)
                    row = DataRow(*(batch.column(i)[row_idx].as_py() for i in range(num_cols)))
                    yield row_key, row

        _batches = super().load_stream(stream)
        data_batches = generate_data_batches(_batches)

        for k, g in groupby(data_batches, key=lambda x: x[0]):
            chained = itertools.chain(g)
            chained_values = map(lambda x: x[1], chained)
            yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, chained_values)

        yield (TransformWithStateInPandasFuncMode.PROCESS_TIMER, None, None)

        yield (TransformWithStateInPandasFuncMode.COMPLETE, None, None)

    def dump_stream(self, iterator, stream):
        """
        Read through an iterator of (iterator of Row), serialize them to Arrow
        RecordBatches, and write batches to stream.
        """
        import pyarrow as pa

        def flatten_iterator():
            # iterator: iter[list[(iter[Row], pdf_type)]]
            for packed in iterator:
                iter_row_with_type = packed[0]
                iter_row = iter_row_with_type[0]
                pdf_type = iter_row_with_type[1]

                rows_as_dict = []
                for row in iter_row:
                    row_as_dict = row.asDict(True)
                    rows_as_dict.append(row_as_dict)

                pdf_schema = pa.schema(list(pdf_type))
                record_batch = pa.RecordBatch.from_pylist(rows_as_dict, schema=pdf_schema)

                # Wrap the batch into a struct before yielding
                wrapped_batch = ArrowBatchTransformer.wrap_struct(record_batch)
                yield wrapped_batch

        return ArrowStreamGroupSerializer.dump_stream(self, flatten_iterator(), stream)


class TransformWithStateInPySparkRowInitStateSerializer(TransformWithStateInPySparkRowSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithStateInPySparkRowInitStateSerializer`.
    Parameters
    ----------
    Same as input parameters in TransformWithStateInPySparkRowSerializer.
    """

    def __init__(self, arrow_max_records_per_batch):
        super().__init__(arrow_max_records_per_batch)
        self.init_key_offsets = None

    def load_stream(self, stream):
        import pyarrow as pa
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )
        from typing import Iterator, Any, Optional, Tuple

        def generate_data_batches(batches) -> Iterator[Tuple[Any, Optional[Any], Optional[Any]]]:
            """
            Deserialize ArrowRecordBatches and return a generator of Row.
            The deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.
            See `TransformWithStateInPySparkPythonInitialStateRunner` for arrow batch schema sent
             from JVM.
            This function flattens the columns of input rows and initial state rows and feed them
             into the data generator.
            """

            def extract_rows(
                cur_batch, col_name, key_offsets
            ) -> Optional[Iterator[Tuple[Any, Any]]]:
                data_column = cur_batch.column(cur_batch.schema.get_field_index(col_name))

                # Check if the entire column is null
                if data_column.null_count == len(data_column):
                    return None

                data_field_names = [
                    data_column.type[i].name for i in range(data_column.type.num_fields)
                ]
                data_field_arrays = [
                    data_column.field(i) for i in range(data_column.type.num_fields)
                ]

                DataRow = Row(*data_field_names)

                table = pa.Table.from_arrays(data_field_arrays, names=data_field_names)

                if table.num_rows == 0:
                    return None

                def row_iterator():
                    for row_idx in range(table.num_rows):
                        key = tuple(table.column(o)[row_idx].as_py() for o in key_offsets)
                        row = DataRow(
                            *(table.column(i)[row_idx].as_py() for i in range(table.num_columns))
                        )
                        yield (key, row)

                return row_iterator()

            """
            The arrow batch is written in the schema:
            schema: StructType = new StructType()
                .add("inputData", dataSchema)
                .add("initState", initStateSchema)
            We'll parse batch into Tuples of (key, inputData, initState) and pass into the Python
             data generator. Each batch will have either init_data or input_data, not mix.
            """
            for batch in batches:
                # Detect which column has data - each batch contains only one type
                input_result = extract_rows(batch, "inputData", self.key_offsets)
                init_result = extract_rows(batch, "initState", self.init_key_offsets)

                assert not (input_result is not None and init_result is not None)

                if input_result is not None:
                    for key, input_data_row in input_result:
                        yield (key, input_data_row, None)
                elif init_result is not None:
                    for key, init_state_row in init_result:
                        yield (key, None, init_state_row)

        _batches = super().load_stream(stream)
        data_batches = generate_data_batches(_batches)

        for k, g in groupby(data_batches, key=lambda x: x[0]):
            input_rows = []
            init_rows = []

            for batch_key, input_row, init_row in g:
                if input_row is not None:
                    input_rows.append(input_row)
                if init_row is not None:
                    init_rows.append(init_row)

                total_len = len(input_rows) + len(init_rows)
                if total_len >= self.arrow_max_records_per_batch:
                    ret_tuple = (iter(input_rows), iter(init_rows))
                    yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, ret_tuple)
                    input_rows = []
                    init_rows = []

            if input_rows or init_rows:
                ret_tuple = (iter(input_rows), iter(init_rows))
                yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, ret_tuple)

        yield (TransformWithStateInPandasFuncMode.PROCESS_TIMER, None, None)

        yield (TransformWithStateInPandasFuncMode.COMPLETE, None, None)
