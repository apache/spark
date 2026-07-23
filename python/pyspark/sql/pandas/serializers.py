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
from typing import IO, TYPE_CHECKING, Any, Iterable, Iterator, List, Optional, Tuple

from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.serializers import (
    Serializer,
    read_int,
    write_int,
    UTF8Deserializer,
)
from pyspark.sql import Row
from pyspark.sql.conversion import (
    ArrowBatchTransformer,
    PandasToArrowConversion,
)
from pyspark.sql.types import (
    DataType,
    StructType,
    StructField,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


def _normalize_packed(packed):
    """
    Normalize UDF output to a uniform tuple-of-tuples form.

    Iterator UDFs yield a single (series, spark_type) tuple directly,
    while batched UDFs return a tuple of tuples ((s1, t1), (s2, t2), ...).
    This function normalizes both forms to a tuple of tuples.
    """
    if len(packed) == 2 and isinstance(packed[1], DataType):
        return (packed,)
    return tuple(packed)


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
    Serializes Arrow record batches as a plain stream.

    Parameters
    ----------
    write_start_stream : bool
        If True, writes the START_ARROW_STREAM marker before the first
        output batch. Default False.
    """

    def __init__(self, write_start_stream: bool = False, flush_per_batch: bool = False) -> None:
        super().__init__()
        self._write_start_stream: bool = write_start_stream
        self._flush_per_batch: bool = flush_per_batch

    def dump_stream(self, iterator: Iterable["pa.RecordBatch"], stream: IO[bytes]) -> None:
        """Optionally prepend START_ARROW_STREAM, then write batches."""
        iterator = iter(iterator)
        if self._write_start_stream:
            iterator = self._write_stream_start(iterator, stream)
        import pyarrow as pa

        writer = None
        try:
            for batch in iterator:
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
                # In pipelined mode, flush after each batch so the JVM can read output
                # while still sending input, rather than buffering all output.
                if self._flush_per_batch:
                    stream.flush()
        finally:
            if writer is not None:
                writer.close()

    def load_stream(self, stream: IO[bytes]) -> Iterator["pa.RecordBatch"]:
        """Load batches from a plain Arrow stream."""
        import pyarrow as pa

        reader = pa.ipc.open_stream(stream)
        for batch in reader:
            yield batch

    def _write_stream_start(
        self, batch_iterator: Iterator["pa.RecordBatch"], stream: IO[bytes]
    ) -> Iterator["pa.RecordBatch"]:
        """Write START_ARROW_STREAM before the first batch, then pass batches through."""
        import itertools

        first = next(batch_iterator, None)
        if first is None:
            return

        # Signal the JVM after the first batch succeeds, so errors during
        # batch creation can be reported before the Arrow stream starts.
        write_int(SpecialLengths.START_ARROW_STREAM, stream)
        yield from itertools.chain([first], batch_iterator)

    def __repr__(self) -> str:
        return "ArrowStreamSerializer(write_start_stream=%s)" % self._write_start_stream


class ArrowStreamGroupSerializer(ArrowStreamSerializer):
    """
    Extends :class:`ArrowStreamSerializer` with group-count protocol for loading
    grouped Arrow record batches (1 dataframe per group).
    """

    def load_stream(self, stream: IO[bytes]) -> Iterator[Iterator["pa.RecordBatch"]]:
        """Yield one iterator of record batches per group from the stream."""
        while dataframes_in_group := read_int(stream):
            if dataframes_in_group == 1:
                yield ArrowStreamSerializer.load_stream(self, stream)
            elif dataframes_in_group > 0:
                raise PySparkValueError(
                    errorClass="INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP",
                    messageParameters={"dataframes_in_group": str(dataframes_in_group)},
                )


class ArrowStreamCoGroupSerializer(ArrowStreamSerializer):
    """
    Extends :class:`ArrowStreamSerializer` with group-count protocol for loading
    cogrouped Arrow record batches (2 dataframes per group).
    """

    def load_stream(
        self, stream: IO[bytes]
    ) -> Iterator[Tuple[List["pa.RecordBatch"], List["pa.RecordBatch"]]]:
        """Yield pairs of (left_batches, right_batches) from the stream."""
        while dataframes_in_group := read_int(stream):
            if dataframes_in_group == 2:
                # Must eagerly load each dataframe to maintain correct stream position
                yield (
                    list(ArrowStreamSerializer.load_stream(self, stream)),
                    list(ArrowStreamSerializer.load_stream(self, stream)),
                )
            elif dataframes_in_group > 0:
                raise PySparkValueError(
                    errorClass="INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP",
                    messageParameters={"dataframes_in_group": str(dataframes_in_group)},
                )


class ArrowStreamUDFSerializer(ArrowStreamSerializer):
    """
    Same as :class:`ArrowStreamSerializer` but it flattens the struct to Arrow record batch
    for applying each function with the raw record arrow batch. See also `DataFrame.mapInArrow`.
    """

    def load_stream(self, stream):
        """
        Flatten the struct into Arrow's record batches.
        """
        batches = super().load_stream(stream)
        flattened = map(ArrowBatchTransformer.flatten_struct, batches)
        return map(lambda b: [b], flattened)

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        """
        batches = self._write_stream_start(
            (ArrowBatchTransformer.wrap_struct(x[0]) for x in iterator), stream
        )
        return super().dump_stream(batches, stream)


class ArrowStreamPandasSerializer(ArrowStreamSerializer):
    """
    Serializes pandas.Series as Arrow data with Arrow streaming format.

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    int_to_decimal_coercion_enabled : bool
        If True, applies additional coercions in Python before converting to Arrow.
        This has performance penalties.
    prefers_large_types : bool
        If True, prefer large Arrow types (e.g., large_string instead of string).
    struct_in_pandas : str, optional
        How to represent struct in pandas ("dict", "row", etc.). Default is "dict".
    ndarray_as_list : bool, optional
        Whether to convert ndarray as list. Default is False.
    prefer_int_ext_dtype : bool, optional
        Whether to convert integers to Pandas ExtensionDType. Default is False.
    df_for_struct : bool, optional
        If True, convert struct columns to DataFrame instead of Series. Default is False.
    """

    def __init__(
        self,
        *,
        timezone,
        safecheck,
        int_to_decimal_coercion_enabled: bool = False,
        prefers_large_types: bool = False,
        struct_in_pandas: str = "dict",
        ndarray_as_list: bool = False,
        prefer_int_ext_dtype: bool = False,
        df_for_struct: bool = False,
        input_type: Optional["StructType"] = None,
        arrow_cast: bool = False,
    ):
        super().__init__()
        self._timezone = timezone
        self._safecheck = safecheck
        self._int_to_decimal_coercion_enabled = int_to_decimal_coercion_enabled
        self._prefers_large_types = prefers_large_types
        self._struct_in_pandas = struct_in_pandas
        self._ndarray_as_list = ndarray_as_list
        self._prefer_int_ext_dtype = prefer_int_ext_dtype
        self._df_for_struct = df_for_struct
        if input_type is not None:
            assert isinstance(input_type, StructType)
        self._input_type = input_type
        self._arrow_cast = arrow_cast

    def dump_stream(self, iterator, stream):
        """
        Make ArrowRecordBatches from Pandas Series and serialize.
        Each element in iterator is:
        - For batched UDFs: tuple of (series, spark_type) tuples: ((s1, t1), (s2, t2), ...)
        - For iterator UDFs: single (series, spark_type) tuple directly
        """

        def create_batch(
            series_tuples: Tuple[Tuple["pd.Series", DataType], ...],
        ) -> "pa.RecordBatch":
            series_data = [s for s, _ in series_tuples]
            types = [t for _, t in series_tuples]
            schema = StructType([StructField(f"_{i}", t) for i, t in enumerate(types)])
            return PandasToArrowConversion.convert(
                series_data,
                schema,
                timezone=self._timezone,
                safecheck=self._safecheck,
                prefers_large_types=self._prefers_large_types,
                int_to_decimal_coercion_enabled=self._int_to_decimal_coercion_enabled,
            )

        super().dump_stream(
            (create_batch(_normalize_packed(packed)) for packed in iterator), stream
        )

    def load_stream(self, stream):
        """
        Deserialize ArrowRecordBatches to an Arrow table and return as a list of pandas.Series.
        """
        yield from map(
            lambda batch: ArrowBatchTransformer.to_pandas(
                batch,
                timezone=self._timezone,
                schema=self._input_type,
                struct_in_pandas=self._struct_in_pandas,
                ndarray_as_list=self._ndarray_as_list,
                prefer_int_ext_dtype=self._prefer_int_ext_dtype,
                df_for_struct=self._df_for_struct,
            ),
            super().load_stream(stream),
        )

    def __repr__(self):
        return "ArrowStreamPandasSerializer"


class ArrowStreamPandasUDFSerializer(ArrowStreamPandasSerializer):
    """
    Serializer used by Python worker to evaluate Pandas UDFs
    """

    def __init__(
        self,
        *,
        timezone,
        safecheck,
        assign_cols_by_name,
        df_for_struct: bool = False,
        struct_in_pandas: str = "dict",
        ndarray_as_list: bool = False,
        prefer_int_ext_dtype: bool = False,
        arrow_cast: bool = False,
        input_type: Optional[StructType] = None,
        int_to_decimal_coercion_enabled: bool = False,
        prefers_large_types: bool = False,
        ignore_unexpected_complex_type_values: bool = False,
        is_legacy: bool = False,
    ):
        super().__init__(
            timezone=timezone,
            safecheck=safecheck,
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            prefers_large_types=prefers_large_types,
            struct_in_pandas=struct_in_pandas,
            ndarray_as_list=ndarray_as_list,
            prefer_int_ext_dtype=prefer_int_ext_dtype,
            df_for_struct=df_for_struct,
            input_type=input_type,
            arrow_cast=arrow_cast,
        )
        self._assign_cols_by_name = assign_cols_by_name
        self._ignore_unexpected_complex_type_values = ignore_unexpected_complex_type_values
        self._is_legacy = is_legacy

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.

        Each element in iterator is:
        - For batched UDFs: tuple of (series, spark_type) tuples: ((s1, t1), (s2, t2), ...)
        - For iterator UDFs: single (series, spark_type) tuple directly
        """
        import pandas as pd

        def create_batch(
            series_tuples: Tuple[Tuple["pd.Series", DataType], ...],
        ) -> "pa.RecordBatch":
            # When struct_in_pandas="dict", UDF must return DataFrame for struct types
            if self._struct_in_pandas == "dict":
                for s, spark_type in series_tuples:
                    if isinstance(spark_type, StructType) and not isinstance(s, pd.DataFrame):
                        raise PySparkValueError(
                            "Invalid return type. Please make sure that the UDF returns a "
                            "pandas.DataFrame when the specified return type is StructType."
                        )

            series_data = [s for s, _ in series_tuples]
            types = [t for _, t in series_tuples]
            schema = StructType([StructField(f"_{i}", t) for i, t in enumerate(types)])
            return PandasToArrowConversion.convert(
                series_data,
                schema,
                timezone=self._timezone,
                safecheck=self._safecheck,
                arrow_cast=self._arrow_cast,
                prefers_large_types=self._prefers_large_types,
                assign_cols_by_name=self._assign_cols_by_name,
                int_to_decimal_coercion_enabled=self._int_to_decimal_coercion_enabled,
                ignore_unexpected_complex_type_values=self._ignore_unexpected_complex_type_values,
                is_legacy=self._is_legacy,
            )

        batches = self._write_stream_start(
            (create_batch(_normalize_packed(packed)) for packed in iterator),
            stream,
        )
        return ArrowStreamSerializer.dump_stream(self, batches, stream)

    def __repr__(self):
        return "ArrowStreamPandasUDFSerializer"


class TransformWithStateInPySparkRowSerializer(ArrowStreamUDFSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithState`.

    Parameters
    ----------
    arrow_max_records_per_batch : int
        Limit of the number of records that can be written to a single ArrowRecordBatch in memory.
    """

    def __init__(self, *, arrow_max_records_per_batch):
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

        _batches = super(ArrowStreamUDFSerializer, self).load_stream(stream)
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

        from pyspark.sql.pandas.types import to_arrow_type

        def flatten_iterator():
            # iterator: iter[list[(iter[Row], spark_type)]]
            for packed in iterator:
                iter_row_with_type = packed[0]
                iter_row = iter_row_with_type[0]
                spark_type = iter_row_with_type[1]

                # Convert spark type to arrow type
                # TODO: WE need to make this configurable, currently using default values.
                arrow_type = to_arrow_type(
                    spark_type,
                    timezone="UTC",
                    prefers_large_types=False,
                )

                rows_as_dict = []
                for row in iter_row:
                    row_as_dict = row.asDict(True)
                    rows_as_dict.append(row_as_dict)

                pdf_schema = pa.schema(list(arrow_type))
                record_batch = pa.RecordBatch.from_pylist(rows_as_dict, schema=pdf_schema)

                yield (record_batch, arrow_type)

        return ArrowStreamUDFSerializer.dump_stream(self, flatten_iterator(), stream)


class TransformWithStateInPySparkRowInitStateSerializer(TransformWithStateInPySparkRowSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithStateInPySparkRowInitStateSerializer`.
    Parameters
    ----------
    Same as input parameters in TransformWithStateInPySparkRowSerializer.
    """

    def __init__(self, *, arrow_max_records_per_batch):
        super().__init__(arrow_max_records_per_batch=arrow_max_records_per_batch)
        self.init_key_offsets = None

    def load_stream(self, stream):
        import pyarrow as pa
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )

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

        _batches = super(ArrowStreamUDFSerializer, self).load_stream(stream)
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
