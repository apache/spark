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

from typing import IO, TYPE_CHECKING, Iterable, Iterator, List, Optional, Tuple

from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.serializers import (
    Serializer,
    read_int,
    write_int,
    UTF8Deserializer,
)
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
