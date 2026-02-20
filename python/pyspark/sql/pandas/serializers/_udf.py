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
UDF and UDTF serializer classes for Arrow/pandas conversions.
"""

from typing import TYPE_CHECKING, List, Optional, Tuple

import pyspark
from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.sql.conversion import (
    ArrowTableToRowsConversion,
    ArrowBatchTransformer,
    LocalDataToArrowConversion,
    PandasToArrowConversion,
    coerce_arrow_array,
)
from pyspark.sql.types import DataType, StructType, StructField

from pyspark.sql.pandas.serializers._base import (
    ArrowStreamSerializer,
    _normalize_packed,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


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


class ArrowStreamUDTFSerializer(ArrowStreamUDFSerializer):
    """
    Same as :class:`ArrowStreamUDFSerializer` but it does not flatten when loading batches.
    """

    def load_stream(self, stream):
        return ArrowStreamSerializer.load_stream(self, stream)


class ArrowStreamArrowUDTFSerializer(ArrowStreamUDTFSerializer):
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

                # Handle empty struct case specially
                if batch.num_columns == 0:
                    coerced_batch = batch  # skip type coercion
                else:
                    expected_field_names = [field.name for field in arrow_return_type]
                    actual_field_names = batch.schema.names

                    if expected_field_names != actual_field_names:
                        raise PySparkTypeError(
                            "Target schema's field names are not matching the record batch's "
                            "field names. "
                            f"Expected: {expected_field_names}, but got: {actual_field_names}."
                        )

                    coerced_arrays = []
                    for i, field in enumerate(arrow_return_type):
                        try:
                            coerced_arrays.append(
                                coerce_arrow_array(
                                    batch.column(i),
                                    field.type,
                                    safecheck=True,
                                )
                            )
                        except (pa.ArrowInvalid, pa.ArrowTypeError):
                            raise PySparkRuntimeError(
                                errorClass="RESULT_COLUMNS_MISMATCH_FOR_ARROW_UDTF",
                                messageParameters={
                                    "expected": str(field.type),
                                    "actual": str(batch.column(i).type),
                                },
                            )
                    coerced_batch = pa.RecordBatch.from_arrays(
                        coerced_arrays, names=expected_field_names
                    )
                yield coerced_batch, arrow_return_type

        return super().dump_stream(apply_type_coercion(), stream)


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
    df_for_struct : bool, optional
        If True, convert struct columns to DataFrame instead of Series. Default is False.
    """

    def __init__(
        self,
        timezone,
        safecheck,
        int_to_decimal_coercion_enabled: bool = False,
        prefers_large_types: bool = False,
        struct_in_pandas: str = "dict",
        ndarray_as_list: bool = False,
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
        timezone,
        safecheck,
        assign_cols_by_name,
        df_for_struct: bool = False,
        struct_in_pandas: str = "dict",
        ndarray_as_list: bool = False,
        arrow_cast: bool = False,
        input_type: Optional[StructType] = None,
        int_to_decimal_coercion_enabled: bool = False,
        prefers_large_types: bool = False,
        ignore_unexpected_complex_type_values: bool = False,
        is_udtf: bool = False,
    ):
        super().__init__(
            timezone,
            safecheck,
            int_to_decimal_coercion_enabled,
            prefers_large_types,
            struct_in_pandas,
            ndarray_as_list,
            df_for_struct,
            input_type,
            arrow_cast,
        )
        self._assign_cols_by_name = assign_cols_by_name
        self._ignore_unexpected_complex_type_values = ignore_unexpected_complex_type_values
        self._is_udtf = is_udtf

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
                is_udtf=self._is_udtf,
            )

        batches = self._write_stream_start(
            (create_batch(_normalize_packed(packed)) for packed in iterator),
            stream,
        )
        return ArrowStreamSerializer.dump_stream(self, batches, stream)

    def __repr__(self):
        return "ArrowStreamPandasUDFSerializer"


class ArrowStreamArrowUDFSerializer(ArrowStreamSerializer):
    """
    Serializer used by Python worker to evaluate Arrow UDFs
    """

    def __init__(
        self,
        safecheck,
        arrow_cast,
    ):
        super().__init__()
        self._safecheck = safecheck
        self._arrow_cast = arrow_cast

    def dump_stream(self, iterator, stream):
        """
        Override because Arrow UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """
        import pyarrow as pa

        def create_batch(
            arr_tuples: List[Tuple["pa.Array", "pa.DataType"]],
        ) -> "pa.RecordBatch":
            arrs = [
                coerce_arrow_array(
                    arr, arrow_type, safecheck=self._safecheck, arrow_cast=self._arrow_cast
                )
                for arr, arrow_type in arr_tuples
            ]
            return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

        def normalize(packed):
            if len(packed) == 2 and isinstance(packed[1], pa.DataType):
                return [packed]
            return list(packed)

        batches = self._write_stream_start(
            (create_batch(normalize(packed)) for packed in iterator), stream
        )
        return ArrowStreamSerializer.dump_stream(self, batches, stream)

    def __repr__(self):
        return "ArrowStreamArrowUDFSerializer"


class ArrowBatchUDFSerializer(ArrowStreamArrowUDFSerializer):
    """
    Serializer used by Python worker to evaluate Arrow Python UDFs
    when the legacy pandas conversion is disabled
    (instead of legacy ArrowStreamPandasUDFSerializer).

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
        super().__init__(
            safecheck=safecheck,
            arrow_cast=True,
        )
        assert isinstance(input_type, StructType)
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
            columns = [
                [conv(v) for v in column.to_pylist()] if conv is not None else column.to_pylist()
                for column, conv in zip(batch.itercolumns(), converters)
            ]
            if len(columns) == 0:
                yield [[pyspark._NoValue] * batch.num_rows]
            else:
                yield columns

    def dump_stream(self, iterator, stream):
        """
        Dumps an iterator of Python values as a stream of Arrow record batches.

        Parameters
        ----------
        iterator : iterator
            Iterator yielding tuple of (data, arrow_type, spark_type) tuples.
            Single UDF: ((results, arrow_type, spark_type),)
            Multiple UDFs: ((r1, t1, s1), (r2, t2, s2), ...)
        stream : object
            Output stream to write the Arrow record batches

        Returns
        -------
        object
            Result of writing the Arrow stream via ArrowStreamArrowUDFSerializer dump_stream
        """
        import pyarrow as pa

        def create_array(results, arrow_type, spark_type):
            conv = LocalDataToArrowConversion._create_converter(
                spark_type,
                none_on_identity=True,
                int_to_decimal_coercion_enabled=self._int_to_decimal_coercion_enabled,
            )
            converted = [conv(res) for res in results] if conv is not None else results
            try:
                return pa.array(converted, type=arrow_type)
            except pa.lib.ArrowInvalid:
                return pa.array(converted).cast(target_type=arrow_type, safe=self._safecheck)

        def py_to_batch():
            for packed in iterator:
                if len(packed) == 3 and isinstance(packed[1], pa.DataType):
                    # single array UDF in a projection
                    yield create_array(packed[0], packed[1], packed[2]), packed[1]
                else:
                    # multiple array UDFs in a projection
                    yield [(create_array(*t), t[1]) for t in packed]

        return super().dump_stream(py_to_batch(), stream)


class ArrowStreamPandasUDTFSerializer(ArrowStreamPandasUDFSerializer):
    """
    Serializer used by Python worker to evaluate Arrow-optimized Python UDTFs.
    """

    def __init__(self, timezone, safecheck, input_type, int_to_decimal_coercion_enabled):
        super().__init__(
            timezone=timezone,
            safecheck=safecheck,
            # The output pandas DataFrame's columns are unnamed.
            assign_cols_by_name=False,
            # Set to 'False' to avoid converting struct type inputs into a pandas DataFrame.
            df_for_struct=False,
            # Defines how struct type inputs are converted. If set to "row", struct type inputs
            # are converted into Rows. Without this setting, a struct type input would be treated
            # as a dictionary. For example, for named_struct('name', 'Alice', 'age', 1),
            # if struct_in_pandas="dict", it becomes {"name": "Alice", "age": 1}
            # if struct_in_pandas="row", it becomes Row(name="Alice", age=1)
            struct_in_pandas="row",
            # When dealing with array type inputs, Arrow converts them into numpy.ndarrays.
            # To ensure consistency across regular and arrow-optimized UDTFs, we further
            # convert these numpy.ndarrays into Python lists.
            ndarray_as_list=True,
            # Enables explicit casting for mismatched return types of Arrow Python UDTFs.
            arrow_cast=True,
            input_type=input_type,
            # Enable additional coercions for UDTF serialization
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            # UDTF-specific: ignore unexpected complex type values in converter
            ignore_unexpected_complex_type_values=True,
            # UDTF-specific: enables broader Arrow exception handling and
            # converts errors to UDTF_ARROW_TYPE_CAST_ERROR
            is_udtf=True,
        )

    def __repr__(self):
        return "ArrowStreamPandasUDTFSerializer"
