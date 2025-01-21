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
import sys
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Union,
    no_type_check,
    overload,
    TYPE_CHECKING,
)
from warnings import warn

from pyspark.errors.exceptions.captured import unwrap_spark_exception
from pyspark.loose_version import LooseVersion
from pyspark.util import _load_from_socket
from pyspark.sql.pandas.serializers import ArrowCollectSerializer
from pyspark.sql.pandas.types import _dedup_names
from pyspark.sql.types import (
    ArrayType,
    MapType,
    TimestampType,
    StructType,
    DataType,
    _create_row,
    StringType,
)
from pyspark.sql.utils import is_timestamp_ntz_preferred
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.errors import PySparkTypeError, PySparkValueError

if TYPE_CHECKING:
    import numpy as np
    import pyarrow as pa
    from py4j.java_gateway import JavaObject

    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike
    from pyspark.sql import DataFrame


class PandasConversionMixin:
    """
    Mix-in for the conversion from Spark to pandas and PyArrow. Currently, only
    :class:`DataFrame` can use this class.
    """

    def toPandas(self) -> "PandasDataFrameLike":
        from pyspark.sql.dataframe import DataFrame

        assert isinstance(self, DataFrame)

        from pyspark.sql.pandas.types import _create_converter_to_pandas
        from pyspark.sql.pandas.utils import require_minimum_pandas_version

        require_minimum_pandas_version()

        import pandas as pd

        jconf = self.sparkSession._jconf

        if jconf.arrowPySparkEnabled():
            use_arrow = True
            try:
                from pyspark.sql.pandas.types import to_arrow_schema
                from pyspark.sql.pandas.utils import require_minimum_pyarrow_version

                require_minimum_pyarrow_version()
                to_arrow_schema(self.schema)
            except Exception as e:
                if jconf.arrowPySparkFallbackEnabled():
                    msg = (
                        "toPandas attempted Arrow optimization because "
                        "'spark.sql.execution.arrow.pyspark.enabled' is set to true; however, "
                        "failed by the reason below:\n  %s\n"
                        "Attempting non-optimization as "
                        "'spark.sql.execution.arrow.pyspark.fallback.enabled' is set to "
                        "true." % str(e)
                    )
                    warn(msg)
                    use_arrow = False
                else:
                    msg = (
                        "toPandas attempted Arrow optimization because "
                        "'spark.sql.execution.arrow.pyspark.enabled' is set to true, but has "
                        "reached the error below and will not continue because automatic fallback "
                        "with 'spark.sql.execution.arrow.pyspark.fallback.enabled' has been set to "
                        "false.\n  %s" % str(e)
                    )
                    warn(msg)
                    raise

            # Try to use Arrow optimization when the schema is supported and the required version
            # of PyArrow is found, if 'spark.sql.execution.arrow.pyspark.enabled' is enabled.
            if use_arrow:
                try:
                    import pyarrow as pa

                    self_destruct = jconf.arrowPySparkSelfDestructEnabled()
                    batches = self._collect_as_arrow(split_batches=self_destruct)
                    if len(batches) > 0:
                        table = pa.Table.from_batches(batches)
                        # Ensure only the table has a reference to the batches, so that
                        # self_destruct (if enabled) is effective
                        del batches
                        # Pandas DataFrame created from PyArrow uses datetime64[ns] for date type
                        # values, but we should use datetime.date to match the behavior with when
                        # Arrow optimization is disabled.
                        pandas_options = {"date_as_object": True}

                        if LooseVersion(pa.__version__) >= LooseVersion("13.0.0"):
                            # A legacy option to coerce date32, date64, duration, and timestamp
                            # time units to nanoseconds when converting to pandas.
                            # This option can only be added since 13.0.0.
                            pandas_options.update(
                                {
                                    "coerce_temporal_nanoseconds": True,
                                }
                            )

                        if self_destruct:
                            # Configure PyArrow to use as little memory as possible:
                            # self_destruct - free columns as they are converted
                            # split_blocks - create a separate Pandas block for each column
                            # use_threads - convert one column at a time
                            pandas_options.update(
                                {
                                    "self_destruct": True,
                                    "split_blocks": True,
                                    "use_threads": False,
                                }
                            )
                        # Rename columns to avoid duplicated column names.
                        pdf = table.rename_columns(
                            [f"col_{i}" for i in range(table.num_columns)]
                        ).to_pandas(**pandas_options)

                        # Rename back to the original column names.
                        pdf.columns = self.columns
                    else:
                        pdf = pd.DataFrame(columns=self.columns)

                    if len(pdf.columns) > 0:
                        timezone = jconf.sessionLocalTimeZone()
                        struct_in_pandas = jconf.pandasStructHandlingMode()

                        error_on_duplicated_field_names = False
                        if struct_in_pandas == "legacy":
                            error_on_duplicated_field_names = True
                            struct_in_pandas = "dict"

                        return pd.concat(
                            [
                                _create_converter_to_pandas(
                                    field.dataType,
                                    field.nullable,
                                    timezone=timezone,
                                    struct_in_pandas=struct_in_pandas,
                                    error_on_duplicated_field_names=error_on_duplicated_field_names,
                                )(pser)
                                for (_, pser), field in zip(pdf.items(), self.schema.fields)
                            ],
                            axis="columns",
                        )
                    else:
                        return pdf
                except Exception as e:
                    # We might have to allow fallback here as well but multiple Spark jobs can
                    # be executed. So, simply fail in this case for now.
                    msg = (
                        "toPandas attempted Arrow optimization because "
                        "'spark.sql.execution.arrow.pyspark.enabled' is set to true, but has "
                        "reached the error below and can not continue. Note that "
                        "'spark.sql.execution.arrow.pyspark.fallback.enabled' does not have an "
                        "effect on failures in the middle of "
                        "computation.\n  %s" % str(e)
                    )
                    warn(msg)
                    raise

        # Below is toPandas without Arrow optimization.
        rows = self.collect()
        if len(rows) > 0:
            pdf = pd.DataFrame.from_records(
                rows, index=range(len(rows)), columns=self.columns  # type: ignore[arg-type]
            )
        else:
            pdf = pd.DataFrame(columns=self.columns)

        if len(pdf.columns) > 0:
            timezone = jconf.sessionLocalTimeZone()
            struct_in_pandas = jconf.pandasStructHandlingMode()

            return pd.concat(
                [
                    _create_converter_to_pandas(
                        field.dataType,
                        field.nullable,
                        timezone=timezone,
                        struct_in_pandas=(
                            "row" if struct_in_pandas == "legacy" else struct_in_pandas
                        ),
                        error_on_duplicated_field_names=False,
                        timestamp_utc_localized=False,
                    )(pser)
                    for (_, pser), field in zip(pdf.items(), self.schema.fields)
                ],
                axis="columns",
            )
        else:
            return pdf

    def toArrow(self) -> "pa.Table":
        from pyspark.sql.dataframe import DataFrame

        assert isinstance(self, DataFrame)

        jconf = self.sparkSession._jconf

        from pyspark.sql.pandas.types import to_arrow_schema
        from pyspark.sql.pandas.utils import require_minimum_pyarrow_version

        require_minimum_pyarrow_version()
        schema = to_arrow_schema(self.schema, error_on_duplicated_field_names_in_struct=True)

        import pyarrow as pa

        self_destruct = jconf.arrowPySparkSelfDestructEnabled()
        batches = self._collect_as_arrow(
            split_batches=self_destruct, empty_list_if_zero_records=False
        )
        table = pa.Table.from_batches(batches).cast(schema)
        # Ensure only the table has a reference to the batches, so that
        # self_destruct (if enabled) is effective
        del batches
        return table

    def _collect_as_arrow(
        self,
        split_batches: bool = False,
        empty_list_if_zero_records: bool = True,
    ) -> List["pa.RecordBatch"]:
        """
        Returns all records as a list of Arrow RecordBatches. PyArrow must be installed
        and available on driver and worker Python environments.
        This is an experimental feature.

        :param split_batches: split batches such that each column is in its own allocation, so
            that the selfDestruct optimization is effective; default False.

        :param empty_list_if_zero_records: If True (the default), returns an empty list if the
            result has 0 records. Otherwise, returns a list of length 1 containing an empty
            Arrow RecordBatch which includes the schema.

        .. note:: Experimental.
        """
        from pyspark.sql.dataframe import DataFrame

        assert isinstance(self, DataFrame)

        with SCCallSiteSync(self._sc):
            (
                port,
                auth_secret,
                jsocket_auth_server,
            ) = self._jdf.collectAsArrowToPython()

        # Collect list of un-ordered batches where last element is a list of correct order indices
        try:
            batch_stream = _load_from_socket((port, auth_secret), ArrowCollectSerializer())
            if split_batches:
                # When spark.sql.execution.arrow.pyspark.selfDestruct.enabled, ensure
                # each column in each record batch is contained in its own allocation.
                # Otherwise, selfDestruct does nothing; it frees each column as its
                # converted, but each column will actually be a list of slices of record
                # batches, and so no memory is actually freed until all columns are
                # converted.
                import pyarrow as pa

                results = []
                for batch_or_indices in batch_stream:
                    if isinstance(batch_or_indices, pa.RecordBatch):
                        batch_or_indices = pa.RecordBatch.from_arrays(
                            [
                                # This call actually reallocates the array
                                pa.concat_arrays([array])
                                for array in batch_or_indices
                            ],
                            schema=batch_or_indices.schema,
                        )
                    results.append(batch_or_indices)
            else:
                results = list(batch_stream)
        finally:
            with unwrap_spark_exception():
                # Join serving thread and raise any exceptions from collectAsArrowToPython
                jsocket_auth_server.getResult()

        # Separate RecordBatches from batch order indices in results
        batches = results[:-1]
        batch_order = results[-1]

        if len(batches) or empty_list_if_zero_records:
            # Re-order the batch list using the correct order
            return [batches[i] for i in batch_order]
        else:
            from pyspark.sql.pandas.types import to_arrow_schema
            import pyarrow as pa

            schema = to_arrow_schema(self.schema)
            empty_arrays = [pa.array([], type=field.type) for field in schema]
            return [pa.RecordBatch.from_arrays(empty_arrays, schema=schema)]


class SparkConversionMixin:
    """
    Min-in for the conversion from pandas and PyArrow to Spark. Currently, only
    :class:`SparkSession` can use this class.
    """

    _jsparkSession: "JavaObject"

    @overload
    def createDataFrame(
        self, data: "PandasDataFrameLike", samplingRatio: Optional[float] = ...
    ) -> "DataFrame":
        ...

    @overload
    def createDataFrame(
        self, data: "pa.Table", samplingRatio: Optional[float] = ...
    ) -> "DataFrame":
        ...

    @overload
    def createDataFrame(
        self,
        data: "PandasDataFrameLike",
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> "DataFrame":
        ...

    @overload
    def createDataFrame(
        self,
        data: "pa.Table",
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> "DataFrame":
        ...

    def createDataFrame(  # type: ignore[misc]
        self,
        data: Union["PandasDataFrameLike", "pa.Table"],
        schema: Optional[Union[StructType, List[str]]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True,
    ) -> "DataFrame":
        from pyspark.sql import SparkSession

        assert isinstance(self, SparkSession)

        timezone = self._jconf.sessionLocalTimeZone()

        if type(data).__name__ == "Table":
            # `data` is a PyArrow Table
            from pyspark.sql.pandas.utils import require_minimum_pyarrow_version

            require_minimum_pyarrow_version()

            import pyarrow as pa

            assert isinstance(data, pa.Table)

            # If no schema supplied by user then get the names of columns only
            if schema is None:
                schema = data.schema.names

            return self._create_from_arrow_table(data, schema, timezone)

        # `data` is a PandasDataFrameLike object
        from pyspark.sql.pandas.utils import require_minimum_pandas_version

        require_minimum_pandas_version()

        # If no schema supplied by user then get the names of columns only
        if schema is None:
            schema = [str(x) if not isinstance(x, str) else x for x in data.columns]

        if self._jconf.arrowPySparkEnabled() and len(data) > 0:
            try:
                return self._create_from_pandas_with_arrow(data, schema, timezone)
            except Exception as e:
                if self._jconf.arrowPySparkFallbackEnabled():
                    msg = (
                        "createDataFrame attempted Arrow optimization because "
                        "'spark.sql.execution.arrow.pyspark.enabled' is set to true; however, "
                        "failed by the reason below:\n  %s\n"
                        "Attempting non-optimization as "
                        "'spark.sql.execution.arrow.pyspark.fallback.enabled' is set to "
                        "true." % str(e)
                    )
                    warn(msg)
                else:
                    msg = (
                        "createDataFrame attempted Arrow optimization because "
                        "'spark.sql.execution.arrow.pyspark.enabled' is set to true, but has "
                        "reached the error below and will not continue because automatic "
                        "fallback with 'spark.sql.execution.arrow.pyspark.fallback.enabled' "
                        "has been set to false.\n  %s" % str(e)
                    )
                    warn(msg)
                    raise
        converted_data = self._convert_from_pandas(data, schema, timezone)
        return self._create_dataframe(converted_data, schema, samplingRatio, verifySchema)

    def _convert_from_pandas(
        self, pdf: "PandasDataFrameLike", schema: Union[StructType, str, List[str]], timezone: str
    ) -> List:
        """
        Convert a pandas.DataFrame to list of records that can be used to make a DataFrame

        Returns
        -------
        list
            list of records
        """
        from pyspark.sql import SparkSession

        assert isinstance(self, SparkSession)

        if timezone is not None:
            from pyspark.sql.pandas.types import (
                _check_series_convert_timestamps_tz_local,
                _get_local_timezone,
            )
            import pandas as pd
            from pandas.core.dtypes.common import is_timedelta64_dtype

            copied = False
            if isinstance(schema, StructType):

                def _create_converter(data_type: DataType) -> Callable[[pd.Series], pd.Series]:
                    if isinstance(data_type, TimestampType):

                        def correct_timestamp(pser: pd.Series) -> pd.Series:
                            return _check_series_convert_timestamps_tz_local(pser, timezone)

                        return correct_timestamp

                    def _converter(dt: DataType) -> Optional[Callable[[Any], Any]]:
                        if isinstance(dt, ArrayType):
                            element_conv = _converter(dt.elementType) or (lambda x: x)

                            def convert_array(value: Any) -> Any:
                                if value is None:
                                    return None
                                else:
                                    return [element_conv(v) for v in value]

                            return convert_array

                        elif isinstance(dt, MapType):
                            key_conv = _converter(dt.keyType) or (lambda x: x)
                            value_conv = _converter(dt.valueType) or (lambda x: x)

                            def convert_map(value: Any) -> Any:
                                if value is None:
                                    return None
                                else:
                                    return {key_conv(k): value_conv(v) for k, v in value.items()}

                            return convert_map

                        elif isinstance(dt, StructType):
                            field_names = dt.names
                            dedup_field_names = _dedup_names(field_names)
                            field_convs = [
                                _converter(f.dataType) or (lambda x: x) for f in dt.fields
                            ]

                            def convert_struct(value: Any) -> Any:
                                if value is None:
                                    return None
                                elif isinstance(value, dict):
                                    _values = [
                                        field_convs[i](value.get(name, None))
                                        for i, name in enumerate(dedup_field_names)
                                    ]
                                    return _create_row(field_names, _values)
                                else:
                                    _values = [
                                        field_convs[i](value[i]) for i, name in enumerate(value)
                                    ]
                                    return _create_row(field_names, _values)

                            return convert_struct

                        elif isinstance(dt, TimestampType):

                            def convert_timestamp(value: Any) -> Any:
                                if value is None:
                                    return None
                                else:
                                    return (
                                        pd.Timestamp(value)
                                        .tz_localize(timezone, ambiguous=False)
                                        .tz_convert(_get_local_timezone())
                                        .tz_localize(None)
                                        .to_pydatetime()
                                    )

                            return convert_timestamp

                        else:
                            return None

                    conv = _converter(data_type)
                    if conv is not None:
                        return lambda pser: pser.apply(conv)  # type: ignore[return-value]
                    else:
                        return lambda pser: pser

                if len(pdf.columns) > 0:
                    pdf = pd.concat(
                        [
                            _create_converter(field.dataType)(pser)
                            for (_, pser), field in zip(pdf.items(), schema.fields)
                        ],
                        axis="columns",
                    )
                    copied = True
            else:
                should_localize = not is_timestamp_ntz_preferred()
                for column, series in pdf.items():
                    s = series
                    if (
                        should_localize
                        and isinstance(s.dtype, pd.DatetimeTZDtype)
                        and s.dt.tz is not None
                    ):
                        s = _check_series_convert_timestamps_tz_local(series, timezone)
                    if s is not series:
                        if not copied:
                            # Copy once if the series is modified to prevent the original
                            # Pandas DataFrame from being updated
                            pdf = pdf.copy()
                            copied = True
                        pdf[column] = s

            for column, series in pdf.items():
                if is_timedelta64_dtype(series):
                    if not copied:
                        pdf = pdf.copy()
                        copied = True
                    # Explicitly set the timedelta as object so the output of numpy records can
                    # hold the timedelta instances as are. Otherwise, it converts to the internal
                    # numeric values.
                    ser = pdf[column]
                    pdf[column] = pd.Series(
                        ser.dt.to_pytimedelta(), index=ser.index, dtype="object", name=ser.name
                    )

        # Convert pandas.DataFrame to list of numpy records
        np_records = pdf.set_axis(
            [f"col_{i}" for i in range(len(pdf.columns))], axis="columns"  # type: ignore[arg-type]
        ).to_records(index=False)

        # Check if any columns need to be fixed for Spark to infer properly
        if len(np_records) > 0:
            record_dtype = self._get_numpy_record_dtype(np_records[0])
            if record_dtype is not None:
                return [r.astype(record_dtype).tolist() for r in np_records]

        # Convert list of numpy records to python lists
        return [r.tolist() for r in np_records]

    def _get_numpy_record_dtype(self, rec: "np.recarray") -> Optional["np.dtype"]:
        """
        Used when converting a pandas.DataFrame to Spark using to_records(), this will correct
        the dtypes of fields in a record so they can be properly loaded into Spark.

        Parameters
        ----------
        rec : numpy.record
            a numpy record to check field dtypes

        Returns
        -------
        numpy.dtype
            corrected dtype for a numpy.record or None if no correction needed
        """
        import numpy as np

        cur_dtypes = rec.dtype
        col_names = cur_dtypes.names
        record_type_list = []
        has_rec_fix = False
        for i in range(len(cur_dtypes)):
            curr_type = cur_dtypes[i]
            # If type is a datetime64 timestamp, convert to microseconds
            # NOTE: if dtype is datetime[ns] then np.record.tolist() will output values as longs,
            # conversion from [us] or lower will lead to py datetime objects, see SPARK-22417
            if curr_type == np.dtype("datetime64[ns]"):
                curr_type = "datetime64[us]"
                has_rec_fix = True
            record_type_list.append((str(col_names[i]), curr_type))
        return np.dtype(record_type_list) if has_rec_fix else None

    def _create_from_pandas_with_arrow(
        self, pdf: "PandasDataFrameLike", schema: Union[StructType, List[str]], timezone: str
    ) -> "DataFrame":
        """
        Create a DataFrame from a given pandas.DataFrame by slicing it into partitions, converting
        to Arrow data, then sending to the JVM to parallelize. If a schema is passed in, the
        data types will be used to coerce the data in Pandas to Arrow conversion.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql.dataframe import DataFrame

        assert isinstance(self, SparkSession)

        from pyspark.sql.pandas.serializers import ArrowStreamPandasSerializer
        from pyspark.sql.types import TimestampType
        from pyspark.sql.pandas.types import (
            from_arrow_type,
            to_arrow_type,
            _deduplicate_field_names,
        )
        from pyspark.sql.pandas.utils import (
            require_minimum_pandas_version,
            require_minimum_pyarrow_version,
        )

        require_minimum_pandas_version()
        require_minimum_pyarrow_version()

        import pandas as pd
        from pandas.api.types import (  # type: ignore[attr-defined]
            is_datetime64_dtype,
        )
        import pyarrow as pa

        infer_pandas_dict_as_map = (
            str(self.conf.get("spark.sql.execution.pandas.inferPandasDictAsMap")).lower() == "true"
        )

        # Create the Spark schema from list of names passed in with Arrow types
        if isinstance(schema, (list, tuple)):
            arrow_schema = pa.Schema.from_pandas(pdf, preserve_index=False)
            prefer_timestamp_ntz = is_timestamp_ntz_preferred()
            struct = StructType()
            if infer_pandas_dict_as_map:
                spark_type: Union[MapType, DataType]
                for name, field in zip(schema, arrow_schema):
                    field_type = field.type
                    if isinstance(field_type, pa.StructType):
                        if len(field_type) == 0:
                            raise PySparkValueError(
                                errorClass="CANNOT_INFER_EMPTY_SCHEMA",
                                messageParameters={},
                            )
                        arrow_type = field_type.field(0).type
                        spark_type = MapType(
                            StringType(), from_arrow_type(arrow_type, prefer_timestamp_ntz)
                        )
                    else:
                        spark_type = from_arrow_type(field_type)
                    struct.add(name, spark_type, nullable=field.nullable)
            else:
                for name, field in zip(schema, arrow_schema):
                    struct.add(
                        name,
                        from_arrow_type(field.type, prefer_timestamp_ntz),
                        nullable=field.nullable,
                    )
            schema = struct

        # Determine arrow types to coerce data when creating batches
        if isinstance(schema, StructType):
            spark_types = [_deduplicate_field_names(f.dataType) for f in schema.fields]
        elif isinstance(schema, DataType):
            raise PySparkTypeError(
                errorClass="UNSUPPORTED_DATA_TYPE_FOR_ARROW",
                messageParameters={"data_type": str(schema)},
            )
        else:
            # Any timestamps must be coerced to be compatible with Spark
            spark_types = [
                TimestampType()
                if is_datetime64_dtype(t) or isinstance(t, pd.DatetimeTZDtype)
                else None
                for t in pdf.dtypes
            ]

        # Slice the DataFrame to be batched
        step = self._jconf.arrowMaxRecordsPerBatch()
        step = step if step > 0 else len(pdf)
        pdf_slices = (pdf.iloc[start : start + step] for start in range(0, len(pdf), step))

        # Create list of Arrow (columns, arrow_type, spark_type) for serializer dump_stream
        arrow_data = [
            [
                (c, to_arrow_type(t) if t is not None else None, t)
                for (_, c), t in zip(pdf_slice.items(), spark_types)
            ]
            for pdf_slice in pdf_slices
        ]

        jsparkSession = self._jsparkSession

        safecheck = self._jconf.arrowSafeTypeConversion()
        ser = ArrowStreamPandasSerializer(timezone, safecheck)

        @no_type_check
        def reader_func(temp_filename):
            return self._jvm.PythonSQLUtils.readArrowStreamFromFile(temp_filename)

        @no_type_check
        def create_iter_server():
            return self._jvm.ArrowIteratorServer()

        # Create Spark DataFrame from Arrow stream file, using one batch per partition
        jiter = self._sc._serialize_to_jvm(arrow_data, ser, reader_func, create_iter_server)
        assert self._jvm is not None
        jdf = self._jvm.PythonSQLUtils.toDataFrame(jiter, schema.json(), jsparkSession)
        df = DataFrame(jdf, self)
        df._schema = schema
        return df

    def _create_from_arrow_table(
        self, table: "pa.Table", schema: Union[StructType, List[str]], timezone: str
    ) -> "DataFrame":
        """
        Create a DataFrame from a given pyarrow.Table by slicing it into partitions then
        sending to the JVM to parallelize.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql.dataframe import DataFrame

        assert isinstance(self, SparkSession)

        from pyspark.sql.pandas.serializers import ArrowStreamSerializer
        from pyspark.sql.pandas.types import (
            from_arrow_type,
            from_arrow_schema,
            to_arrow_schema,
            _check_arrow_table_timestamps_localize,
        )
        from pyspark.sql.pandas.utils import require_minimum_pyarrow_version

        require_minimum_pyarrow_version()

        prefer_timestamp_ntz = is_timestamp_ntz_preferred()

        # Create the Spark schema from list of names passed in with Arrow types
        if isinstance(schema, (list, tuple)):
            table = table.rename_columns(schema)
            arrow_schema = table.schema
            struct = StructType()
            for name, field in zip(schema, arrow_schema):
                struct.add(
                    name,
                    from_arrow_type(field.type, prefer_timestamp_ntz),
                    nullable=field.nullable,
                )
            schema = struct

        if not isinstance(schema, StructType):
            schema = from_arrow_schema(table.schema, prefer_timestamp_ntz=prefer_timestamp_ntz)

        table = _check_arrow_table_timestamps_localize(table, schema, True, timezone).cast(
            to_arrow_schema(schema, error_on_duplicated_field_names_in_struct=True)
        )

        # Chunk the Arrow Table into RecordBatches
        chunk_size = self._jconf.arrowMaxRecordsPerBatch()
        arrow_data = table.to_batches(max_chunksize=chunk_size)

        jsparkSession = self._jsparkSession

        ser = ArrowStreamSerializer()

        @no_type_check
        def reader_func(temp_filename):
            return self._jvm.PythonSQLUtils.readArrowStreamFromFile(temp_filename)

        @no_type_check
        def create_iter_server():
            return self._jvm.ArrowIteratorServer()

        # Create Spark DataFrame from Arrow stream file, using one batch per partition
        jiter = self._sc._serialize_to_jvm(arrow_data, ser, reader_func, create_iter_server)
        assert self._jvm is not None
        jdf = self._jvm.PythonSQLUtils.toDataFrame(jiter, schema.json(), jsparkSession)
        df = DataFrame(jdf, self)
        df._schema = schema
        return df


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.pandas.conversion

    globs = pyspark.sql.pandas.conversion.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("sql.pandas.conversion tests").getOrCreate()
    )
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.pandas.conversion,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
