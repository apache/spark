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
import itertools
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Type,
    Union,
    cast,
    no_type_check,
    overload,
    TYPE_CHECKING,
)
from warnings import warn

from pyspark.errors import UnsupportedOperationException
from pyspark.rdd import _load_from_socket
from pyspark.sql.pandas.serializers import ArrowCollectSerializer
from pyspark.sql.types import (
    IntegralType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
    TimestampType,
    TimestampNTZType,
    DayTimeIntervalType,
    StructField,
    StructType,
    DataType,
    Row,
    _create_row,
)
from pyspark.sql.utils import is_timestamp_ntz_preferred
from pyspark.traceback_utils import SCCallSiteSync

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from py4j.java_gateway import JavaObject

    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike
    from pyspark.sql import DataFrame


class PandasConversionMixin:
    """
    Mix-in for the conversion from Spark to pandas. Currently, only :class:`DataFrame`
    can use this class.
    """

    def toPandas(self) -> "PandasDataFrameLike":
        """
        Returns the contents of this :class:`DataFrame` as Pandas ``pandas.DataFrame``.

        This is only available if Pandas is installed and available.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        This method should only be used if the resulting Pandas ``pandas.DataFrame`` is
        expected to be small, as all the data is loaded into the driver's memory.

        Usage with ``spark.sql.execution.arrow.pyspark.enabled=True`` is experimental.

        Examples
        --------
        >>> df.toPandas()  # doctest: +SKIP
           age   name
        0    2  Alice
        1    5    Bob
        """
        from pyspark.sql.dataframe import DataFrame

        assert isinstance(self, DataFrame)

        from pyspark.sql.pandas.utils import require_minimum_pandas_version

        require_minimum_pandas_version()

        import pandas as pd

        jconf = self.sparkSession._jconf
        timezone = jconf.sessionLocalTimeZone()
        struct_in_pandas_as = jconf.pandasStructHandlingMode()

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
                    import pyarrow

                    self_destruct = jconf.arrowPySparkSelfDestructEnabled()
                    batches = self._collect_as_arrow(split_batches=self_destruct)
                    if len(batches) > 0:
                        table = pyarrow.Table.from_batches(batches)
                        # Ensure only the table has a reference to the batches, so that
                        # self_destruct (if enabled) is effective
                        del batches
                        # Pandas DataFrame created from PyArrow uses datetime64[ns] for date type
                        # values, but we should use datetime.date to match the behavior with when
                        # Arrow optimization is disabled.
                        pandas_options = {"date_as_object": True}
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

                        error_on_duplicated_field_names = False
                        if struct_in_pandas_as == "legacy":
                            error_on_duplicated_field_names = True
                            struct_in_pandas_as = "dict"

                        return pd.concat(
                            [
                                PandasConversionMixin._create_converter(
                                    field,
                                    timezone=timezone,
                                    struct_in_pandas=struct_in_pandas_as,
                                    error_on_duplicated_field_names=error_on_duplicated_field_names,
                                )(pser)
                                for (_, pser), field in zip(pdf.items(), self.schema.fields)
                            ],
                            axis="columns",
                        )
                    else:
                        tmp_column_names = ["col_{}".format(i) for i in range(len(self.columns))]
                        corrected_panda_types = {}
                        for index, field in enumerate(self.schema):
                            pandas_type = PandasConversionMixin._to_corrected_pandas_type(
                                field.dataType
                            )
                            corrected_panda_types[tmp_column_names[index]] = (
                                object if pandas_type is None else pandas_type
                            )

                        pdf = pd.DataFrame(columns=tmp_column_names).astype(
                            dtype=corrected_panda_types
                        )
                        pdf.columns = self.columns
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
        pdf = pd.DataFrame.from_records(self.collect(), columns=self.columns)

        return pd.concat(
            [
                PandasConversionMixin._create_converter(
                    field,
                    timezone=timezone,
                    struct_in_pandas=(
                        "row" if struct_in_pandas_as == "legacy" else struct_in_pandas_as
                    ),
                    error_on_duplicated_field_names=False,
                )(pser)
                for (_, pser), field in zip(pdf.items(), self.schema.fields)
            ],
            axis="columns",
        )

    @staticmethod
    def _to_corrected_pandas_type(dt: DataType) -> Optional[Type]:
        """
        When converting Spark SQL records to Pandas `pandas.DataFrame`, the inferred data type
        may be wrong. This method gets the corrected data type for Pandas if that type may be
        inferred incorrectly.
        """
        import numpy as np

        if type(dt) == ByteType:
            return np.int8
        elif type(dt) == ShortType:
            return np.int16
        elif type(dt) == IntegerType:
            return np.int32
        elif type(dt) == LongType:
            return np.int64
        elif type(dt) == FloatType:
            return np.float32
        elif type(dt) == DoubleType:
            return np.float64
        elif type(dt) == BooleanType:
            return bool
        elif type(dt) == TimestampType:
            return np.datetime64
        elif type(dt) == TimestampNTZType:
            return np.datetime64
        elif type(dt) == DayTimeIntervalType:
            return np.timedelta64
        else:
            return None

    def _collect_as_arrow(self, split_batches: bool = False) -> List["pa.RecordBatch"]:
        """
        Returns all records as a list of ArrowRecordBatches, pyarrow must be installed
        and available on driver and worker Python environments.
        This is an experimental feature.

        :param split_batches: split batches such that each column is in its own allocation, so
            that the selfDestruct optimization is effective; default False.

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
            # Join serving thread and raise any exceptions from collectAsArrowToPython
            jsocket_auth_server.getResult()

        # Separate RecordBatches from batch order indices in results
        batches = results[:-1]
        batch_order = results[-1]

        # Re-order the batch list using the correct order
        return [batches[i] for i in batch_order]

    @staticmethod
    def _create_converter(
        field: StructField,
        *,
        timezone: Optional[str],
        struct_in_pandas: str,
        error_on_duplicated_field_names: bool,
    ) -> Callable[["pd.Series"], "pd.Series"]:
        import numpy as np
        import pandas as pd
        from pandas.core.dtypes.common import is_datetime64tz_dtype
        from pyspark.sql.pandas.types import _check_series_convert_timestamps_local_tz

        pandas_type = PandasConversionMixin._to_corrected_pandas_type(field.dataType)

        if pandas_type is not None:
            # SPARK-21766: if an integer field is nullable and has null values, it can be
            # inferred by pandas as a float column. If we convert the column with NaN back
            # to integer type e.g., np.int16, we will hit an exception. So we use the
            # pandas-inferred float type, rather than the corrected type from the schema
            # in this case.
            if isinstance(field.dataType, IntegralType) and field.nullable:

                def correct_dtype(pser: pd.Series) -> pd.Series:
                    if pser.isnull().any():
                        return pser.astype(np.float64, copy=False)
                    else:
                        return pser.astype(pandas_type, copy=False)

            elif isinstance(field.dataType, BooleanType) and field.nullable:

                def correct_dtype(pser: pd.Series) -> pd.Series:
                    if pser.isnull().any():
                        return pser.astype(object, copy=False)
                    else:
                        return pser.astype(pandas_type, copy=False)

            elif isinstance(field.dataType, TimestampType):
                assert timezone is not None

                def correct_dtype(pser: pd.Series) -> pd.Series:
                    if not is_datetime64tz_dtype(pser.dtype):
                        pser = pser.astype(np.dtype("datetime64[ns]"), copy=False)
                    return _check_series_convert_timestamps_local_tz(
                        pser, timezone=cast(str, timezone)
                    )

            elif isinstance(field.dataType, TimestampNTZType):

                def correct_dtype(pser: pd.Series) -> pd.Series:
                    return pser.astype(np.dtype("datetime64[ns]"), copy=False)

            elif isinstance(field.dataType, DayTimeIntervalType):

                def correct_dtype(pser: pd.Series) -> pd.Series:
                    return pser.astype(np.dtype("timedelta64[ns]"), copy=False)

            else:

                def correct_dtype(pser: pd.Series) -> pd.Series:
                    return pser.astype(pandas_type, copy=False)

            return correct_dtype

        def _converter(dt: DataType) -> Optional[Callable[[Any], Any]]:

            if isinstance(dt, ArrayType):
                _element_conv = _converter(dt.elementType)
                if _element_conv is None:
                    return None

                def convert_array(value: Any) -> Any:
                    if value is None:
                        return None
                    elif isinstance(value, np.ndarray):
                        return np.array([_element_conv(v) for v in value])  # type: ignore[misc]
                    else:
                        assert isinstance(value, list)
                        return [_element_conv(v) for v in value]  # type: ignore[misc]

                return convert_array

            elif isinstance(dt, MapType):
                _key_conv = _converter(dt.keyType) or (lambda x: x)
                _value_conv = _converter(dt.valueType) or (lambda x: x)

                def convert_map(value: Any) -> Any:
                    if value is None:
                        return None
                    elif isinstance(value, list):
                        return {_key_conv(k): _value_conv(v) for k, v in value}
                    else:
                        assert isinstance(value, dict)
                        return {_key_conv(k): _value_conv(v) for k, v in value.items()}

                return convert_map

            elif isinstance(dt, StructType):

                field_names = dt.names

                if error_on_duplicated_field_names and len(set(field_names)) != len(field_names):
                    raise UnsupportedOperationException(
                        error_class="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
                        message_parameters={"field_names": str(field_names)},
                    )

                dedup_field_names = _dedup_names(field_names)

                field_convs = [_converter(f.dataType) or (lambda x: x) for f in dt.fields]

                if struct_in_pandas == "row":

                    def convert_struct_as_row(value: Any) -> Any:
                        if value is None:
                            return None
                        elif isinstance(value, dict):
                            _values = [
                                field_convs[i](value.get(name, None))
                                for i, name in enumerate(dedup_field_names)
                            ]
                            return _create_row(field_names, _values)
                        else:
                            assert isinstance(value, Row)
                            _values = [field_convs[i](value[i]) for i, name in enumerate(value)]
                            return _create_row(field_names, _values)

                    return convert_struct_as_row

                elif struct_in_pandas == "dict":

                    def convert_struct_as_dict(value: Any) -> Any:
                        if value is None:
                            return None
                        elif isinstance(value, dict):
                            return {
                                name: field_convs[i](value.get(name, None))
                                for i, name in enumerate(dedup_field_names)
                            }
                        else:
                            assert isinstance(value, Row)
                            return {
                                dedup_field_names[i]: field_convs[i](v) for i, v in enumerate(value)
                            }

                    return convert_struct_as_dict

                else:
                    raise ValueError(f"Unknown value for `struct_in_pandas`: {struct_in_pandas}")

            else:
                return None

        conv = _converter(field.dataType)
        if conv is not None:
            return lambda pser: pser.apply(conv)  # type: ignore[return-value]
        else:
            return lambda pser: pser


class SparkConversionMixin:
    """
    Min-in for the conversion from pandas to Spark. Currently, only :class:`SparkSession`
    can use this class.
    """

    _jsparkSession: "JavaObject"

    @overload
    def createDataFrame(
        self, data: "PandasDataFrameLike", samplingRatio: Optional[float] = ...
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

    def createDataFrame(  # type: ignore[misc]
        self,
        data: "PandasDataFrameLike",
        schema: Optional[Union[StructType, List[str]]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True,
    ) -> "DataFrame":
        from pyspark.sql import SparkSession

        assert isinstance(self, SparkSession)

        from pyspark.sql.pandas.utils import require_minimum_pandas_version

        require_minimum_pandas_version()

        timezone = self._jconf.sessionLocalTimeZone()

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
        import pandas as pd
        from pyspark.sql import SparkSession

        assert isinstance(self, SparkSession)

        if timezone is not None:
            from pyspark.sql.pandas.types import _check_series_convert_timestamps_tz_local
            from pandas.core.dtypes.common import is_datetime64tz_dtype, is_timedelta64_dtype

            copied = False
            if isinstance(schema, StructType):
                for field in schema:
                    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
                    if isinstance(field.dataType, TimestampType):
                        s = _check_series_convert_timestamps_tz_local(pdf[field.name], timezone)
                        if s is not pdf[field.name]:
                            if not copied:
                                # Copy once if the series is modified to prevent the original
                                # Pandas DataFrame from being updated
                                pdf = pdf.copy()
                                copied = True
                            pdf[field.name] = s
            else:
                should_localize = not is_timestamp_ntz_preferred()
                for column, series in pdf.items():
                    s = series
                    if should_localize and is_datetime64tz_dtype(s.dtype) and s.dt.tz is not None:
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
        np_records = pdf.to_records(index=False)

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
        from pyspark.sql.pandas.types import from_arrow_type, to_arrow_type
        from pyspark.sql.pandas.utils import (
            require_minimum_pandas_version,
            require_minimum_pyarrow_version,
        )

        require_minimum_pandas_version()
        require_minimum_pyarrow_version()

        from pandas.api.types import (  # type: ignore[attr-defined]
            is_datetime64_dtype,
            is_datetime64tz_dtype,
        )
        import pyarrow as pa

        # Create the Spark schema from list of names passed in with Arrow types
        if isinstance(schema, (list, tuple)):
            arrow_schema = pa.Schema.from_pandas(pdf, preserve_index=False)
            struct = StructType()
            prefer_timestamp_ntz = is_timestamp_ntz_preferred()
            for name, field in zip(schema, arrow_schema):
                struct.add(
                    name, from_arrow_type(field.type, prefer_timestamp_ntz), nullable=field.nullable
                )
            schema = struct

        # Determine arrow types to coerce data when creating batches
        if isinstance(schema, StructType):
            arrow_types = [to_arrow_type(f.dataType) for f in schema.fields]
        elif isinstance(schema, DataType):
            raise ValueError("Single data type %s is not supported with Arrow" % str(schema))
        else:
            # Any timestamps must be coerced to be compatible with Spark
            arrow_types = [
                to_arrow_type(TimestampType())
                if is_datetime64_dtype(t) or is_datetime64tz_dtype(t)
                else None
                for t in pdf.dtypes
            ]

        # Slice the DataFrame to be batched
        step = self._jconf.arrowMaxRecordsPerBatch()
        pdf_slices = (pdf.iloc[start : start + step] for start in range(0, len(pdf), step))

        # Create list of Arrow (columns, type) for serializer dump_stream
        arrow_data = [
            [(c, t) for (_, c), t in zip(pdf_slice.items(), arrow_types)]
            for pdf_slice in pdf_slices
        ]

        jsparkSession = self._jsparkSession

        safecheck = self._jconf.arrowSafeTypeConversion()
        col_by_name = True  # col by name only applies to StructType columns, can't happen here
        ser = ArrowStreamPandasSerializer(timezone, safecheck, col_by_name)

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


def _dedup_names(names: List[str]) -> List[str]:
    if len(set(names)) == len(names):
        return names
    else:

        def _gen_dedup(_name: str) -> Callable[[], str]:
            _i = itertools.count()
            return lambda: f"{_name}_{next(_i)}"

        def _gen_identity(_name: str) -> Callable[[], str]:
            return lambda: _name

        gen_new_name = {
            name: _gen_dedup(name) if len(list(group)) > 1 else _gen_identity(name)
            for name, group in itertools.groupby(sorted(names))
        }
        return [gen_new_name[name]() for name in names]


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
