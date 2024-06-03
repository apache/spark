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
Type-specific codes between pandas and PyArrow. Also contains some utils to correct
pandas instances during the type conversion.
"""
import datetime
import itertools
from typing import Any, Callable, Iterable, List, Optional, Union, TYPE_CHECKING

from pyspark.sql.types import (
    cast,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    IntegralType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    BinaryType,
    DateType,
    TimestampType,
    TimestampNTZType,
    DayTimeIntervalType,
    ArrayType,
    MapType,
    StructType,
    StructField,
    NullType,
    DataType,
    UserDefinedType,
    VariantType,
    VariantVal,
    _create_row,
)
from pyspark.errors import PySparkTypeError, UnsupportedOperationException, PySparkValueError
from pyspark.loose_version import LooseVersion

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

    from pyspark.sql.pandas._typing import SeriesLike as PandasSeriesLike


def to_arrow_type(
    dt: DataType,
    error_on_duplicated_field_names_in_struct: bool = False,
    timestamp_utc: bool = True,
) -> "pa.DataType":
    """
    Convert Spark data type to PyArrow type

    Parameters
    ----------
    dt : :class:`DataType`
        The Spark data type.
    error_on_duplicated_field_names_in_struct: bool, default False
        Whether to raise an exception when there are duplicated field names in a
        :class:`pyspark.sql.types.StructType`. (default ``False``)
    timestamp_utc : bool, default True
        If ``True`` (the default), :class:`TimestampType` is converted to a timezone-aware
        :class:`pyarrow.TimestampType` with UTC as the timezone. If ``False``,
        :class:`TimestampType` is converted to a timezone-naive :class:`pyarrow.TimestampType`.
        The JVM expects timezone-aware timestamps to be in UTC. Always keep this set to ``True``
        except in special cases, such as when this function is used in a test.

    Returns
    -------
    :class:`pyarrow.DataType`
    """
    import pyarrow as pa

    if type(dt) == BooleanType:
        arrow_type = pa.bool_()
    elif type(dt) == ByteType:
        arrow_type = pa.int8()
    elif type(dt) == ShortType:
        arrow_type = pa.int16()
    elif type(dt) == IntegerType:
        arrow_type = pa.int32()
    elif type(dt) == LongType:
        arrow_type = pa.int64()
    elif type(dt) == FloatType:
        arrow_type = pa.float32()
    elif type(dt) == DoubleType:
        arrow_type = pa.float64()
    elif type(dt) == DecimalType:
        arrow_type = pa.decimal128(dt.precision, dt.scale)
    elif type(dt) == StringType:
        arrow_type = pa.string()
    elif type(dt) == BinaryType:
        arrow_type = pa.binary()
    elif type(dt) == DateType:
        arrow_type = pa.date32()
    elif type(dt) == TimestampType and timestamp_utc:
        # Timestamps should be in UTC, JVM Arrow timestamps require a timezone to be read
        arrow_type = pa.timestamp("us", tz="UTC")
    elif type(dt) == TimestampType:
        arrow_type = pa.timestamp("us", tz=None)
    elif type(dt) == TimestampNTZType:
        arrow_type = pa.timestamp("us", tz=None)
    elif type(dt) == DayTimeIntervalType:
        arrow_type = pa.duration("us")
    elif type(dt) == ArrayType:
        field = pa.field(
            "element",
            to_arrow_type(dt.elementType, error_on_duplicated_field_names_in_struct, timestamp_utc),
            nullable=dt.containsNull,
        )
        arrow_type = pa.list_(field)
    elif type(dt) == MapType:
        key_field = pa.field(
            "key",
            to_arrow_type(dt.keyType, error_on_duplicated_field_names_in_struct, timestamp_utc),
            nullable=False,
        )
        value_field = pa.field(
            "value",
            to_arrow_type(dt.valueType, error_on_duplicated_field_names_in_struct, timestamp_utc),
            nullable=dt.valueContainsNull,
        )
        arrow_type = pa.map_(key_field, value_field)
    elif type(dt) == StructType:
        field_names = dt.names
        if error_on_duplicated_field_names_in_struct and len(set(field_names)) != len(field_names):
            raise UnsupportedOperationException(
                error_class="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
                message_parameters={"field_names": str(field_names)},
            )
        fields = [
            pa.field(
                field.name,
                to_arrow_type(
                    field.dataType, error_on_duplicated_field_names_in_struct, timestamp_utc
                ),
                nullable=field.nullable,
            )
            for field in dt
        ]
        arrow_type = pa.struct(fields)
    elif type(dt) == NullType:
        arrow_type = pa.null()
    elif isinstance(dt, UserDefinedType):
        arrow_type = to_arrow_type(
            dt.sqlType(), error_on_duplicated_field_names_in_struct, timestamp_utc
        )
    elif type(dt) == VariantType:
        fields = [
            pa.field("value", pa.binary(), nullable=False),
            pa.field("metadata", pa.binary(), nullable=False),
        ]
        arrow_type = pa.struct(fields)
    else:
        raise PySparkTypeError(
            error_class="UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION",
            message_parameters={"data_type": str(dt)},
        )
    return arrow_type


def to_arrow_schema(
    schema: StructType,
    error_on_duplicated_field_names_in_struct: bool = False,
    timestamp_utc: bool = True,
) -> "pa.Schema":
    """
    Convert a schema from Spark to Arrow

    Parameters
    ----------
    schema : :class:`StructType`
        The Spark schema.
    error_on_duplicated_field_names_in_struct: bool, default False
        Whether to raise an exception when there are duplicated field names in an inner
        :class:`pyspark.sql.types.StructType`. (default ``False``)
    timestamp_utc : bool, default True
        If ``True`` (the default), :class:`TimestampType` is converted to a timezone-aware
        :class:`pyarrow.TimestampType` with UTC as the timezone. If ``False``,
        :class:`TimestampType` is converted to a timezone-naive :class:`pyarrow.TimestampType`.
        The JVM expects timezone-aware timestamps to be in UTC. Always keep this set to ``True``
        except in special cases, such as when this function is used in a test

    Returns
    -------
    :class:`pyarrow.Schema`
    """
    import pyarrow as pa

    fields = [
        pa.field(
            field.name,
            to_arrow_type(field.dataType, error_on_duplicated_field_names_in_struct, timestamp_utc),
            nullable=field.nullable,
        )
        for field in schema
    ]
    return pa.schema(fields)


def from_arrow_type(at: "pa.DataType", prefer_timestamp_ntz: bool = False) -> DataType:
    """Convert pyarrow type to Spark data type."""
    import pyarrow.types as types

    spark_type: DataType
    if types.is_boolean(at):
        spark_type = BooleanType()
    elif types.is_int8(at):
        spark_type = ByteType()
    elif types.is_int16(at):
        spark_type = ShortType()
    elif types.is_int32(at):
        spark_type = IntegerType()
    elif types.is_int64(at):
        spark_type = LongType()
    elif types.is_float32(at):
        spark_type = FloatType()
    elif types.is_float64(at):
        spark_type = DoubleType()
    elif types.is_decimal(at):
        spark_type = DecimalType(precision=at.precision, scale=at.scale)
    elif types.is_string(at):
        spark_type = StringType()
    elif types.is_large_string(at):
        spark_type = StringType()
    elif types.is_binary(at):
        spark_type = BinaryType()
    elif types.is_fixed_size_binary(at):
        spark_type = BinaryType()
    elif types.is_large_binary(at):
        spark_type = BinaryType()
    elif types.is_date32(at):
        spark_type = DateType()
    elif types.is_timestamp(at) and prefer_timestamp_ntz and at.tz is None:
        spark_type = TimestampNTZType()
    elif types.is_timestamp(at):
        spark_type = TimestampType()
    elif types.is_duration(at):
        spark_type = DayTimeIntervalType()
    elif types.is_list(at):
        spark_type = ArrayType(from_arrow_type(at.value_type, prefer_timestamp_ntz))
    elif types.is_fixed_size_list(at):
        import pyarrow as pa

        if LooseVersion(pa.__version__) < LooseVersion("14.0.0"):
            # PyArrow versions before 14.0.0 do not support casting FixedSizeListArray to ListArray
            raise PySparkTypeError(
                error_class="UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION",
                message_parameters={"data_type": str(at)},
            )
        spark_type = ArrayType(from_arrow_type(at.value_type, prefer_timestamp_ntz))
    elif types.is_large_list(at):
        spark_type = ArrayType(from_arrow_type(at.value_type, prefer_timestamp_ntz))
    elif types.is_map(at):
        spark_type = MapType(
            from_arrow_type(at.key_type, prefer_timestamp_ntz),
            from_arrow_type(at.item_type, prefer_timestamp_ntz),
        )
    elif types.is_struct(at):
        return StructType(
            [
                StructField(
                    field.name,
                    from_arrow_type(field.type, prefer_timestamp_ntz),
                    nullable=field.nullable,
                )
                for field in at
            ]
        )
    elif types.is_dictionary(at):
        spark_type = from_arrow_type(at.value_type, prefer_timestamp_ntz)
    elif types.is_null(at):
        spark_type = NullType()
    else:
        raise PySparkTypeError(
            error_class="UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION",
            message_parameters={"data_type": str(at)},
        )
    return spark_type


def from_arrow_schema(arrow_schema: "pa.Schema", prefer_timestamp_ntz: bool = False) -> StructType:
    """Convert schema from Arrow to Spark."""
    return StructType(
        [
            StructField(
                field.name,
                from_arrow_type(field.type, prefer_timestamp_ntz),
                nullable=field.nullable,
            )
            for field in arrow_schema
        ]
    )


def _get_local_timezone() -> str:
    """Get local timezone using pytz with environment variable, or dateutil.

    If there is a 'TZ' environment variable, pass it to pandas to use pytz and use it as timezone
    string, otherwise use the special word 'dateutil/:' which means that pandas uses dateutil and
    it reads system configuration to know the system local timezone.

    See also:
    - https://github.com/pandas-dev/pandas/blob/0.19.x/pandas/tslib.pyx#L1753
    - https://github.com/dateutil/dateutil/blob/2.6.1/dateutil/tz/tz.py#L1338
    """
    import os

    return os.environ.get("TZ", "dateutil/:")


def _check_arrow_array_timestamps_localize(
    a: Union["pa.Array", "pa.ChunkedArray"],
    dt: DataType,
    truncate: bool = True,
    timezone: Optional[str] = None,
) -> Union["pa.Array", "pa.ChunkedArray"]:
    """
    Convert Arrow timestamps to timezone-naive in the specified timezone if the specified Spark
    data type is TimestampType, and optionally truncate nanosecond timestamps to microseconds.

    This function works on Arrow Arrays and ChunkedArrays, and it recurses to convert nested
    timestamps.

    Parameters
    ----------
    a : :class:`pyarrow.Array` or :class:`pyarrow.ChunkedArray`
    dt : :class:`DataType`
        The Spark data type corresponding to the Arrow Array to be converted.
    truncate : bool, default True
        Whether to truncate nanosecond timestamps to microseconds. (default ``True``)
    timezone : str, optional
        The timezone to convert from. If there is a timestamp type, it's required.

    Returns
    -------
    :class:`pyarrow.Array` or :class:`pyarrow.ChunkedArray`
    """
    import pyarrow.types as types
    import pyarrow as pa
    import pyarrow.compute as pc

    if isinstance(a, pa.ChunkedArray) and (types.is_nested(a.type) or types.is_dictionary(a.type)):
        return pa.chunked_array(
            [
                _check_arrow_array_timestamps_localize(chunk, dt, truncate, timezone)
                for chunk in a.iterchunks()
            ]
        )

    if types.is_timestamp(a.type) and truncate and a.type.unit == "ns":
        a = pc.floor_temporal(a, unit="microsecond")

    if types.is_timestamp(a.type) and a.type.tz is None and type(dt) == TimestampType:
        assert timezone is not None

        # Only localize timestamps that will become Spark TimestampType columns.
        # Do not localize timestamps that will become Spark TimestampNTZType columns.
        return pc.assume_timezone(a, timezone)
    if types.is_list(a.type):
        # Return the ListArray as-is if it contains no nested fields or timestamps
        if not types.is_nested(a.type.value_type) and not types.is_timestamp(a.type.value_type):
            return a

        at: ArrayType = cast(ArrayType, dt)
        return pa.ListArray.from_arrays(
            a.offsets,
            _check_arrow_array_timestamps_localize(a.values, at.elementType, truncate, timezone),
            mask=a.is_null() if a.null_count else None,
        )
    if types.is_map(a.type):
        # Return the MapArray as-is if it contains no nested fields or timestamps
        if (
            not types.is_nested(a.type.key_type)
            and not types.is_nested(a.type.item_type)
            and not types.is_timestamp(a.type.key_type)
            and not types.is_timestamp(a.type.item_type)
        ):
            return a

        mt: MapType = cast(MapType, dt)
        # TODO(SPARK-48302): Do not replace nulls in MapArray with empty lists
        return pa.MapArray.from_arrays(
            a.offsets,
            _check_arrow_array_timestamps_localize(a.keys, mt.keyType, truncate, timezone),
            _check_arrow_array_timestamps_localize(a.items, mt.valueType, truncate, timezone),
        )
    if types.is_struct(a.type):
        # Return the StructArray as-is if it contains no nested fields or timestamps
        if all(
            [
                not types.is_nested(a.type.field(i).type)
                and not types.is_timestamp(a.type.field(i).type)
                for i in range(a.type.num_fields)
            ]
        ):
            return a

        st: StructType = cast(StructType, dt)
        assert len(a.type) == len(st.fields)

        return pa.StructArray.from_arrays(
            [
                _check_arrow_array_timestamps_localize(
                    a.field(i), st.fields[i].dataType, truncate, timezone
                )
                for i in range(len(a.type))
            ],
            [a.type[i].name for i in range(len(a.type))],
            mask=a.is_null() if a.null_count else None,
        )
    if types.is_dictionary(a.type):
        return pa.DictionaryArray.from_arrays(
            a.indices,
            _check_arrow_array_timestamps_localize(a.dictionary, dt, truncate, timezone),
        )
    return a


def _check_arrow_table_timestamps_localize(
    table: "pa.Table", schema: StructType, truncate: bool = True, timezone: Optional[str] = None
) -> "pa.Table":
    """
    Convert timestamps in a PyArrow Table to timezone-naive in the specified timezone if the
    corresponding Spark data type is TimestampType in the specified Spark schema is TimestampType,
    and optionally truncate nanosecond timestamps to microseconds.

    Parameters
    ----------
    table : :class:`pyarrow.Table`
    schema : :class:`StructType`
        The Spark schema corresponding to the schema of the Arrow Table.
    truncate : bool, default True
        Whether to truncate nanosecond timestamps to microseconds. (default ``True``)
    timezone : str, optional
        The timezone to convert from. If there is a timestamp type, it's required.

    Returns
    -------
    :class:`pyarrow.Table`
    """
    import pyarrow.types as types
    import pyarrow as pa

    # Return the table as-is if it contains no nested fields or timestamps
    if all([not types.is_nested(at) and not types.is_timestamp(at) for at in table.schema.types]):
        return table

    assert len(table.schema) == len(schema.fields)

    return pa.Table.from_arrays(
        [
            _check_arrow_array_timestamps_localize(a, f.dataType, truncate, timezone)
            for a, f in zip(table.columns, schema.fields)
        ],
        schema=table.schema,
    )


def _check_series_localize_timestamps(s: "PandasSeriesLike", timezone: str) -> "PandasSeriesLike":
    """
    Convert timezone aware timestamps to timezone-naive in the specified timezone or local timezone.

    If the input series is not a timestamp series, then the same series is returned. If the input
    series is a timestamp series, then a converted series is returned.

    Parameters
    ----------
    s : pandas.Series
    timezone : str
        the timezone to convert. if None then use local timezone

    Returns
    -------
    pandas.Series
        `pandas.Series` that have been converted to tz-naive
    """
    from pyspark.sql.pandas.utils import require_minimum_pandas_version

    require_minimum_pandas_version()

    import pandas as pd

    tz = timezone or _get_local_timezone()
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        return s.dt.tz_convert(tz).dt.tz_localize(None)
    else:
        return s


def _check_series_convert_timestamps_internal(
    s: "PandasSeriesLike", timezone: str
) -> "PandasSeriesLike":
    """
    Convert a tz-naive timestamp in the specified timezone or local timezone to UTC normalized for
    Spark internal storage

    Parameters
    ----------
    s : pandas.Series
    timezone : str
        the timezone to convert. if None then use local timezone

    Returns
    -------
    pandas.Series
        `pandas.Series` where if it is a timestamp, has been UTC normalized without a time zone
    """
    from pyspark.sql.pandas.utils import require_minimum_pandas_version

    require_minimum_pandas_version()

    import pandas as pd
    from pandas.api.types import (  # type: ignore[attr-defined]
        is_datetime64_dtype,
    )

    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64_dtype(s.dtype):
        # When tz_localize a tz-naive timestamp, the result is ambiguous if the tz-naive
        # timestamp is during the hour when the clock is adjusted backward during due to
        # daylight saving time (dst).
        # E.g., for America/New_York, the clock is adjusted backward on 2015-11-01 2:00 to
        # 2015-11-01 1:00 from dst-time to standard time, and therefore, when tz_localize
        # a tz-naive timestamp 2015-11-01 1:30 with America/New_York timezone, it can be either
        # dst time (2015-01-01 1:30-0400) or standard time (2015-11-01 1:30-0500).
        #
        # Here we explicit choose to use standard time. This matches the default behavior of
        # pytz.
        #
        # Here are some code to help understand this behavior:
        # >>> import datetime
        # >>> import pandas as pd
        # >>> import pytz
        # >>>
        # >>> t = datetime.datetime(2015, 11, 1, 1, 30)
        # >>> ts = pd.Series([t])
        # >>> tz = pytz.timezone('America/New_York')
        # >>>
        # >>> ts.dt.tz_localize(tz, ambiguous=True)
        # 0   2015-11-01 01:30:00-04:00
        # dtype: datetime64[ns, America/New_York]
        # >>>
        # >>> ts.dt.tz_localize(tz, ambiguous=False)
        # 0   2015-11-01 01:30:00-05:00
        # dtype: datetime64[ns, America/New_York]
        # >>>
        # >>> str(tz.localize(t))
        # '2015-11-01 01:30:00-05:00'
        tz = timezone or _get_local_timezone()
        return s.dt.tz_localize(tz, ambiguous=False).dt.tz_convert("UTC")
    elif isinstance(s.dtype, pd.DatetimeTZDtype):
        return s.dt.tz_convert("UTC")
    else:
        return s


def _check_series_convert_timestamps_localize(
    s: "PandasSeriesLike", from_timezone: Optional[str], to_timezone: Optional[str]
) -> "PandasSeriesLike":
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    Parameters
    ----------
    s : pandas.Series
    from_timezone : str
        the timezone to convert from. if None then use local timezone
    to_timezone : str
        the timezone to convert to. if None then use local timezone

    Returns
    -------
    pandas.Series
        `pandas.Series` where if it is a timestamp, has been converted to tz-naive
    """
    from pyspark.sql.pandas.utils import require_minimum_pandas_version

    require_minimum_pandas_version()

    import pandas as pd
    from pandas.api.types import (  # type: ignore[attr-defined]
        is_datetime64_dtype,
    )

    from_tz = from_timezone or _get_local_timezone()
    to_tz = to_timezone or _get_local_timezone()
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        return s.dt.tz_convert(to_tz).dt.tz_localize(None)
    elif is_datetime64_dtype(s.dtype) and from_tz != to_tz:
        # `s.dt.tz_localize('tzlocal()')` doesn't work properly when including NaT.
        return cast(
            "PandasSeriesLike",
            s.apply(
                lambda ts: ts.tz_localize(from_tz, ambiguous=False)
                .tz_convert(to_tz)
                .tz_localize(None)
                if ts is not pd.NaT
                else pd.NaT
            ),
        )
    else:
        return s


def _check_series_convert_timestamps_local_tz(
    s: "PandasSeriesLike", timezone: str
) -> "PandasSeriesLike":
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    Parameters
    ----------
    s : pandas.Series
    timezone : str
        the timezone to convert to. if None then use local timezone

    Returns
    -------
    pandas.Series
        `pandas.Series` where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, None, timezone)


def _check_series_convert_timestamps_tz_local(
    s: "PandasSeriesLike", timezone: str
) -> "PandasSeriesLike":
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    Parameters
    ----------
    s : pandas.Series
    timezone : str
        the timezone to convert from. if None then use local timezone

    Returns
    -------
    pandas.Series
        `pandas.Series` where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, timezone, None)


def _convert_map_items_to_dict(s: "PandasSeriesLike") -> "PandasSeriesLike":
    """
    Convert a series with items as list of (key, value), as made from an Arrow column of map type,
    to dict for compatibility with non-arrow MapType columns.
    :param s: pandas.Series of lists of (key, value) pairs
    :return: pandas.Series of dictionaries
    """
    return cast("PandasSeriesLike", s.apply(lambda m: None if m is None else {k: v for k, v in m}))


def _convert_dict_to_map_items(s: "PandasSeriesLike") -> "PandasSeriesLike":
    """
    Convert a series of dictionaries to list of (key, value) pairs to match expected data
    for Arrow column of map type.
    :param s: pandas.Series of dictionaries
    :return: pandas.Series of lists of (key, value) pairs
    """
    return cast("PandasSeriesLike", s.apply(lambda d: list(d.items()) if d is not None else None))


def _to_corrected_pandas_type(dt: DataType) -> Optional[Any]:
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
        return np.dtype("datetime64[ns]")
    elif type(dt) == TimestampNTZType:
        return np.dtype("datetime64[ns]")
    elif type(dt) == DayTimeIntervalType:
        return np.dtype("timedelta64[ns]")
    else:
        return None


def _create_converter_to_pandas(
    data_type: DataType,
    nullable: bool = True,
    *,
    timezone: Optional[str] = None,
    struct_in_pandas: Optional[str] = None,
    error_on_duplicated_field_names: bool = True,
    timestamp_utc_localized: bool = True,
    ndarray_as_list: bool = False,
) -> Callable[["pd.Series"], "pd.Series"]:
    """
    Create a converter of pandas Series that is created from Spark's Python objects,
    or `pyarrow.Table.to_pandas` method.

    Parameters
    ----------
    data_type : :class:`DataType`
        The data type corresponding to the pandas Series to be converted.
    nullable : bool, optional
        Whether the column is nullable or not. (default ``True``)
    timezone : str, optional
        The timezone to convert from. If there is a timestamp type, it's required.
    struct_in_pandas : str, optional
        How to handle struct type. If there is a struct type, it's required.
        When ``row``, :class:`Row` object will be used.
        When ``dict``, :class:`dict` will be used. If there are duplicated field names,
        The fields will be suffixed, like `a_0`, `a_1`.
        Must be one of: ``row``, ``dict``.
    error_on_duplicated_field_names : bool, optional
        Whether raise an exception when there are duplicated field names.
        (default ``True``)
    timestamp_utc_localized : bool, optional
        Whether the timestamp values are localized to UTC or not.
        The timestamp values from Arrow are localized to UTC,
        whereas the ones from `df.collect()` are localized to the local timezone.
    ndarray_as_list : bool, optional
        Whether `np.ndarray` is converted to a list or not (default ``False``).

    Returns
    -------
    The converter of `pandas.Series`
    """
    import numpy as np
    import pandas as pd

    pandas_type = _to_corrected_pandas_type(data_type)

    if pandas_type is not None:
        # SPARK-21766: if an integer field is nullable and has null values, it can be
        # inferred by pandas as a float column. If we convert the column with NaN back
        # to integer type e.g., np.int16, we will hit an exception. So we use the
        # pandas-inferred float type, rather than the corrected type from the schema
        # in this case.
        if isinstance(data_type, IntegralType) and nullable:

            def correct_dtype(pser: pd.Series) -> pd.Series:
                if pser.isnull().any():
                    return pser.astype(np.float64, copy=False)
                else:
                    return pser.astype(pandas_type, copy=False)

        elif isinstance(data_type, BooleanType) and nullable:

            def correct_dtype(pser: pd.Series) -> pd.Series:
                if pser.isnull().any():
                    return pser.astype(object, copy=False)
                else:
                    return pser.astype(pandas_type, copy=False)

        elif isinstance(data_type, TimestampType):
            assert timezone is not None

            def correct_dtype(pser: pd.Series) -> pd.Series:
                if not isinstance(pser.dtype, pd.DatetimeTZDtype):
                    pser = pser.astype(pandas_type, copy=False)
                return _check_series_convert_timestamps_local_tz(pser, timezone=timezone)

        else:

            def correct_dtype(pser: pd.Series) -> pd.Series:
                return pser.astype(pandas_type, copy=False)

        return correct_dtype

    def _converter(
        dt: DataType, _struct_in_pandas: Optional[str], _ndarray_as_list: bool
    ) -> Optional[Callable[[Any], Any]]:
        if isinstance(dt, ArrayType):
            _element_conv = _converter(dt.elementType, _struct_in_pandas, _ndarray_as_list)

            if _ndarray_as_list:
                if _element_conv is None:

                    def convert_array_ndarray_as_list(value: Any) -> Any:
                        # In Arrow Python UDF, ArrayType is converted to `np.ndarray`
                        # whereas a list is expected.
                        return list(value)

                else:
                    assert _element_conv is not None

                    def convert_array_ndarray_as_list(value: Any) -> Any:
                        # In Arrow Python UDF, ArrayType is converted to `np.ndarray`
                        # whereas a list is expected.
                        return [_element_conv(v) if v is not None else None for v in value]

                return convert_array_ndarray_as_list
            else:
                if _element_conv is None:
                    return None

                assert _element_conv is not None

                def convert_array_ndarray_as_ndarray(value: Any) -> Any:
                    if isinstance(value, np.ndarray):
                        # `pyarrow.Table.to_pandas` uses `np.ndarray`.
                        return np.array(
                            [_element_conv(v) if v is not None else None for v in value]
                        )
                    else:
                        # otherwise, `list` should be used.
                        return [_element_conv(v) if v is not None else None for v in value]

                return convert_array_ndarray_as_ndarray

        elif isinstance(dt, MapType):
            _key_conv = _converter(dt.keyType, _struct_in_pandas, _ndarray_as_list)
            _value_conv = _converter(dt.valueType, _struct_in_pandas, _ndarray_as_list)

            if _key_conv is None and _value_conv is None:

                def convert_map(value: Any) -> Any:
                    # `pyarrow.Table.to_pandas` uses `list` of key-value tuple.
                    # otherwise, `dict` should be used.
                    return dict(value)

            else:

                def convert_map(value: Any) -> Any:
                    if isinstance(value, list):
                        # `pyarrow.Table.to_pandas` uses `list` of key-value tuple.
                        return {
                            (_key_conv(k) if _key_conv is not None and k is not None else k): (
                                _value_conv(v) if _value_conv is not None and v is not None else v
                            )
                            for k, v in value
                        }
                    else:
                        # otherwise, `dict` should be used.
                        return {
                            (_key_conv(k) if _key_conv is not None and k is not None else k): (
                                _value_conv(v) if _value_conv is not None and v is not None else v
                            )
                            for k, v in value.items()
                        }

            return convert_map

        elif isinstance(dt, StructType):
            assert _struct_in_pandas is not None

            field_names = dt.names

            if error_on_duplicated_field_names and len(set(field_names)) != len(field_names):
                raise UnsupportedOperationException(
                    error_class="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
                    message_parameters={"field_names": str(field_names)},
                )

            dedup_field_names = _dedup_names(field_names)

            field_convs = [
                _converter(f.dataType, _struct_in_pandas, _ndarray_as_list) for f in dt.fields
            ]

            if _struct_in_pandas == "row":
                if all(conv is None for conv in field_convs):

                    def convert_struct_as_row(value: Any) -> Any:
                        if isinstance(value, dict):
                            # `pyarrow.Table.to_pandas` uses `dict`.
                            _values = [
                                value.get(name, None) for i, name in enumerate(dedup_field_names)
                            ]
                            return _create_row(field_names, _values)
                        else:
                            # otherwise, `Row` should be used.
                            return _create_row(field_names, value)

                else:

                    def convert_struct_as_row(value: Any) -> Any:
                        if isinstance(value, dict):
                            # `pyarrow.Table.to_pandas` uses `dict`.
                            _values = [
                                conv(v) if conv is not None and v is not None else v
                                for conv, v in zip(
                                    field_convs,
                                    (value.get(name, None) for name in dedup_field_names),
                                )
                            ]
                            return _create_row(field_names, _values)
                        else:
                            # otherwise, `Row` should be used.
                            _values = [
                                conv(v) if conv is not None and v is not None else v
                                for conv, v in zip(field_convs, value)
                            ]
                            return _create_row(field_names, _values)

                return convert_struct_as_row

            elif _struct_in_pandas == "dict":
                if all(conv is None for conv in field_convs):

                    def convert_struct_as_dict(value: Any) -> Any:
                        if isinstance(value, dict):
                            # `pyarrow.Table.to_pandas` uses `dict`.
                            return {name: value.get(name, None) for name in dedup_field_names}
                        else:
                            # otherwise, `Row` should be used.
                            return dict(zip(dedup_field_names, value))

                else:

                    def convert_struct_as_dict(value: Any) -> Any:
                        if isinstance(value, dict):
                            # `pyarrow.Table.to_pandas` uses `dict`.
                            return {
                                name: conv(v) if conv is not None and v is not None else v
                                for name, conv, v in zip(
                                    dedup_field_names,
                                    field_convs,
                                    (value.get(name, None) for name in dedup_field_names),
                                )
                            }
                        else:
                            # otherwise, `Row` should be used.
                            return {
                                name: conv(v) if conv is not None and v is not None else v
                                for name, conv, v in zip(dedup_field_names, field_convs, value)
                            }

                return convert_struct_as_dict

            else:
                raise PySparkValueError(
                    error_class="UNKNOWN_VALUE_FOR",
                    message_parameters={"var": str(_struct_in_pandas)},
                )

        elif isinstance(dt, TimestampType):
            assert timezone is not None

            local_tz: Union[datetime.tzinfo, str] = (
                datetime.timezone.utc if timestamp_utc_localized else _get_local_timezone()
            )

            def convert_timestamp(value: Any) -> Any:
                if isinstance(value, datetime.datetime) and value.tzinfo is not None:
                    ts = pd.Timestamp(value)
                else:
                    ts = pd.Timestamp(value).tz_localize(local_tz)
                return ts.tz_convert(timezone).tz_localize(None)

            return convert_timestamp

        elif isinstance(dt, TimestampNTZType):

            def convert_timestamp_ntz(value: Any) -> Any:
                return pd.Timestamp(value)

            return convert_timestamp_ntz

        elif isinstance(dt, UserDefinedType):
            udt: UserDefinedType = dt

            conv = _converter(udt.sqlType(), _struct_in_pandas="row", _ndarray_as_list=True)

            if conv is None:

                def convert_udt(value: Any) -> Any:
                    if hasattr(value, "__UDT__"):
                        assert isinstance(value.__UDT__, type(udt))
                        return value
                    else:
                        return udt.deserialize(value)

            else:

                def convert_udt(value: Any) -> Any:
                    if hasattr(value, "__UDT__"):
                        assert isinstance(value.__UDT__, type(udt))
                        return value
                    else:
                        return udt.deserialize(conv(value))

            return convert_udt

        elif isinstance(dt, VariantType):

            def convert_variant(value: Any) -> Any:
                if (
                    isinstance(value, dict)
                    and all(key in value for key in ["value", "metadata"])
                    and all(isinstance(value[key], bytes) for key in ["value", "metadata"])
                ):
                    return VariantVal(value["value"], value["metadata"])
                else:
                    raise PySparkValueError(error_class="MALFORMED_VARIANT")

            return convert_variant

        else:
            return None

    conv = _converter(data_type, struct_in_pandas, ndarray_as_list)
    if conv is not None:
        return lambda pser: pser.apply(  # type: ignore[return-value]
            lambda x: conv(x) if x is not None else None
        )
    else:
        return lambda pser: pser


def _create_converter_from_pandas(
    data_type: DataType,
    *,
    timezone: Optional[str] = None,
    error_on_duplicated_field_names: bool = True,
    ignore_unexpected_complex_type_values: bool = False,
) -> Callable[["pd.Series"], "pd.Series"]:
    """
    Create a converter of pandas Series to create Spark DataFrame with Arrow optimization.

    Parameters
    ----------
    data_type : :class:`DataType`
        The data type corresponding to the pandas Series to be converted.
    timezone : str, optional
        The timezone to convert from. If there is a timestamp type, it's required.
    error_on_duplicated_field_names : bool, optional
        Whether raise an exception when there are duplicated field names.
        (default ``True``)
    ignore_unexpected_complex_type_values : bool, optional
        Whether ignore the case where unexpected values are given for complex types.
        If ``False``, each complex type expects:

        * array type: :class:`Iterable`
        * map type: :class:`dict`
        * struct type: :class:`dict` or :class:`tuple`

        and raise an AssertionError when the given value is not the expected type.
        If ``True``, just ignore and return the give value.
        (default ``False``)

    Returns
    -------
    The converter of `pandas.Series`
    """
    import pandas as pd

    if isinstance(data_type, TimestampType):
        assert timezone is not None

        def correct_timestamp(pser: pd.Series) -> pd.Series:
            return _check_series_convert_timestamps_internal(pser, timezone)

        return correct_timestamp

    def _converter(dt: DataType) -> Optional[Callable[[Any], Any]]:
        if isinstance(dt, ArrayType):
            _element_conv = _converter(dt.elementType)

            if ignore_unexpected_complex_type_values:
                if _element_conv is None:

                    def convert_array(value: Any) -> Any:
                        if isinstance(value, Iterable):
                            return list(value)
                        else:
                            return value

                else:
                    assert _element_conv is not None

                    def convert_array(value: Any) -> Any:
                        if isinstance(value, Iterable):
                            return [_element_conv(v) if v is not None else None for v in value]
                        else:
                            return value

            else:
                if _element_conv is None:

                    def convert_array(value: Any) -> Any:
                        return list(value)

                else:
                    assert _element_conv is not None

                    def convert_array(value: Any) -> Any:
                        # Iterable
                        return [_element_conv(v) if v is not None else None for v in value]

            return convert_array

        elif isinstance(dt, MapType):
            _key_conv = _converter(dt.keyType)
            _value_conv = _converter(dt.valueType)

            if ignore_unexpected_complex_type_values:
                if _key_conv is None and _value_conv is None:

                    def convert_map(value: Any) -> Any:
                        if isinstance(value, dict):
                            return list(value.items())
                        else:
                            return value

                else:

                    def convert_map(value: Any) -> Any:
                        if isinstance(value, dict):
                            return [
                                (
                                    _key_conv(k) if _key_conv is not None and k is not None else k,
                                    _value_conv(v)
                                    if _value_conv is not None and v is not None
                                    else v,
                                )
                                for k, v in value.items()
                            ]
                        else:
                            return value

            else:
                if _key_conv is None and _value_conv is None:

                    def convert_map(value: Any) -> Any:
                        # dict
                        return list(value.items())

                else:

                    def convert_map(value: Any) -> Any:
                        # dict
                        return [
                            (
                                _key_conv(k) if _key_conv is not None and k is not None else k,
                                _value_conv(v) if _value_conv is not None and v is not None else v,
                            )
                            for k, v in value.items()
                        ]

            return convert_map

        elif isinstance(dt, StructType):
            field_names = dt.names

            if error_on_duplicated_field_names and len(set(field_names)) != len(field_names):
                raise UnsupportedOperationException(
                    error_class="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
                    message_parameters={"field_names": str(field_names)},
                )

            dedup_field_names = _dedup_names(field_names)

            field_convs = [_converter(f.dataType) for f in dt.fields]

            if ignore_unexpected_complex_type_values:
                if all(conv is None for conv in field_convs):

                    def convert_struct(value: Any) -> Any:
                        if isinstance(value, dict):
                            return {
                                name: value.get(key, None)
                                for name, key in zip(dedup_field_names, field_names)
                            }
                        elif isinstance(value, tuple):
                            return dict(zip(dedup_field_names, value))
                        else:
                            return value

                else:

                    def convert_struct(value: Any) -> Any:
                        if isinstance(value, dict):
                            return {
                                name: conv(v) if conv is not None and v is not None else v
                                for name, conv, v in zip(
                                    dedup_field_names,
                                    field_convs,
                                    (value.get(key, None) for key in field_names),
                                )
                            }
                        elif isinstance(value, tuple):
                            return {
                                name: conv(v) if conv is not None and v is not None else v
                                for name, conv, v in zip(dedup_field_names, field_convs, value)
                            }
                        else:
                            return value

            else:
                if all(conv is None for conv in field_convs):

                    def convert_struct(value: Any) -> Any:
                        if isinstance(value, dict):
                            return {
                                name: value.get(key, None)
                                for name, key in zip(dedup_field_names, field_names)
                            }
                        else:
                            # tuple
                            return dict(zip(dedup_field_names, value))

                else:

                    def convert_struct(value: Any) -> Any:
                        if isinstance(value, dict):
                            return {
                                name: conv(v) if conv is not None and v is not None else v
                                for name, conv, v in zip(
                                    dedup_field_names,
                                    field_convs,
                                    (value.get(key, None) for key in field_names),
                                )
                            }
                        else:
                            # tuple
                            return {
                                name: conv(v) if conv is not None and v is not None else v
                                for name, conv, v in zip(dedup_field_names, field_convs, value)
                            }

            return convert_struct

        elif isinstance(dt, TimestampType):
            assert timezone is not None

            def convert_timestamp(value: Any) -> Any:
                if isinstance(value, datetime.datetime) and value.tzinfo is not None:
                    ts = pd.Timestamp(value)
                else:
                    ts = pd.Timestamp(value).tz_localize(timezone)
                return ts.to_pydatetime()

            return convert_timestamp

        elif isinstance(dt, UserDefinedType):
            udt: UserDefinedType = dt

            conv = _converter(udt.sqlType())

            if conv is None:

                def convert_udt(value: Any) -> Any:
                    return udt.serialize(value)

            else:

                def convert_udt(value: Any) -> Any:
                    return conv(udt.serialize(value))

            return convert_udt

        return None

    conv = _converter(data_type)
    if conv is not None:
        return lambda pser: pser.apply(  # type: ignore[return-value]
            lambda x: conv(x) if x is not None else None
        )
    else:
        return lambda pser: pser


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


def _deduplicate_field_names(dt: DataType) -> DataType:
    if isinstance(dt, StructType):
        dedup_field_names = _dedup_names(dt.names)

        return StructType(
            [
                StructField(
                    dedup_field_names[i],
                    _deduplicate_field_names(field.dataType),
                    nullable=field.nullable,
                )
                for i, field in enumerate(dt.fields)
            ]
        )
    elif isinstance(dt, ArrayType):
        return ArrayType(_deduplicate_field_names(dt.elementType), containsNull=dt.containsNull)
    elif isinstance(dt, MapType):
        return MapType(
            _deduplicate_field_names(dt.keyType),
            _deduplicate_field_names(dt.valueType),
            valueContainsNull=dt.valueContainsNull,
        )
    else:
        return dt
