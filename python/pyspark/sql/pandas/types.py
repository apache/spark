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

from pyspark.sql.types import *


def to_arrow_type(dt):
    """ Convert Spark data type to pyarrow type
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
    elif type(dt) == TimestampType:
        # Timestamps should be in UTC, JVM Arrow timestamps require a timezone to be read
        arrow_type = pa.timestamp('us', tz='UTC')
    elif type(dt) == ArrayType:
        if type(dt.elementType) in [StructType, TimestampType]:
            raise TypeError("Unsupported type in conversion to Arrow: " + str(dt))
        arrow_type = pa.list_(to_arrow_type(dt.elementType))
    elif type(dt) == StructType:
        if any(type(field.dataType) == StructType for field in dt):
            raise TypeError("Nested StructType not supported in conversion to Arrow")
        fields = [pa.field(field.name, to_arrow_type(field.dataType), nullable=field.nullable)
                  for field in dt]
        arrow_type = pa.struct(fields)
    else:
        raise TypeError("Unsupported type in conversion to Arrow: " + str(dt))
    return arrow_type


def to_arrow_schema(schema):
    """ Convert a schema from Spark to Arrow
    """
    import pyarrow as pa
    fields = [pa.field(field.name, to_arrow_type(field.dataType), nullable=field.nullable)
              for field in schema]
    return pa.schema(fields)


def from_arrow_type(at):
    """ Convert pyarrow type to Spark data type.
    """
    import pyarrow.types as types
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
    elif types.is_binary(at):
        spark_type = BinaryType()
    elif types.is_date32(at):
        spark_type = DateType()
    elif types.is_timestamp(at):
        spark_type = TimestampType()
    elif types.is_list(at):
        if types.is_timestamp(at.value_type):
            raise TypeError("Unsupported type in conversion from Arrow: " + str(at))
        spark_type = ArrayType(from_arrow_type(at.value_type))
    elif types.is_struct(at):
        if any(types.is_struct(field.type) for field in at):
            raise TypeError("Nested StructType not supported in conversion from Arrow: " + str(at))
        return StructType(
            [StructField(field.name, from_arrow_type(field.type), nullable=field.nullable)
             for field in at])
    else:
        raise TypeError("Unsupported type in conversion from Arrow: " + str(at))
    return spark_type


def from_arrow_schema(arrow_schema):
    """ Convert schema from Arrow to Spark.
    """
    return StructType(
        [StructField(field.name, from_arrow_type(field.type), nullable=field.nullable)
         for field in arrow_schema])


def _get_local_timezone():
    """ Get local timezone using pytz with environment variable, or dateutil.

    If there is a 'TZ' environment variable, pass it to pandas to use pytz and use it as timezone
    string, otherwise use the special word 'dateutil/:' which means that pandas uses dateutil and
    it reads system configuration to know the system local timezone.

    See also:
    - https://github.com/pandas-dev/pandas/blob/0.19.x/pandas/tslib.pyx#L1753
    - https://github.com/dateutil/dateutil/blob/2.6.1/dateutil/tz/tz.py#L1338
    """
    import os
    return os.environ.get('TZ', 'dateutil/:')


def _check_series_localize_timestamps(s, timezone):
    """
    Convert timezone aware timestamps to timezone-naive in the specified timezone or local timezone.

    If the input series is not a timestamp series, then the same series is returned. If the input
    series is a timestamp series, then a converted series is returned.

    :param s: pandas.Series
    :param timezone: the timezone to convert. if None then use local timezone
    :return pandas.Series that have been converted to tz-naive
    """
    from pyspark.sql.pandas.utils import require_minimum_pandas_version
    require_minimum_pandas_version()

    from pandas.api.types import is_datetime64tz_dtype
    tz = timezone or _get_local_timezone()
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert(tz).dt.tz_localize(None)
    else:
        return s


def _check_series_convert_timestamps_internal(s, timezone):
    """
    Convert a tz-naive timestamp in the specified timezone or local timezone to UTC normalized for
    Spark internal storage

    :param s: a pandas.Series
    :param timezone: the timezone to convert. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been UTC normalized without a time zone
    """
    from pyspark.sql.pandas.utils import require_minimum_pandas_version
    require_minimum_pandas_version()

    from pandas.api.types import is_datetime64_dtype, is_datetime64tz_dtype
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
        return s.dt.tz_localize(tz, ambiguous=False).dt.tz_convert('UTC')
    elif is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert('UTC')
    else:
        return s


def _check_series_convert_timestamps_localize(s, from_timezone, to_timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param from_timezone: the timezone to convert from. if None then use local timezone
    :param to_timezone: the timezone to convert to. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    from pyspark.sql.pandas.utils import require_minimum_pandas_version
    require_minimum_pandas_version()

    import pandas as pd
    from pandas.api.types import is_datetime64tz_dtype, is_datetime64_dtype
    from_tz = from_timezone or _get_local_timezone()
    to_tz = to_timezone or _get_local_timezone()
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert(to_tz).dt.tz_localize(None)
    elif is_datetime64_dtype(s.dtype) and from_tz != to_tz:
        # `s.dt.tz_localize('tzlocal()')` doesn't work properly when including NaT.
        return s.apply(
            lambda ts: ts.tz_localize(from_tz, ambiguous=False).tz_convert(to_tz).tz_localize(None)
            if ts is not pd.NaT else pd.NaT)
    else:
        return s


def _check_series_convert_timestamps_local_tz(s, timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param timezone: the timezone to convert to. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, None, timezone)


def _check_series_convert_timestamps_tz_local(s, timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param timezone: the timezone to convert from. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, timezone, None)
