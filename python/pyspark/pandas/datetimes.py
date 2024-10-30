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
Date/Time related functions on pandas-on-Spark Series
"""
from typing import Any, Optional, Union, no_type_check

import numpy as np
import pandas as pd  # noqa: F401
from pandas.tseries.offsets import DateOffset

import pyspark.pandas as ps
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, TimestampType, TimestampNTZType, IntegerType
from pyspark.pandas import DataFrame
from pyspark.pandas.config import option_context


class DatetimeMethods:
    """Date/Time methods for pandas-on-Spark Series"""

    def __init__(self, series: "ps.Series"):
        if not isinstance(series.spark.data_type, (DateType, TimestampType, TimestampNTZType)):
            raise ValueError(
                "Cannot call DatetimeMethods on type {}".format(series.spark.data_type)
            )
        self._data = series

    # Properties
    @property
    def date(self) -> "ps.Series":
        """
        Returns a Series of python datetime.date objects (namely, the date
        part of Timestamps without timezone information).
        """
        # TODO: Hit a weird exception
        # syntax error in attribute name: `to_date(`start_date`)` with alias
        return self._data.spark.transform(F.to_date)

    @property
    def time(self) -> "ps.Series":
        raise NotImplementedError()

    @property
    def timetz(self) -> "ps.Series":
        raise NotImplementedError()

    @property
    def year(self) -> "ps.Series":
        """
        The year of the datetime.
        """
        return self._data.spark.transform(lambda c: F.year(c).cast(IntegerType()))

    @property
    def month(self) -> "ps.Series":
        """
        The month of the timestamp as January = 1 December = 12.
        """
        return self._data.spark.transform(lambda c: F.month(c).cast(IntegerType()))

    @property
    def day(self) -> "ps.Series":
        """
        The days of the datetime.
        """
        return self._data.spark.transform(lambda c: F.dayofmonth(c).cast(IntegerType()))

    @property
    def hour(self) -> "ps.Series":
        """
        The hours of the datetime.
        """
        return self._data.spark.transform(lambda c: F.hour(c).cast(IntegerType()))

    @property
    def minute(self) -> "ps.Series":
        """
        The minutes of the datetime.
        """
        return self._data.spark.transform(lambda c: F.minute(c).cast(IntegerType()))

    @property
    def second(self) -> "ps.Series":
        """
        The seconds of the datetime.
        """
        return self._data.spark.transform(lambda c: F.second(c).cast(IntegerType()))

    @property
    def microsecond(self) -> "ps.Series":
        """
        The microseconds of the datetime.
        """

        def pandas_microsecond(s) -> ps.Series[np.int32]:  # type: ignore[no-untyped-def]
            return s.dt.microsecond

        return self._data.pandas_on_spark.transform_batch(pandas_microsecond)

    @property
    def nanosecond(self) -> "ps.Series":
        raise NotImplementedError()

    def isocalendar(self) -> "ps.DataFrame":
        """
        Calculate year, week, and day according to the ISO 8601 standard.

            .. versionadded:: 4.0.0

        Returns
        -------
        DataFrame
            With columns year, week and day.

        .. note:: Returns have int64 type instead of UInt32 as is in pandas due to UInt32
            is not supported by spark

        Examples
        --------
        >>> dfs = ps.from_pandas(pd.date_range(start='2019-12-29', freq='D', periods=4).to_series())
        >>> dfs.dt.isocalendar()
                    year  week  day
        2019-12-29  2019    52    7
        2019-12-30  2020     1    1
        2019-12-31  2020     1    2
        2020-01-01  2020     1    3

        >>> dfs.dt.isocalendar().week
        2019-12-29    52
        2019-12-30     1
        2019-12-31     1
        2020-01-01     1
        Name: week, dtype: int64
        """

        return_types = [self._data.index.dtype, int, int, int]

        def pandas_isocalendar(  # type: ignore[no-untyped-def]
            pdf,
        ) -> ps.DataFrame[return_types]:  # type: ignore[valid-type]
            # cast to int64 due to UInt32 is not supported by spark
            return pdf[pdf.columns[0]].dt.isocalendar().astype(np.int64).reset_index()

        with option_context("compute.default_index_type", "distributed"):
            psdf = self._data.to_frame().pandas_on_spark.apply_batch(pandas_isocalendar)

        return DataFrame(
            psdf._internal.copy(
                spark_frame=psdf._internal.spark_frame,
                index_spark_columns=psdf._internal.data_spark_columns[:1],
                index_fields=psdf._internal.data_fields[:1],
                data_spark_columns=psdf._internal.data_spark_columns[1:],
                data_fields=psdf._internal.data_fields[1:],
                column_labels=[("year",), ("week",), ("day",)],
            )
        )

    @property
    def dayofweek(self) -> "ps.Series":
        """
        The day of the week with Monday=0, Sunday=6.

        Return the day of the week. It is assumed the week starts on
        Monday, which is denoted by 0 and ends on Sunday which is denoted
        by 6. This method is available on both Series with datetime
        values (using the `dt` accessor).

        Returns
        -------
        Series
            Containing integers indicating the day number.

        See Also
        --------
        Series.dt.dayofweek : Alias.
        Series.dt.weekday : Alias.
        Series.dt.day_name : Returns the name of the day of the week.

        Examples
        --------
        >>> s = ps.from_pandas(pd.date_range('2016-12-31', '2017-01-08', freq='D').to_series())
        >>> s.dt.dayofweek
        2016-12-31    5
        2017-01-01    6
        2017-01-02    0
        2017-01-03    1
        2017-01-04    2
        2017-01-05    3
        2017-01-06    4
        2017-01-07    5
        2017-01-08    6
        dtype: int32
        """

        def pandas_dayofweek(s) -> ps.Series[np.int32]:  # type: ignore[no-untyped-def]
            return s.dt.dayofweek

        return self._data.pandas_on_spark.transform_batch(pandas_dayofweek)

    @property
    def weekday(self) -> "ps.Series":
        return self.dayofweek

    weekday.__doc__ = dayofweek.__doc__

    @property
    def dayofyear(self) -> "ps.Series":
        """
        The ordinal day of the year.
        """

        def pandas_dayofyear(s) -> ps.Series[np.int32]:  # type: ignore[no-untyped-def]
            return s.dt.dayofyear

        return self._data.pandas_on_spark.transform_batch(pandas_dayofyear)

    @property
    def quarter(self) -> "ps.Series":
        """
        The quarter of the date.
        """

        def pandas_quarter(s) -> ps.Series[np.int32]:  # type: ignore[no-untyped-def]
            return s.dt.quarter

        return self._data.pandas_on_spark.transform_batch(pandas_quarter)

    @property
    def is_month_start(self) -> "ps.Series":
        """
        Indicates whether the date is the first day of the month.

        Returns
        -------
        Series
            For Series, returns a Series with boolean values.

        See Also
        --------
        is_month_end : Return a boolean indicating whether the date
            is the last day of the month.

        Examples
        --------
        This method is available on Series with datetime values under
        the ``.dt`` accessor.

        >>> s = ps.Series(pd.date_range("2018-02-27", periods=3))
        >>> s
        0   2018-02-27
        1   2018-02-28
        2   2018-03-01
        dtype: datetime64[ns]

        >>> s.dt.is_month_start
        0    False
        1    False
        2     True
        dtype: bool
        """

        def pandas_is_month_start(s) -> ps.Series[bool]:  # type: ignore[no-untyped-def]
            return s.dt.is_month_start

        return self._data.pandas_on_spark.transform_batch(pandas_is_month_start)

    @property
    def is_month_end(self) -> "ps.Series":
        """
        Indicates whether the date is the last day of the month.

        Returns
        -------
        Series
            For Series, returns a Series with boolean values.

        See Also
        --------
        is_month_start : Return a boolean indicating whether the date
            is the first day of the month.

        Examples
        --------
        This method is available on Series with datetime values under
        the ``.dt`` accessor.

        >>> s = ps.Series(pd.date_range("2018-02-27", periods=3))
        >>> s
        0   2018-02-27
        1   2018-02-28
        2   2018-03-01
        dtype: datetime64[ns]

        >>> s.dt.is_month_end
        0    False
        1     True
        2    False
        dtype: bool
        """

        def pandas_is_month_end(s) -> ps.Series[bool]:  # type: ignore[no-untyped-def]
            return s.dt.is_month_end

        return self._data.pandas_on_spark.transform_batch(pandas_is_month_end)

    @property
    def is_quarter_start(self) -> "ps.Series":
        """
        Indicator for whether the date is the first day of a quarter.

        Returns
        -------
        is_quarter_start : Series
            The same type as the original data with boolean values. Series will
            have the same name and index.

        See Also
        --------
        quarter : Return the quarter of the date.
        is_quarter_end : Similar property for indicating the quarter start.

        Examples
        --------
        This method is available on Series with datetime values under
        the ``.dt`` accessor.

        >>> df = ps.DataFrame({'dates': pd.date_range("2017-03-30",
        ...                   periods=4)})
        >>> df
               dates
        0 2017-03-30
        1 2017-03-31
        2 2017-04-01
        3 2017-04-02

        >>> df.dates.dt.quarter
        0    1
        1    1
        2    2
        3    2
        Name: dates, dtype: int32

        >>> df.dates.dt.is_quarter_start
        0    False
        1    False
        2     True
        3    False
        Name: dates, dtype: bool
        """

        def pandas_is_quarter_start(s) -> ps.Series[bool]:  # type: ignore[no-untyped-def]
            return s.dt.is_quarter_start

        return self._data.pandas_on_spark.transform_batch(pandas_is_quarter_start)

    @property
    def is_quarter_end(self) -> "ps.Series":
        """
        Indicator for whether the date is the last day of a quarter.

        Returns
        -------
        is_quarter_end : Series
            The same type as the original data with boolean values. Series will
            have the same name and index.

        See Also
        --------
        quarter : Return the quarter of the date.
        is_quarter_start : Similar property indicating the quarter start.

        Examples
        --------
        This method is available on Series with datetime values under
        the ``.dt`` accessor.

        >>> df = ps.DataFrame({'dates': pd.date_range("2017-03-30",
        ...                   periods=4)})
        >>> df
               dates
        0 2017-03-30
        1 2017-03-31
        2 2017-04-01
        3 2017-04-02

        >>> df.dates.dt.quarter
        0    1
        1    1
        2    2
        3    2
        Name: dates, dtype: int32

        >>> df.dates.dt.is_quarter_start
        0    False
        1    False
        2     True
        3    False
        Name: dates, dtype: bool
        """

        def pandas_is_quarter_end(s) -> ps.Series[bool]:  # type: ignore[no-untyped-def]
            return s.dt.is_quarter_end

        return self._data.pandas_on_spark.transform_batch(pandas_is_quarter_end)

    @property
    def is_year_start(self) -> "ps.Series":
        """
        Indicate whether the date is the first day of a year.

        Returns
        -------
        Series
            The same type as the original data with boolean values. Series will
            have the same name and index.

        See Also
        --------
        is_year_end : Similar property indicating the last day of the year.

        Examples
        --------
        This method is available on Series with datetime values under
        the ``.dt`` accessor.

        >>> dates = ps.Series(pd.date_range("2017-12-30", periods=3))
        >>> dates
        0   2017-12-30
        1   2017-12-31
        2   2018-01-01
        dtype: datetime64[ns]

        >>> dates.dt.is_year_start
        0    False
        1    False
        2     True
        dtype: bool
        """

        def pandas_is_year_start(s) -> ps.Series[bool]:  # type: ignore[no-untyped-def]
            return s.dt.is_year_start

        return self._data.pandas_on_spark.transform_batch(pandas_is_year_start)

    @property
    def is_year_end(self) -> "ps.Series":
        """
        Indicate whether the date is the last day of the year.

        Returns
        -------
        Series
            The same type as the original data with boolean values. Series will
            have the same name and index.

        See Also
        --------
        is_year_start : Similar property indicating the start of the year.

        Examples
        --------
        This method is available on Series with datetime values under
        the ``.dt`` accessor.

        >>> dates = ps.Series(pd.date_range("2017-12-30", periods=3))
        >>> dates
        0   2017-12-30
        1   2017-12-31
        2   2018-01-01
        dtype: datetime64[ns]

        >>> dates.dt.is_year_end
        0    False
        1     True
        2    False
        dtype: bool
        """

        def pandas_is_year_end(s) -> ps.Series[bool]:  # type: ignore[no-untyped-def]
            return s.dt.is_year_end

        return self._data.pandas_on_spark.transform_batch(pandas_is_year_end)

    @property
    def is_leap_year(self) -> "ps.Series":
        """
        Boolean indicator if the date belongs to a leap year.

        A leap year is a year, which has 366 days (instead of 365) including
        29th of February as an intercalary day.
        Leap years are years which are multiples of four with the exception
        of years divisible by 100 but not by 400.

        Returns
        -------
        Series
             Booleans indicating if dates belong to a leap year.

        Examples
        --------
        This method is available on Series with datetime values under
        the ``.dt`` accessor.

        >>> dates_series = ps.Series(pd.date_range("2012-01-01", "2015-01-01", freq="Y"))
        >>> dates_series
        0   2012-12-31
        1   2013-12-31
        2   2014-12-31
        dtype: datetime64[ns]

        >>> dates_series.dt.is_leap_year
        0     True
        1    False
        2    False
        dtype: bool
        """

        def pandas_is_leap_year(s) -> ps.Series[bool]:  # type: ignore[no-untyped-def]
            return s.dt.is_leap_year

        return self._data.pandas_on_spark.transform_batch(pandas_is_leap_year)

    @property
    def daysinmonth(self) -> "ps.Series":
        """
        The number of days in the month.
        """

        def pandas_daysinmonth(s) -> ps.Series[np.int32]:  # type: ignore[no-untyped-def]
            return s.dt.daysinmonth

        return self._data.pandas_on_spark.transform_batch(pandas_daysinmonth)

    @property
    def days_in_month(self) -> "ps.Series":
        return self.daysinmonth

    days_in_month.__doc__ = daysinmonth.__doc__

    # Methods

    @no_type_check
    def tz_localize(self, tz) -> "ps.Series":
        """
        Localize tz-naive Datetime column to tz-aware Datetime column.
        """
        # Neither tz-naive or tz-aware datetime exists in Spark
        raise NotImplementedError()

    @no_type_check
    def tz_convert(self, tz) -> "ps.Series":
        """
        Convert tz-aware Datetime column from one time zone to another.
        """
        # tz-aware datetime doesn't exist in Spark
        raise NotImplementedError()

    def normalize(self) -> "ps.Series":
        """
        Convert times to midnight.

        The time component of the date-time is converted to midnight i.e.
        00:00:00. This is useful in cases, when the time does not matter.
        Length is unaltered. The time zones are unaffected.

        This method is available on Series with datetime values under
        the ``.dt`` accessor, and directly on Datetime Array.

        Returns
        -------
        Series
            The same type as the original data. Series will have the same
            name and index.

        See Also
        --------
        floor : Floor the series to the specified freq.
        ceil : Ceil the series to the specified freq.
        round : Round the series to the specified freq.

        Examples
        --------
        >>> series = ps.Series(pd.Series(pd.date_range('2012-1-1 12:45:31', periods=3, freq='ME')))
        >>> series.dt.normalize()
        0   2012-01-31
        1   2012-02-29
        2   2012-03-31
        dtype: datetime64[ns]
        """

        def pandas_normalize(s) -> ps.Series[np.datetime64]:  # type: ignore[no-untyped-def]
            return s.dt.normalize()

        return self._data.pandas_on_spark.transform_batch(pandas_normalize)

    def strftime(self, date_format: str) -> "ps.Series":
        """
        Convert to a string Series using specified date_format.

        Return an series of formatted strings specified by date_format, which
        supports the same string format as the python standard library. Details
        of the string format can be found in the python string format
        doc.

        Parameters
        ----------
        date_format : str
            Date format string (example: "%%Y-%%m-%%d").

        Returns
        -------
        Series
            Series of formatted strings.

        See Also
        --------
        to_datetime : Convert the given argument to datetime.
        normalize : Return series with times to midnight.
        round : Round the series to the specified freq.
        floor : Floor the series to the specified freq.

        Examples
        --------
        >>> series = ps.Series(pd.date_range(pd.Timestamp("2018-03-10 09:00"),
        ...                                  periods=3, freq='s'))
        >>> series
        0   2018-03-10 09:00:00
        1   2018-03-10 09:00:01
        2   2018-03-10 09:00:02
        dtype: datetime64[ns]

        >>> series.dt.strftime('%B %d, %Y, %r')
        0    March 10, 2018, 09:00:00 AM
        1    March 10, 2018, 09:00:01 AM
        2    March 10, 2018, 09:00:02 AM
        dtype: object
        """

        def pandas_strftime(s) -> ps.Series[str]:  # type: ignore[no-untyped-def]
            return s.dt.strftime(date_format)

        return self._data.pandas_on_spark.transform_batch(pandas_strftime)

    def round(self, freq: Union[str, DateOffset], *args: Any, **kwargs: Any) -> "ps.Series":
        """
        Perform round operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to round the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end).

        nonexistent : 'shift_forward', 'shift_backward, 'NaT', timedelta, default 'raise'
            A nonexistent time does not exist in a particular timezone
            where clocks moved forward due to DST.

            - 'shift_forward' will shift the nonexistent time forward to the
              closest existing time
            - 'shift_backward' will shift the nonexistent time backward to the
              closest existing time
            - 'NaT' will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - 'raise' will raise an NonExistentTimeError if there are
              nonexistent times

            .. note:: this option only works with pandas 0.24.0+

        Returns
        -------
        Series
            a Series with the same index for a Series.

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        >>> series = ps.Series(pd.date_range('1/1/2018 11:59:00', periods=3, freq='min'))
        >>> series
        0   2018-01-01 11:59:00
        1   2018-01-01 12:00:00
        2   2018-01-01 12:01:00
        dtype: datetime64[ns]

        >>> series.dt.round("H")
        0   2018-01-01 12:00:00
        1   2018-01-01 12:00:00
        2   2018-01-01 12:00:00
        dtype: datetime64[ns]
        """

        def pandas_round(s) -> ps.Series[np.datetime64]:  # type: ignore[no-untyped-def]
            return s.dt.round(freq, *args, **kwargs)

        return self._data.pandas_on_spark.transform_batch(pandas_round)

    def floor(self, freq: Union[str, DateOffset], *args: Any, **kwargs: Any) -> "ps.Series":
        """
        Perform floor operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to floor the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end).

        nonexistent : 'shift_forward', 'shift_backward, 'NaT', timedelta, default 'raise'
            A nonexistent time does not exist in a particular timezone
            where clocks moved forward due to DST.

            - 'shift_forward' will shift the nonexistent time forward to the
              closest existing time
            - 'shift_backward' will shift the nonexistent time backward to the
              closest existing time
            - 'NaT' will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - 'raise' will raise an NonExistentTimeError if there are
              nonexistent times

            .. note:: this option only works with pandas 0.24.0+

        Returns
        -------
        Series
            a Series with the same index for a Series.

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        >>> series = ps.Series(pd.date_range('1/1/2018 11:59:00', periods=3, freq='min'))
        >>> series
        0   2018-01-01 11:59:00
        1   2018-01-01 12:00:00
        2   2018-01-01 12:01:00
        dtype: datetime64[ns]

        >>> series.dt.floor("H")
        0   2018-01-01 11:00:00
        1   2018-01-01 12:00:00
        2   2018-01-01 12:00:00
        dtype: datetime64[ns]
        """

        def pandas_floor(s) -> ps.Series[np.datetime64]:  # type: ignore[no-untyped-def]
            return s.dt.floor(freq, *args, **kwargs)

        return self._data.pandas_on_spark.transform_batch(pandas_floor)

    def ceil(self, freq: Union[str, DateOffset], *args: Any, **kwargs: Any) -> "ps.Series":
        """
        Perform ceil operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to round the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end).

        nonexistent : 'shift_forward', 'shift_backward, 'NaT', timedelta, default 'raise'
            A nonexistent time does not exist in a particular timezone
            where clocks moved forward due to DST.

            - 'shift_forward' will shift the nonexistent time forward to the
              closest existing time
            - 'shift_backward' will shift the nonexistent time backward to the
              closest existing time
            - 'NaT' will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - 'raise' will raise an NonExistentTimeError if there are
              nonexistent times

            .. note:: this option only works with pandas 0.24.0+

        Returns
        -------
        Series
            a Series with the same index for a Series.

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        >>> series = ps.Series(pd.date_range('1/1/2018 11:59:00', periods=3, freq='min'))
        >>> series
        0   2018-01-01 11:59:00
        1   2018-01-01 12:00:00
        2   2018-01-01 12:01:00
        dtype: datetime64[ns]

        >>> series.dt.ceil("H")
        0   2018-01-01 12:00:00
        1   2018-01-01 12:00:00
        2   2018-01-01 13:00:00
        dtype: datetime64[ns]
        """

        def pandas_ceil(s) -> ps.Series[np.datetime64]:  # type: ignore[no-untyped-def]
            return s.dt.ceil(freq, *args, **kwargs)

        return self._data.pandas_on_spark.transform_batch(pandas_ceil)

    def month_name(self, locale: Optional[str] = None) -> "ps.Series":
        """
        Return the month names of the series with specified locale.

        Parameters
        ----------
        locale : str, optional
            Locale determining the language in which to return the month name.
            Default is English locale.

        Returns
        -------
        Series
            Series of month names.

        Examples
        --------
        >>> series = ps.Series(pd.date_range(start='2018-01', freq='ME', periods=3))
        >>> series
        0   2018-01-31
        1   2018-02-28
        2   2018-03-31
        dtype: datetime64[ns]

        >>> series.dt.month_name()
        0     January
        1    February
        2       March
        dtype: object
        """

        def pandas_month_name(s) -> ps.Series[str]:  # type: ignore[no-untyped-def]
            return s.dt.month_name(locale=locale)

        return self._data.pandas_on_spark.transform_batch(pandas_month_name)

    def day_name(self, locale: Optional[str] = None) -> "ps.Series":
        """
        Return the day names of the series with specified locale.

        Parameters
        ----------
        locale : str, optional
            Locale determining the language in which to return the day name.
            Default is English locale.

        Returns
        -------
        Series
            Series of day names.

        Examples
        --------
        >>> series = ps.Series(pd.date_range(start='2018-01-01', freq='D', periods=3))
        >>> series
        0   2018-01-01
        1   2018-01-02
        2   2018-01-03
        dtype: datetime64[ns]

        >>> series.dt.day_name()
        0       Monday
        1      Tuesday
        2    Wednesday
        dtype: object
        """

        def pandas_day_name(s) -> ps.Series[str]:  # type: ignore[no-untyped-def]
            return s.dt.day_name(locale=locale)

        return self._data.pandas_on_spark.transform_batch(pandas_day_name)


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.datetimes

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.datetimes.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.datetimes tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.datetimes,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
