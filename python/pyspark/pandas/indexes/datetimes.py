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
import datetime
from functools import partial
from typing import Any, Optional, Union, cast, no_type_check

import pandas as pd
from pandas.api.types import is_hashable
from pandas.tseries.offsets import DateOffset
from pyspark._globals import _NoValue

from pyspark import pandas as ps
from pyspark.pandas.indexes.base import Index
from pyspark.pandas.missing.indexes import MissingPandasLikeDatetimeIndex
from pyspark.pandas.series import Series, first_series
from pyspark.pandas.utils import verify_temp_column_name


class DatetimeIndex(Index):
    """
    Immutable ndarray-like of datetime64 data.

    Parameters
    ----------
    data : array-like (1-dimensional), optional
        Optional datetime-like data to construct index with.
    freq : str or pandas offset object, optional
        One of pandas date offset strings or corresponding objects. The string
        'infer' can be passed in order to set the frequency of the index as the
        inferred frequency upon creation.
    normalize : bool, default False
        Normalize start/end dates to midnight before generating date range.
    closed : {'left', 'right'}, optional
        Set whether to include `start` and `end` that are on the
        boundary. The default includes boundary points on either end.
    ambiguous : 'infer', bool-ndarray, 'NaT', default 'raise'
        When clocks moved backward due to DST, ambiguous times may arise.
        For example in Central European Time (UTC+01), when going from 03:00
        DST to 02:00 non-DST, 02:30:00 local time occurs both at 00:30:00 UTC
        and at 01:30:00 UTC. In such a situation, the `ambiguous` parameter
        dictates how ambiguous times should be handled.

        - 'infer' will attempt to infer fall dst-transition hours based on
          order
        - bool-ndarray where True signifies a DST time, False signifies a
          non-DST time (note that this flag is only applicable for ambiguous
          times)
        - 'NaT' will return NaT where there are ambiguous times
        - 'raise' will raise an AmbiguousTimeError if there are ambiguous times.
    dayfirst : bool, default False
        If True, parse dates in `data` with the day first order.
    yearfirst : bool, default False
        If True parse dates in `data` with the year first order.
    dtype : numpy.dtype or str, default None
        Note that the only NumPy dtype allowed is ‘datetime64[ns]’.
    copy : bool, default False
        Make a copy of input ndarray.
    name : label, default None
        Name to be stored in the index.

    See Also
    --------
    Index : The base pandas Index type.
    to_datetime : Convert argument to datetime.

    Examples
    --------
    >>> ps.DatetimeIndex(['1970-01-01', '1970-01-01', '1970-01-01'])
    DatetimeIndex(['1970-01-01', '1970-01-01', '1970-01-01'], dtype='datetime64[ns]', freq=None)

    From a Series:

    >>> from datetime import datetime
    >>> s = ps.Series([datetime(2021, 3, 1), datetime(2021, 3, 2)], index=[10, 20])
    >>> ps.DatetimeIndex(s)
    DatetimeIndex(['2021-03-01', '2021-03-02'], dtype='datetime64[ns]', freq=None)

    From an Index:

    >>> idx = ps.DatetimeIndex(['1970-01-01', '1970-01-01', '1970-01-01'])
    >>> ps.DatetimeIndex(idx)
    DatetimeIndex(['1970-01-01', '1970-01-01', '1970-01-01'], dtype='datetime64[ns]', freq=None)
    """

    @no_type_check
    def __new__(
        cls,
        data=None,
        freq=_NoValue,
        normalize=False,
        closed=None,
        ambiguous="raise",
        dayfirst=False,
        yearfirst=False,
        dtype=None,
        copy=False,
        name=None,
    ) -> "DatetimeIndex":
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, (Series, Index)):
            if dtype is None:
                dtype = "datetime64[ns]"
            return cast(DatetimeIndex, Index(data, dtype=dtype, copy=copy, name=name))

        kwargs = dict(
            data=data,
            normalize=normalize,
            closed=closed,
            ambiguous=ambiguous,
            dayfirst=dayfirst,
            yearfirst=yearfirst,
            dtype=dtype,
            copy=copy,
            name=name,
        )
        if freq is not _NoValue:
            kwargs["freq"] = freq

        return cast(DatetimeIndex, ps.from_pandas(pd.DatetimeIndex(**kwargs)))

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeDatetimeIndex, item):
            property_or_func = getattr(MissingPandasLikeDatetimeIndex, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)  # type: ignore
            else:
                return partial(property_or_func, self)
        raise AttributeError("'DatetimeIndex' object has no attribute '{}'".format(item))

    # Properties
    @property
    def year(self) -> Index:
        """
        The year of the datetime.
        """
        return Index(self.to_series().dt.year)

    @property
    def month(self) -> Index:
        """
        The month of the timestamp as January = 1 December = 12.
        """
        return Index(self.to_series().dt.month)

    @property
    def day(self) -> Index:
        """
        The days of the datetime.
        """
        return Index(self.to_series().dt.day)

    @property
    def hour(self) -> Index:
        """
        The hours of the datetime.
        """
        return Index(self.to_series().dt.hour)

    @property
    def minute(self) -> Index:
        """
        The minutes of the datetime.
        """
        return Index(self.to_series().dt.minute)

    @property
    def second(self) -> Index:
        """
        The seconds of the datetime.
        """
        return Index(self.to_series().dt.second)

    @property
    def microsecond(self) -> Index:
        """
        The microseconds of the datetime.
        """
        return Index(self.to_series().dt.microsecond)

    @property
    def week(self) -> Index:
        """
        The week ordinal of the year.
        """
        return Index(self.to_series().dt.week)

    @property
    def weekofyear(self) -> Index:
        return Index(self.to_series().dt.weekofyear)

    weekofyear.__doc__ = week.__doc__

    @property
    def dayofweek(self) -> Index:
        """
        The day of the week with Monday=0, Sunday=6.
        Return the day of the week. It is assumed the week starts on
        Monday, which is denoted by 0 and ends on Sunday which is denoted
        by 6. This method is available on both Series with datetime
        values (using the `dt` accessor) or DatetimeIndex.

        Returns
        -------
        Series or Index
            Containing integers indicating the day number.

        See Also
        --------
        Series.dt.dayofweek : Alias.
        Series.dt.weekday : Alias.
        Series.dt.day_name : Returns the name of the day of the week.

        Examples
        --------
        >>> idx = ps.date_range('2016-12-31', '2017-01-08', freq='D')
        >>> idx.dayofweek
        Int64Index([5, 6, 0, 1, 2, 3, 4, 5, 6], dtype='int64')
        """
        return Index(self.to_series().dt.dayofweek)

    @property
    def day_of_week(self) -> Index:
        return self.dayofweek

    day_of_week.__doc__ = dayofweek.__doc__

    @property
    def weekday(self) -> Index:
        return Index(self.to_series().dt.weekday)

    weekday.__doc__ = dayofweek.__doc__

    @property
    def dayofyear(self) -> Index:
        """
        The ordinal day of the year.
        """
        return Index(self.to_series().dt.dayofyear)

    @property
    def day_of_year(self) -> Index:
        return self.dayofyear

    day_of_year.__doc__ = dayofyear.__doc__

    @property
    def quarter(self) -> Index:
        """
        The quarter of the date.
        """
        return Index(self.to_series().dt.quarter)

    @property
    def is_month_start(self) -> Index:
        """
        Indicates whether the date is the first day of the month.

        Returns
        -------
        Index
            Returns a Index with boolean values

        See Also
        --------
        is_month_end : Return a boolean indicating whether the date
            is the last day of the month.

        Examples
        --------
        >>> idx = ps.date_range("2018-02-27", periods=3)
        >>> idx.is_month_start
        Index([False, False, True], dtype='object')
        """
        return Index(self.to_series().dt.is_month_start)

    @property
    def is_month_end(self) -> Index:
        """
        Indicates whether the date is the last day of the month.

        Returns
        -------
        Index
            Returns a Index with boolean values.

        See Also
        --------
        is_month_start : Return a boolean indicating whether the date
            is the first day of the month.

        Examples
        --------
        >>> idx = ps.date_range("2018-02-27", periods=3)
        >>> idx.is_month_end
        Index([False, True, False], dtype='object')
        """
        return Index(self.to_series().dt.is_month_end)

    @property
    def is_quarter_start(self) -> Index:
        """
        Indicator for whether the date is the first day of a quarter.

        Returns
        -------
        is_quarter_start : Index
            Returns an Index with boolean values.

        See Also
        --------
        quarter : Return the quarter of the date.
        is_quarter_end : Similar property for indicating the quarter start.

        Examples
        --------
        >>> idx = ps.date_range('2017-03-30', periods=4)
        >>> idx.is_quarter_start
        Index([False, False, True, False], dtype='object')
        """
        return Index(self.to_series().dt.is_quarter_start)

    @property
    def is_quarter_end(self) -> Index:
        """
        Indicator for whether the date is the last day of a quarter.

        Returns
        -------
        is_quarter_end : Index
            Returns an Index with boolean values.

        See Also
        --------
        quarter : Return the quarter of the date.
        is_quarter_start : Similar property indicating the quarter start.

        Examples
        --------
        >>> idx = ps.date_range('2017-03-30', periods=4)
        >>> idx.is_quarter_end
        Index([False, True, False, False], dtype='object')
        """
        return Index(self.to_series().dt.is_quarter_end)

    @property
    def is_year_start(self) -> Index:
        """
        Indicate whether the date is the first day of a year.

        Returns
        -------
        Index
            Returns an Index with boolean values.

        See Also
        --------
        is_year_end : Similar property indicating the last day of the year.

        Examples
        --------
        >>> idx = ps.date_range("2017-12-30", periods=3)
        >>> idx.is_year_start
        Index([False, False, True], dtype='object')
        """
        return Index(self.to_series().dt.is_year_start)

    @property
    def is_year_end(self) -> Index:
        """
        Indicate whether the date is the last day of the year.

        Returns
        -------
        Index
            Returns an Index with boolean values.

        See Also
        --------
        is_year_start : Similar property indicating the start of the year.

        Examples
        --------
        >>> idx = ps.date_range("2017-12-30", periods=3)
        >>> idx.is_year_end
        Index([False, True, False], dtype='object')
        """
        return Index(self.to_series().dt.is_year_end)

    @property
    def is_leap_year(self) -> Index:
        """
        Boolean indicator if the date belongs to a leap year.

        A leap year is a year, which has 366 days (instead of 365) including
        29th of February as an intercalary day.
        Leap years are years which are multiples of four with the exception
        of years divisible by 100 but not by 400.

        Returns
        -------
        Index
             Booleans indicating if dates belong to a leap year.

        Examples
        --------
        >>> idx = ps.date_range("2012-01-01", "2015-01-01", freq="Y")
        >>> idx.is_leap_year
        Index([True, False, False], dtype='object')
        """
        return Index(self.to_series().dt.is_leap_year)

    @property
    def daysinmonth(self) -> Index:
        """
        The number of days in the month.
        """
        return Index(self.to_series().dt.daysinmonth)

    @property
    def days_in_month(self) -> Index:
        return Index(self.to_series().dt.days_in_month)

    days_in_month.__doc__ = daysinmonth.__doc__

    # Methods
    def ceil(self, freq: Union[str, DateOffset], *args: Any, **kwargs: Any) -> "DatetimeIndex":
        """
        Perform ceil operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to ceil the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end).

        Returns
        -------
        DatetimeIndex

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        >>> rng = ps.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng.ceil('H')  # doctest: +NORMALIZE_WHITESPACE
        DatetimeIndex(['2018-01-01 12:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 13:00:00'],
                      dtype='datetime64[ns]', freq=None)
        """
        disallow_nanoseconds(freq)

        return DatetimeIndex(self.to_series().dt.ceil(freq, *args, **kwargs))

    def floor(self, freq: Union[str, DateOffset], *args: Any, **kwargs: Any) -> "DatetimeIndex":
        """
        Perform floor operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to floor the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end).

        Returns
        -------
        DatetimeIndex

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        >>> rng = ps.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng.floor("H")  # doctest: +NORMALIZE_WHITESPACE
        DatetimeIndex(['2018-01-01 11:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:00:00'],
                      dtype='datetime64[ns]', freq=None)
        """
        disallow_nanoseconds(freq)

        return DatetimeIndex(self.to_series().dt.floor(freq, *args, **kwargs))

    def round(self, freq: Union[str, DateOffset], *args: Any, **kwargs: Any) -> "DatetimeIndex":
        """
        Perform round operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to round the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end).

        Returns
        -------
        DatetimeIndex

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        >>> rng = ps.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng.round("H")  # doctest: +NORMALIZE_WHITESPACE
        DatetimeIndex(['2018-01-01 12:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:00:00'],
                      dtype='datetime64[ns]', freq=None)
        """
        disallow_nanoseconds(freq)

        return DatetimeIndex(self.to_series().dt.round(freq, *args, **kwargs))

    def month_name(self, locale: Optional[str] = None) -> Index:
        """
        Return the month names of the DatetimeIndex with specified locale.

        Parameters
        ----------
        locale : str, optional
            Locale determining the language in which to return the month name.
            Default is English locale.

        Returns
        -------
        Index
            Index of month names.

        Examples
        --------
        >>> idx = ps.date_range(start='2018-01', freq='M', periods=3)
        >>> idx.month_name()
        Index(['January', 'February', 'March'], dtype='object')
        """
        return Index(self.to_series().dt.month_name(locale))

    def day_name(self, locale: Optional[str] = None) -> Index:
        """
        Return the day names of the series with specified locale.

        Parameters
        ----------
        locale : str, optional
            Locale determining the language in which to return the day name.
            Default is English locale.

        Returns
        -------
        Index
            Index of day names.

        Examples
        --------
        >>> idx = ps.date_range(start='2018-01-01', freq='D', periods=3)
        >>> idx.day_name()
        Index(['Monday', 'Tuesday', 'Wednesday'], dtype='object')
        """
        return Index(self.to_series().dt.day_name(locale))

    def normalize(self) -> "DatetimeIndex":
        """
        Convert times to midnight.

        The time component of the date-time is converted to midnight i.e.
        00:00:00. This is useful in cases, when the time does not matter.
        Length is unaltered. The timezones are unaffected.

        This method is available on Series with datetime values under
        the ``.dt`` accessor.

        Returns
        -------
        DatetimeIndex
            The same type as the original data.

        See Also
        --------
        floor : Floor the series to the specified freq.
        ceil : Ceil the series to the specified freq.
        round : Round the series to the specified freq.

        Examples
        --------
        >>> idx = ps.date_range(start='2014-08-01 10:00', freq='H', periods=3)
        >>> idx.normalize()
        DatetimeIndex(['2014-08-01', '2014-08-01', '2014-08-01'], dtype='datetime64[ns]', freq=None)
        """
        return DatetimeIndex(self.to_series().dt.normalize())

    def strftime(self, date_format: str) -> Index:
        """
        Convert to a string Index using specified date_format.

        Return an Index of formatted strings specified by date_format, which
        supports the same string format as the python standard library. Details
        of the string format can be found in python string format
        doc.

        Parameters
        ----------
        date_format : str
            Date format string (example: "%%Y-%%m-%%d").

        Returns
        -------
        Index
            Index of formatted strings.

        See Also
        --------
        normalize : Return series with times to midnight.
        round : Round the series to the specified freq.
        floor : Floor the series to the specified freq.

        Examples
        --------
        >>> idx = ps.date_range(pd.Timestamp("2018-03-10 09:00"), periods=3, freq='s')
        >>> idx.strftime('%B %d, %Y, %r')  # doctest: +NORMALIZE_WHITESPACE
        Index(['March 10, 2018, 09:00:00 AM', 'March 10, 2018, 09:00:01 AM',
               'March 10, 2018, 09:00:02 AM'],
              dtype='object')
        """
        return Index(self.to_series().dt.strftime(date_format))

    def indexer_between_time(
        self,
        start_time: Union[datetime.time, str],
        end_time: Union[datetime.time, str],
        include_start: bool = True,
        include_end: bool = True,
    ) -> Index:
        """
        Return index locations of values between particular times of day
        (example: 9:00-9:30AM).

        Parameters
        ----------
        start_time, end_time : datetime.time, str
            Time passed either as object (datetime.time) or as string in
            appropriate format ("%H:%M", "%H%M", "%I:%M%p", "%I%M%p",
            "%H:%M:%S", "%H%M%S", "%I:%M:%S%p","%I%M%S%p").
        include_start : bool, default True
        include_end : bool, default True

        Returns
        -------
        values_between_time : Index of integers

        Examples
        --------
        >>> psidx = ps.date_range("2000-01-01", periods=3, freq="T")
        >>> psidx  # doctest: +NORMALIZE_WHITESPACE
        DatetimeIndex(['2000-01-01 00:00:00', '2000-01-01 00:01:00',
                       '2000-01-01 00:02:00'],
                      dtype='datetime64[ns]', freq=None)

        >>> psidx.indexer_between_time("00:01", "00:02").sort_values()
        Int64Index([1, 2], dtype='int64')

        >>> psidx.indexer_between_time("00:01", "00:02", include_end=False)
        Int64Index([1], dtype='int64')

        >>> psidx.indexer_between_time("00:01", "00:02", include_start=False)
        Int64Index([2], dtype='int64')
        """

        @no_type_check
        def pandas_between_time(pdf) -> ps.DataFrame[int]:
            return pdf.between_time(start_time, end_time, include_start, include_end)

        psdf = self.to_frame()[[]]
        id_column_name = verify_temp_column_name(psdf, "__id_column__")
        psdf = psdf.pandas_on_spark.attach_id_column("distributed-sequence", id_column_name)
        with ps.option_context("compute.default_index_type", "distributed"):
            # The attached index in the statement below will be dropped soon,
            # so we enforce “distributed” default index type
            psdf = psdf.pandas_on_spark.apply_batch(pandas_between_time)
        return ps.Index(first_series(psdf).rename(self.name))

    def indexer_at_time(self, time: Union[datetime.time, str], asof: bool = False) -> Index:
        """
        Return index locations of values at particular time of day
        (example: 9:30AM).

        Parameters
        ----------
        time : datetime.time or str
            Time passed in either as object (datetime.time) or as string in
            appropriate format ("%H:%M", "%H%M", "%I:%M%p", "%I%M%p",
            "%H:%M:%S", "%H%M%S", "%I:%M:%S%p", "%I%M%S%p").

        Returns
        -------
        values_at_time : Index of integers

        Examples
        --------
        >>> psidx = ps.date_range("2000-01-01", periods=3, freq="T")
        >>> psidx  # doctest: +NORMALIZE_WHITESPACE
        DatetimeIndex(['2000-01-01 00:00:00', '2000-01-01 00:01:00',
                       '2000-01-01 00:02:00'],
                      dtype='datetime64[ns]', freq=None)

        >>> psidx.indexer_at_time("00:00")
        Int64Index([0], dtype='int64')

        >>> psidx.indexer_at_time("00:01")
        Int64Index([1], dtype='int64')
        """
        if asof:
            raise NotImplementedError("'asof' argument is not supported")

        @no_type_check
        def pandas_at_time(pdf) -> ps.DataFrame[int]:
            return pdf.at_time(time, asof)

        psdf = self.to_frame()[[]]
        id_column_name = verify_temp_column_name(psdf, "__id_column__")
        psdf = psdf.pandas_on_spark.attach_id_column("distributed-sequence", id_column_name)
        with ps.option_context("compute.default_index_type", "distributed"):
            # The attached index in the statement below will be dropped soon,
            # so we enforce “distributed” default index type
            psdf = psdf.pandas_on_spark.apply_batch(pandas_at_time)
        return ps.Index(first_series(psdf).rename(self.name))


def disallow_nanoseconds(freq: Union[str, DateOffset]) -> None:
    if freq in ["N", "ns"]:
        raise ValueError("nanoseconds is not supported")


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.indexes.datetimes

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.indexes.datetimes.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.indexes.datetimes tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.indexes.datetimes,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
