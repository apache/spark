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
from typing import cast, no_type_check, Any
from functools import partial

import pandas as pd
from pandas.api.types import is_hashable

from pyspark import pandas as ps
from pyspark._globals import _NoValue
from pyspark.pandas.indexes.base import Index
from pyspark.pandas.missing.indexes import MissingPandasLikeTimedeltaIndex
from pyspark.pandas.series import Series
from pyspark.pandas.spark import functions as SF
from pyspark.sql import functions as F


HOURS_PER_DAY = 24
MINUTES_PER_HOUR = 60
SECONDS_PER_MINUTE = 60
MILLIS_PER_SECOND = 1000
MICROS_PER_MILLIS = 1000

SECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE
SECONDS_PER_DAY = HOURS_PER_DAY * SECONDS_PER_HOUR
MICROS_PER_SECOND = MILLIS_PER_SECOND * MICROS_PER_MILLIS


class TimedeltaIndex(Index):
    """
    Immutable ndarray-like of timedelta64 data, represented internally as int64, and
    which can be boxed to timedelta objects.

    Parameters
    ----------
    data  : array-like (1-dimensional), optional
        Optional timedelta-like data to construct index with.
    unit : unit of the arg (D,h,m,s,ms,us,ns) denote the unit, optional
        Which is an integer/float number.
    freq : str or pandas offset object, optional
        One of pandas date offset strings or corresponding objects. The string
        'infer' can be passed in order to set the frequency of the index as the
        inferred frequency upon creation.
    copy  : bool
        Make a copy of input ndarray.
    name : object
        Name to be stored in the index.

    See Also
    --------
    Index : The base pandas Index type.

    Examples
    --------
    >>> from datetime import timedelta
    >>> ps.TimedeltaIndex([timedelta(1), timedelta(microseconds=2)])
    TimedeltaIndex(['1 days 00:00:00', '0 days 00:00:00.000002'], dtype='timedelta64[ns]', freq=None)

    From an Series:

    >>> s = ps.Series([timedelta(1), timedelta(microseconds=2)], index=[10, 20])
    >>> ps.TimedeltaIndex(s)
    TimedeltaIndex(['1 days 00:00:00', '0 days 00:00:00.000002'], dtype='timedelta64[ns]', freq=None)

    From an Index:

    >>> idx = ps.TimedeltaIndex([timedelta(1), timedelta(microseconds=2)])
    >>> ps.TimedeltaIndex(idx)
    TimedeltaIndex(['1 days 00:00:00', '0 days 00:00:00.000002'], dtype='timedelta64[ns]', freq=None)
    """

    @no_type_check
    def __new__(
        cls,
        data=None,
        unit=None,
        freq=_NoValue,
        closed=None,
        dtype=None,
        copy=False,
        name=None,
    ) -> "TimedeltaIndex":
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, (Series, Index)):
            if dtype is None:
                dtype = "timedelta64[ns]"
            return cast(TimedeltaIndex, Index(data, dtype=dtype, copy=copy, name=name))

        kwargs = dict(
            data=data,
            unit=unit,
            closed=closed,
            dtype=dtype,
            copy=copy,
            name=name,
        )
        if freq is not _NoValue:
            kwargs["freq"] = freq

        return cast(TimedeltaIndex, ps.from_pandas(pd.TimedeltaIndex(**kwargs)))

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeTimedeltaIndex, item):
            property_or_func = getattr(MissingPandasLikeTimedeltaIndex, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)

        raise AttributeError("'TimedeltaIndex' object has no attribute '{}'".format(item))

    @property
    def days(self) -> Index:
        """
        Number of days for each element.
        """

        @no_type_check
        def pandas_days(x) -> int:
            return x.days

        return Index(self.to_series().transform(pandas_days))

    @property
    def seconds(self) -> Index:
        """
        Number of seconds (>= 0 and less than 1 day) for each element.
        """

        @no_type_check
        def get_seconds(scol):
            hour_scol = SF.date_part("HOUR", scol)
            minute_scol = SF.date_part("MINUTE", scol)
            second_scol = SF.date_part("SECOND", scol)
            return (
                F.when(
                    hour_scol < 0,
                    SECONDS_PER_DAY + hour_scol * SECONDS_PER_HOUR,
                ).otherwise(hour_scol * SECONDS_PER_HOUR)
                + F.when(
                    minute_scol < 0,
                    SECONDS_PER_DAY + minute_scol * SECONDS_PER_MINUTE,
                ).otherwise(minute_scol * SECONDS_PER_MINUTE)
                + F.when(
                    second_scol < 0,
                    SECONDS_PER_DAY + second_scol,
                ).otherwise(second_scol)
            ).cast("int")

        return Index(self.to_series().spark.transform(get_seconds))

    @property
    def microseconds(self) -> Index:
        """
        Number of microseconds (>= 0 and less than 1 second) for each element.
        """

        @no_type_check
        def get_microseconds(scol):
            second_scol = SF.date_part("SECOND", scol)
            return (
                (
                    F.when(
                        (second_scol >= 0) & (second_scol < 1),
                        second_scol,
                    )
                    .when(second_scol < 0, 1 + second_scol)
                    .otherwise(0)
                )
                * MICROS_PER_SECOND
            ).cast("int")

        return Index(self.to_series().spark.transform(get_microseconds))
