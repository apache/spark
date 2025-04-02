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
from abc import ABCMeta, abstractmethod
from functools import partial
from typing import Any, Callable, Generic, List, Optional

import numpy as np

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.internal import InternalFunction as SF
from pyspark.pandas.missing.window import (
    MissingPandasLikeRolling,
    MissingPandasLikeRollingGroupby,
    MissingPandasLikeExpanding,
    MissingPandasLikeExpandingGroupby,
    MissingPandasLikeExponentialMoving,
    MissingPandasLikeExponentialMovingGroupby,
)
from pyspark import pandas as ps  # noqa: F401
from pyspark.pandas._typing import FrameLike
from pyspark.pandas.groupby import GroupBy, DataFrameGroupBy
from pyspark.pandas.internal import NATURAL_ORDER_COLUMN_NAME, SPARK_INDEX_NAME_FORMAT
from pyspark.pandas.utils import scol_for
from pyspark.sql.column import Column
from pyspark.sql.types import (
    DoubleType,
)
from pyspark.sql.window import WindowSpec


class RollingAndExpanding(Generic[FrameLike], metaclass=ABCMeta):
    def __init__(self, window: WindowSpec, min_periods: int):
        self._window = window
        # This unbounded Window is later used to handle 'min_periods' for now.
        self._unbounded_window = Window.orderBy(NATURAL_ORDER_COLUMN_NAME).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        self._min_periods = min_periods

    @abstractmethod
    def _apply_as_series_or_frame(self, func: Callable[[Column], Column]) -> FrameLike:
        """
        Wraps a function that handles Spark column in order
        to support it in both pandas-on-Spark Series and DataFrame.
        Note that the given `func` name should be same as the API's method name.
        """
        pass

    @abstractmethod
    def count(self) -> FrameLike:
        pass

    def sum(self) -> FrameLike:
        def sum(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                F.sum(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(sum)

    def min(self) -> FrameLike:
        def min(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                F.min(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(min)

    def max(self) -> FrameLike:
        def max(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                F.max(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(max)

    def mean(self) -> FrameLike:
        def mean(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                F.mean(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(mean)

    def quantile(self, q: float, accuracy: int = 10000) -> FrameLike:
        def quantile(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                F.percentile_approx(scol.cast(DoubleType()), q, accuracy).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(quantile)

    def std(self) -> FrameLike:
        def std(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                F.stddev(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(std)

    def var(self) -> FrameLike:
        def var(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                F.variance(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(var)

    def skew(self) -> FrameLike:
        def skew(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                SF.skew(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(skew)

    def kurt(self) -> FrameLike:
        def kurt(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                SF.kurt(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(kurt)


class RollingLike(RollingAndExpanding[FrameLike]):
    def __init__(
        self,
        window: int,
        min_periods: Optional[int] = None,
    ):
        if window < 0:
            raise ValueError("window must be >= 0")
        if (min_periods is not None) and (min_periods < 0):
            raise ValueError("min_periods must be >= 0")
        if min_periods is None:
            # TODO: 'min_periods' is not equivalent in pandas because it does not count NA as
            #  a value.
            min_periods = window

        window_spec = Window.orderBy(NATURAL_ORDER_COLUMN_NAME).rowsBetween(
            Window.currentRow - (window - 1), Window.currentRow
        )

        super().__init__(window_spec, min_periods)

    def count(self) -> FrameLike:
        def count(scol: Column) -> Column:
            return F.count(scol).over(self._window)

        return self._apply_as_series_or_frame(count).astype("float64")  # type: ignore[attr-defined]


class Rolling(RollingLike[FrameLike]):
    def __init__(
        self,
        psdf_or_psser: FrameLike,
        window: int,
        min_periods: Optional[int] = None,
    ):
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import Series

        super().__init__(window, min_periods)

        self._psdf_or_psser = psdf_or_psser

        if not isinstance(psdf_or_psser, (DataFrame, Series)):
            raise TypeError(
                "psdf_or_psser must be a series or dataframe; however, got: %s"
                % type(psdf_or_psser)
            )

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeRolling, item):
            property_or_func = getattr(MissingPandasLikeRolling, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        raise AttributeError(item)

    def _apply_as_series_or_frame(self, func: Callable[[Column], Column]) -> FrameLike:
        return self._psdf_or_psser._apply_series_op(
            lambda psser: psser._with_new_scol(func(psser.spark.column)),  # TODO: dtype?
            should_resolve=True,
        )

    def count(self) -> FrameLike:
        """
        The rolling count of any non-NaN observations inside the window.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Return type is the same as the original object with `np.float64` dtype.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.count : Count of the full Series.
        pyspark.pandas.DataFrame.count : Count of the full DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 3, float("nan"), 10])
        >>> s.rolling(1).count()
        0    1.0
        1    1.0
        2    0.0
        3    1.0
        dtype: float64

        >>> s.rolling(3).count()
        0    1.0
        1    2.0
        2    2.0
        3    2.0
        dtype: float64

        >>> s.to_frame().rolling(1).count()
             0
        0  1.0
        1  1.0
        2  0.0
        3  1.0

        >>> s.to_frame().rolling(3).count()
             0
        0  1.0
        1  2.0
        2  2.0
        3  2.0
        """
        return super().count()

    def sum(self) -> FrameLike:
        """
        Calculate rolling summation of given DataFrame or Series.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Same type as the input, with the same index, containing the
            rolling summation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.sum : Reducing sum for Series.
        pyspark.pandas.DataFrame.sum : Reducing sum for DataFrame.

        Examples
        --------
        >>> s = ps.Series([4, 3, 5, 2, 6])
        >>> s
        0    4
        1    3
        2    5
        3    2
        4    6
        dtype: int64

        >>> s.rolling(2).sum()
        0    NaN
        1    7.0
        2    8.0
        3    7.0
        4    8.0
        dtype: float64

        >>> s.rolling(3).sum()
        0     NaN
        1     NaN
        2    12.0
        3    10.0
        4    13.0
        dtype: float64

        For DataFrame, each rolling summation is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df
           A   B
        0  4  16
        1  3   9
        2  5  25
        3  2   4
        4  6  36

        >>> df.rolling(2).sum()
             A     B
        0  NaN   NaN
        1  7.0  25.0
        2  8.0  34.0
        3  7.0  29.0
        4  8.0  40.0

        >>> df.rolling(3).sum()
              A     B
        0   NaN   NaN
        1   NaN   NaN
        2  12.0  50.0
        3  10.0  38.0
        4  13.0  65.0
        """
        return super().sum()

    def min(self) -> FrameLike:
        """
        Calculate the rolling minimum.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the rolling
            calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with a Series.
        pyspark.pandas.DataFrame.rolling : Calling object with a DataFrame.
        pyspark.pandas.Series.min : Similar method for Series.
        pyspark.pandas.DataFrame.min : Similar method for DataFrame.

        Examples
        --------
        >>> s = ps.Series([4, 3, 5, 2, 6])
        >>> s
        0    4
        1    3
        2    5
        3    2
        4    6
        dtype: int64

        >>> s.rolling(2).min()
        0    NaN
        1    3.0
        2    3.0
        3    2.0
        4    2.0
        dtype: float64

        >>> s.rolling(3).min()
        0    NaN
        1    NaN
        2    3.0
        3    2.0
        4    2.0
        dtype: float64

        For DataFrame, each rolling minimum is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df
           A   B
        0  4  16
        1  3   9
        2  5  25
        3  2   4
        4  6  36

        >>> df.rolling(2).min()
             A    B
        0  NaN  NaN
        1  3.0  9.0
        2  3.0  9.0
        3  2.0  4.0
        4  2.0  4.0

        >>> df.rolling(3).min()
             A    B
        0  NaN  NaN
        1  NaN  NaN
        2  3.0  9.0
        3  2.0  4.0
        4  2.0  4.0
        """
        return super().min()

    def max(self) -> FrameLike:
        """
        Calculate the rolling maximum.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Return type is determined by the caller.

        See Also
        --------
        pyspark.pandas.Series.rolling : Series rolling.
        pyspark.pandas.DataFrame.rolling : DataFrame rolling.
        pyspark.pandas.Series.max : Similar method for Series.
        pyspark.pandas.DataFrame.max : Similar method for DataFrame.

        Examples
        --------
        >>> s = ps.Series([4, 3, 5, 2, 6])
        >>> s
        0    4
        1    3
        2    5
        3    2
        4    6
        dtype: int64

        >>> s.rolling(2).max()
        0    NaN
        1    4.0
        2    5.0
        3    5.0
        4    6.0
        dtype: float64

        >>> s.rolling(3).max()
        0    NaN
        1    NaN
        2    5.0
        3    5.0
        4    6.0
        dtype: float64

        For DataFrame, each rolling maximum is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df
           A   B
        0  4  16
        1  3   9
        2  5  25
        3  2   4
        4  6  36

        >>> df.rolling(2).max()
             A     B
        0  NaN   NaN
        1  4.0  16.0
        2  5.0  25.0
        3  5.0  25.0
        4  6.0  36.0

        >>> df.rolling(3).max()
             A     B
        0  NaN   NaN
        1  NaN   NaN
        2  5.0  25.0
        3  5.0  25.0
        4  6.0  36.0
        """
        return super().max()

    def mean(self) -> FrameLike:
        """
        Calculate the rolling mean of the values.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the rolling
            calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.mean : Equivalent method for Series.
        pyspark.pandas.DataFrame.mean : Equivalent method for DataFrame.

        Examples
        --------
        >>> s = ps.Series([4, 3, 5, 2, 6])
        >>> s
        0    4
        1    3
        2    5
        3    2
        4    6
        dtype: int64

        >>> s.rolling(2).mean()
        0    NaN
        1    3.5
        2    4.0
        3    3.5
        4    4.0
        dtype: float64

        >>> s.rolling(3).mean()
        0         NaN
        1         NaN
        2    4.000000
        3    3.333333
        4    4.333333
        dtype: float64

        For DataFrame, each rolling mean is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df
           A   B
        0  4  16
        1  3   9
        2  5  25
        3  2   4
        4  6  36

        >>> df.rolling(2).mean()
             A     B
        0  NaN   NaN
        1  3.5  12.5
        2  4.0  17.0
        3  3.5  14.5
        4  4.0  20.0

        >>> df.rolling(3).mean()
                  A          B
        0       NaN        NaN
        1       NaN        NaN
        2  4.000000  16.666667
        3  3.333333  12.666667
        4  4.333333  21.666667
        """
        return super().mean()

    def quantile(self, quantile: float, accuracy: int = 10000) -> FrameLike:
        """
        Calculate the rolling quantile of the values.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        quantile : float
            Value between 0 and 1 providing the quantile to compute.

            .. deprecated:: 4.0.0
                This will be renamed to ‘q’ in a future version.

        accuracy : int, optional
            Default accuracy of approximation. Larger value means better accuracy.
            The relative error can be deduced by 1.0 / accuracy.
            This is a panda-on-Spark specific parameter.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the rolling
            calculation.

        Notes
        -----
        `quantile` in pandas-on-Spark are using distributed percentile approximation
        algorithm unlike pandas, the result might be different with pandas, also `interpolation`
        parameter is not supported yet.

        the current implementation of this API uses Spark's Window without
        specifying partition specification. This leads to move all data into
        single partition in single machine and could cause serious
        performance degradation. Avoid this method against very large dataset.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling rolling with Series data.
        pyspark.pandas.DataFrame.rolling : Calling rolling with DataFrames.
        pyspark.pandas.Series.quantile : Aggregating quantile for Series.
        pyspark.pandas.DataFrame.quantile : Aggregating quantile for DataFrame.

        Examples
        --------
        >>> s = ps.Series([4, 3, 5, 2, 6])
        >>> s
        0    4
        1    3
        2    5
        3    2
        4    6
        dtype: int64

        >>> s.rolling(2).quantile(0.5)
        0    NaN
        1    3.0
        2    3.0
        3    2.0
        4    2.0
        dtype: float64

        >>> s.rolling(3).quantile(0.5)
        0    NaN
        1    NaN
        2    4.0
        3    3.0
        4    5.0
        dtype: float64

        For DataFrame, each rolling quantile is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df
           A   B
        0  4  16
        1  3   9
        2  5  25
        3  2   4
        4  6  36

        >>> df.rolling(2).quantile(0.5)
             A    B
        0  NaN  NaN
        1  3.0  9.0
        2  3.0  9.0
        3  2.0  4.0
        4  2.0  4.0

        >>> df.rolling(3).quantile(0.5)
             A     B
        0  NaN   NaN
        1  NaN   NaN
        2  4.0  16.0
        3  3.0   9.0
        4  5.0  25.0
        """
        return super().quantile(quantile, accuracy)

    def std(self) -> FrameLike:
        """
        Calculate rolling standard deviation.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the rolling calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.std : Equivalent method for Series.
        pyspark.pandas.DataFrame.std : Equivalent method for DataFrame.
        numpy.std : Equivalent method for Numpy array.

        Examples
        --------
        >>> s = ps.Series([5, 5, 6, 7, 5, 5, 5])
        >>> s.rolling(3).std()
        0         NaN
        1         NaN
        2    0.577350
        3    1.000000
        4    1.000000
        5    1.154701
        6    0.000000
        dtype: float64

        For DataFrame, each rolling standard deviation is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.rolling(2).std()
                  A          B
        0       NaN        NaN
        1  0.000000   0.000000
        2  0.707107   7.778175
        3  0.707107   9.192388
        4  1.414214  16.970563
        5  0.000000   0.000000
        6  0.000000   0.000000
        """
        return super().std()

    def var(self) -> FrameLike:
        """
        Calculate unbiased rolling variance.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the rolling calculation.

        See Also
        --------
        Series.rolling : Calling object with Series data.
        DataFrame.rolling : Calling object with DataFrames.
        Series.var : Equivalent method for Series.
        DataFrame.var : Equivalent method for DataFrame.
        numpy.var : Equivalent method for Numpy array.

        Examples
        --------
        >>> s = ps.Series([5, 5, 6, 7, 5, 5, 5])
        >>> s.rolling(3).var()
        0         NaN
        1         NaN
        2    0.333333
        3    1.000000
        4    1.000000
        5    1.333333
        6    0.000000
        dtype: float64

        For DataFrame, each unbiased rolling variance is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.rolling(2).var()
             A      B
        0  NaN    NaN
        1  0.0    0.0
        2  0.5   60.5
        3  0.5   84.5
        4  2.0  288.0
        5  0.0    0.0
        6  0.0    0.0
        """
        return super().var()

    def skew(self) -> FrameLike:
        """
        Calculate unbiased rolling skew.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the rolling calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.std : Equivalent method for Series.
        pyspark.pandas.DataFrame.std : Equivalent method for DataFrame.
        numpy.std : Equivalent method for Numpy array.

        Examples
        --------
        >>> s = ps.Series([5, 5, 6, 7, 5, 1, 5, 9])
        >>> s.rolling(3).skew()
        0         NaN
        1         NaN
        2    1.732051
        3    0.000000
        4    0.000000
        5   -0.935220
        6   -1.732051
        7    0.000000
        dtype: float64

        For DataFrame, each rolling standard deviation is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.rolling(5).skew()
                  A         B
        0       NaN       NaN
        1       NaN       NaN
        2       NaN       NaN
        3       NaN       NaN
        4  1.257788  1.369456
        5 -1.492685 -0.526039
        6 -1.492685 -0.526039
        7 -0.551618  0.686072
        """
        return super().skew()

    def kurt(self) -> FrameLike:
        """
        Calculate unbiased rolling kurtosis.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the rolling calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.var : Equivalent method for Series.
        pyspark.pandas.DataFrame.var : Equivalent method for DataFrame.
        numpy.var : Equivalent method for Numpy array.

        Examples
        --------
        >>> s = ps.Series([5, 5, 6, 7, 5, 1, 5, 9])
        >>> s.rolling(4).kurt()
        0         NaN
        1         NaN
        2         NaN
        3   -1.289256
        4   -1.289256
        5    2.234867
        6    2.227147
        7    1.500000
        dtype: float64

        For DataFrame, each unbiased rolling variance is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.rolling(5).kurt()
                  A         B
        0       NaN       NaN
        1       NaN       NaN
        2       NaN       NaN
        3       NaN       NaN
        4  0.312500  0.906336
        5  2.818047  1.016942
        6  2.818047  1.016942
        7  0.867769  0.389750
        """
        return super().kurt()


class RollingGroupby(RollingLike[FrameLike]):
    def __init__(
        self,
        groupby: GroupBy[FrameLike],
        window: int,
        min_periods: Optional[int] = None,
    ):
        super().__init__(window, min_periods)

        self._groupby = groupby
        self._window = self._window.partitionBy(*[ser.spark.column for ser in groupby._groupkeys])
        self._unbounded_window = self._unbounded_window.partitionBy(
            *[ser.spark.column for ser in groupby._groupkeys]
        )

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeRollingGroupby, item):
            property_or_func = getattr(MissingPandasLikeRollingGroupby, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        raise AttributeError(item)

    def _apply_as_series_or_frame(self, func: Callable[[Column], Column]) -> FrameLike:
        """
        Wraps a function that handles Spark column in order
        to support it in both pandas-on-Spark Series and DataFrame.
        Note that the given `func` name should be same as the API's method name.
        """
        from pyspark.pandas import DataFrame

        groupby = self._groupby
        psdf = groupby._psdf

        # Here we need to include grouped key as an index, and shift previous index.
        #   [index_column0, index_column1] -> [grouped key, index_column0, index_column1]
        new_index_scols: List[Column] = []
        new_index_spark_column_names = []
        new_index_names = []
        new_index_fields = []
        for groupkey in groupby._groupkeys:
            index_column_name = SPARK_INDEX_NAME_FORMAT(len(new_index_scols))
            new_index_scols.append(groupkey.spark.column.alias(index_column_name))
            new_index_spark_column_names.append(index_column_name)
            new_index_names.append(groupkey._column_label)
            new_index_fields.append(groupkey._internal.data_fields[0].copy(name=index_column_name))

        for new_index_scol, index_name, index_field in zip(
            psdf._internal.index_spark_columns,
            psdf._internal.index_names,
            psdf._internal.index_fields,
        ):
            index_column_name = SPARK_INDEX_NAME_FORMAT(len(new_index_scols))
            new_index_scols.append(new_index_scol.alias(index_column_name))
            new_index_spark_column_names.append(index_column_name)
            new_index_names.append(index_name)
            new_index_fields.append(index_field.copy(name=index_column_name))

        if groupby._agg_columns_selected:
            agg_columns = groupby._agg_columns
        else:
            # pandas doesn't keep the groupkey as a column from 1.3 for DataFrameGroupBy
            column_labels_to_exclude = groupby._column_labels_to_exclude.copy()
            if isinstance(groupby, DataFrameGroupBy):
                for groupkey in groupby._groupkeys:  # type: ignore[attr-defined]
                    column_labels_to_exclude.add(groupkey._internal.column_labels[0])
            agg_columns = [
                psdf._psser_for(label)
                for label in psdf._internal.column_labels
                if label not in column_labels_to_exclude
            ]

        applied = []
        for agg_column in agg_columns:
            applied.append(agg_column._with_new_scol(func(agg_column.spark.column)))  # TODO: dtype?

        # Seems like pandas filters out when grouped key is NA.
        cond = groupby._groupkeys[0].spark.column.isNotNull()
        for c in groupby._groupkeys[1:]:
            cond = cond | c.spark.column.isNotNull()

        sdf = psdf._internal.spark_frame.filter(cond).select(
            new_index_scols + [c.spark.column for c in applied]
        )

        internal = psdf._internal.copy(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in new_index_spark_column_names],
            index_names=new_index_names,
            index_fields=new_index_fields,
            column_labels=[c._column_label for c in applied],
            data_spark_columns=[
                scol_for(sdf, c._internal.data_spark_column_names[0]) for c in applied
            ],
            data_fields=[c._internal.data_fields[0] for c in applied],
        )

        return groupby._handle_output(DataFrame(internal))

    def count(self) -> FrameLike:
        """
        The rolling count of any non-NaN observations inside the window.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.count : Count of the full Series.
        pyspark.pandas.DataFrame.count : Count of the full DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).rolling(3).count().sort_index()
        2  0     1.0
           1     2.0
        3  2     1.0
           3     2.0
           4     3.0
        4  5     1.0
           6     2.0
           7     3.0
           8     3.0
        5  9     1.0
           10    2.0
        dtype: float64

        For DataFrame, each rolling count is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).rolling(2).count().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                B
        A
        2 0   1.0
          1   2.0
        3 2   1.0
          3   2.0
          4   2.0
        4 5   1.0
          6   2.0
          7   2.0
          8   2.0
        5 9   1.0
          10  2.0
        """
        return super().count()

    def sum(self) -> FrameLike:
        """
        The rolling summation of any non-NaN observations inside the window.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the rolling
            calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.sum : Sum of the full Series.
        pyspark.pandas.DataFrame.sum : Sum of the full DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).rolling(3).sum().sort_index()
        2  0      NaN
           1      NaN
        3  2      NaN
           3      NaN
           4      9.0
        4  5      NaN
           6      NaN
           7     12.0
           8     12.0
        5  9      NaN
           10     NaN
        dtype: float64

        For DataFrame, each rolling summation is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).rolling(2).sum().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    NaN
          1    8.0
        3 2    NaN
          3   18.0
          4   18.0
        4 5    NaN
          6   32.0
          7   32.0
          8   32.0
        5 9    NaN
          10  50.0
        """
        return super().sum()

    def min(self) -> FrameLike:
        """
        The rolling minimum of any non-NaN observations inside the window.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the rolling
            calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.min : Min of the full Series.
        pyspark.pandas.DataFrame.min : Min of the full DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).rolling(3).min().sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     4.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each rolling minimum is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).rolling(2).min().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    NaN
          1    4.0
        3 2    NaN
          3    9.0
          4    9.0
        4 5    NaN
          6   16.0
          7   16.0
          8   16.0
        5 9    NaN
          10  25.0
        """
        return super().min()

    def max(self) -> FrameLike:
        """
        The rolling maximum of any non-NaN observations inside the window.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the rolling
            calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.max : Max of the full Series.
        pyspark.pandas.DataFrame.max : Max of the full DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).rolling(3).max().sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     4.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each rolling maximum is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).rolling(2).max().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    NaN
          1    4.0
        3 2    NaN
          3    9.0
          4    9.0
        4 5    NaN
          6   16.0
          7   16.0
          8   16.0
        5 9    NaN
          10  25.0
        """
        return super().max()

    def mean(self) -> FrameLike:
        """
        The rolling mean of any non-NaN observations inside the window.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the rolling
            calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.mean : Mean of the full Series.
        pyspark.pandas.DataFrame.mean : Mean of the full DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).rolling(3).mean().sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     4.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each rolling mean is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).rolling(2).mean().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    NaN
          1    4.0
        3 2    NaN
          3    9.0
          4    9.0
        4 5    NaN
          6   16.0
          7   16.0
          8   16.0
        5 9    NaN
          10  25.0
        """
        return super().mean()

    def quantile(self, quantile: float, accuracy: int = 10000) -> FrameLike:
        """
        Calculate rolling quantile.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        quantile : float
            Value between 0 and 1 providing the quantile to compute.
        accuracy : int, optional
            Default accuracy of approximation. Larger value means better accuracy.
            The relative error can be deduced by 1.0 / accuracy.
            This is a panda-on-Spark specific parameter.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the rolling
            calculation.

        Notes
        -----
        `quantile` in pandas-on-Spark are using distributed percentile approximation
        algorithm unlike pandas, the result might be different with pandas, also `interpolation`
        parameter is not supported yet.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling rolling with Series data.
        pyspark.pandas.DataFrame.rolling : Calling rolling with DataFrames.
        pyspark.pandas.Series.quantile : Aggregating quantile for Series.
        pyspark.pandas.DataFrame.quantile : Aggregating quantile for DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).rolling(3).quantile(0.5).sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     4.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each rolling quantile is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).rolling(2).quantile(0.5).sort_index()
                 B
        A
        2 0    NaN
          1    4.0
        3 2    NaN
          3    9.0
          4    9.0
        4 5    NaN
          6   16.0
          7   16.0
          8   16.0
        5 9    NaN
          10  25.0
        """
        return super().quantile(quantile, accuracy)

    def std(self) -> FrameLike:
        """
        Calculate rolling standard deviation.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the rolling calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.std : Equivalent method for Series.
        pyspark.pandas.DataFrame.std : Equivalent method for DataFrame.
        numpy.std : Equivalent method for Numpy array.
        """
        return super().std()

    def var(self) -> FrameLike:
        """
        Calculate unbiased rolling variance.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the rolling calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.var : Equivalent method for Series.
        pyspark.pandas.DataFrame.var : Equivalent method for DataFrame.
        numpy.var : Equivalent method for Numpy array.
        """
        return super().var()

    def skew(self) -> FrameLike:
        """
        Calculate unbiased rolling skew.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the rolling calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.std : Equivalent method for Series.
        pyspark.pandas.DataFrame.std : Equivalent method for DataFrame.
        numpy.std : Equivalent method for Numpy array.
        """
        return super().skew()

    def kurt(self) -> FrameLike:
        """
        Calculate unbiased rolling kurtosis.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the rolling calculation.

        See Also
        --------
        pyspark.pandas.Series.rolling : Calling object with Series data.
        pyspark.pandas.DataFrame.rolling : Calling object with DataFrames.
        pyspark.pandas.Series.var : Equivalent method for Series.
        pyspark.pandas.DataFrame.var : Equivalent method for DataFrame.
        numpy.var : Equivalent method for Numpy array.
        """
        return super().kurt()


class ExpandingLike(RollingAndExpanding[FrameLike]):
    def __init__(self, min_periods: int = 1):
        if min_periods < 0:
            raise ValueError("min_periods must be >= 0")

        window = Window.orderBy(NATURAL_ORDER_COLUMN_NAME).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )

        super().__init__(window, min_periods)

    def count(self) -> FrameLike:
        def count(scol: Column) -> Column:
            return F.when(
                F.row_number().over(self._unbounded_window) >= self._min_periods,
                F.count(scol).over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(count).astype("float64")  # type: ignore[attr-defined]


class Expanding(ExpandingLike[FrameLike]):
    def __init__(self, psdf_or_psser: FrameLike, min_periods: int = 1):
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import Series

        super().__init__(min_periods)

        if not isinstance(psdf_or_psser, (DataFrame, Series)):
            raise TypeError(
                "psdf_or_psser must be a series or dataframe; however, got: %s"
                % type(psdf_or_psser)
            )
        self._psdf_or_psser = psdf_or_psser

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeExpanding, item):
            property_or_func = getattr(MissingPandasLikeExpanding, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        raise AttributeError(item)

    # TODO: when add 'axis' parameter, should add to here too.
    def __repr__(self) -> str:
        return "Expanding [min_periods={}]".format(self._min_periods)

    _apply_as_series_or_frame = Rolling._apply_as_series_or_frame

    def count(self) -> FrameLike:
        """
        The expanding count of any non-NaN observations inside the window.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.count : Count of the full Series.
        pyspark.pandas.DataFrame.count : Count of the full DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 3, float("nan"), 10])
        >>> s.expanding().count()
        0    1.0
        1    2.0
        2    2.0
        3    3.0
        dtype: float64

        >>> s.to_frame().expanding().count()
             0
        0  1.0
        1  2.0
        2  2.0
        3  3.0
        """
        return super().count()

    def sum(self) -> FrameLike:
        """
        Calculate expanding summation of given DataFrame or Series.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Same type as the input, with the same index, containing the
            expanding summation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.sum : Reducing sum for Series.
        pyspark.pandas.DataFrame.sum : Reducing sum for DataFrame.

        Examples
        --------
        >>> s = ps.Series([1, 2, 3, 4, 5])
        >>> s
        0    1
        1    2
        2    3
        3    4
        4    5
        dtype: int64

        >>> s.expanding(3).sum()
        0     NaN
        1     NaN
        2     6.0
        3    10.0
        4    15.0
        dtype: float64

        For DataFrame, each expanding summation is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df
           A   B
        0  1   1
        1  2   4
        2  3   9
        3  4  16
        4  5  25

        >>> df.expanding(3).sum()
              A     B
        0   NaN   NaN
        1   NaN   NaN
        2   6.0  14.0
        3  10.0  30.0
        4  15.0  55.0
        """
        return super().sum()

    def min(self) -> FrameLike:
        """
        Calculate the expanding minimum.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with a Series.
        pyspark.pandas.DataFrame.expanding : Calling object with a DataFrame.
        pyspark.pandas.Series.min : Similar method for Series.
        pyspark.pandas.DataFrame.min : Similar method for DataFrame.

        Examples
        --------
        Performing a expanding minimum with a window size of 3.

        >>> s = ps.Series([4, 3, 5, 2, 6])
        >>> s.expanding(3).min()
        0    NaN
        1    NaN
        2    3.0
        3    2.0
        4    2.0
        dtype: float64
        """
        return super().min()

    def max(self) -> FrameLike:
        """
        Calculate the expanding maximum.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Return type is determined by the caller.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.max : Similar method for Series.
        pyspark.pandas.DataFrame.max : Similar method for DataFrame.

        Examples
        --------
        Performing a expanding minimum with a window size of 3.

        >>> s = ps.Series([4, 3, 5, 2, 6])
        >>> s.expanding(3).max()
        0    NaN
        1    NaN
        2    5.0
        3    5.0
        4    6.0
        dtype: float64
        """
        return super().max()

    def mean(self) -> FrameLike:
        """
        Calculate the expanding mean of the values.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.mean : Equivalent method for Series.
        pyspark.pandas.DataFrame.mean : Equivalent method for DataFrame.

        Examples
        --------
        The below examples will show expanding mean calculations with window sizes of
        two and three, respectively.

        >>> s = ps.Series([1, 2, 3, 4])
        >>> s.expanding(2).mean()
        0    NaN
        1    1.5
        2    2.0
        3    2.5
        dtype: float64

        >>> s.expanding(3).mean()
        0    NaN
        1    NaN
        2    2.0
        3    2.5
        dtype: float64
        """
        return super().mean()

    def quantile(self, quantile: float, accuracy: int = 10000) -> FrameLike:
        """
        Calculate the expanding quantile of the values.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        Parameters
        ----------
        quantile : float
            Value between 0 and 1 providing the quantile to compute.
        accuracy : int, optional
            Default accuracy of approximation. Larger value means better accuracy.
            The relative error can be deduced by 1.0 / accuracy.
            This is a panda-on-Spark specific parameter.

        Notes
        -----
        `quantile` in pandas-on-Spark are using distributed percentile approximation
        algorithm unlike pandas, the result might be different with pandas (the result is
        similar to the interpolation set to `lower`), also `interpolation` parameter is
        not supported yet.

        the current implementation of this API uses Spark's Window without
        specifying partition specification. This leads to move all data into
        single partition in single machine and could cause serious
        performance degradation. Avoid this method against very large dataset.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling expanding with Series data.
        pyspark.pandas.DataFrame.expanding : Calling expanding with DataFrames.
        pyspark.pandas.Series.quantile : Aggregating quantile for Series.
        pyspark.pandas.DataFrame.quantile : Aggregating quantile for DataFrame.

        Examples
        --------
        The below examples will show expanding quantile calculations with window sizes of
        two and three, respectively.

        >>> s = ps.Series([1, 2, 3, 4])
        >>> s.expanding(2).quantile(0.5)
        0    NaN
        1    1.0
        2    2.0
        3    2.0
        dtype: float64

        >>> s.expanding(3).quantile(0.5)
        0    NaN
        1    NaN
        2    2.0
        3    2.0
        dtype: float64
        """
        return super().quantile(quantile, accuracy)

    def std(self) -> FrameLike:
        """
        Calculate expanding standard deviation.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the expanding calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.std : Equivalent method for Series.
        pyspark.pandas.DataFrame.std : Equivalent method for DataFrame.
        numpy.std : Equivalent method for Numpy array.

        Examples
        --------
        >>> s = ps.Series([5, 5, 6, 7, 5, 5, 5])
        >>> s.expanding(3).std()
        0         NaN
        1         NaN
        2    0.577350
        3    0.957427
        4    0.894427
        5    0.836660
        6    0.786796
        dtype: float64

        For DataFrame, each expanding standard deviation variance is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.expanding(2).std()
                  A          B
        0       NaN        NaN
        1  0.000000   0.000000
        2  0.577350   6.350853
        3  0.957427  11.412712
        4  0.894427  10.630146
        5  0.836660   9.928075
        6  0.786796   9.327379
        """
        return super().std()

    def var(self) -> FrameLike:
        """
        Calculate unbiased expanding variance.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the expanding calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.var : Equivalent method for Series.
        pyspark.pandas.DataFrame.var : Equivalent method for DataFrame.
        numpy.var : Equivalent method for Numpy array.

        Examples
        --------
        >>> s = ps.Series([5, 5, 6, 7, 5, 5, 5])
        >>> s.expanding(3).var()
        0         NaN
        1         NaN
        2    0.333333
        3    0.916667
        4    0.800000
        5    0.700000
        6    0.619048
        dtype: float64

        For DataFrame, each unbiased expanding variance is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.expanding(2).var()
                  A           B
        0       NaN         NaN
        1  0.000000    0.000000
        2  0.333333   40.333333
        3  0.916667  130.250000
        4  0.800000  113.000000
        5  0.700000   98.566667
        6  0.619048   87.000000
        """
        return super().var()

    def skew(self) -> FrameLike:
        """
        Calculate unbiased expanding skew.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the expanding calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.std : Equivalent method for Series.
        pyspark.pandas.DataFrame.std : Equivalent method for DataFrame.
        numpy.std : Equivalent method for Numpy array.

        Examples
        --------
        >>> s = ps.Series([5, 5, 6, 7, 5, 1, 5, 9])
        >>> s.expanding(3).skew()
        0         NaN
        1         NaN
        2    1.732051
        3    0.854563
        4    1.257788
        5   -1.571593
        6   -1.657542
        7   -0.521760
        dtype: float64

        For DataFrame, each expanding standard deviation variance is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.expanding(5).skew()
                  A         B
        0       NaN       NaN
        1       NaN       NaN
        2       NaN       NaN
        3       NaN       NaN
        4  1.257788  1.369456
        5 -1.571593 -0.423309
        6 -1.657542 -0.355737
        7 -0.521760  1.116874
        """
        return super().skew()

    def kurt(self) -> FrameLike:
        """
        Calculate unbiased expanding kurtosis.

        .. note:: the current implementation of this API uses Spark's Window without
            specifying partition specification. This leads to move all data into
            single partition in single machine and could cause serious
            performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the expanding calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.var : Equivalent method for Series.
        pyspark.pandas.DataFrame.var : Equivalent method for DataFrame.
        numpy.var : Equivalent method for Numpy array.

        Examples
        --------
        >>> s = ps.Series([5, 5, 6, 7, 5, 1, 5, 9])
        >>> s.expanding(4).kurt()
        0         NaN
        1         NaN
        2         NaN
        3   -1.289256
        4    0.312500
        5    3.419520
        6    4.028185
        7    2.230373
        dtype: float64

        For DataFrame, each unbiased expanding variance is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.expanding(5).kurt()
                  A         B
        0       NaN       NaN
        1       NaN       NaN
        2       NaN       NaN
        3       NaN       NaN
        4  0.312500  0.906336
        5  3.419520  1.486581
        6  4.028185  1.936169
        7  2.230373  2.273792
        """
        return super().kurt()


class ExpandingGroupby(ExpandingLike[FrameLike]):
    def __init__(self, groupby: GroupBy[FrameLike], min_periods: int = 1):
        super().__init__(min_periods)

        self._groupby = groupby
        self._window = self._window.partitionBy(*[ser.spark.column for ser in groupby._groupkeys])
        self._unbounded_window = self._window.partitionBy(
            *[ser.spark.column for ser in groupby._groupkeys]
        )

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeExpandingGroupby, item):
            property_or_func = getattr(MissingPandasLikeExpandingGroupby, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        raise AttributeError(item)

    _apply_as_series_or_frame = RollingGroupby._apply_as_series_or_frame

    def count(self) -> FrameLike:
        """
        The expanding count of any non-NaN observations inside the window.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.count : Count of the full Series.
        pyspark.pandas.DataFrame.count : Count of the full DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).expanding(3).count().sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     3.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each expanding count is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).expanding(2).count().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                B
        A
        2 0   NaN
          1   2.0
        3 2   NaN
          3   2.0
          4   3.0
        4 5   NaN
          6   2.0
          7   3.0
          8   4.0
        5 9   NaN
          10  2.0
        """
        return super().count()

    def sum(self) -> FrameLike:
        """
        Calculate expanding summation of given DataFrame or Series.

        Returns
        -------
        Series or DataFrame
            Same type as the input, with the same index, containing the
            expanding summation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.sum : Reducing sum for Series.
        pyspark.pandas.DataFrame.sum : Reducing sum for DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).expanding(3).sum().sort_index()
        2  0      NaN
           1      NaN
        3  2      NaN
           3      NaN
           4      9.0
        4  5      NaN
           6      NaN
           7     12.0
           8     16.0
        5  9      NaN
           10     NaN
        dtype: float64

        For DataFrame, each expanding summation is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).expanding(2).sum().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    NaN
          1    8.0
        3 2    NaN
          3   18.0
          4   27.0
        4 5    NaN
          6   32.0
          7   48.0
          8   64.0
        5 9    NaN
          10  50.0
        """
        return super().sum()

    def min(self) -> FrameLike:
        """
        Calculate the expanding minimum.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with a Series.
        pyspark.pandas.DataFrame.expanding : Calling object with a DataFrame.
        pyspark.pandas.Series.min : Similar method for Series.
        pyspark.pandas.DataFrame.min : Similar method for DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).expanding(3).min().sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     4.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each expanding minimum is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).expanding(2).min().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    NaN
          1    4.0
        3 2    NaN
          3    9.0
          4    9.0
        4 5    NaN
          6   16.0
          7   16.0
          8   16.0
        5 9    NaN
          10  25.0
        """
        return super().min()

    def max(self) -> FrameLike:
        """
        Calculate the expanding maximum.

        Returns
        -------
        Series or DataFrame
            Return type is determined by the caller.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.max : Similar method for Series.
        pyspark.pandas.DataFrame.max : Similar method for DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).expanding(3).max().sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     4.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each expanding maximum is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).expanding(2).max().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    NaN
          1    4.0
        3 2    NaN
          3    9.0
          4    9.0
        4 5    NaN
          6   16.0
          7   16.0
          8   16.0
        5 9    NaN
          10  25.0
        """
        return super().max()

    def mean(self) -> FrameLike:
        """
        Calculate the expanding mean of the values.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.mean : Equivalent method for Series.
        pyspark.pandas.DataFrame.mean : Equivalent method for DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).expanding(3).mean().sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     4.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each expanding mean is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).expanding(2).mean().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    NaN
          1    4.0
        3 2    NaN
          3    9.0
          4    9.0
        4 5    NaN
          6   16.0
          7   16.0
          8   16.0
        5 9    NaN
          10  25.0
        """
        return super().mean()

    def quantile(self, quantile: float, accuracy: int = 10000) -> FrameLike:
        """
         Calculate the expanding quantile of the values.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        quantile : float
            Value between 0 and 1 providing the quantile to compute.
        accuracy : int, optional
            Default accuracy of approximation. Larger value means better accuracy.
            The relative error can be deduced by 1.0 / accuracy.
            This is a panda-on-Spark specific parameter.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the expanding
            calculation.

        Notes
        -----
        `quantile` in pandas-on-Spark are using distributed percentile approximation
        algorithm unlike pandas, the result might be different with pandas, also `interpolation`
        parameter is not supported yet.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling expanding with Series data.
        pyspark.pandas.DataFrame.expanding : Calling expanding with DataFrames.
        pyspark.pandas.Series.quantile : Aggregating quantile for Series.
        pyspark.pandas.DataFrame.quantile : Aggregating quantile for DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).expanding(3).quantile(0.5).sort_index()
        2  0     NaN
           1     NaN
        3  2     NaN
           3     NaN
           4     3.0
        4  5     NaN
           6     NaN
           7     4.0
           8     4.0
        5  9     NaN
           10    NaN
        dtype: float64

        For DataFrame, each expanding quantile is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).expanding(2).quantile(0.5).sort_index()
                 B
        A
        2 0    NaN
          1    4.0
        3 2    NaN
          3    9.0
          4    9.0
        4 5    NaN
          6   16.0
          7   16.0
          8   16.0
        5 9    NaN
          10  25.0
        """
        return super().quantile(quantile, accuracy)

    def std(self) -> FrameLike:
        """
        Calculate expanding standard deviation.


        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the expanding calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding: Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.std : Equivalent method for Series.
        pyspark.pandas.DataFrame.std : Equivalent method for DataFrame.
        numpy.std : Equivalent method for Numpy array.
        """
        return super().std()

    def var(self) -> FrameLike:
        """
        Calculate unbiased expanding variance.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the expanding calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.var : Equivalent method for Series.
        pyspark.pandas.DataFrame.var : Equivalent method for DataFrame.
        numpy.var : Equivalent method for Numpy array.
        """
        return super().var()

    def skew(self) -> FrameLike:
        """
        Calculate expanding standard skew.


        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the expanding calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding: Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.std : Equivalent method for Series.
        pyspark.pandas.DataFrame.std : Equivalent method for DataFrame.
        numpy.std : Equivalent method for Numpy array.
        """
        return super().skew()

    def kurt(self) -> FrameLike:
        """
        Calculate unbiased expanding kurtosis.

        Returns
        -------
        Series or DataFrame
            Returns the same object type as the caller of the expanding calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.var : Equivalent method for Series.
        pyspark.pandas.DataFrame.var : Equivalent method for DataFrame.
        numpy.var : Equivalent method for Numpy array.
        """
        return super().kurt()


class ExponentialMovingLike(Generic[FrameLike], metaclass=ABCMeta):
    def __init__(
        self,
        window: WindowSpec,
        com: Optional[float] = None,
        span: Optional[float] = None,
        halflife: Optional[float] = None,
        alpha: Optional[float] = None,
        min_periods: Optional[int] = None,
        ignore_na: bool = False,
    ):
        if (min_periods is not None) and (min_periods < 0):
            raise ValueError("min_periods must be >= 0")
        if min_periods is None:
            min_periods = 0
        self._min_periods = min_periods
        self._ignore_na = ignore_na

        self._window = window
        # This unbounded Window is later used to handle 'min_periods' for now.
        self._unbounded_window = Window.orderBy(NATURAL_ORDER_COLUMN_NAME).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )

        if (com is not None) and (not com >= 0):
            raise ValueError("com must be >= 0")
        self._com = com

        if (span is not None) and (not span >= 1):
            raise ValueError("span must be >= 1")
        self._span = span

        if (halflife is not None) and (not halflife > 0):
            raise ValueError("halflife must be > 0")
        self._halflife = halflife

        if (alpha is not None) and (not 0 < alpha <= 1):
            raise ValueError("alpha must be in (0, 1]")
        self._alpha = alpha

    def _compute_unified_alpha(self) -> float:
        unified_alpha = np.nan
        opt_count = 0

        if self._com is not None:
            unified_alpha = 1.0 / (1 + self._com)
            opt_count += 1
        if self._span is not None:
            unified_alpha = 2.0 / (1 + self._span)
            opt_count += 1
        if self._halflife is not None:
            unified_alpha = 1.0 - np.exp(-np.log(2) / self._halflife)
            opt_count += 1
        if self._alpha is not None:
            unified_alpha = self._alpha
            opt_count += 1

        if opt_count == 0:
            raise ValueError("Must pass one of com, span, halflife, or alpha")
        if opt_count != 1:
            raise ValueError("com, span, halflife, and alpha are mutually exclusive")

        # convert possible numpy.float64 to float for lit function
        return float(unified_alpha)

    @abstractmethod
    def _apply_as_series_or_frame(self, func: Callable[[Column], Column]) -> FrameLike:
        """
        Wraps a function that handles Spark column in order
        to support it in both pandas-on-Spark Series and DataFrame.
        Note that the given `func` name should be same as the API's method name.
        """
        pass

    def mean(self) -> FrameLike:
        unified_alpha = self._compute_unified_alpha()

        def mean(scol: Column) -> Column:
            col_ewm = SF.ewm(scol, unified_alpha, self._ignore_na)
            return F.when(
                F.count(F.when(~scol.isNull(), 1).otherwise(None)).over(self._unbounded_window)
                >= self._min_periods,
                col_ewm.over(self._window),
            ).otherwise(F.lit(None))

        return self._apply_as_series_or_frame(mean)


class ExponentialMoving(ExponentialMovingLike[FrameLike]):
    def __init__(
        self,
        psdf_or_psser: FrameLike,
        com: Optional[float] = None,
        span: Optional[float] = None,
        halflife: Optional[float] = None,
        alpha: Optional[float] = None,
        min_periods: Optional[int] = None,
        ignore_na: bool = False,
    ):
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import Series

        if not isinstance(psdf_or_psser, (DataFrame, Series)):
            raise TypeError(
                "psdf_or_psser must be a series or dataframe; however, got: %s"
                % type(psdf_or_psser)
            )
        self._psdf_or_psser = psdf_or_psser

        window_spec = Window.orderBy(NATURAL_ORDER_COLUMN_NAME).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )

        super().__init__(window_spec, com, span, halflife, alpha, min_periods, ignore_na)

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeExponentialMoving, item):
            property_or_func = getattr(MissingPandasLikeExponentialMoving, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        raise AttributeError(item)

    _apply_as_series_or_frame = Rolling._apply_as_series_or_frame

    def mean(self) -> FrameLike:
        """
        Calculate an online exponentially weighted mean.

        Notes
        -----
        There are behavior differences between pandas-on-Spark and pandas.

        * the current implementation of this API uses Spark's Window without
          specifying partition specification. This leads to move all data into
          single partition in single machine and could cause serious
          performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the exponentially
            calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.mean : Equivalent method for Series.
        pyspark.pandas.DataFrame.mean : Equivalent method for DataFrame.

        Examples
        --------
        The below examples will show computing exponentially weighted moving average.

        >>> df = ps.DataFrame({'s1': [.2, .0, .6, .2, .4, .5, .6], 's2': [2, 1, 3, 1, 0, 0, 0]})
        >>> df.ewm(com=0.1).mean()
                 s1        s2
        0  0.200000  2.000000
        1  0.016667  1.083333
        2  0.547368  2.827068
        3  0.231557  1.165984
        4  0.384688  0.105992
        5  0.489517  0.009636
        6  0.589956  0.000876

        >>> df.s2.ewm(halflife=1.5, min_periods=3).mean()
        0         NaN
        1         NaN
        2    2.182572
        3    1.663174
        4    0.979949
        5    0.593155
        6    0.364668
        Name: s2, dtype: float64
        """
        return super().mean()

    # TODO: when add 'adjust' parameter, should add to here too.
    def __repr__(self) -> str:
        return (
            "ExponentialMoving [com={}, span={}, halflife={}, alpha={}, "
            "min_periods={}, ignore_na={}]".format(
                self._com,
                self._span,
                self._halflife,
                self._alpha,
                self._min_periods,
                self._ignore_na,
            )
        )


class ExponentialMovingGroupby(ExponentialMovingLike[FrameLike]):
    def __init__(
        self,
        groupby: GroupBy[FrameLike],
        com: Optional[float] = None,
        span: Optional[float] = None,
        halflife: Optional[float] = None,
        alpha: Optional[float] = None,
        min_periods: Optional[int] = None,
        ignore_na: bool = False,
    ):
        window_spec = Window.orderBy(NATURAL_ORDER_COLUMN_NAME).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        super().__init__(window_spec, com, span, halflife, alpha, min_periods, ignore_na)

        self._groupby = groupby
        self._window = self._window.partitionBy(*[ser.spark.column for ser in groupby._groupkeys])
        self._unbounded_window = self._unbounded_window.partitionBy(
            *[ser.spark.column for ser in groupby._groupkeys]
        )

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeExponentialMovingGroupby, item):
            property_or_func = getattr(MissingPandasLikeExponentialMovingGroupby, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        raise AttributeError(item)

    _apply_as_series_or_frame = RollingGroupby._apply_as_series_or_frame

    def mean(self) -> FrameLike:
        """
        Calculate an online exponentially weighted mean.

        Notes
        -----
        There are behavior differences between pandas-on-Spark and pandas.

        * the current implementation of this API uses Spark's Window without
          specifying partition specification. This leads to move all data into
          single partition in single machine and could cause serious
          performance degradation. Avoid this method against very large dataset.

        Returns
        -------
        Series or DataFrame
            Returned object type is determined by the caller of the exponentially
            calculation.

        See Also
        --------
        pyspark.pandas.Series.expanding : Calling object with Series data.
        pyspark.pandas.DataFrame.expanding : Calling object with DataFrames.
        pyspark.pandas.Series.mean : Equivalent method for Series.
        pyspark.pandas.DataFrame.mean : Equivalent method for DataFrame.

        Examples
        --------
        >>> s = ps.Series([2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5])
        >>> s.groupby(s).ewm(alpha=0.5).mean().sort_index()
        2  0     2.0
           1     2.0
        3  2     3.0
           3     3.0
           4     3.0
        4  5     4.0
           6     4.0
           7     4.0
           8     4.0
        5  9     5.0
           10    5.0
        dtype: float64

        For DataFrame, each ewm mean is computed column-wise.

        >>> df = ps.DataFrame({"A": s.to_numpy(), "B": s.to_numpy() ** 2})
        >>> df.groupby(df.A).ewm(alpha=0.5).mean().sort_index()  # doctest: +NORMALIZE_WHITESPACE
                 B
        A
        2 0    4.0
          1    4.0
        3 2    9.0
          3    9.0
          4    9.0
        4 5   16.0
          6   16.0
          7   16.0
          8   16.0
        5 9   25.0
          10  25.0
        """
        return super().mean()

    # TODO: when add 'adjust' parameter, should add to here too.
    def __repr__(self) -> str:
        return (
            "ExponentialMovingGroupby [com={}, span={}, halflife={}, alpha={}, "
            "min_periods={}, ignore_na={}]".format(
                self._com,
                self._span,
                self._halflife,
                self._alpha,
                self._min_periods,
                self._ignore_na,
            )
        )


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.window

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.window.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]").appName("pyspark.pandas.window tests").getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.window,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
