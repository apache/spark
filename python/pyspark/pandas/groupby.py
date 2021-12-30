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
A wrapper for GroupedData to behave similar to pandas GroupBy.
"""

from abc import ABCMeta, abstractmethod
import inspect
from collections import defaultdict, namedtuple
from distutils.version import LooseVersion
from functools import partial
from itertools import product
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    Mapping,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
    TYPE_CHECKING,
)
import warnings

import pandas as pd
from pandas.api.types import is_hashable, is_list_like  # type: ignore[attr-defined]

if LooseVersion(pd.__version__) >= LooseVersion("1.3.0"):
    from pandas.core.common import _builtin_table  # type: ignore[attr-defined]
else:
    from pandas.core.base import SelectionMixin

    _builtin_table = SelectionMixin._builtin_table  # type: ignore[attr-defined]

from pyspark.sql import Column, DataFrame as SparkDataFrame, Window, functions as F
from pyspark.sql.types import (
    NumericType,
    StructField,
    StructType,
    StringType,
)

from pyspark import pandas as ps  # For running doctests and reference resolution in PyCharm.
from pyspark.pandas._typing import Axis, FrameLike, Label, Name
from pyspark.pandas.typedef import infer_return_type, DataFrameType, ScalarType, SeriesType
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    HIDDEN_COLUMNS,
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_INDEX_NAME_FORMAT,
    SPARK_DEFAULT_SERIES_NAME,
    SPARK_INDEX_NAME_PATTERN,
)
from pyspark.pandas.missing.groupby import (
    MissingPandasLikeDataFrameGroupBy,
    MissingPandasLikeSeriesGroupBy,
)
from pyspark.pandas.series import Series, first_series
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.config import get_option
from pyspark.pandas.utils import (
    align_diff_frames,
    is_name_like_tuple,
    is_name_like_value,
    name_like_string,
    same_anchor,
    scol_for,
    verify_temp_column_name,
    log_advice,
)
from pyspark.pandas.spark.utils import as_nullable_spark_type, force_decimal_precision_scale
from pyspark.pandas.exceptions import DataError

if TYPE_CHECKING:
    from pyspark.pandas.window import RollingGroupby, ExpandingGroupby


# to keep it the same as pandas
NamedAgg = namedtuple("NamedAgg", ["column", "aggfunc"])


class GroupBy(Generic[FrameLike], metaclass=ABCMeta):
    """
    :ivar _psdf: The parent dataframe that is used to perform the groupby
    :type _psdf: DataFrame
    :ivar _groupkeys: The list of keys that will be used to perform the grouping
    :type _groupkeys: List[Series]
    """

    def __init__(
        self,
        psdf: DataFrame,
        groupkeys: List[Series],
        as_index: bool,
        dropna: bool,
        column_labels_to_exclude: Set[Label],
        agg_columns_selected: bool,
        agg_columns: List[Series],
    ):
        self._psdf = psdf
        self._groupkeys = groupkeys
        self._as_index = as_index
        self._dropna = dropna
        self._column_labels_to_exclude = column_labels_to_exclude
        self._agg_columns_selected = agg_columns_selected
        self._agg_columns = agg_columns

    @property
    def _groupkeys_scols(self) -> List[Column]:
        return [s.spark.column for s in self._groupkeys]

    @property
    def _agg_columns_scols(self) -> List[Column]:
        return [s.spark.column for s in self._agg_columns]

    @abstractmethod
    def _apply_series_op(
        self,
        op: Callable[["SeriesGroupBy"], Series],
        should_resolve: bool = False,
        numeric_only: bool = False,
    ) -> FrameLike:
        pass

    @abstractmethod
    def _cleanup_and_return(self, psdf: DataFrame) -> FrameLike:
        pass

    # TODO: Series support is not implemented yet.
    # TODO: not all arguments are implemented comparing to pandas' for now.
    def aggregate(
        self,
        func_or_funcs: Optional[Union[str, List[str], Dict[Name, Union[str, List[str]]]]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> DataFrame:
        """Aggregate using one or more operations over the specified axis.

        Parameters
        ----------
        func_or_funcs : dict, str or list
             a dict mapping from column name (string) to
             aggregate functions (string or list of strings).

        Returns
        -------
        Series or DataFrame

            The return can be:

            * Series : when DataFrame.agg is called with a single function
            * DataFrame : when DataFrame.agg is called with several functions

            Return Series or DataFrame.

        Notes
        -----
        `agg` is an alias for `aggregate`. Use the alias.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 1, 2, 2],
        ...                    'B': [1, 2, 3, 4],
        ...                    'C': [0.362, 0.227, 1.267, -0.562]},
        ...                   columns=['A', 'B', 'C'])

        >>> df
           A  B      C
        0  1  1  0.362
        1  1  2  0.227
        2  2  3  1.267
        3  2  4 -0.562

        Different aggregations per column

        >>> aggregated = df.groupby('A').agg({'B': 'min', 'C': 'sum'})
        >>> aggregated[['B', 'C']].sort_index()  # doctest: +NORMALIZE_WHITESPACE
           B      C
        A
        1  1  0.589
        2  3  0.705

        >>> aggregated = df.groupby('A').agg({'B': ['min', 'max']})
        >>> aggregated.sort_index()  # doctest: +NORMALIZE_WHITESPACE
             B
           min  max
        A
        1    1    2
        2    3    4

        >>> aggregated = df.groupby('A').agg('min')
        >>> aggregated.sort_index()  # doctest: +NORMALIZE_WHITESPACE
             B      C
        A
        1    1  0.227
        2    3 -0.562

        >>> aggregated = df.groupby('A').agg(['min', 'max'])
        >>> aggregated.sort_index()  # doctest: +NORMALIZE_WHITESPACE
             B           C
           min  max    min    max
        A
        1    1    2  0.227  0.362
        2    3    4 -0.562  1.267

        To control the output names with different aggregations per column, pandas-on-Spark
        also supports 'named aggregation' or nested renaming in .agg. It can also be
        used when applying multiple aggregation functions to specific columns.

        >>> aggregated = df.groupby('A').agg(b_max=ps.NamedAgg(column='B', aggfunc='max'))
        >>> aggregated.sort_index()  # doctest: +NORMALIZE_WHITESPACE
             b_max
        A
        1        2
        2        4

        >>> aggregated = df.groupby('A').agg(b_max=('B', 'max'), b_min=('B', 'min'))
        >>> aggregated.sort_index()  # doctest: +NORMALIZE_WHITESPACE
             b_max   b_min
        A
        1        2       1
        2        4       3

        >>> aggregated = df.groupby('A').agg(b_max=('B', 'max'), c_min=('C', 'min'))
        >>> aggregated.sort_index()  # doctest: +NORMALIZE_WHITESPACE
             b_max   c_min
        A
        1        2   0.227
        2        4  -0.562
        """
        # I think current implementation of func and arguments in pandas-on-Spark for aggregate
        # is different than pandas, later once arguments are added, this could be removed.
        if func_or_funcs is None and kwargs is None:
            raise ValueError("No aggregation argument or function specified.")

        relabeling = func_or_funcs is None and is_multi_agg_with_relabel(**kwargs)
        if relabeling:
            (
                func_or_funcs,
                columns,
                order,
            ) = normalize_keyword_aggregation(  # type: ignore[assignment]
                kwargs
            )

        if not isinstance(func_or_funcs, (str, list)):
            if not isinstance(func_or_funcs, dict) or not all(
                is_name_like_value(key)
                and (
                    isinstance(value, str)
                    or isinstance(value, list)
                    and all(isinstance(v, str) for v in value)
                )
                for key, value in func_or_funcs.items()
            ):
                raise ValueError(
                    "aggs must be a dict mapping from column name "
                    "to aggregate functions (string or list of strings)."
                )

        else:
            agg_cols = [col.name for col in self._agg_columns]
            func_or_funcs = {col: func_or_funcs for col in agg_cols}

        psdf: DataFrame = DataFrame(
            GroupBy._spark_groupby(self._psdf, func_or_funcs, self._groupkeys)
        )

        if self._dropna:
            psdf = DataFrame(
                psdf._internal.with_new_sdf(
                    psdf._internal.spark_frame.dropna(
                        subset=psdf._internal.index_spark_column_names
                    )
                )
            )

        if not self._as_index:
            should_drop_index = set(
                i for i, gkey in enumerate(self._groupkeys) if gkey._psdf is not self._psdf
            )
            if len(should_drop_index) > 0:
                psdf = psdf.reset_index(level=should_drop_index, drop=True)
            if len(should_drop_index) < len(self._groupkeys):
                psdf = psdf.reset_index()

        if relabeling:
            psdf = psdf[order]
            psdf.columns = columns  # type: ignore[assignment]
        return psdf

    agg = aggregate

    @staticmethod
    def _spark_groupby(
        psdf: DataFrame,
        func: Mapping[Name, Union[str, List[str]]],
        groupkeys: Sequence[Series] = (),
    ) -> InternalFrame:
        groupkey_names = [SPARK_INDEX_NAME_FORMAT(i) for i in range(len(groupkeys))]
        groupkey_scols = [s.spark.column.alias(name) for s, name in zip(groupkeys, groupkey_names)]

        multi_aggs = any(isinstance(v, list) for v in func.values())
        reordered = []
        data_columns = []
        column_labels = []
        for key, value in func.items():
            label = key if is_name_like_tuple(key) else (key,)
            if len(label) != psdf._internal.column_labels_level:
                raise TypeError("The length of the key must be the same as the column label level.")
            for aggfunc in [value] if isinstance(value, str) else value:
                column_label = tuple(list(label) + [aggfunc]) if multi_aggs else label
                column_labels.append(column_label)

                data_col = name_like_string(column_label)
                data_columns.append(data_col)

                col_name = psdf._internal.spark_column_name_for(label)
                if aggfunc == "nunique":
                    reordered.append(
                        F.expr("count(DISTINCT `{0}`) as `{1}`".format(col_name, data_col))
                    )

                # Implement "quartiles" aggregate function for ``describe``.
                elif aggfunc == "quartiles":
                    reordered.append(
                        F.expr(
                            "percentile_approx(`{0}`, array(0.25, 0.5, 0.75)) as `{1}`".format(
                                col_name, data_col
                            )
                        )
                    )

                else:
                    reordered.append(
                        F.expr("{1}(`{0}`) as `{2}`".format(col_name, aggfunc, data_col))
                    )

        sdf = psdf._internal.spark_frame.select(groupkey_scols + psdf._internal.data_spark_columns)
        sdf = sdf.groupby(*groupkey_names).agg(*reordered)

        return InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in groupkey_names],
            index_names=[psser._column_label for psser in groupkeys],
            index_fields=[
                psser._internal.data_fields[0].copy(name=name)
                for psser, name in zip(groupkeys, groupkey_names)
            ],
            column_labels=column_labels,
            data_spark_columns=[scol_for(sdf, col) for col in data_columns],
        )

    def count(self) -> FrameLike:
        """
        Compute count of group, excluding missing values.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 1, 2, 1, 2],
        ...                    'B': [np.nan, 2, 3, 4, 5],
        ...                    'C': [1, 2, 1, 1, 2]}, columns=['A', 'B', 'C'])
        >>> df.groupby('A').count().sort_index()  # doctest: +NORMALIZE_WHITESPACE
            B  C
        A
        1  2  3
        2  2  2
        """
        return self._reduce_for_stat_function(F.count, only_numeric=False)

    # TODO: We should fix See Also when Series implementation is finished.
    def first(self) -> FrameLike:
        """
        Compute first of group values.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby
        """
        return self._reduce_for_stat_function(F.first, only_numeric=False)

    def last(self) -> FrameLike:
        """
        Compute last of group values.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby
        """
        return self._reduce_for_stat_function(
            lambda col: F.last(col, ignorenulls=True), only_numeric=False
        )

    def max(self) -> FrameLike:
        """
        Compute max of group values.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby
        """
        return self._reduce_for_stat_function(F.max, only_numeric=False)

    # TODO: examples should be updated.
    def mean(self) -> FrameLike:
        """
        Compute mean of groups, excluding missing values.

        Returns
        -------
        pyspark.pandas.Series or pyspark.pandas.DataFrame

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 1, 2, 1, 2],
        ...                    'B': [np.nan, 2, 3, 4, 5],
        ...                    'C': [1, 2, 1, 1, 2]}, columns=['A', 'B', 'C'])

        Groupby one column and return the mean of the remaining columns in
        each group.

        >>> df.groupby('A').mean().sort_index()  # doctest: +NORMALIZE_WHITESPACE
             B         C
        A
        1  3.0  1.333333
        2  4.0  1.500000
        """

        return self._reduce_for_stat_function(F.mean, only_numeric=True)

    def min(self) -> FrameLike:
        """
        Compute min of group values.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby
        """
        return self._reduce_for_stat_function(F.min, only_numeric=False)

    # TODO: sync the doc.
    def std(self, ddof: int = 1) -> FrameLike:
        """
        Compute standard deviation of groups, excluding missing values.

        Parameters
        ----------
        ddof : int, default 1
            Delta Degrees of Freedom. The divisor used in calculations is N - ddof,
            where N represents the number of elements.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby
        """
        assert ddof in (0, 1)

        return self._reduce_for_stat_function(
            F.stddev_pop if ddof == 0 else F.stddev_samp, only_numeric=True
        )

    def sum(self) -> FrameLike:
        """
        Compute sum of group values

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby
        """
        return self._reduce_for_stat_function(F.sum, only_numeric=True)

    # TODO: sync the doc.
    def var(self, ddof: int = 1) -> FrameLike:
        """
        Compute variance of groups, excluding missing values.

        Parameters
        ----------
        ddof : int, default 1
            Delta Degrees of Freedom. The divisor used in calculations is N - ddof,
            where N represents the number of elements.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby
        """
        assert ddof in (0, 1)

        return self._reduce_for_stat_function(
            F.var_pop if ddof == 0 else F.var_samp, only_numeric=True
        )

    # TODO: skipna should be implemented.
    def all(self) -> FrameLike:
        """
        Returns True if all values in the group are truthful, else False.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        ...                    'B': [True, True, True, False, False,
        ...                          False, None, True, None, False]},
        ...                   columns=['A', 'B'])
        >>> df
           A      B
        0  1   True
        1  1   True
        2  2   True
        3  2  False
        4  3  False
        5  3  False
        6  4   None
        7  4   True
        8  5   None
        9  5  False

        >>> df.groupby('A').all().sort_index()  # doctest: +NORMALIZE_WHITESPACE
               B
        A
        1   True
        2  False
        3  False
        4   True
        5  False
        """
        return self._reduce_for_stat_function(
            lambda col: F.min(F.coalesce(col.cast("boolean"), SF.lit(True))), only_numeric=False
        )

    # TODO: skipna should be implemented.
    def any(self) -> FrameLike:
        """
        Returns True if any value in the group is truthful, else False.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        ...                    'B': [True, True, True, False, False,
        ...                          False, None, True, None, False]},
        ...                   columns=['A', 'B'])
        >>> df
           A      B
        0  1   True
        1  1   True
        2  2   True
        3  2  False
        4  3  False
        5  3  False
        6  4   None
        7  4   True
        8  5   None
        9  5  False

        >>> df.groupby('A').any().sort_index()  # doctest: +NORMALIZE_WHITESPACE
               B
        A
        1   True
        2   True
        3  False
        4   True
        5  False
        """
        return self._reduce_for_stat_function(
            lambda col: F.max(F.coalesce(col.cast("boolean"), SF.lit(False))), only_numeric=False
        )

    # TODO: groupby multiply columns should be implemented.
    def size(self) -> Series:
        """
        Compute group sizes.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 2, 2, 3, 3, 3],
        ...                    'B': [1, 1, 2, 3, 3, 3]},
        ...                   columns=['A', 'B'])
        >>> df
           A  B
        0  1  1
        1  2  1
        2  2  2
        3  3  3
        4  3  3
        5  3  3

        >>> df.groupby('A').size().sort_index()
        A
        1    1
        2    2
        3    3
        dtype: int64

        >>> df.groupby(['A', 'B']).size().sort_index()
        A  B
        1  1    1
        2  1    1
           2    1
        3  3    3
        dtype: int64

        For Series,

        >>> df.B.groupby(df.A).size().sort_index()
        A
        1    1
        2    2
        3    3
        Name: B, dtype: int64

        >>> df.groupby(df.A).B.size().sort_index()
        A
        1    1
        2    2
        3    3
        Name: B, dtype: int64
        """
        groupkeys = self._groupkeys
        groupkey_names = [SPARK_INDEX_NAME_FORMAT(i) for i in range(len(groupkeys))]
        groupkey_scols = [s.spark.column.alias(name) for s, name in zip(groupkeys, groupkey_names)]
        sdf = self._psdf._internal.spark_frame.select(
            groupkey_scols + self._psdf._internal.data_spark_columns
        )
        sdf = sdf.groupby(*groupkey_names).count()
        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in groupkey_names],
            index_names=[psser._column_label for psser in groupkeys],
            index_fields=[
                psser._internal.data_fields[0].copy(name=name)
                for psser, name in zip(groupkeys, groupkey_names)
            ],
            column_labels=[None],
            data_spark_columns=[scol_for(sdf, "count")],
        )
        return first_series(DataFrame(internal))

    def diff(self, periods: int = 1) -> FrameLike:
        """
        First discrete difference of element.

        Calculates the difference of a DataFrame element compared with another element in the
        DataFrame group (default is the element in the same column of the previous row).

        Parameters
        ----------
        periods : int, default 1
            Periods to shift for calculating difference, accepts negative values.

        Returns
        -------
        diffed : DataFrame or Series

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 2, 3, 4, 5, 6],
        ...                    'b': [1, 1, 2, 3, 5, 8],
        ...                    'c': [1, 4, 9, 16, 25, 36]}, columns=['a', 'b', 'c'])
        >>> df
           a  b   c
        0  1  1   1
        1  2  1   4
        2  3  2   9
        3  4  3  16
        4  5  5  25
        5  6  8  36

        >>> df.groupby(['b']).diff().sort_index()
             a    c
        0  NaN  NaN
        1  1.0  3.0
        2  NaN  NaN
        3  NaN  NaN
        4  NaN  NaN
        5  NaN  NaN

        Difference with previous column in a group.

        >>> df.groupby(['b'])['a'].diff().sort_index()
        0    NaN
        1    1.0
        2    NaN
        3    NaN
        4    NaN
        5    NaN
        Name: a, dtype: float64
        """
        return self._apply_series_op(
            lambda sg: sg._psser._diff(periods, part_cols=sg._groupkeys_scols), should_resolve=True
        )

    def cumcount(self, ascending: bool = True) -> Series:
        """
        Number each item in each group from 0 to the length of that group - 1.

        Essentially this is equivalent to

        .. code-block:: python

            self.apply(lambda x: pd.Series(np.arange(len(x)), x.index))

        Parameters
        ----------
        ascending : bool, default True
            If False, number in reverse, from length of group - 1 to 0.

        Returns
        -------
        Series
            Sequence number of each element within each group.

        Examples
        --------

        >>> df = ps.DataFrame([['a'], ['a'], ['a'], ['b'], ['b'], ['a']],
        ...                   columns=['A'])
        >>> df
           A
        0  a
        1  a
        2  a
        3  b
        4  b
        5  a
        >>> df.groupby('A').cumcount().sort_index()
        0    0
        1    1
        2    2
        3    0
        4    1
        5    3
        dtype: int64
        >>> df.groupby('A').cumcount(ascending=False).sort_index()
        0    3
        1    2
        2    1
        3    1
        4    0
        5    0
        dtype: int64
        """
        ret = (
            self._groupkeys[0]
            .rename()
            .spark.transform(lambda _: SF.lit(0))
            ._cum(F.count, True, part_cols=self._groupkeys_scols, ascending=ascending)
            - 1
        )
        internal = ret._internal.resolved_copy
        return first_series(DataFrame(internal))

    def cummax(self) -> FrameLike:
        """
        Cumulative max for each group.

        Returns
        -------
        Series or DataFrame

        See Also
        --------
        Series.cummax
        DataFrame.cummax

        Examples
        --------
        >>> df = ps.DataFrame(
        ...     [[1, None, 4], [1, 0.1, 3], [1, 20.0, 2], [4, 10.0, 1]],
        ...     columns=list('ABC'))
        >>> df
           A     B  C
        0  1   NaN  4
        1  1   0.1  3
        2  1  20.0  2
        3  4  10.0  1

        By default, iterates over rows and finds the sum in each column.

        >>> df.groupby("A").cummax().sort_index()
              B  C
        0   NaN  4
        1   0.1  4
        2  20.0  4
        3  10.0  1

        It works as below in Series.

        >>> df.C.groupby(df.A).cummax().sort_index()
        0    4
        1    4
        2    4
        3    1
        Name: C, dtype: int64
        """
        return self._apply_series_op(
            lambda sg: sg._psser._cum(F.max, True, part_cols=sg._groupkeys_scols),
            should_resolve=True,
            numeric_only=True,
        )

    def cummin(self) -> FrameLike:
        """
        Cumulative min for each group.

        Returns
        -------
        Series or DataFrame

        See Also
        --------
        Series.cummin
        DataFrame.cummin

        Examples
        --------
        >>> df = ps.DataFrame(
        ...     [[1, None, 4], [1, 0.1, 3], [1, 20.0, 2], [4, 10.0, 1]],
        ...     columns=list('ABC'))
        >>> df
           A     B  C
        0  1   NaN  4
        1  1   0.1  3
        2  1  20.0  2
        3  4  10.0  1

        By default, iterates over rows and finds the sum in each column.

        >>> df.groupby("A").cummin().sort_index()
              B  C
        0   NaN  4
        1   0.1  3
        2   0.1  2
        3  10.0  1

        It works as below in Series.

        >>> df.B.groupby(df.A).cummin().sort_index()
        0     NaN
        1     0.1
        2     0.1
        3    10.0
        Name: B, dtype: float64
        """
        return self._apply_series_op(
            lambda sg: sg._psser._cum(F.min, True, part_cols=sg._groupkeys_scols),
            should_resolve=True,
            numeric_only=True,
        )

    def cumprod(self) -> FrameLike:
        """
        Cumulative product for each group.

        Returns
        -------
        Series or DataFrame

        See Also
        --------
        Series.cumprod
        DataFrame.cumprod

        Examples
        --------
        >>> df = ps.DataFrame(
        ...     [[1, None, 4], [1, 0.1, 3], [1, 20.0, 2], [4, 10.0, 1]],
        ...     columns=list('ABC'))
        >>> df
           A     B  C
        0  1   NaN  4
        1  1   0.1  3
        2  1  20.0  2
        3  4  10.0  1

        By default, iterates over rows and finds the sum in each column.

        >>> df.groupby("A").cumprod().sort_index()
              B   C
        0   NaN   4
        1   0.1  12
        2   2.0  24
        3  10.0   1

        It works as below in Series.

        >>> df.B.groupby(df.A).cumprod().sort_index()
        0     NaN
        1     0.1
        2     2.0
        3    10.0
        Name: B, dtype: float64
        """
        return self._apply_series_op(
            lambda sg: sg._psser._cumprod(True, part_cols=sg._groupkeys_scols),
            should_resolve=True,
            numeric_only=True,
        )

    def cumsum(self) -> FrameLike:
        """
        Cumulative sum for each group.

        Returns
        -------
        Series or DataFrame

        See Also
        --------
        Series.cumsum
        DataFrame.cumsum

        Examples
        --------
        >>> df = ps.DataFrame(
        ...     [[1, None, 4], [1, 0.1, 3], [1, 20.0, 2], [4, 10.0, 1]],
        ...     columns=list('ABC'))
        >>> df
           A     B  C
        0  1   NaN  4
        1  1   0.1  3
        2  1  20.0  2
        3  4  10.0  1

        By default, iterates over rows and finds the sum in each column.

        >>> df.groupby("A").cumsum().sort_index()
              B  C
        0   NaN  4
        1   0.1  7
        2  20.1  9
        3  10.0  1

        It works as below in Series.

        >>> df.B.groupby(df.A).cumsum().sort_index()
        0     NaN
        1     0.1
        2    20.1
        3    10.0
        Name: B, dtype: float64
        """
        return self._apply_series_op(
            lambda sg: sg._psser._cumsum(True, part_cols=sg._groupkeys_scols),
            should_resolve=True,
            numeric_only=True,
        )

    def apply(self, func: Callable, *args: Any, **kwargs: Any) -> Union[DataFrame, Series]:
        """
        Apply function `func` group-wise and combine the results together.

        The function passed to `apply` must take a DataFrame as its first
        argument and return a DataFrame. `apply` will
        then take care of combining the results back together into a single
        dataframe. `apply` is therefore a highly flexible
        grouping method.

        While `apply` is a very flexible method, its downside is that
        using it can be quite a bit slower than using more specific methods
        like `agg` or `transform`. pandas-on-Spark offers a wide range of method that will
        be much faster than using `apply` for their specific purposes, so try to
        use them before reaching for `apply`.

        .. note:: this API executes the function once to infer the type which is
            potentially expensive, for instance, when the dataset is created after
            aggregations or sorting.

            To avoid this, specify return type in ``func``, for instance, as below:

            >>> def pandas_div(x) -> ps.DataFrame[int, [float, float]]:
            ...     return x[['B', 'C']] / x[['B', 'C']]

            If the return type is specified, the output column names become
            `c0, c1, c2 ... cn`. These names are positionally mapped to the returned
            DataFrame in ``func``.

            To specify the column names, you can assign them in a NumPy compound type style
            as below:

            >>> def pandas_div(x) -> ps.DataFrame[("index", int), [("a", float), ("b", float)]]:
            ...     return x[['B', 'C']] / x[['B', 'C']]

            >>> pdf = pd.DataFrame({'B': [1.], 'C': [3.]})
            >>> def plus_one(x) -> ps.DataFrame[
            ...         (pdf.index.name, pdf.index.dtype), zip(pdf.columns, pdf.dtypes)]:
            ...     return x[['B', 'C']] / x[['B', 'C']]

        .. note:: the dataframe within ``func`` is actually a pandas dataframe. Therefore,
            any pandas API within this function is allowed.

        Parameters
        ----------
        func : callable
            A callable that takes a DataFrame as its first argument, and
            returns a dataframe.
        *args
            Positional arguments to pass to func.
        **kwargs
            Keyword arguments to pass to func.

        Returns
        -------
        applied : DataFrame or Series

        See Also
        --------
        aggregate : Apply aggregate function to the GroupBy object.
        DataFrame.apply : Apply a function to a DataFrame.
        Series.apply : Apply a function to a Series.

        Examples
        --------
        >>> df = ps.DataFrame({'A': 'a a b'.split(),
        ...                    'B': [1, 2, 3],
        ...                    'C': [4, 6, 5]}, columns=['A', 'B', 'C'])
        >>> g = df.groupby('A')

        Notice that ``g`` has two groups, ``a`` and ``b``.
        Calling `apply` in various ways, we can get different grouping results:

        Below the functions passed to `apply` takes a DataFrame as
        its argument and returns a DataFrame. `apply` combines the result for
        each group together into a new DataFrame:

        >>> def plus_min(x):
        ...     return x + x.min()
        >>> g.apply(plus_min).sort_index()  # doctest: +NORMALIZE_WHITESPACE
            A  B   C
        0  aa  2   8
        1  aa  3  10
        2  bb  6  10

        >>> g.apply(sum).sort_index()  # doctest: +NORMALIZE_WHITESPACE
            A  B   C
        A
        a  aa  3  10
        b   b  3   5

        >>> g.apply(len).sort_index()  # doctest: +NORMALIZE_WHITESPACE
        A
        a    2
        b    1
        dtype: int64

        You can specify the type hint and prevent schema inference for better performance.

        >>> def pandas_div(x) -> ps.DataFrame[int, [float, float]]:
        ...     return x[['B', 'C']] / x[['B', 'C']]
        >>> g.apply(pandas_div).sort_index()  # doctest: +NORMALIZE_WHITESPACE
            c0   c1
        0  1.0  1.0
        1  1.0  1.0
        2  1.0  1.0

        >>> def pandas_div(x) -> ps.DataFrame[("index", int), [("f1", float), ("f2", float)]]:
        ...     return x[['B', 'C']] / x[['B', 'C']]
        >>> g.apply(pandas_div).sort_index()  # doctest: +NORMALIZE_WHITESPACE
                f1   f2
        index
        0      1.0  1.0
        1      1.0  1.0
        2      1.0  1.0

        In case of Series, it works as below.

        >>> def plus_max(x) -> ps.Series[np.int]:
        ...     return x + x.max()
        >>> df.B.groupby(df.A).apply(plus_max).sort_index()  # doctest: +SKIP
        0    6
        1    3
        2    4
        Name: B, dtype: int64

        >>> def plus_min(x):
        ...     return x + x.min()
        >>> df.B.groupby(df.A).apply(plus_min).sort_index()
        0    2
        1    3
        2    6
        Name: B, dtype: int64

        You can also return a scalar value as a aggregated value of the group:

        >>> def plus_length(x) -> np.int:
        ...     return len(x)
        >>> df.B.groupby(df.A).apply(plus_length).sort_index()  # doctest: +SKIP
        0    1
        1    2
        Name: B, dtype: int64

        The extra arguments to the function can be passed as below.

        >>> def calculation(x, y, z) -> np.int:
        ...     return len(x) + y * z
        >>> df.B.groupby(df.A).apply(calculation, 5, z=10).sort_index()  # doctest: +SKIP
        0    51
        1    52
        Name: B, dtype: int64
        """
        if not callable(func):
            raise TypeError("%s object is not callable" % type(func).__name__)

        spec = inspect.getfullargspec(func)
        return_sig = spec.annotations.get("return", None)
        should_infer_schema = return_sig is None
        should_retain_index = should_infer_schema

        is_series_groupby = isinstance(self, SeriesGroupBy)

        psdf = self._psdf

        if self._agg_columns_selected:
            agg_columns = self._agg_columns
        else:
            agg_columns = [
                psdf._psser_for(label)
                for label in psdf._internal.column_labels
                if label not in self._column_labels_to_exclude
            ]

        psdf, groupkey_labels, groupkey_names = GroupBy._prepare_group_map_apply(
            psdf, self._groupkeys, agg_columns
        )

        if is_series_groupby:
            name = psdf.columns[-1]
            pandas_apply = _builtin_table.get(func, func)
        else:
            f = _builtin_table.get(func, func)

            def pandas_apply(pdf: pd.DataFrame, *a: Any, **k: Any) -> Any:
                return f(pdf.drop(groupkey_names, axis=1), *a, **k)

        should_return_series = False

        if should_infer_schema:
            # Here we execute with the first 1000 to get the return type.
            log_advice(
                "If the type hints is not specified for `grouby.apply`, "
                "it is expensive to infer the data type internally."
            )
            limit = get_option("compute.shortcut_limit")
            pdf = psdf.head(limit + 1)._to_internal_pandas()
            groupkeys = [
                pdf[groupkey_name].rename(psser.name)
                for groupkey_name, psser in zip(groupkey_names, self._groupkeys)
            ]
            grouped = pdf.groupby(groupkeys)
            if is_series_groupby:
                pser_or_pdf = grouped[name].apply(pandas_apply, *args, **kwargs)
            else:
                pser_or_pdf = grouped.apply(pandas_apply, *args, **kwargs)
            psser_or_psdf = ps.from_pandas(pser_or_pdf)

            if len(pdf) <= limit:
                if isinstance(psser_or_psdf, ps.Series) and is_series_groupby:
                    psser_or_psdf = psser_or_psdf.rename(cast(SeriesGroupBy, self)._psser.name)
                return cast(Union[Series, DataFrame], psser_or_psdf)

            if len(grouped) <= 1:
                with warnings.catch_warnings():
                    warnings.simplefilter("always")
                    warnings.warn(
                        "The amount of data for return type inference might not be large enough. "
                        "Consider increasing an option `compute.shortcut_limit`."
                    )

            if isinstance(psser_or_psdf, Series):
                should_return_series = True
                psdf_from_pandas = psser_or_psdf._psdf
            else:
                psdf_from_pandas = cast(DataFrame, psser_or_psdf)

            index_fields = [
                field.normalize_spark_type() for field in psdf_from_pandas._internal.index_fields
            ]
            data_fields = [
                field.normalize_spark_type() for field in psdf_from_pandas._internal.data_fields
            ]
            return_schema = StructType([field.struct_field for field in index_fields + data_fields])
        else:
            return_type = infer_return_type(func)
            if not is_series_groupby and isinstance(return_type, SeriesType):
                raise TypeError(
                    "Series as a return type hint at frame groupby is not supported "
                    "currently; however got [%s]. Use DataFrame type hint instead." % return_sig
                )

            if isinstance(return_type, DataFrameType):
                data_fields = cast(DataFrameType, return_type).data_fields
                return_schema = cast(DataFrameType, return_type).spark_type
                index_fields = cast(DataFrameType, return_type).index_fields
                should_retain_index = len(index_fields) > 0
                psdf_from_pandas = None
            else:
                should_return_series = True
                dtype = cast(Union[SeriesType, ScalarType], return_type).dtype
                spark_type = cast(Union[SeriesType, ScalarType], return_type).spark_type
                if is_series_groupby:
                    data_fields = [
                        InternalField(
                            dtype=dtype, struct_field=StructField(name=name, dataType=spark_type)
                        )
                    ]
                else:
                    data_fields = [
                        InternalField(
                            dtype=dtype,
                            struct_field=StructField(
                                name=SPARK_DEFAULT_SERIES_NAME, dataType=spark_type
                            ),
                        )
                    ]
                return_schema = StructType([field.struct_field for field in data_fields])

        def pandas_groupby_apply(pdf: pd.DataFrame) -> pd.DataFrame:

            if is_series_groupby:
                pdf_or_ser = pdf.groupby(groupkey_names)[name].apply(pandas_apply, *args, **kwargs)
            else:
                pdf_or_ser = pdf.groupby(groupkey_names).apply(pandas_apply, *args, **kwargs)
                if should_return_series and isinstance(pdf_or_ser, pd.DataFrame):
                    pdf_or_ser = pdf_or_ser.stack()

            if not isinstance(pdf_or_ser, pd.DataFrame):
                return pd.DataFrame(pdf_or_ser)
            else:
                return pdf_or_ser

        sdf = GroupBy._spark_group_map_apply(
            psdf,
            pandas_groupby_apply,
            [psdf._internal.spark_column_for(label) for label in groupkey_labels],
            return_schema,
            retain_index=should_retain_index,
        )

        if should_retain_index:
            # If schema is inferred, we can restore indexes too.
            if psdf_from_pandas is not None:
                internal = psdf_from_pandas._internal.with_new_sdf(
                    spark_frame=sdf, index_fields=index_fields, data_fields=data_fields
                )
            else:
                index_names: Optional[List[Optional[Tuple[Any, ...]]]] = None

                index_spark_columns = [
                    scol_for(sdf, index_field.struct_field.name) for index_field in index_fields
                ]

                if not any(
                    [
                        SPARK_INDEX_NAME_PATTERN.match(index_field.struct_field.name)
                        for index_field in index_fields
                    ]
                ):
                    index_names = [(index_field.struct_field.name,) for index_field in index_fields]
                internal = InternalFrame(
                    spark_frame=sdf,
                    index_names=index_names,
                    index_spark_columns=index_spark_columns,
                    index_fields=index_fields,
                    data_fields=data_fields,
                )
        else:
            # Otherwise, it loses index.
            internal = InternalFrame(
                spark_frame=sdf, index_spark_columns=None, data_fields=data_fields
            )

        if should_return_series:
            psser = first_series(DataFrame(internal))
            if is_series_groupby:
                psser = psser.rename(cast(SeriesGroupBy, self)._psser.name)
            return psser
        else:
            return DataFrame(internal)

    # TODO: implement 'dropna' parameter
    def filter(self, func: Callable[[FrameLike], FrameLike]) -> FrameLike:
        """
        Return a copy of a DataFrame excluding elements from groups that
        do not satisfy the boolean criterion specified by func.

        Parameters
        ----------
        f : function
            Function to apply to each subframe. Should return True or False.
        dropna : Drop groups that do not pass the filter. True by default;
            if False, groups that evaluate False are filled with NaNs.

        Returns
        -------
        filtered : DataFrame or Series

        Notes
        -----
        Each subframe is endowed the attribute 'name' in case you need to know
        which group you are working on.

        Examples
        --------
        >>> df = ps.DataFrame({'A' : ['foo', 'bar', 'foo', 'bar',
        ...                           'foo', 'bar'],
        ...                    'B' : [1, 2, 3, 4, 5, 6],
        ...                    'C' : [2.0, 5., 8., 1., 2., 9.]}, columns=['A', 'B', 'C'])
        >>> grouped = df.groupby('A')
        >>> grouped.filter(lambda x: x['B'].mean() > 3.)
             A  B    C
        1  bar  2  5.0
        3  bar  4  1.0
        5  bar  6  9.0

        >>> df.B.groupby(df.A).filter(lambda x: x.mean() > 3.)
        1    2
        3    4
        5    6
        Name: B, dtype: int64
        """
        if not callable(func):
            raise TypeError("%s object is not callable" % type(func).__name__)

        is_series_groupby = isinstance(self, SeriesGroupBy)

        psdf = self._psdf

        if self._agg_columns_selected:
            agg_columns = self._agg_columns
        else:
            agg_columns = [
                psdf._psser_for(label)
                for label in psdf._internal.column_labels
                if label not in self._column_labels_to_exclude
            ]

        data_schema = (
            psdf[agg_columns]._internal.resolved_copy.spark_frame.drop(*HIDDEN_COLUMNS).schema
        )

        psdf, groupkey_labels, groupkey_names = GroupBy._prepare_group_map_apply(
            psdf, self._groupkeys, agg_columns
        )

        if is_series_groupby:

            def pandas_filter(pdf: pd.DataFrame) -> pd.DataFrame:
                return pd.DataFrame(pdf.groupby(groupkey_names)[pdf.columns[-1]].filter(func))

        else:
            f = _builtin_table.get(func, func)

            def wrapped_func(pdf: pd.DataFrame) -> pd.DataFrame:
                return f(pdf.drop(groupkey_names, axis=1))

            def pandas_filter(pdf: pd.DataFrame) -> pd.DataFrame:
                return pdf.groupby(groupkey_names).filter(wrapped_func).drop(groupkey_names, axis=1)

        sdf = GroupBy._spark_group_map_apply(
            psdf,
            pandas_filter,
            [psdf._internal.spark_column_for(label) for label in groupkey_labels],
            data_schema,
            retain_index=True,
        )

        psdf = DataFrame(self._psdf[agg_columns]._internal.with_new_sdf(sdf))
        if is_series_groupby:
            return cast(FrameLike, first_series(psdf))
        else:
            return cast(FrameLike, psdf)

    @staticmethod
    def _prepare_group_map_apply(
        psdf: DataFrame, groupkeys: List[Series], agg_columns: List[Series]
    ) -> Tuple[DataFrame, List[Label], List[str]]:
        groupkey_labels: List[Label] = [
            verify_temp_column_name(psdf, "__groupkey_{}__".format(i))
            for i in range(len(groupkeys))
        ]
        psdf = psdf[[s.rename(label) for s, label in zip(groupkeys, groupkey_labels)] + agg_columns]
        groupkey_names = [label if len(label) > 1 else label[0] for label in groupkey_labels]
        return DataFrame(psdf._internal.resolved_copy), groupkey_labels, groupkey_names

    @staticmethod
    def _spark_group_map_apply(
        psdf: DataFrame,
        func: Callable[[pd.DataFrame], pd.DataFrame],
        groupkeys_scols: List[Column],
        return_schema: StructType,
        retain_index: bool,
    ) -> SparkDataFrame:
        output_func = GroupBy._make_pandas_df_builder_func(psdf, func, return_schema, retain_index)
        sdf = psdf._internal.spark_frame.drop(*HIDDEN_COLUMNS)
        return sdf.groupby(*groupkeys_scols).applyInPandas(output_func, return_schema)

    @staticmethod
    def _make_pandas_df_builder_func(
        psdf: DataFrame,
        func: Callable[[pd.DataFrame], pd.DataFrame],
        return_schema: StructType,
        retain_index: bool,
    ) -> Callable[[pd.DataFrame], pd.DataFrame]:
        """
        Creates a function that can be used inside the pandas UDF. This function can construct
        the same pandas DataFrame as if the pandas-on-Spark DataFrame is collected to driver side.
        The index, column labels, etc. are re-constructed within the function.
        """
        from pyspark.sql.utils import is_timestamp_ntz_preferred

        arguments_for_restore_index = psdf._internal.arguments_for_restore_index
        prefer_timestamp_ntz = is_timestamp_ntz_preferred()

        def rename_output(pdf: pd.DataFrame) -> pd.DataFrame:
            pdf = InternalFrame.restore_index(pdf.copy(), **arguments_for_restore_index)

            pdf = func(pdf)

            # If schema should be inferred, we don't restore index. pandas seems restoring
            # the index in some cases.
            # When Spark output type is specified, without executing it, we don't know
            # if we should restore the index or not. For instance, see the example in
            # https://github.com/pyspark.pandas/issues/628.
            pdf, _, _, _, _ = InternalFrame.prepare_pandas_frame(
                pdf, retain_index=retain_index, prefer_timestamp_ntz=prefer_timestamp_ntz
            )

            # Just positionally map the column names to given schema's.
            pdf.columns = return_schema.names

            return pdf

        return rename_output

    def rank(self, method: str = "average", ascending: bool = True) -> FrameLike:
        """
        Provide the rank of values within each group.

        Parameters
        ----------
        method : {'average', 'min', 'max', 'first', 'dense'}, default 'average'
            * average: average rank of group
            * min: lowest rank in group
            * max: highest rank in group
            * first: ranks assigned in order they appear in the array
            * dense: like 'min', but rank always increases by 1 between groups
        ascending : boolean, default True
            False for ranks by high (1) to low (N)

        Returns
        -------
        DataFrame with ranking of values within each group

        Examples
        --------

        >>> df = ps.DataFrame({
        ...     'a': [1, 1, 1, 2, 2, 2, 3, 3, 3],
        ...     'b': [1, 2, 2, 2, 3, 3, 3, 4, 4]}, columns=['a', 'b'])
        >>> df
           a  b
        0  1  1
        1  1  2
        2  1  2
        3  2  2
        4  2  3
        5  2  3
        6  3  3
        7  3  4
        8  3  4

        >>> df.groupby("a").rank().sort_index()
             b
        0  1.0
        1  2.5
        2  2.5
        3  1.0
        4  2.5
        5  2.5
        6  1.0
        7  2.5
        8  2.5

        >>> df.b.groupby(df.a).rank(method='max').sort_index()
        0    1.0
        1    3.0
        2    3.0
        3    1.0
        4    3.0
        5    3.0
        6    1.0
        7    3.0
        8    3.0
        Name: b, dtype: float64

        """
        return self._apply_series_op(
            lambda sg: sg._psser._rank(method, ascending, part_cols=sg._groupkeys_scols),
            should_resolve=True,
        )

    # TODO: add axis parameter
    def idxmax(self, skipna: bool = True) -> FrameLike:
        """
        Return index of first occurrence of maximum over requested axis in group.
        NA/null values are excluded.

        Parameters
        ----------
        skipna : boolean, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.

        See Also
        --------
        Series.idxmax
        DataFrame.idxmax
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 2, 2, 3],
        ...                    'b': [1, 2, 3, 4, 5],
        ...                    'c': [5, 4, 3, 2, 1]}, columns=['a', 'b', 'c'])

        >>> df.groupby(['a'])['b'].idxmax().sort_index() # doctest: +NORMALIZE_WHITESPACE
        a
        1  1
        2  3
        3  4
        Name: b, dtype: int64

        >>> df.groupby(['a']).idxmax().sort_index() # doctest: +NORMALIZE_WHITESPACE
           b  c
        a
        1  1  0
        2  3  2
        3  4  4
        """
        if self._psdf._internal.index_level != 1:
            raise ValueError("idxmax only support one-level index now")

        groupkey_names = ["__groupkey_{}__".format(i) for i in range(len(self._groupkeys))]

        sdf = self._psdf._internal.spark_frame
        for s, name in zip(self._groupkeys, groupkey_names):
            sdf = sdf.withColumn(name, s.spark.column)
        index = self._psdf._internal.index_spark_column_names[0]

        stat_exprs = []
        for psser, scol in zip(self._agg_columns, self._agg_columns_scols):
            name = psser._internal.data_spark_column_names[0]

            if skipna:
                order_column = scol.desc_nulls_last()
            else:
                order_column = scol.desc_nulls_first()

            window = Window.partitionBy(*groupkey_names).orderBy(
                order_column, NATURAL_ORDER_COLUMN_NAME
            )
            sdf = sdf.withColumn(
                name, F.when(F.row_number().over(window) == 1, scol_for(sdf, index)).otherwise(None)
            )
            stat_exprs.append(F.max(scol_for(sdf, name)).alias(name))

        sdf = sdf.groupby(*groupkey_names).agg(*stat_exprs)

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in groupkey_names],
            index_names=[psser._column_label for psser in self._groupkeys],
            index_fields=[
                psser._internal.data_fields[0].copy(name=name)
                for psser, name in zip(self._groupkeys, groupkey_names)
            ],
            column_labels=[psser._column_label for psser in self._agg_columns],
            data_spark_columns=[
                scol_for(sdf, psser._internal.data_spark_column_names[0])
                for psser in self._agg_columns
            ],
        )
        return self._cleanup_and_return(DataFrame(internal))

    # TODO: add axis parameter
    def idxmin(self, skipna: bool = True) -> FrameLike:
        """
        Return index of first occurrence of minimum over requested axis in group.
        NA/null values are excluded.

        Parameters
        ----------
        skipna : boolean, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.

        See Also
        --------
        Series.idxmin
        DataFrame.idxmin
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 2, 2, 3],
        ...                    'b': [1, 2, 3, 4, 5],
        ...                    'c': [5, 4, 3, 2, 1]}, columns=['a', 'b', 'c'])

        >>> df.groupby(['a'])['b'].idxmin().sort_index() # doctest: +NORMALIZE_WHITESPACE
        a
        1    0
        2    2
        3    4
        Name: b, dtype: int64

        >>> df.groupby(['a']).idxmin().sort_index() # doctest: +NORMALIZE_WHITESPACE
           b  c
        a
        1  0  1
        2  2  3
        3  4  4
        """
        if self._psdf._internal.index_level != 1:
            raise ValueError("idxmin only support one-level index now")

        groupkey_names = ["__groupkey_{}__".format(i) for i in range(len(self._groupkeys))]

        sdf = self._psdf._internal.spark_frame
        for s, name in zip(self._groupkeys, groupkey_names):
            sdf = sdf.withColumn(name, s.spark.column)
        index = self._psdf._internal.index_spark_column_names[0]

        stat_exprs = []
        for psser, scol in zip(self._agg_columns, self._agg_columns_scols):
            name = psser._internal.data_spark_column_names[0]

            if skipna:
                order_column = scol.asc_nulls_last()
            else:
                order_column = scol.asc_nulls_first()

            window = Window.partitionBy(*groupkey_names).orderBy(
                order_column, NATURAL_ORDER_COLUMN_NAME
            )
            sdf = sdf.withColumn(
                name, F.when(F.row_number().over(window) == 1, scol_for(sdf, index)).otherwise(None)
            )
            stat_exprs.append(F.max(scol_for(sdf, name)).alias(name))

        sdf = sdf.groupby(*groupkey_names).agg(*stat_exprs)

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in groupkey_names],
            index_names=[psser._column_label for psser in self._groupkeys],
            index_fields=[
                psser._internal.data_fields[0].copy(name=name)
                for psser, name in zip(self._groupkeys, groupkey_names)
            ],
            column_labels=[psser._column_label for psser in self._agg_columns],
            data_spark_columns=[
                scol_for(sdf, psser._internal.data_spark_column_names[0])
                for psser in self._agg_columns
            ],
        )
        return self._cleanup_and_return(DataFrame(internal))

    def fillna(
        self,
        value: Optional[Any] = None,
        method: Optional[str] = None,
        axis: Optional[Axis] = None,
        inplace: bool = False,
        limit: Optional[int] = None,
    ) -> FrameLike:
        """Fill NA/NaN values in group.

        Parameters
        ----------
        value : scalar, dict, Series
            Value to use to fill holes. alternately a dict/Series of values
            specifying which value to use for each column.
            DataFrame is not supported.
        method : {'backfill', 'bfill', 'pad', 'ffill', None}, default None
            Method to use for filling holes in reindexed Series pad / ffill: propagate last valid
            observation forward to next valid backfill / bfill:
            use NEXT valid observation to fill gap
        axis : {0 or `index`}
            1 and `columns` are not supported.
        inplace : boolean, default False
            Fill in place (do not create a new object)
        limit : int, default None
            If method is specified, this is the maximum number of consecutive NaN values to
            forward/backward fill. In other words, if there is a gap with more than this number of
            consecutive NaNs, it will only be partially filled. If method is not specified,
            this is the maximum number of entries along the entire axis where NaNs will be filled.
            Must be greater than 0 if not None

        Returns
        -------
        DataFrame
            DataFrame with NA entries filled.

        Examples
        --------
        >>> df = ps.DataFrame({
        ...     'A': [1, 1, 2, 2],
        ...     'B': [2, 4, None, 3],
        ...     'C': [None, None, None, 1],
        ...     'D': [0, 1, 5, 4]
        ...     },
        ...     columns=['A', 'B', 'C', 'D'])
        >>> df
           A    B    C  D
        0  1  2.0  NaN  0
        1  1  4.0  NaN  1
        2  2  NaN  NaN  5
        3  2  3.0  1.0  4

        We can also propagate non-null values forward or backward in group.

        >>> df.groupby(['A'])['B'].fillna(method='ffill').sort_index()
        0    2.0
        1    4.0
        2    NaN
        3    3.0
        Name: B, dtype: float64

        >>> df.groupby(['A']).fillna(method='bfill').sort_index()
             B    C  D
        0  2.0  NaN  0
        1  4.0  NaN  1
        2  3.0  1.0  5
        3  3.0  1.0  4
        """
        return self._apply_series_op(
            lambda sg: sg._psser._fillna(
                value=value, method=method, axis=axis, limit=limit, part_cols=sg._groupkeys_scols
            ),
            should_resolve=(method is not None),
        )

    def bfill(self, limit: Optional[int] = None) -> FrameLike:
        """
        Synonym for `DataFrame.fillna()` with ``method=`bfill```.

        Parameters
        ----------
        axis : {0 or `index`}
            1 and `columns` are not supported.
        inplace : boolean, default False
            Fill in place (do not create a new object)
        limit : int, default None
            If method is specified, this is the maximum number of consecutive NaN values to
            forward/backward fill. In other words, if there is a gap with more than this number of
            consecutive NaNs, it will only be partially filled. If method is not specified,
            this is the maximum number of entries along the entire axis where NaNs will be filled.
            Must be greater than 0 if not None

        Returns
        -------
        DataFrame
            DataFrame with NA entries filled.

        Examples
        --------
        >>> df = ps.DataFrame({
        ...     'A': [1, 1, 2, 2],
        ...     'B': [2, 4, None, 3],
        ...     'C': [None, None, None, 1],
        ...     'D': [0, 1, 5, 4]
        ...     },
        ...     columns=['A', 'B', 'C', 'D'])
        >>> df
           A    B    C  D
        0  1  2.0  NaN  0
        1  1  4.0  NaN  1
        2  2  NaN  NaN  5
        3  2  3.0  1.0  4

        Propagate non-null values backward.

        >>> df.groupby(['A']).bfill().sort_index()
             B    C  D
        0  2.0  NaN  0
        1  4.0  NaN  1
        2  3.0  1.0  5
        3  3.0  1.0  4
        """
        return self.fillna(method="bfill", limit=limit)

    backfill = bfill

    def ffill(self, limit: Optional[int] = None) -> FrameLike:
        """
        Synonym for `DataFrame.fillna()` with ``method=`ffill```.

        Parameters
        ----------
        axis : {0 or `index`}
            1 and `columns` are not supported.
        inplace : boolean, default False
            Fill in place (do not create a new object)
        limit : int, default None
            If method is specified, this is the maximum number of consecutive NaN values to
            forward/backward fill. In other words, if there is a gap with more than this number of
            consecutive NaNs, it will only be partially filled. If method is not specified,
            this is the maximum number of entries along the entire axis where NaNs will be filled.
            Must be greater than 0 if not None

        Returns
        -------
        DataFrame
            DataFrame with NA entries filled.

        Examples
        --------
        >>> df = ps.DataFrame({
        ...     'A': [1, 1, 2, 2],
        ...     'B': [2, 4, None, 3],
        ...     'C': [None, None, None, 1],
        ...     'D': [0, 1, 5, 4]
        ...     },
        ...     columns=['A', 'B', 'C', 'D'])
        >>> df
           A    B    C  D
        0  1  2.0  NaN  0
        1  1  4.0  NaN  1
        2  2  NaN  NaN  5
        3  2  3.0  1.0  4

        Propagate non-null values forward.

        >>> df.groupby(['A']).ffill().sort_index()
             B    C  D
        0  2.0  NaN  0
        1  4.0  NaN  1
        2  NaN  NaN  5
        3  3.0  1.0  4
        """
        return self.fillna(method="ffill", limit=limit)

    pad = ffill

    def _limit(self, n: int, asc: bool) -> FrameLike:
        """
        Private function for tail and head.
        """
        psdf = self._psdf

        if self._agg_columns_selected:
            agg_columns = self._agg_columns
        else:
            agg_columns = [
                psdf._psser_for(label)
                for label in psdf._internal.column_labels
                if label not in self._column_labels_to_exclude
            ]

        psdf, groupkey_labels, _ = GroupBy._prepare_group_map_apply(
            psdf,
            self._groupkeys,
            agg_columns,
        )

        groupkey_scols = [psdf._internal.spark_column_for(label) for label in groupkey_labels]

        sdf = psdf._internal.spark_frame
        tmp_col = verify_temp_column_name(sdf, "__row_number__")

        # This part is handled differently depending on whether it is a tail or a head.
        window = (
            Window.partitionBy(*groupkey_scols).orderBy(F.col(NATURAL_ORDER_COLUMN_NAME).asc())
            if asc
            else Window.partitionBy(*groupkey_scols).orderBy(
                F.col(NATURAL_ORDER_COLUMN_NAME).desc()
            )
        )

        sdf = (
            sdf.withColumn(tmp_col, F.row_number().over(window))
            .filter(F.col(tmp_col) <= n)
            .drop(tmp_col)
        )

        internal = psdf._internal.with_new_sdf(sdf)
        return self._cleanup_and_return(DataFrame(internal).drop(groupkey_labels, axis=1))

    def head(self, n: int = 5) -> FrameLike:
        """
        Return first n rows of each group.

        Returns
        -------
        DataFrame or Series

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
        ...                    'b': [2, 3, 1, 4, 6, 9, 8, 10, 7, 5],
        ...                    'c': [3, 5, 2, 5, 1, 2, 6, 4, 3, 6]},
        ...                   columns=['a', 'b', 'c'],
        ...                   index=[7, 2, 4, 1, 3, 4, 9, 10, 5, 6])
        >>> df
            a   b  c
        7   1   2  3
        2   1   3  5
        4   1   1  2
        1   1   4  5
        3   2   6  1
        4   2   9  2
        9   2   8  6
        10  3  10  4
        5   3   7  3
        6   3   5  6

        >>> df.groupby('a').head(2).sort_index()
            a   b  c
        2   1   3  5
        3   2   6  1
        4   2   9  2
        5   3   7  3
        7   1   2  3
        10  3  10  4

        >>> df.groupby('a')['b'].head(2).sort_index()
        2      3
        3      6
        4      9
        5      7
        7      2
        10    10
        Name: b, dtype: int64
        """
        return self._limit(n, asc=True)

    def tail(self, n: int = 5) -> FrameLike:
        """
        Return last n rows of each group.

        Similar to `.apply(lambda x: x.tail(n))`, but it returns a subset of rows from
        the original DataFrame with original index and order preserved (`as_index` flag is ignored).

        Does not work for negative values of n.

        Returns
        -------
        DataFrame or Series

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
        ...                    'b': [2, 3, 1, 4, 6, 9, 8, 10, 7, 5],
        ...                    'c': [3, 5, 2, 5, 1, 2, 6, 4, 3, 6]},
        ...                   columns=['a', 'b', 'c'],
        ...                   index=[7, 2, 3, 1, 3, 4, 9, 10, 5, 6])
        >>> df
            a   b  c
        7   1   2  3
        2   1   3  5
        3   1   1  2
        1   1   4  5
        3   2   6  1
        4   2   9  2
        9   2   8  6
        10  3  10  4
        5   3   7  3
        6   3   5  6

        >>> df.groupby('a').tail(2).sort_index()
           a  b  c
        1  1  4  5
        3  1  1  2
        4  2  9  2
        5  3  7  3
        6  3  5  6
        9  2  8  6

        >>> df.groupby('a')['b'].tail(2).sort_index()
        1    4
        3    1
        4    9
        5    7
        6    5
        9    8
        Name: b, dtype: int64
        """
        return self._limit(n, asc=False)

    def shift(self, periods: int = 1, fill_value: Optional[Any] = None) -> FrameLike:
        """
        Shift each group by periods observations.

        Parameters
        ----------
        periods : integer, default 1
            number of periods to shift
        fill_value : optional

        Returns
        -------
        Series or DataFrame
            Object shifted within each group.

        Examples
        --------

        >>> df = ps.DataFrame({
        ...     'a': [1, 1, 1, 2, 2, 2, 3, 3, 3],
        ...     'b': [1, 2, 2, 2, 3, 3, 3, 4, 4]}, columns=['a', 'b'])
        >>> df
           a  b
        0  1  1
        1  1  2
        2  1  2
        3  2  2
        4  2  3
        5  2  3
        6  3  3
        7  3  4
        8  3  4

        >>> df.groupby('a').shift().sort_index()  # doctest: +SKIP
             b
        0  NaN
        1  1.0
        2  2.0
        3  NaN
        4  2.0
        5  3.0
        6  NaN
        7  3.0
        8  4.0

        >>> df.groupby('a').shift(periods=-1, fill_value=0).sort_index()  # doctest: +SKIP
           b
        0  2
        1  2
        2  0
        3  3
        4  3
        5  0
        6  4
        7  4
        8  0
        """
        return self._apply_series_op(
            lambda sg: sg._psser._shift(periods, fill_value, part_cols=sg._groupkeys_scols),
            should_resolve=True,
        )

    def transform(self, func: Callable[..., pd.Series], *args: Any, **kwargs: Any) -> FrameLike:
        """
        Apply function column-by-column to the GroupBy object.

        The function passed to `transform` must take a Series as its first
        argument and return a Series. The given function is executed for
        each series in each grouped data.

        While `transform` is a very flexible method, its downside is that
        using it can be quite a bit slower than using more specific methods
        like `agg` or `transform`. pandas-on-Spark offers a wide range of method that will
        be much faster than using `transform` for their specific purposes, so try to
        use them before reaching for `transform`.

        .. note:: this API executes the function once to infer the type which is
             potentially expensive, for instance, when the dataset is created after
             aggregations or sorting.

             To avoid this, specify return type in ``func``, for instance, as below:

             >>> def convert_to_string(x) -> ps.Series[str]:
             ...     return x.apply("a string {}".format)

            When the given function has the return type annotated, the original index of the
            GroupBy object will be lost and a default index will be attached to the result.
            Please be careful about configuring the default index. See also `Default Index Type
            <https://koalas.readthedocs.io/en/latest/user_guide/options.html#default-index-type>`_.

        .. note:: the series within ``func`` is actually a pandas series. Therefore,
            any pandas API within this function is allowed.


        Parameters
        ----------
        func : callable
            A callable that takes a Series as its first argument, and
            returns a Series.
        *args
            Positional arguments to pass to func.
        **kwargs
            Keyword arguments to pass to func.

        Returns
        -------
        applied : DataFrame

        See Also
        --------
        aggregate : Apply aggregate function to the GroupBy object.
        Series.apply : Apply a function to a Series.

        Examples
        --------

        >>> df = ps.DataFrame({'A': [0, 0, 1],
        ...                    'B': [1, 2, 3],
        ...                    'C': [4, 6, 5]}, columns=['A', 'B', 'C'])

        >>> g = df.groupby('A')

        Notice that ``g`` has two groups, ``0`` and ``1``.
        Calling `transform` in various ways, we can get different grouping results:
        Below the functions passed to `transform` takes a Series as
        its argument and returns a Series. `transform` applies the function on each series
        in each grouped data, and combine them into a new DataFrame:

        >>> def convert_to_string(x) -> ps.Series[str]:
        ...     return x.apply("a string {}".format)
        >>> g.transform(convert_to_string)  # doctest: +NORMALIZE_WHITESPACE
                    B           C
        0  a string 1  a string 4
        1  a string 2  a string 6
        2  a string 3  a string 5

        >>> def plus_max(x) -> ps.Series[np.int]:
        ...     return x + x.max()
        >>> g.transform(plus_max)  # doctest: +NORMALIZE_WHITESPACE
           B   C
        0  3  10
        1  4  12
        2  6  10

        You can omit the type hint and let pandas-on-Spark infer its type.

        >>> def plus_min(x):
        ...     return x + x.min()
        >>> g.transform(plus_min)  # doctest: +NORMALIZE_WHITESPACE
           B   C
        0  2   8
        1  3  10
        2  6  10

        In case of Series, it works as below.

        >>> df.B.groupby(df.A).transform(plus_max)
        0    3
        1    4
        2    6
        Name: B, dtype: int64

        >>> (df * -1).B.groupby(df.A).transform(abs)
        0    1
        1    2
        2    3
        Name: B, dtype: int64

        You can also specify extra arguments to pass to the function.

        >>> def calculation(x, y, z) -> ps.Series[np.int]:
        ...     return x + x.min() + y + z
        >>> g.transform(calculation, 5, z=20)  # doctest: +NORMALIZE_WHITESPACE
            B   C
        0  27  33
        1  28  35
        2  31  35
        """
        if not callable(func):
            raise TypeError("%s object is not callable" % type(func).__name__)

        spec = inspect.getfullargspec(func)
        return_sig = spec.annotations.get("return", None)

        psdf, groupkey_labels, groupkey_names = GroupBy._prepare_group_map_apply(
            self._psdf, self._groupkeys, agg_columns=self._agg_columns
        )

        def pandas_transform(pdf: pd.DataFrame) -> pd.DataFrame:
            return pdf.groupby(groupkey_names).transform(func, *args, **kwargs)

        should_infer_schema = return_sig is None

        if should_infer_schema:
            # Here we execute with the first 1000 to get the return type.
            # If the records were less than 1000, it uses pandas API directly for a shortcut.
            log_advice(
                "If the type hints is not specified for `grouby.transform`, "
                "it is expensive to infer the data type internally."
            )
            limit = get_option("compute.shortcut_limit")
            pdf = psdf.head(limit + 1)._to_internal_pandas()
            pdf = pdf.groupby(groupkey_names).transform(func, *args, **kwargs)
            psdf_from_pandas: DataFrame = DataFrame(pdf)
            return_schema = force_decimal_precision_scale(
                as_nullable_spark_type(
                    psdf_from_pandas._internal.spark_frame.drop(*HIDDEN_COLUMNS).schema
                )
            )
            if len(pdf) <= limit:
                return self._cleanup_and_return(psdf_from_pandas)

            sdf = GroupBy._spark_group_map_apply(
                psdf,
                pandas_transform,
                [psdf._internal.spark_column_for(label) for label in groupkey_labels],
                return_schema,
                retain_index=True,
            )
            # If schema is inferred, we can restore indexes too.
            internal = psdf_from_pandas._internal.with_new_sdf(
                sdf,
                index_fields=[
                    field.copy(nullable=True) for field in psdf_from_pandas._internal.index_fields
                ],
                data_fields=[
                    field.copy(nullable=True) for field in psdf_from_pandas._internal.data_fields
                ],
            )
        else:
            return_type = infer_return_type(func)
            if not isinstance(return_type, SeriesType):
                raise TypeError(
                    "Expected the return type of this function to be of Series type, "
                    "but found type {}".format(return_type)
                )

            dtype = cast(SeriesType, return_type).dtype
            spark_type = cast(SeriesType, return_type).spark_type

            data_fields = [
                InternalField(dtype=dtype, struct_field=StructField(name=c, dataType=spark_type))
                for c in psdf._internal.data_spark_column_names
                if c not in groupkey_names
            ]

            return_schema = StructType([field.struct_field for field in data_fields])

            sdf = GroupBy._spark_group_map_apply(
                psdf,
                pandas_transform,
                [psdf._internal.spark_column_for(label) for label in groupkey_labels],
                return_schema,
                retain_index=False,
            )
            # Otherwise, it loses index.
            internal = InternalFrame(
                spark_frame=sdf, index_spark_columns=None, data_fields=data_fields
            )

        return self._cleanup_and_return(DataFrame(internal))

    def nunique(self, dropna: bool = True) -> FrameLike:
        """
        Return DataFrame with number of distinct observations per group for each column.

        Parameters
        ----------
        dropna : boolean, default True
            Don’t include NaN in the counts.

        Returns
        -------
        nunique : DataFrame or Series

        Examples
        --------

        >>> df = ps.DataFrame({'id': ['spam', 'egg', 'egg', 'spam',
        ...                           'ham', 'ham'],
        ...                    'value1': [1, 5, 5, 2, 5, 5],
        ...                    'value2': list('abbaxy')}, columns=['id', 'value1', 'value2'])
        >>> df
             id  value1 value2
        0  spam       1      a
        1   egg       5      b
        2   egg       5      b
        3  spam       2      a
        4   ham       5      x
        5   ham       5      y

        >>> df.groupby('id').nunique().sort_index() # doctest: +SKIP
              value1  value2
        id
        egg        1       1
        ham        1       2
        spam       2       1

        >>> df.groupby('id')['value1'].nunique().sort_index() # doctest: +NORMALIZE_WHITESPACE
        id
        egg     1
        ham     1
        spam    2
        Name: value1, dtype: int64
        """
        if dropna:
            stat_function = lambda col: F.countDistinct(col)
        else:
            stat_function = lambda col: (
                F.countDistinct(col)
                + F.when(F.count(F.when(col.isNull(), 1).otherwise(None)) >= 1, 1).otherwise(0)
            )

        return self._reduce_for_stat_function(stat_function, only_numeric=False)

    def rolling(
        self, window: int, min_periods: Optional[int] = None
    ) -> "RollingGroupby[FrameLike]":
        """
        Return an rolling grouper, providing rolling
        functionality per group.

        .. note:: 'min_periods' in pandas-on-Spark works as a fixed window size unlike pandas.
        Unlike pandas, NA is also counted as the period. This might be changed
        in the near future.

        Parameters
        ----------
        window : int, or offset
            Size of the moving window.
            This is the number of observations used for calculating the statistic.
            Each window will be a fixed size.

        min_periods : int, default 1
            Minimum number of observations in window required to have a value
            (otherwise result is NA).

        See Also
        --------
        Series.groupby
        DataFrame.groupby
        """
        from pyspark.pandas.window import RollingGroupby

        return RollingGroupby(self, window, min_periods=min_periods)

    def expanding(self, min_periods: int = 1) -> "ExpandingGroupby[FrameLike]":
        """
        Return an expanding grouper, providing expanding
        functionality per group.

        .. note:: 'min_periods' in pandas-on-Spark works as a fixed window size unlike pandas.
        Unlike pandas, NA is also counted as the period. This might be changed
        in the near future.

        Parameters
        ----------
        min_periods : int, default 1
            Minimum number of observations in window required to have a value
            (otherwise result is NA).

        See Also
        --------
        Series.groupby
        DataFrame.groupby
        """
        from pyspark.pandas.window import ExpandingGroupby

        return ExpandingGroupby(self, min_periods=min_periods)

    def get_group(self, name: Union[Name, List[Name]]) -> FrameLike:
        """
        Construct DataFrame from group with provided name.

        Parameters
        ----------
        name : object
            The name of the group to get as a DataFrame.

        Returns
        -------
        group : same type as obj

        Examples
        --------
        >>> psdf = ps.DataFrame([('falcon', 'bird', 389.0),
        ...                     ('parrot', 'bird', 24.0),
        ...                     ('lion', 'mammal', 80.5),
        ...                     ('monkey', 'mammal', np.nan)],
        ...                    columns=['name', 'class', 'max_speed'],
        ...                    index=[0, 2, 3, 1])
        >>> psdf
             name   class  max_speed
        0  falcon    bird      389.0
        2  parrot    bird       24.0
        3    lion  mammal       80.5
        1  monkey  mammal        NaN

        >>> psdf.groupby("class").get_group("bird").sort_index()
             name class  max_speed
        0  falcon  bird      389.0
        2  parrot  bird       24.0

        >>> psdf.groupby("class").get_group("mammal").sort_index()
             name   class  max_speed
        1  monkey  mammal        NaN
        3    lion  mammal       80.5
        """
        groupkeys = self._groupkeys
        if not is_hashable(name):
            raise TypeError("unhashable type: '{}'".format(type(name).__name__))
        elif len(groupkeys) > 1:
            if not isinstance(name, tuple):
                raise ValueError("must supply a tuple to get_group with multiple grouping keys")
            if len(groupkeys) != len(name):
                raise ValueError(
                    "must supply a same-length tuple to get_group with multiple grouping keys"
                )
        if not is_list_like(name):
            name = [name]
        cond = SF.lit(True)
        for groupkey, item in zip(groupkeys, name):
            scol = groupkey.spark.column
            cond = cond & (scol == item)
        if self._agg_columns_selected:
            internal = self._psdf._internal
            spark_frame = internal.spark_frame.select(
                internal.index_spark_columns + self._agg_columns_scols
            ).filter(cond)

            internal = internal.copy(
                spark_frame=spark_frame,
                index_spark_columns=[
                    scol_for(spark_frame, col) for col in internal.index_spark_column_names
                ],
                column_labels=[s._column_label for s in self._agg_columns],
                data_spark_columns=[
                    scol_for(spark_frame, s._internal.data_spark_column_names[0])
                    for s in self._agg_columns
                ],
                data_fields=[s._internal.data_fields[0] for s in self._agg_columns],
            )
        else:
            internal = self._psdf._internal.with_filter(cond)
        if internal.spark_frame.head() is None:
            raise KeyError(name)

        return self._cleanup_and_return(DataFrame(internal))

    def median(self, numeric_only: bool = True, accuracy: int = 10000) -> FrameLike:
        """
        Compute median of groups, excluding missing values.

        For multiple groupings, the result index will be a MultiIndex

        .. note:: Unlike pandas', the median in pandas-on-Spark is an approximated median based upon
            approximate percentile computation because computing median across a large dataset
            is extremely expensive.

        Parameters
        ----------
        numeric_only : bool, default True
            Include only float, int, boolean columns. False is not supported. This parameter
            is mainly for pandas compatibility.

        Returns
        -------
        Series or DataFrame
            Median of values within each group.

        Examples
        --------
        >>> psdf = ps.DataFrame({'a': [1., 1., 1., 1., 2., 2., 2., 3., 3., 3.],
        ...                     'b': [2., 3., 1., 4., 6., 9., 8., 10., 7., 5.],
        ...                     'c': [3., 5., 2., 5., 1., 2., 6., 4., 3., 6.]},
        ...                    columns=['a', 'b', 'c'],
        ...                    index=[7, 2, 4, 1, 3, 4, 9, 10, 5, 6])
        >>> psdf
              a     b    c
        7   1.0   2.0  3.0
        2   1.0   3.0  5.0
        4   1.0   1.0  2.0
        1   1.0   4.0  5.0
        3   2.0   6.0  1.0
        4   2.0   9.0  2.0
        9   2.0   8.0  6.0
        10  3.0  10.0  4.0
        5   3.0   7.0  3.0
        6   3.0   5.0  6.0

        DataFrameGroupBy

        >>> psdf.groupby('a').median().sort_index()  # doctest: +NORMALIZE_WHITESPACE
               b    c
        a
        1.0  2.0  3.0
        2.0  8.0  2.0
        3.0  7.0  4.0

        SeriesGroupBy

        >>> psdf.groupby('a')['b'].median().sort_index()
        a
        1.0    2.0
        2.0    8.0
        3.0    7.0
        Name: b, dtype: float64
        """
        if not isinstance(accuracy, int):
            raise TypeError(
                "accuracy must be an integer; however, got [%s]" % type(accuracy).__name__
            )

        stat_function = lambda col: F.percentile_approx(col, 0.5, accuracy)
        return self._reduce_for_stat_function(stat_function, only_numeric=numeric_only)

    def _reduce_for_stat_function(
        self, sfun: Callable[[Column], Column], only_numeric: bool
    ) -> FrameLike:
        groupkey_names = [SPARK_INDEX_NAME_FORMAT(i) for i in range(len(self._groupkeys))]
        groupkey_scols = [s.alias(name) for s, name in zip(self._groupkeys_scols, groupkey_names)]

        agg_columns = [
            psser
            for psser in self._agg_columns
            if isinstance(psser.spark.data_type, NumericType) or not only_numeric
        ]

        sdf = self._psdf._internal.spark_frame.select(
            *groupkey_scols, *[psser.spark.column for psser in agg_columns]
        )

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in groupkey_names],
            index_names=[psser._column_label for psser in self._groupkeys],
            index_fields=[
                psser._internal.data_fields[0].copy(name=name)
                for psser, name in zip(self._groupkeys, groupkey_names)
            ],
            data_spark_columns=[
                scol_for(sdf, psser._internal.data_spark_column_names[0]) for psser in agg_columns
            ],
            column_labels=[psser._column_label for psser in agg_columns],
            data_fields=[psser._internal.data_fields[0] for psser in agg_columns],
            column_label_names=self._psdf._internal.column_label_names,
        )
        psdf: DataFrame = DataFrame(internal)

        if len(psdf._internal.column_labels) > 0:
            stat_exprs = []
            for label in psdf._internal.column_labels:
                psser = psdf._psser_for(label)
                stat_exprs.append(
                    sfun(psser._dtype_op.nan_to_null(psser).spark.column).alias(
                        psser._internal.data_spark_column_names[0]
                    )
                )
            sdf = sdf.groupby(*groupkey_names).agg(*stat_exprs)
        else:
            sdf = sdf.select(*groupkey_names).distinct()

        internal = internal.copy(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in groupkey_names],
            data_spark_columns=[scol_for(sdf, col) for col in internal.data_spark_column_names],
            data_fields=None,
        )
        psdf = DataFrame(internal)

        if self._dropna:
            psdf = DataFrame(
                psdf._internal.with_new_sdf(
                    psdf._internal.spark_frame.dropna(
                        subset=psdf._internal.index_spark_column_names
                    )
                )
            )

        if not self._as_index:
            should_drop_index = set(
                i for i, gkey in enumerate(self._groupkeys) if gkey._psdf is not self._psdf
            )
            if len(should_drop_index) > 0:
                psdf = psdf.reset_index(level=should_drop_index, drop=True)
            if len(should_drop_index) < len(self._groupkeys):
                psdf = psdf.reset_index()
        return self._cleanup_and_return(psdf)

    @staticmethod
    def _resolve_grouping_from_diff_dataframes(
        psdf: DataFrame, by: List[Union[Series, Label]]
    ) -> Tuple[DataFrame, List[Series], Set[Label]]:
        column_labels_level = psdf._internal.column_labels_level

        column_labels = []
        additional_pssers = []
        additional_column_labels = []
        tmp_column_labels = set()
        for i, col_or_s in enumerate(by):
            if isinstance(col_or_s, Series):
                if col_or_s._psdf is psdf:
                    column_labels.append(col_or_s._column_label)
                elif same_anchor(col_or_s, psdf):
                    temp_label = verify_temp_column_name(psdf, "__tmp_groupkey_{}__".format(i))
                    column_labels.append(temp_label)
                    additional_pssers.append(col_or_s.rename(temp_label))
                    additional_column_labels.append(temp_label)
                else:
                    temp_label = verify_temp_column_name(
                        psdf,
                        tuple(
                            ([""] * (column_labels_level - 1)) + ["__tmp_groupkey_{}__".format(i)]
                        ),
                    )
                    column_labels.append(temp_label)
                    tmp_column_labels.add(temp_label)
            elif isinstance(col_or_s, tuple):
                psser = psdf[col_or_s]
                if not isinstance(psser, Series):
                    raise ValueError(name_like_string(col_or_s))
                column_labels.append(col_or_s)
            else:
                raise ValueError(col_or_s)

        psdf = DataFrame(
            psdf._internal.with_new_columns(
                [psdf._psser_for(label) for label in psdf._internal.column_labels]
                + additional_pssers
            )
        )

        def assign_columns(
            psdf: DataFrame, this_column_labels: List[Label], that_column_labels: List[Label]
        ) -> Iterator[Tuple[Series, Label]]:
            raise NotImplementedError(
                "Duplicated labels with groupby() and "
                "'compute.ops_on_diff_frames' option are not supported currently "
                "Please use unique labels in series and frames."
            )

        for col_or_s, label in zip(by, column_labels):
            if label in tmp_column_labels:
                psser = col_or_s
                psdf = align_diff_frames(
                    assign_columns,
                    psdf,
                    psser.rename(label),
                    fillna=False,
                    how="inner",
                    preserve_order_column=True,
                )

        tmp_column_labels |= set(additional_column_labels)

        new_by_series = []
        for col_or_s, label in zip(by, column_labels):
            if label in tmp_column_labels:
                psser = col_or_s
                new_by_series.append(psdf._psser_for(label).rename(psser.name))
            else:
                new_by_series.append(psdf._psser_for(label))

        return psdf, new_by_series, tmp_column_labels

    @staticmethod
    def _resolve_grouping(psdf: DataFrame, by: List[Union[Series, Label]]) -> List[Series]:
        new_by_series = []
        for col_or_s in by:
            if isinstance(col_or_s, Series):
                new_by_series.append(col_or_s)
            elif isinstance(col_or_s, tuple):
                psser = psdf[col_or_s]
                if not isinstance(psser, Series):
                    raise ValueError(name_like_string(col_or_s))
                new_by_series.append(psser)
            else:
                raise ValueError(col_or_s)
        return new_by_series


class DataFrameGroupBy(GroupBy[DataFrame]):
    @staticmethod
    def _build(
        psdf: DataFrame, by: List[Union[Series, Label]], as_index: bool, dropna: bool
    ) -> "DataFrameGroupBy":
        if any(isinstance(col_or_s, Series) and not same_anchor(psdf, col_or_s) for col_or_s in by):
            (
                psdf,
                new_by_series,
                column_labels_to_exclude,
            ) = GroupBy._resolve_grouping_from_diff_dataframes(psdf, by)
        else:
            new_by_series = GroupBy._resolve_grouping(psdf, by)
            column_labels_to_exclude = set()
        return DataFrameGroupBy(
            psdf,
            new_by_series,
            as_index=as_index,
            dropna=dropna,
            column_labels_to_exclude=column_labels_to_exclude,
        )

    def __init__(
        self,
        psdf: DataFrame,
        by: List[Series],
        as_index: bool,
        dropna: bool,
        column_labels_to_exclude: Set[Label],
        agg_columns: List[Label] = None,
    ):
        agg_columns_selected = agg_columns is not None
        if agg_columns_selected:
            for label in agg_columns:
                if label in column_labels_to_exclude:
                    raise KeyError(label)
        else:
            agg_columns = [
                label
                for label in psdf._internal.column_labels
                if not any(label == key._column_label and key._psdf is psdf for key in by)
                and label not in column_labels_to_exclude
            ]

        super().__init__(
            psdf=psdf,
            groupkeys=by,
            as_index=as_index,
            dropna=dropna,
            column_labels_to_exclude=column_labels_to_exclude,
            agg_columns_selected=agg_columns_selected,
            agg_columns=[psdf[label] for label in agg_columns],
        )

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeDataFrameGroupBy, item):
            property_or_func = getattr(MissingPandasLikeDataFrameGroupBy, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        return self.__getitem__(item)

    def __getitem__(self, item: Any) -> GroupBy:
        if self._as_index and is_name_like_value(item):
            return SeriesGroupBy(
                self._psdf._psser_for(item if is_name_like_tuple(item) else (item,)),
                self._groupkeys,
                dropna=self._dropna,
            )
        else:
            if is_name_like_tuple(item):
                item = [item]
            elif is_name_like_value(item):
                item = [(item,)]
            else:
                item = [i if is_name_like_tuple(i) else (i,) for i in item]
            if not self._as_index:
                groupkey_names = set(key._column_label for key in self._groupkeys)
                for name in item:
                    if name in groupkey_names:
                        raise ValueError(
                            "cannot insert {}, already exists".format(name_like_string(name))
                        )
            return DataFrameGroupBy(
                self._psdf,
                self._groupkeys,
                as_index=self._as_index,
                dropna=self._dropna,
                column_labels_to_exclude=self._column_labels_to_exclude,
                agg_columns=item,
            )

    def _apply_series_op(
        self,
        op: Callable[["SeriesGroupBy"], Series],
        should_resolve: bool = False,
        numeric_only: bool = False,
    ) -> DataFrame:
        applied = []
        for column in self._agg_columns:
            applied.append(op(cast(SeriesGroupBy, column.groupby(self._groupkeys))))
        if numeric_only:
            applied = [col for col in applied if isinstance(col.spark.data_type, NumericType)]
            if not applied:
                raise DataError("No numeric types to aggregate")
        internal = self._psdf._internal.with_new_columns(applied, keep_order=False)
        if should_resolve:
            internal = internal.resolved_copy
        return DataFrame(internal)

    def _cleanup_and_return(self, psdf: DataFrame) -> DataFrame:
        return psdf

    # TODO: Implement 'percentiles', 'include', and 'exclude' arguments.
    # TODO: Add ``DataFrame.select_dtypes`` to See Also when 'include'
    #   and 'exclude' arguments are implemented.
    def describe(self) -> DataFrame:
        """
        Generate descriptive statistics that summarize the central tendency,
        dispersion and shape of a dataset's distribution, excluding
        ``NaN`` values.

        Analyzes both numeric and object series, as well
        as ``DataFrame`` column sets of mixed data types. The output
        will vary depending on what is provided. Refer to the notes
        below for more detail.

        .. note:: Unlike pandas, the percentiles in pandas-on-Spark are based upon
            approximate percentile computation because computing percentiles
            across a large dataset is extremely expensive.

        Returns
        -------
        DataFrame
            Summary statistics of the DataFrame provided.

        See Also
        --------
        DataFrame.count
        DataFrame.max
        DataFrame.min
        DataFrame.mean
        DataFrame.std

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
        >>> df
           a  b  c
        0  1  4  7
        1  1  5  8
        2  3  6  9

        Describing a ``DataFrame``. By default only numeric fields
        are returned.

        >>> described = df.groupby('a').describe()
        >>> described.sort_index()  # doctest: +NORMALIZE_WHITESPACE
              b                                        c
          count mean       std min 25% 50% 75% max count mean       std min 25% 50% 75% max
        a
        1   2.0  4.5  0.707107 4.0 4.0 4.0 5.0 5.0   2.0  7.5  0.707107 7.0 7.0 7.0 8.0 8.0
        3   1.0  6.0       NaN 6.0 6.0 6.0 6.0 6.0   1.0  9.0       NaN 9.0 9.0 9.0 9.0 9.0

        """
        for col in self._agg_columns:
            if isinstance(col.spark.data_type, StringType):
                raise NotImplementedError(
                    "DataFrameGroupBy.describe() doesn't support for string type for now"
                )

        psdf = self.aggregate(["count", "mean", "std", "min", "quartiles", "max"])
        sdf = psdf._internal.spark_frame
        agg_column_labels = [col._column_label for col in self._agg_columns]
        formatted_percentiles = ["25%", "50%", "75%"]

        # Split "quartiles" columns into first, second, and third quartiles.
        for label in agg_column_labels:
            quartiles_col = name_like_string(tuple(list(label) + ["quartiles"]))
            for i, percentile in enumerate(formatted_percentiles):
                sdf = sdf.withColumn(
                    name_like_string(tuple(list(label) + [percentile])),
                    scol_for(sdf, quartiles_col)[i],
                )
            sdf = sdf.drop(quartiles_col)

        # Reorder columns lexicographically by agg column followed by stats.
        stats = ["count", "mean", "std", "min"] + formatted_percentiles + ["max"]
        column_labels = [tuple(list(label) + [s]) for label, s in product(agg_column_labels, stats)]
        data_columns = map(name_like_string, column_labels)

        # Reindex the DataFrame to reflect initial grouping and agg columns.
        internal = psdf._internal.copy(
            spark_frame=sdf,
            column_labels=column_labels,
            data_spark_columns=[scol_for(sdf, col) for col in data_columns],
            data_fields=None,
        )

        # Cast columns to ``"float64"`` to match `pandas.DataFrame.groupby`.
        return DataFrame(internal).astype("float64")


class SeriesGroupBy(GroupBy[Series]):
    @staticmethod
    def _build(
        psser: Series, by: List[Union[Series, Label]], as_index: bool, dropna: bool
    ) -> "SeriesGroupBy":
        if any(
            isinstance(col_or_s, Series) and not same_anchor(psser, col_or_s) for col_or_s in by
        ):
            psdf, new_by_series, _ = GroupBy._resolve_grouping_from_diff_dataframes(
                psser.to_frame(), by
            )
            return SeriesGroupBy(
                first_series(psdf).rename(psser.name),
                new_by_series,
                as_index=as_index,
                dropna=dropna,
            )
        else:
            new_by_series = GroupBy._resolve_grouping(psser._psdf, by)
            return SeriesGroupBy(psser, new_by_series, as_index=as_index, dropna=dropna)

    def __init__(self, psser: Series, by: List[Series], as_index: bool = True, dropna: bool = True):
        if not as_index:
            raise TypeError("as_index=False only valid with DataFrame")
        super().__init__(
            psdf=psser._psdf,
            groupkeys=by,
            as_index=True,
            dropna=dropna,
            column_labels_to_exclude=set(),
            agg_columns_selected=True,
            agg_columns=[psser],
        )
        self._psser = psser

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeSeriesGroupBy, item):
            property_or_func = getattr(MissingPandasLikeSeriesGroupBy, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        raise AttributeError(item)

    def _apply_series_op(
        self,
        op: Callable[["SeriesGroupBy"], Series],
        should_resolve: bool = False,
        numeric_only: bool = False,
    ) -> Series:
        if numeric_only and not isinstance(self._agg_columns[0].spark.data_type, NumericType):
            raise DataError("No numeric types to aggregate")
        psser = op(self)
        if should_resolve:
            internal = psser._internal.resolved_copy
            return first_series(DataFrame(internal))
        else:
            return psser.copy()

    def _cleanup_and_return(self, psdf: DataFrame) -> Series:
        return first_series(psdf).rename().rename(self._psser.name)

    def agg(self, *args: Any, **kwargs: Any) -> None:
        return MissingPandasLikeSeriesGroupBy.agg(self, *args, **kwargs)

    def aggregate(self, *args: Any, **kwargs: Any) -> None:
        return MissingPandasLikeSeriesGroupBy.aggregate(self, *args, **kwargs)

    def size(self) -> Series:
        return super().size().rename(self._psser.name)

    size.__doc__ = GroupBy.size.__doc__

    # TODO: add keep parameter
    def nsmallest(self, n: int = 5) -> Series:
        """
        Return the smallest `n` elements.

        Parameters
        ----------
        n : int
            Number of items to retrieve.

        See Also
        --------
        pyspark.pandas.Series.nsmallest
        pyspark.pandas.DataFrame.nsmallest

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 1, 2, 2, 2, 3, 3, 3],
        ...                    'b': [1, 2, 2, 2, 3, 3, 3, 4, 4]}, columns=['a', 'b'])

        >>> df.groupby(['a'])['b'].nsmallest(1).sort_index()  # doctest: +NORMALIZE_WHITESPACE
        a
        1  0    1
        2  3    2
        3  6    3
        Name: b, dtype: int64
        """
        if self._psser._internal.index_level > 1:
            raise ValueError("nsmallest do not support multi-index now")

        groupkey_col_names = [SPARK_INDEX_NAME_FORMAT(i) for i in range(len(self._groupkeys))]
        sdf = self._psser._internal.spark_frame.select(
            *[scol.alias(name) for scol, name in zip(self._groupkeys_scols, groupkey_col_names)],
            *[
                scol.alias(SPARK_INDEX_NAME_FORMAT(i + len(self._groupkeys)))
                for i, scol in enumerate(self._psser._internal.index_spark_columns)
            ],
            self._psser.spark.column,
            NATURAL_ORDER_COLUMN_NAME,
        )

        window = Window.partitionBy(*groupkey_col_names).orderBy(
            scol_for(sdf, self._psser._internal.data_spark_column_names[0]).asc(),
            NATURAL_ORDER_COLUMN_NAME,
        )

        temp_rank_column = verify_temp_column_name(sdf, "__rank__")
        sdf = (
            sdf.withColumn(temp_rank_column, F.row_number().over(window))
            .filter(F.col(temp_rank_column) <= n)
            .drop(temp_rank_column)
        ).drop(NATURAL_ORDER_COLUMN_NAME)

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=(
                [scol_for(sdf, col) for col in groupkey_col_names]
                + [
                    scol_for(sdf, SPARK_INDEX_NAME_FORMAT(i + len(self._groupkeys)))
                    for i in range(self._psdf._internal.index_level)
                ]
            ),
            index_names=(
                [psser._column_label for psser in self._groupkeys]
                + self._psdf._internal.index_names
            ),
            index_fields=(
                [
                    psser._internal.data_fields[0].copy(name=name)
                    for psser, name in zip(self._groupkeys, groupkey_col_names)
                ]
                + [
                    field.copy(name=SPARK_INDEX_NAME_FORMAT(i + len(self._groupkeys)))
                    for i, field in enumerate(self._psdf._internal.index_fields)
                ]
            ),
            column_labels=[self._psser._column_label],
            data_spark_columns=[scol_for(sdf, self._psser._internal.data_spark_column_names[0])],
            data_fields=[self._psser._internal.data_fields[0]],
        )
        return first_series(DataFrame(internal))

    # TODO: add keep parameter
    def nlargest(self, n: int = 5) -> Series:
        """
        Return the first n rows ordered by columns in descending order in group.

        Return the first n rows with the smallest values in columns, in descending order.
        The columns that are not specified are returned as well, but not used for ordering.

        Parameters
        ----------
        n : int
            Number of items to retrieve.

        See Also
        --------
        pyspark.pandas.Series.nlargest
        pyspark.pandas.DataFrame.nlargest

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 1, 2, 2, 2, 3, 3, 3],
        ...                    'b': [1, 2, 2, 2, 3, 3, 3, 4, 4]}, columns=['a', 'b'])

        >>> df.groupby(['a'])['b'].nlargest(1).sort_index()  # doctest: +NORMALIZE_WHITESPACE
        a
        1  1    2
        2  4    3
        3  7    4
        Name: b, dtype: int64
        """
        if self._psser._internal.index_level > 1:
            raise ValueError("nlargest do not support multi-index now")

        groupkey_col_names = [SPARK_INDEX_NAME_FORMAT(i) for i in range(len(self._groupkeys))]
        sdf = self._psser._internal.spark_frame.select(
            *[scol.alias(name) for scol, name in zip(self._groupkeys_scols, groupkey_col_names)],
            *[
                scol.alias(SPARK_INDEX_NAME_FORMAT(i + len(self._groupkeys)))
                for i, scol in enumerate(self._psser._internal.index_spark_columns)
            ],
            self._psser.spark.column,
            NATURAL_ORDER_COLUMN_NAME,
        )

        window = Window.partitionBy(*groupkey_col_names).orderBy(
            scol_for(sdf, self._psser._internal.data_spark_column_names[0]).desc(),
            NATURAL_ORDER_COLUMN_NAME,
        )

        temp_rank_column = verify_temp_column_name(sdf, "__rank__")
        sdf = (
            sdf.withColumn(temp_rank_column, F.row_number().over(window))
            .filter(F.col(temp_rank_column) <= n)
            .drop(temp_rank_column)
        ).drop(NATURAL_ORDER_COLUMN_NAME)

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=(
                [scol_for(sdf, col) for col in groupkey_col_names]
                + [
                    scol_for(sdf, SPARK_INDEX_NAME_FORMAT(i + len(self._groupkeys)))
                    for i in range(self._psdf._internal.index_level)
                ]
            ),
            index_names=(
                [psser._column_label for psser in self._groupkeys]
                + self._psdf._internal.index_names
            ),
            index_fields=(
                [
                    psser._internal.data_fields[0].copy(name=name)
                    for psser, name in zip(self._groupkeys, groupkey_col_names)
                ]
                + [
                    field.copy(name=SPARK_INDEX_NAME_FORMAT(i + len(self._groupkeys)))
                    for i, field in enumerate(self._psdf._internal.index_fields)
                ]
            ),
            column_labels=[self._psser._column_label],
            data_spark_columns=[scol_for(sdf, self._psser._internal.data_spark_column_names[0])],
            data_fields=[self._psser._internal.data_fields[0]],
        )
        return first_series(DataFrame(internal))

    # TODO: add bins, normalize parameter
    def value_counts(
        self, sort: Optional[bool] = None, ascending: Optional[bool] = None, dropna: bool = True
    ) -> Series:
        """
        Compute group sizes.

        Parameters
        ----------
        sort : boolean, default None
            Sort by frequencies.
        ascending : boolean, default False
            Sort in ascending order.
        dropna : boolean, default True
            Don't include counts of NaN.

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> df = ps.DataFrame({'A': [1, 2, 2, 3, 3, 3],
        ...                    'B': [1, 1, 2, 3, 3, 3]},
        ...                   columns=['A', 'B'])
        >>> df
           A  B
        0  1  1
        1  2  1
        2  2  2
        3  3  3
        4  3  3
        5  3  3

        >>> df.groupby('A')['B'].value_counts().sort_index()  # doctest: +NORMALIZE_WHITESPACE
        A  B
        1  1    1
        2  1    1
           2    1
        3  3    3
        Name: B, dtype: int64
        """
        groupkeys = self._groupkeys + self._agg_columns
        groupkey_names = [SPARK_INDEX_NAME_FORMAT(i) for i in range(len(groupkeys))]
        groupkey_cols = [s.spark.column.alias(name) for s, name in zip(groupkeys, groupkey_names)]

        sdf = self._psdf._internal.spark_frame
        agg_column = self._agg_columns[0]._internal.data_spark_column_names[0]
        sdf = sdf.groupby(*groupkey_cols).count().withColumnRenamed("count", agg_column)

        if sort:
            if ascending:
                sdf = sdf.orderBy(scol_for(sdf, agg_column).asc())
            else:
                sdf = sdf.orderBy(scol_for(sdf, agg_column).desc())

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in groupkey_names],
            index_names=[psser._column_label for psser in groupkeys],
            index_fields=[
                psser._internal.data_fields[0].copy(name=name)
                for psser, name in zip(groupkeys, groupkey_names)
            ],
            column_labels=[self._agg_columns[0]._column_label],
            data_spark_columns=[scol_for(sdf, agg_column)],
        )
        return first_series(DataFrame(internal))

    def unique(self) -> Series:
        """
        Return unique values in group.

        Uniques are returned in order of unknown. It does NOT sort.

        See Also
        --------
        pyspark.pandas.Series.unique
        pyspark.pandas.Index.unique

        Examples
        --------
        >>> df = ps.DataFrame({'a': [1, 1, 1, 2, 2, 2, 3, 3, 3],
        ...                    'b': [1, 2, 2, 2, 3, 3, 3, 4, 4]}, columns=['a', 'b'])

        >>> df.groupby(['a'])['b'].unique().sort_index()  # doctest: +SKIP
        a
        1    [1, 2]
        2    [2, 3]
        3    [3, 4]
        Name: b, dtype: object
        """
        return self._reduce_for_stat_function(F.collect_set, only_numeric=False)


def is_multi_agg_with_relabel(**kwargs: Any) -> bool:
    """
    Check whether the kwargs pass to .agg look like multi-agg with relabling.

    Parameters
    ----------
    **kwargs : dict

    Returns
    -------
    bool

    Examples
    --------
    >>> is_multi_agg_with_relabel(a='max')
    False
    >>> is_multi_agg_with_relabel(a_max=('a', 'max'),
    ...                            a_min=('a', 'min'))
    True
    >>> is_multi_agg_with_relabel()
    False
    """
    if not kwargs:
        return False
    return all(isinstance(v, tuple) and len(v) == 2 for v in kwargs.values())


def normalize_keyword_aggregation(
    kwargs: Dict[str, Tuple[Name, str]],
) -> Tuple[Dict[Name, List[str]], List[str], List[Tuple]]:
    """
    Normalize user-provided kwargs.

    Transforms from the new ``Dict[str, NamedAgg]`` style kwargs
    to the old defaultdict[str, List[scalar]].

    Parameters
    ----------
    kwargs : dict

    Returns
    -------
    aggspec : dict
        The transformed kwargs.
    columns : List[str]
        The user-provided keys.
    order : List[Tuple[str, str]]
        Pairs of the input and output column names.

    Examples
    --------
    >>> normalize_keyword_aggregation({'output': ('input', 'sum')})
    (defaultdict(<class 'list'>, {'input': ['sum']}), ['output'], [('input', 'sum')])
    """
    aggspec: Dict[Union[Any, Tuple], List[str]] = defaultdict(list)
    order: List[Tuple] = []
    columns, pairs = zip(*kwargs.items())

    for column, aggfunc in pairs:
        if column in aggspec:
            aggspec[column].append(aggfunc)
        else:
            aggspec[column] = [aggfunc]

        order.append((column, aggfunc))
    # For MultiIndex, we need to flatten the tuple, e.g. (('y', 'A'), 'max') needs to be
    # flattened to ('y', 'A', 'max'), it won't do anything on normal Index.
    if isinstance(order[0][0], tuple):
        order = [(*levs, method) for levs, method in order]
    return aggspec, list(columns), order


def _test() -> None:
    import os
    import doctest
    import sys
    import numpy
    from pyspark.sql import SparkSession
    import pyspark.pandas.groupby

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.groupby.__dict__.copy()
    globs["np"] = numpy
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.groupby tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.groupby,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
