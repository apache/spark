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

from functools import partial, reduce
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union, cast, no_type_check

import pandas as pd
from pandas.api.types import is_hashable, is_list_like  # type: ignore[attr-defined]

from pyspark.sql import functions as F, Column, Window
from pyspark.sql.types import DataType

# For running doctests and reference resolution in PyCharm.
from pyspark import pandas as ps
from pyspark.pandas._typing import Label, Name, Scalar
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.indexes.base import Index
from pyspark.pandas.missing.indexes import MissingPandasLikeMultiIndex
from pyspark.pandas.series import Series, first_series
from pyspark.pandas.utils import (
    compare_disallow_null,
    is_name_like_tuple,
    name_like_string,
    scol_for,
    verify_temp_column_name,
    validate_index_loc,
)
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_INDEX_NAME_FORMAT,
)


class MultiIndex(Index):
    """
    pandas-on-Spark MultiIndex that corresponds to pandas MultiIndex logically. This might hold
    Spark Column internally.

    Parameters
    ----------
    levels : sequence of arrays
        The unique labels for each level.
    codes : sequence of arrays
        Integers for each level designating which label at each location.
    sortorder : optional int
        Level of sortedness (must be lexicographically sorted by that
        level).
    names : optional sequence of objects
        Names for each of the index levels. (name is accepted for compat).
    copy : bool, default False
        Copy the meta-data.
    verify_integrity : bool, default True
        Check that the levels/codes are consistent and valid.

    See Also
    --------
    MultiIndex.from_arrays  : Convert list of arrays to MultiIndex.
    MultiIndex.from_product : Create a MultiIndex from the cartesian product
                              of iterables.
    MultiIndex.from_tuples  : Convert list of tuples to a MultiIndex.
    MultiIndex.from_frame   : Make a MultiIndex from a DataFrame.
    Index : A single-level Index.

    Examples
    --------
    >>> ps.DataFrame({'a': ['a', 'b', 'c']}, index=[[1, 2, 3], [4, 5, 6]]).index  # doctest: +SKIP
    MultiIndex([(1, 4),
                (2, 5),
                (3, 6)],
               )

    >>> ps.DataFrame({'a': [1, 2, 3]}, index=[list('abc'), list('def')]).index  # doctest: +SKIP
    MultiIndex([('a', 'd'),
                ('b', 'e'),
                ('c', 'f')],
               )
    """

    @no_type_check
    def __new__(
        cls,
        levels=None,
        codes=None,
        sortorder=None,
        names=None,
        dtype=None,
        copy=False,
        name=None,
        verify_integrity: bool = True,
    ) -> "MultiIndex":
        pidx = pd.MultiIndex(
            levels=levels,
            codes=codes,
            sortorder=sortorder,
            names=names,
            dtype=dtype,
            copy=copy,
            name=name,
            verify_integrity=verify_integrity,
        )
        return ps.from_pandas(pidx)

    @property
    def _internal(self) -> InternalFrame:
        internal = self._psdf._internal
        scol = F.struct(*internal.index_spark_columns)
        return internal.copy(
            column_labels=[None],
            data_spark_columns=[scol],
            data_fields=[None],
            column_label_names=None,
        )

    @property
    def _column_label(self) -> Optional[Label]:
        return None

    def __abs__(self) -> "MultiIndex":
        raise TypeError("TypeError: cannot perform __abs__ with this index type: MultiIndex")

    def _with_new_scol(
        self, scol: Column, *, field: Optional[InternalField] = None
    ) -> "MultiIndex":
        raise NotImplementedError("Not supported for type MultiIndex")

    @no_type_check
    def any(self, *args, **kwargs) -> None:
        raise TypeError("cannot perform any with this index type: MultiIndex")

    @no_type_check
    def all(self, *args, **kwargs) -> None:
        raise TypeError("cannot perform all with this index type: MultiIndex")

    @staticmethod
    def from_tuples(
        tuples: List[Tuple],
        sortorder: Optional[int] = None,
        names: Optional[List[Name]] = None,
    ) -> "MultiIndex":
        """
        Convert list of tuples to MultiIndex.

        Parameters
        ----------
        tuples : list / sequence of tuple-likes
            Each tuple is the index of one row/column.
        sortorder : int or None
            Level of sortedness (must be lexicographically sorted by that level).
        names : list / sequence of str, optional
            Names for the levels in the index.

        Returns
        -------
        index : MultiIndex

        Examples
        --------

        >>> tuples = [(1, 'red'), (1, 'blue'),
        ...           (2, 'red'), (2, 'blue')]
        >>> ps.MultiIndex.from_tuples(tuples, names=('number', 'color'))  # doctest: +SKIP
        MultiIndex([(1,  'red'),
                    (1, 'blue'),
                    (2,  'red'),
                    (2, 'blue')],
                   names=['number', 'color'])
        """
        return cast(
            MultiIndex,
            ps.from_pandas(
                pd.MultiIndex.from_tuples(tuples=tuples, sortorder=sortorder, names=names)
            ),
        )

    @staticmethod
    def from_arrays(
        arrays: List[List],
        sortorder: Optional[int] = None,
        names: Optional[List[Name]] = None,
    ) -> "MultiIndex":
        """
        Convert arrays to MultiIndex.

        Parameters
        ----------
        arrays: list / sequence of array-likes
            Each array-like gives one level’s value for each data point. len(arrays)
            is the number of levels.
        sortorder: int or None
            Level of sortedness (must be lexicographically sorted by that level).
        names: list / sequence of str, optional
            Names for the levels in the index.

        Returns
        -------
        index: MultiIndex

        Examples
        --------

        >>> arrays = [[1, 1, 2, 2], ['red', 'blue', 'red', 'blue']]
        >>> ps.MultiIndex.from_arrays(arrays, names=('number', 'color'))  # doctest: +SKIP
        MultiIndex([(1,  'red'),
                    (1, 'blue'),
                    (2,  'red'),
                    (2, 'blue')],
                   names=['number', 'color'])
        """
        return cast(
            MultiIndex,
            ps.from_pandas(
                pd.MultiIndex.from_arrays(arrays=arrays, sortorder=sortorder, names=names)
            ),
        )

    @staticmethod
    def from_product(
        iterables: List[List],
        sortorder: Optional[int] = None,
        names: Optional[List[Name]] = None,
    ) -> "MultiIndex":
        """
        Make a MultiIndex from the cartesian product of multiple iterables.

        Parameters
        ----------
        iterables : list / sequence of iterables
            Each iterable has unique labels for each level of the index.
        sortorder : int or None
            Level of sortedness (must be lexicographically sorted by that
            level).
        names : list / sequence of str, optional
            Names for the levels in the index.

        Returns
        -------
        index : MultiIndex

        See Also
        --------
        MultiIndex.from_arrays : Convert list of arrays to MultiIndex.
        MultiIndex.from_tuples : Convert list of tuples to MultiIndex.

        Examples
        --------
        >>> numbers = [0, 1, 2]
        >>> colors = ['green', 'purple']
        >>> ps.MultiIndex.from_product([numbers, colors],
        ...                            names=['number', 'color'])  # doctest: +SKIP
        MultiIndex([(0,  'green'),
                    (0, 'purple'),
                    (1,  'green'),
                    (1, 'purple'),
                    (2,  'green'),
                    (2, 'purple')],
                   names=['number', 'color'])
        """
        return cast(
            MultiIndex,
            ps.from_pandas(
                pd.MultiIndex.from_product(iterables=iterables, sortorder=sortorder, names=names)
            ),
        )

    @staticmethod
    def from_frame(df: DataFrame, names: Optional[List[Name]] = None) -> "MultiIndex":
        """
        Make a MultiIndex from a DataFrame.

        Parameters
        ----------
        df : DataFrame
            DataFrame to be converted to MultiIndex.
        names : list-like, optional
            If no names are provided, use the column names, or tuple of column
            names if the column is a MultiIndex. If a sequence, overwrite
            names with the given sequence.

        Returns
        -------
        MultiIndex
            The MultiIndex representation of the given DataFrame.

        See Also
        --------
        MultiIndex.from_arrays : Convert list of arrays to MultiIndex.
        MultiIndex.from_tuples : Convert list of tuples to MultiIndex.
        MultiIndex.from_product : Make a MultiIndex from cartesian product
                                  of iterables.

        Examples
        --------
        >>> df = ps.DataFrame([['HI', 'Temp'], ['HI', 'Precip'],
        ...                    ['NJ', 'Temp'], ['NJ', 'Precip']],
        ...                   columns=['a', 'b'])
        >>> df  # doctest: +SKIP
              a       b
        0    HI    Temp
        1    HI  Precip
        2    NJ    Temp
        3    NJ  Precip

        >>> ps.MultiIndex.from_frame(df)  # doctest: +SKIP
        MultiIndex([('HI',   'Temp'),
                    ('HI', 'Precip'),
                    ('NJ',   'Temp'),
                    ('NJ', 'Precip')],
                   names=['a', 'b'])

        Using explicit names, instead of the column names

        >>> ps.MultiIndex.from_frame(df, names=['state', 'observation'])  # doctest: +SKIP
        MultiIndex([('HI',   'Temp'),
                    ('HI', 'Precip'),
                    ('NJ',   'Temp'),
                    ('NJ', 'Precip')],
                   names=['state', 'observation'])
        """
        if not isinstance(df, DataFrame):
            raise TypeError("Input must be a DataFrame")
        sdf = df._to_spark()

        if names is None:
            names = df._internal.column_labels
        elif not is_list_like(names):
            raise TypeError("Names should be list-like for a MultiIndex")
        else:
            names = [name if is_name_like_tuple(name) else (name,) for name in names]

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in sdf.columns],
            index_names=names,
        )
        return cast(MultiIndex, DataFrame(internal).index)

    @property
    def name(self) -> Name:
        raise PandasNotImplementedError(class_name="pd.MultiIndex", property_name="name")

    @name.setter
    def name(self, name: Name) -> None:
        raise PandasNotImplementedError(class_name="pd.MultiIndex", property_name="name")

    @property
    def dtypes(self) -> pd.Series:
        """Return the dtypes as a Series for the underlying MultiIndex.

        .. versionadded:: 3.3.0

        Returns
        -------
        pd.Series
            The data type of each level.

        Examples
        --------
        >>> psmidx = ps.MultiIndex.from_arrays(
        ...     [[0, 1, 2, 3, 4, 5, 6, 7, 8], [1, 2, 3, 4, 5, 6, 7, 8, 9]],
        ...     names=("zero", "one"),
        ... )
        >>> psmidx.dtypes
        zero    int64
        one     int64
        dtype: object
        """
        return pd.Series(
            [field.dtype for field in self._internal.index_fields],
            index=pd.Index(
                [name if len(name) > 1 else name[0] for name in self._internal.index_names]
            ),
        )

    def _verify_for_rename(self, name: List[Name]) -> List[Label]:  # type: ignore[override]
        if is_list_like(name):
            if self._internal.index_level != len(name):
                raise ValueError(
                    "Length of new names must be {}, got {}".format(
                        self._internal.index_level, len(name)
                    )
                )
            if any(not is_hashable(n) for n in name):
                raise TypeError("MultiIndex.name must be a hashable type")
            return [n if is_name_like_tuple(n) else (n,) for n in name]
        else:
            raise TypeError("Must pass list-like as `names`.")

    def swaplevel(self, i: int = -2, j: int = -1) -> "MultiIndex":
        """
        Swap level i with level j.
        Calling this method does not change the ordering of the values.

        Parameters
        ----------
        i : int, str, default -2
            First level of index to be swapped. Can pass level name as string.
            Parameter types can be mixed.
        j : int, str, default -1
            Second level of index to be swapped. Can pass level name as string.
            Parameter types can be mixed.

        Returns
        -------
        MultiIndex
            A new MultiIndex.

        Examples
        --------
        >>> midx = ps.MultiIndex.from_arrays([['a', 'b'], [1, 2]], names = ['word', 'number'])
        >>> midx  # doctest: +SKIP
        MultiIndex([('a', 1),
                    ('b', 2)],
                   names=['word', 'number'])

        >>> midx.swaplevel(0, 1)  # doctest: +SKIP
        MultiIndex([(1, 'a'),
                    (2, 'b')],
                   names=['number', 'word'])

        >>> midx.swaplevel('number', 'word')  # doctest: +SKIP
        MultiIndex([(1, 'a'),
                    (2, 'b')],
                   names=['number', 'word'])
        """
        for index in (i, j):
            if not isinstance(index, int) and index not in self.names:
                raise KeyError("Level %s not found" % index)

        i = i if isinstance(i, int) else self.names.index(i)
        j = j if isinstance(j, int) else self.names.index(j)

        for index in (i, j):
            if index >= len(self.names) or index < -len(self.names):
                raise IndexError(
                    "Too many levels: Index has only %s levels, "
                    "%s is not a valid level number" % (len(self.names), index)
                )

        index_map = list(
            zip(
                self._internal.index_spark_columns,
                self._internal.index_names,
                self._internal.index_fields,
            )
        )
        index_map[i], index_map[j] = index_map[j], index_map[i]
        index_spark_columns, index_names, index_fields = zip(*index_map)
        internal = self._internal.copy(
            index_spark_columns=list(index_spark_columns),
            index_names=list(index_names),
            index_fields=list(index_fields),
            column_labels=[],
            data_spark_columns=[],
            data_fields=[],
        )
        return cast(MultiIndex, DataFrame(internal).index)

    @property
    def levshape(self) -> Tuple[int, ...]:
        """
        A tuple with the length of each level.

        Examples
        --------
        >>> midx = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y'), ('c', 'z')])
        >>> midx  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y'),
                    ('c', 'z')],
                   )

        >>> midx.levshape
        (3, 3)
        """
        result = self._internal.spark_frame.agg(
            *(F.countDistinct(c) for c in self._internal.index_spark_columns)
        ).collect()[0]
        return tuple(result)

    @staticmethod
    def _comparator_for_monotonic_increasing(
        data_type: DataType,
    ) -> Callable[[Column, Column, Callable[[Column, Column], Column]], Column]:
        return compare_disallow_null

    def _is_monotonic(self, order: str) -> bool:
        if order == "increasing":
            return self._is_monotonic_increasing().all()
        else:
            return self._is_monotonic_decreasing().all()

    def _is_monotonic_increasing(self) -> Series:
        window = Window.orderBy(NATURAL_ORDER_COLUMN_NAME).rowsBetween(-1, -1)

        cond = F.lit(True)
        has_not_null = F.lit(True)
        for scol in self._internal.index_spark_columns[::-1]:
            data_type = self._internal.spark_type_for(scol)
            prev = F.lag(scol, 1).over(window)
            compare = MultiIndex._comparator_for_monotonic_increasing(data_type)
            # Since pandas 1.1.4, null value is not allowed at any levels of MultiIndex.
            # Therefore, we should check `has_not_null` over all levels.
            has_not_null = has_not_null & scol.isNotNull()
            cond = F.when(scol.eqNullSafe(prev), cond).otherwise(compare(scol, prev, Column.__gt__))

        cond = has_not_null & (prev.isNull() | cond)

        cond_name = verify_temp_column_name(
            self._internal.spark_frame.select(self._internal.index_spark_columns),
            "__is_monotonic_increasing_cond__",
        )

        sdf = self._internal.spark_frame.select(
            self._internal.index_spark_columns + [cond.alias(cond_name)]
        )

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )

        return first_series(DataFrame(internal))

    @staticmethod
    def _comparator_for_monotonic_decreasing(
        data_type: DataType,
    ) -> Callable[[Column, Column, Callable[[Column, Column], Column]], Column]:
        return compare_disallow_null

    def _is_monotonic_decreasing(self) -> Series:
        window = Window.orderBy(NATURAL_ORDER_COLUMN_NAME).rowsBetween(-1, -1)

        cond = F.lit(True)
        has_not_null = F.lit(True)
        for scol in self._internal.index_spark_columns[::-1]:
            data_type = self._internal.spark_type_for(scol)
            prev = F.lag(scol, 1).over(window)
            compare = MultiIndex._comparator_for_monotonic_increasing(data_type)
            # Since pandas 1.1.4, null value is not allowed at any levels of MultiIndex.
            # Therefore, we should check `has_not_null` over all levels.
            has_not_null = has_not_null & scol.isNotNull()
            cond = F.when(scol.eqNullSafe(prev), cond).otherwise(compare(scol, prev, Column.__lt__))

        cond = has_not_null & (prev.isNull() | cond)

        cond_name = verify_temp_column_name(
            self._internal.spark_frame.select(self._internal.index_spark_columns),
            "__is_monotonic_decreasing_cond__",
        )

        sdf = self._internal.spark_frame.select(
            self._internal.index_spark_columns + [cond.alias(cond_name)]
        )

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )

        return first_series(DataFrame(internal))

    def to_frame(  # type: ignore[override]
        self, index: bool = True, name: Optional[List[Name]] = None
    ) -> DataFrame:
        """
        Create a DataFrame with the levels of the MultiIndex as columns.
        Column ordering is determined by the DataFrame constructor with data as
        a dict.

        Parameters
        ----------
        index : boolean, default True
            Set the index of the returned DataFrame as the original MultiIndex.
        name : list / sequence of strings, optional
            The passed names should substitute index level names.

        Returns
        -------
        DataFrame : a DataFrame containing the original MultiIndex data.

        See Also
        --------
        DataFrame

        Examples
        --------
        >>> tuples = [(1, 'red'), (1, 'blue'),
        ...           (2, 'red'), (2, 'blue')]
        >>> idx = ps.MultiIndex.from_tuples(tuples, names=('number', 'color'))
        >>> idx  # doctest: +SKIP
        MultiIndex([(1,  'red'),
                    (1, 'blue'),
                    (2,  'red'),
                    (2, 'blue')],
                   names=['number', 'color'])
        >>> idx.to_frame()  # doctest: +NORMALIZE_WHITESPACE
                      number color
        number color
        1      red         1   red
               blue        1  blue
        2      red         2   red
               blue        2  blue

        By default, the original Index is reused. To enforce a new Index:

        >>> idx.to_frame(index=False)
           number color
        0       1   red
        1       1  blue
        2       2   red
        3       2  blue

        To override the name of the resulting column, specify `name`:

        >>> idx.to_frame(name=['n', 'c'])  # doctest: +NORMALIZE_WHITESPACE
                      n     c
        number color
        1      red    1   red
               blue   1  blue
        2      red    2   red
               blue   2  blue
        """
        if name is None:
            name = [
                name if name is not None else (i,)
                for i, name in enumerate(self._internal.index_names)
            ]
        elif is_list_like(name):
            if len(name) != self._internal.index_level:
                raise ValueError("'name' should have same length as number of levels on index.")
            name = [n if is_name_like_tuple(n) else (n,) for n in name]
        else:
            raise TypeError("'name' must be a list / sequence of column names.")

        return self._to_frame(index=index, names=name)

    def to_pandas(self) -> pd.MultiIndex:
        """
        Return a pandas MultiIndex.

        .. note:: This method should only be used if the resulting pandas object is expected
                  to be small, as all the data is loaded into the driver's memory.

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'],
        ...                   index=[list('abcd'), list('efgh')])
        >>> df['dogs'].index.to_pandas()  # doctest: +SKIP
        MultiIndex([('a', 'e'),
                    ('b', 'f'),
                    ('c', 'g'),
                    ('d', 'h')],
                   )
        """
        # TODO: We might need to handle internal state change.
        # So far, we don't have any functions to change the internal state of MultiIndex except for
        # series-like operations. In that case, it creates a new Index object instead of MultiIndex.
        return cast(pd.MultiIndex, super().to_pandas())

    def _to_pandas(self) -> pd.MultiIndex:
        """
        Same as `to_pandas()`, without issuing the advice log for internal usage.
        """
        return cast(pd.MultiIndex, super()._to_pandas())

    def nunique(self, dropna: bool = True, approx: bool = False, rsd: float = 0.05) -> int:
        raise NotImplementedError("nunique is not defined for MultiIndex")

    # TODO: add 'name' parameter after pd.MultiIndex.name is implemented
    def copy(self, deep: Optional[bool] = None) -> "MultiIndex":  # type: ignore[override]
        """
        Make a copy of this object.

        Parameters
        ----------
        deep : None
            this parameter is not supported but just dummy parameter to match pandas.

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'],
        ...                   index=[list('abcd'), list('efgh')])
        >>> df['dogs'].index  # doctest: +SKIP
        MultiIndex([('a', 'e'),
                    ('b', 'f'),
                    ('c', 'g'),
                    ('d', 'h')],
                   )

        Copy index

        >>> df.index.copy()  # doctest: +SKIP
        MultiIndex([('a', 'e'),
                    ('b', 'f'),
                    ('c', 'g'),
                    ('d', 'h')],
                   )
        """
        return cast(MultiIndex, super().copy(deep=deep))

    def symmetric_difference(  # type: ignore[override]
        self,
        other: Index,
        result_name: Optional[List[Name]] = None,
        sort: Optional[bool] = None,
    ) -> "MultiIndex":
        """
        Compute the symmetric difference of two MultiIndex objects.

        Parameters
        ----------
        other : Index or array-like
        result_name : list
        sort : True or None, default None
            Whether to sort the resulting index.
            * True : Attempt to sort the result.
            * None : Do not sort the result.

        Returns
        -------
        symmetric_difference : MultiIndex

        Notes
        -----
        ``symmetric_difference`` contains elements that appear in either
        ``idx1`` or ``idx2`` but not both. Equivalent to the Index created by
        ``idx1.difference(idx2) | idx2.difference(idx1)`` with duplicates
        dropped.

        Examples
        --------
        >>> midx1 = pd.MultiIndex([['lama', 'cow', 'falcon'],
        ...                        ['speed', 'weight', 'length']],
        ...                       [[0, 0, 0, 1, 1, 1, 2, 2, 2],
        ...                        [0, 0, 0, 0, 1, 2, 0, 1, 2]])
        >>> midx2 = pd.MultiIndex([['pandas-on-Spark', 'cow', 'falcon'],
        ...                        ['speed', 'weight', 'length']],
        ...                       [[0, 0, 0, 1, 1, 1, 2, 2, 2],
        ...                        [0, 0, 0, 0, 1, 2, 0, 1, 2]])
        >>> s1 = ps.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3],
        ...                index=midx1)
        >>> s2 = ps.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3],
        ...              index=midx2)

        >>> s1.index.symmetric_difference(s2.index)  # doctest: +SKIP
        MultiIndex([('pandas-on-Spark', 'speed'),
                    (  'lama', 'speed')],
                   )

        You can set names of the result Index.

        >>> s1.index.symmetric_difference(s2.index, result_name=['a', 'b'])  # doctest: +SKIP
        MultiIndex([('pandas-on-Spark', 'speed'),
                    (  'lama', 'speed')],
                   names=['a', 'b'])

        You can set sort to `True`, if you want to sort the resulting index.

        >>> s1.index.symmetric_difference(s2.index, sort=True)  # doctest: +SKIP
        MultiIndex([('pandas-on-Spark', 'speed'),
                    (  'lama', 'speed')],
                   )

        You can also use the ``^`` operator:

        >>> s1.index ^ s2.index  # doctest: +SKIP
        MultiIndex([('pandas-on-Spark', 'speed'),
                    (  'lama', 'speed')],
                   )
        """
        if type(self) != type(other):
            raise NotImplementedError(
                "Doesn't support symmetric_difference between Index & MultiIndex for now"
            )

        sdf_self = self._psdf._internal.spark_frame.select(self._internal.index_spark_columns)
        sdf_other = other._psdf._internal.spark_frame.select(other._internal.index_spark_columns)

        sdf_symdiff = sdf_self.union(sdf_other).subtract(sdf_self.intersect(sdf_other))

        if sort:
            sdf_symdiff = sdf_symdiff.sort(*self._internal.index_spark_columns)

        internal = InternalFrame(
            spark_frame=sdf_symdiff,
            index_spark_columns=[
                scol_for(sdf_symdiff, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )
        result = cast(MultiIndex, DataFrame(internal).index)

        if result_name:
            result.names = result_name

        return result

    # TODO: ADD error parameter
    def drop(self, codes: List[Any], level: Optional[Union[int, Name]] = None) -> "MultiIndex":
        """
        Make new MultiIndex with passed list of labels deleted

        Parameters
        ----------
        codes : array-like
            Must be a list of tuples
        level : int or level name, default None

        Returns
        -------
        dropped : MultiIndex

        Examples
        --------
        >>> index = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y'), ('c', 'z')])
        >>> index # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y'),
                    ('c', 'z')],
                   )

        >>> index.drop(['a']) # doctest: +SKIP
        MultiIndex([('b', 'y'),
                    ('c', 'z')],
                   )

        >>> index.drop(['x', 'y'], level=1) # doctest: +SKIP
        MultiIndex([('c', 'z')],
                   )
        """
        internal = self._internal.resolved_copy
        sdf = internal.spark_frame
        index_scols = internal.index_spark_columns
        if level is None:
            scol = index_scols[0]
        elif isinstance(level, int):
            scol = index_scols[level]
        else:
            scol = None
            for index_spark_column, index_name in zip(
                internal.index_spark_columns, internal.index_names
            ):
                if not isinstance(level, tuple):
                    level = (level,)
                if level == index_name:
                    if scol is not None:
                        raise ValueError(
                            "The name {} occurs multiple times, use a level number".format(
                                name_like_string(level)
                            )
                        )
                    scol = index_spark_column
            if scol is None:
                raise KeyError("Level {} not found".format(name_like_string(level)))
        sdf = sdf[~scol.isin(codes)]

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, col) for col in internal.index_spark_column_names],
            index_names=internal.index_names,
            index_fields=internal.index_fields,
            column_labels=[],
            data_spark_columns=[],
            data_fields=[],
        )
        return cast(MultiIndex, DataFrame(internal).index)

    def drop_duplicates(self, keep: Union[bool, str] = "first") -> "MultiIndex":
        """
        Return MultiIndex with duplicate values removed.

        Parameters
        ----------
        keep : {'first', 'last', ``False``}, default 'first'
            Method to handle dropping duplicates:
            - 'first' : Drop duplicates except for the first occurrence.
            - 'last' : Drop duplicates except for the last occurrence.
            - ``False`` : Drop all duplicates.

        Returns
        -------
        deduplicated : MultiIndex

        See Also
        --------
        Series.drop_duplicates : Equivalent method on Series.
        DataFrame.drop_duplicates : Equivalent method on DataFrame.

        Examples
        --------
        Generate a MultiIndex with duplicate values.

        >>> arrays = [[1, 2, 3, 1, 2], ["red", "blue", "black", "red", "blue"]]
        >>> midx = ps.MultiIndex.from_arrays(arrays, names=("number", "color"))
        >>> midx
        MultiIndex([(1,   'red'),
                    (2,  'blue'),
                    (3, 'black'),
                    (1,   'red'),
                    (2,  'blue')],
                   names=['number', 'color'])

        >>> midx.drop_duplicates()
        MultiIndex([(1,   'red'),
                    (2,  'blue'),
                    (3, 'black')],
                   names=['number', 'color'])

        >>> midx.drop_duplicates(keep='first')
        MultiIndex([(1,   'red'),
                    (2,  'blue'),
                    (3, 'black')],
                   names=['number', 'color'])

        >>> midx.drop_duplicates(keep='last')
        MultiIndex([(3, 'black'),
                    (1,   'red'),
                    (2,  'blue')],
                   names=['number', 'color'])

        >>> midx.drop_duplicates(keep=False)
        MultiIndex([(3, 'black')],
                   names=['number', 'color'])
        """
        with ps.option_context("compute.default_index_type", "distributed"):
            # The attached index caused by `reset_index` below is used for sorting only,
            # and it will be dropped soon,
            # so we enforce “distributed” default index type
            psdf = self.to_frame().reset_index(drop=True)
        return ps.MultiIndex.from_frame(psdf.drop_duplicates(keep=keep).sort_index())

    def argmax(self) -> None:
        raise TypeError("reduction operation 'argmax' not allowed for this dtype")

    def argmin(self) -> None:
        raise TypeError("reduction operation 'argmin' not allowed for this dtype")

    def asof(self, label: Any) -> None:
        raise NotImplementedError(
            "only the default get_loc method is currently supported for MultiIndex"
        )

    @property
    def is_all_dates(self) -> bool:
        """
        is_all_dates always returns False for MultiIndex

        Examples
        --------
        >>> from datetime import datetime

        >>> idx = ps.MultiIndex.from_tuples(
        ...     [(datetime(2019, 1, 1, 0, 0, 0), datetime(2019, 1, 1, 0, 0, 0)),
        ...      (datetime(2019, 1, 1, 0, 0, 0), datetime(2019, 1, 1, 0, 0, 0))])
        >>> idx  # doctest: +SKIP
        MultiIndex([('2019-01-01', '2019-01-01'),
                    ('2019-01-01', '2019-01-01')],
                   )

        >>> idx.is_all_dates
        False
        """
        return False

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeMultiIndex, item):
            property_or_func = getattr(MissingPandasLikeMultiIndex, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
        raise AttributeError("'MultiIndex' object has no attribute '{}'".format(item))

    def _get_level_number(self, level: Union[int, Name]) -> int:
        """
        Return the level number if a valid level is given.
        """
        count = self.names.count(level)
        if (count > 1) and not isinstance(level, int):
            raise ValueError("The name %s occurs multiple times, use a level number" % level)
        if level in self.names:
            level = self.names.index(level)
        elif isinstance(level, int):
            nlevels = self.nlevels
            if level >= nlevels:
                raise IndexError(
                    "Too many levels: Index has only %d "
                    "levels, %d is not a valid level number" % (nlevels, level)
                )
            if level < 0:
                if (level + nlevels) < 0:
                    raise IndexError(
                        "Too many levels: Index has only %d levels, "
                        "not %d" % (nlevels, level + 1)
                    )
                level = level + nlevels
        else:
            raise KeyError("Level %s not found" % str(level))

        return level

    def get_level_values(self, level: Union[int, Name]) -> Index:
        """
        Return vector of label values for requested level,
        equal to the length of the index.

        Parameters
        ----------
        level : int or str
            ``level`` is either the integer position of the level in the
            MultiIndex, or the name of the level.

        Returns
        -------
        values : Index
            Values is a level of this MultiIndex converted to
            a single :class:`Index` (or subclass thereof).

        Examples
        --------

        Create a MultiIndex:

        >>> mi = ps.MultiIndex.from_tuples([('x', 'a'), ('x', 'b'), ('y', 'a')])
        >>> mi.names = ['level_1', 'level_2']

        Get level values by supplying level as either integer or name:

        >>> mi.get_level_values(0)
        Index(['x', 'x', 'y'], dtype='object', name='level_1')

        >>> mi.get_level_values('level_2')
        Index(['a', 'b', 'a'], dtype='object', name='level_2')
        """
        level = self._get_level_number(level)
        index_scol = self._internal.index_spark_columns[level]
        index_name = self._internal.index_names[level]
        index_field = self._internal.index_fields[level]
        internal = self._internal.copy(
            index_spark_columns=[index_scol],
            index_names=[index_name],
            index_fields=[index_field],
            column_labels=[],
            data_spark_columns=[],
            data_fields=[],
        )
        return DataFrame(internal).index

    def insert(self, loc: int, item: Any) -> Index:
        """
        Make new MultiIndex inserting new item at location.

        Follows Python list.append semantics for negative values.

        .. versionchanged:: 3.4.0
           Raise IndexError when loc is out of bounds to follow Pandas 1.4+ behavior

        Parameters
        ----------
        loc : int
        item : object

        Returns
        -------
        new_index : MultiIndex

        Examples
        --------
        >>> psmidx = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        >>> psmidx.insert(3, ("h", "j"))  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y'),
                    ('c', 'z'),
                    ('h', 'j')],
                   )

        For negative values

        >>> psmidx.insert(-2, ("h", "j"))  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('h', 'j'),
                    ('b', 'y'),
                    ('c', 'z')],
                   )
        """
        validate_index_loc(self, loc)
        loc = loc + len(self) if loc < 0 else loc

        index_name: List[Label] = [(name,) for name in self._internal.index_spark_column_names]
        sdf_before = self.to_frame(name=index_name)[:loc]._to_spark()
        sdf_middle = Index([item]).to_frame(name=index_name)._to_spark()
        sdf_after = self.to_frame(name=index_name)[loc:]._to_spark()
        sdf = sdf_before.union(sdf_middle).union(sdf_after)

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=[InternalField(field.dtype) for field in self._internal.index_fields],
        )
        return DataFrame(internal).index

    def item(self) -> Tuple[Scalar, ...]:
        """
        Return the first element of the underlying data as a python tuple.

        Returns
        -------
        tuple
            The first element of MultiIndex.

        Raises
        ------
        ValueError
            If the data is not length-1.

        Examples
        --------
        >>> psmidx = ps.MultiIndex.from_tuples([('a', 'x')])
        >>> psmidx.item()
        ('a', 'x')
        """
        return self._psdf.head(2)._to_internal_pandas().index.item()

    def intersection(self, other: Union[DataFrame, Series, Index, List]) -> "MultiIndex":
        """
        Form the intersection of two Index objects.

        This returns a new Index with elements common to the index and `other`.

        Parameters
        ----------
        other : Index or array-like

        Returns
        -------
        intersection : MultiIndex

        Examples
        --------
        >>> midx1 = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        >>> midx2 = ps.MultiIndex.from_tuples([("c", "z"), ("d", "w")])
        >>> midx1.intersection(midx2).sort_values()  # doctest: +SKIP
        MultiIndex([('c', 'z')],
                   )
        """
        if isinstance(other, Series) or not is_list_like(other):
            raise TypeError("other must be a MultiIndex or a list of tuples")
        elif isinstance(other, DataFrame):
            raise ValueError("Index data must be 1-dimensional")
        elif isinstance(other, MultiIndex):
            spark_frame_other = other.to_frame()._to_spark()
            keep_name = self.names == other.names
        elif isinstance(other, Index):
            # Always returns an empty MultiIndex if `other` is Index.
            return cast(MultiIndex, self.to_frame().head(0).index)
        elif not all(isinstance(item, tuple) for item in other):
            raise TypeError("other must be a MultiIndex or a list of tuples")
        else:
            other = MultiIndex.from_tuples(list(other))
            spark_frame_other = cast(MultiIndex, other).to_frame()._to_spark()
            keep_name = True

        index_fields = self._index_fields_for_union_like(other, func_name="intersection")

        default_name: List[Name] = [SPARK_INDEX_NAME_FORMAT(i) for i in range(self.nlevels)]
        spark_frame_self = self.to_frame(name=default_name)._to_spark()
        spark_frame_intersected = spark_frame_self.intersect(spark_frame_other)
        if keep_name:
            index_names = self._internal.index_names
        else:
            index_names = None

        internal = InternalFrame(
            spark_frame=spark_frame_intersected,
            index_spark_columns=[
                scol_for(spark_frame_intersected, cast(str, col)) for col in default_name
            ],
            index_names=index_names,
            index_fields=index_fields,
        )
        return cast(MultiIndex, DataFrame(internal).index)

    def equal_levels(self, other: "MultiIndex") -> bool:
        """
        Return True if the levels of both MultiIndex objects are the same

        .. versionadded:: 3.3.0

        Examples
        --------
        >>> psmidx1 = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        >>> psmidx2 = ps.MultiIndex.from_tuples([("b", "y"), ("a", "x"), ("c", "z")])
        >>> psmidx1.equal_levels(psmidx2)
        True

        >>> psmidx2 = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "j")])
        >>> psmidx1.equal_levels(psmidx2)
        False
        """
        nlevels = self.nlevels
        if nlevels != other.nlevels:
            return False

        self_sdf = self._internal.spark_frame
        other_sdf = other._internal.spark_frame
        subtract_list = []
        for nlevel in range(nlevels):
            self_index_scol = self._internal.index_spark_columns[nlevel]
            other_index_scol = other._internal.index_spark_columns[nlevel]
            self_subtract_other = self_sdf.select(self_index_scol).subtract(
                other_sdf.select(other_index_scol)
            )
            subtract_list.append(self_subtract_other)

        unioned_subtracts = reduce(lambda x, y: x.union(y), subtract_list)
        return len(unioned_subtracts.head(1)) == 0

    @property
    def hasnans(self) -> bool:
        raise NotImplementedError("hasnans is not defined for MultiIndex")

    @property
    def inferred_type(self) -> str:
        """
        Return a string of the type inferred from the values.
        """
        # Always returns "mixed" for MultiIndex
        return "mixed"

    @property
    def asi8(self) -> None:
        """
        Integer representation of the values.
        """
        # Always returns None for MultiIndex
        return None

    def factorize(
        self, sort: bool = True, na_sentinel: Optional[int] = -1
    ) -> Tuple["MultiIndex", pd.Index]:
        return MissingPandasLikeMultiIndex.factorize(self, sort=sort, na_sentinel=na_sentinel)

    def __iter__(self) -> Iterator:
        return MissingPandasLikeMultiIndex.__iter__(self)

    def map(
        self,
        mapper: Union[dict, Callable[[Any], Any], pd.Series] = None,
        na_action: Optional[str] = None,
    ) -> "Index":
        return MissingPandasLikeMultiIndex.map(self, mapper, na_action)


def _test() -> None:
    import os
    import doctest
    import sys
    import numpy
    from pyspark.sql import SparkSession
    import pyspark.pandas.indexes.multi

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.indexes.multi.__dict__.copy()
    globs["np"] = numpy
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.indexes.multi tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.indexes.multi,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
