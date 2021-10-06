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

from functools import partial
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union, cast, no_type_check
import warnings

import pandas as pd
import numpy as np
from pandas.api.types import (
    is_list_like,
    is_interval_dtype,
    is_bool_dtype,
    is_categorical_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
from pandas.core.accessor import CachedAccessor
from pandas.io.formats.printing import pprint_thing
from pandas.api.types import CategoricalDtype, is_hashable
from pandas._libs import lib

from pyspark.sql import functions as F, Column
from pyspark.sql.types import FractionalType, IntegralType, TimestampType, TimestampNTZType

from pyspark import pandas as ps  # For running doctests and reference resolution in PyCharm.
from pyspark.pandas._typing import Dtype, Label, Name, Scalar
from pyspark.pandas.config import get_option, option_context
from pyspark.pandas.base import IndexOpsMixin
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.missing.indexes import MissingPandasLikeIndex
from pyspark.pandas.series import Series, first_series
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.spark.accessors import SparkIndexMethods
from pyspark.pandas.utils import (
    is_name_like_tuple,
    is_name_like_value,
    name_like_string,
    same_anchor,
    scol_for,
    verify_temp_column_name,
    validate_bool_kwarg,
    ERROR_MESSAGE_CANNOT_COMBINE,
)
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    DEFAULT_SERIES_NAME,
    SPARK_DEFAULT_INDEX_NAME,
    SPARK_INDEX_NAME_FORMAT,
)


class Index(IndexOpsMixin):
    """
    pandas-on-Spark Index that corresponds to pandas Index logically. This might hold Spark Column
    internally.

    Parameters
    ----------
    data : array-like (1-dimensional)
    dtype : dtype, default None
        If dtype is None, we find the dtype that best fits the data.
        If an actual dtype is provided, we coerce to that dtype if it's safe.
        Otherwise, an error will be raised.
    copy : bool
        Make a copy of input ndarray.
    name : object
        Name to be stored in the index.
    tupleize_cols : bool (default: True)
        When True, attempt to create a MultiIndex if possible.

    See Also
    --------
    MultiIndex : A multi-level, or hierarchical, Index.
    DatetimeIndex : Index of datetime64 data.
    Int64Index : A special case of :class:`Index` with purely integer labels.
    Float64Index : A special case of :class:`Index` with purely float labels.

    Examples
    --------
    >>> ps.DataFrame({'a': ['a', 'b', 'c']}, index=[1, 2, 3]).index
    Int64Index([1, 2, 3], dtype='int64')

    >>> ps.DataFrame({'a': [1, 2, 3]}, index=list('abc')).index
    Index(['a', 'b', 'c'], dtype='object')

    >>> ps.Index([1, 2, 3])
    Int64Index([1, 2, 3], dtype='int64')

    >>> ps.Index(list('abc'))
    Index(['a', 'b', 'c'], dtype='object')

    From a Series:

    >>> s = ps.Series([1, 2, 3], index=[10, 20, 30])
    >>> ps.Index(s)
    Int64Index([1, 2, 3], dtype='int64')

    From an Index:

    >>> idx = ps.Index([1, 2, 3])
    >>> ps.Index(idx)
    Int64Index([1, 2, 3], dtype='int64')
    """

    def __new__(
        cls,
        data: Optional[Any] = None,
        dtype: Optional[Union[str, Dtype]] = None,
        copy: bool = False,
        name: Optional[Name] = None,
        tupleize_cols: bool = True,
        **kwargs: Any
    ) -> "Index":
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, Series):
            if dtype is not None:
                data = data.astype(dtype)
            if name is not None:
                data = data.rename(name)

            internal = InternalFrame(
                spark_frame=data._internal.spark_frame,
                index_spark_columns=data._internal.data_spark_columns,
                index_names=data._internal.column_labels,
                index_fields=data._internal.data_fields,
                column_labels=[],
                data_spark_columns=[],
                data_fields=[],
            )
            return DataFrame(internal).index
        elif isinstance(data, Index):
            if copy:
                data = data.copy()
            if dtype is not None:
                data = data.astype(dtype)
            if name is not None:
                data = data.rename(name)
            return data

        return cast(
            Index,
            ps.from_pandas(
                pd.Index(
                    data=data,
                    dtype=dtype,
                    copy=copy,
                    name=name,
                    tupleize_cols=tupleize_cols,
                    **kwargs
                )
            ),
        )

    @staticmethod
    def _new_instance(anchor: DataFrame) -> "Index":
        from pyspark.pandas.indexes.category import CategoricalIndex
        from pyspark.pandas.indexes.datetimes import DatetimeIndex
        from pyspark.pandas.indexes.multi import MultiIndex
        from pyspark.pandas.indexes.numeric import Float64Index, Int64Index

        if anchor._internal.index_level > 1:
            instance = object.__new__(MultiIndex)
        elif isinstance(anchor._internal.index_fields[0].dtype, CategoricalDtype):
            instance = object.__new__(CategoricalIndex)
        elif isinstance(
            anchor._internal.spark_type_for(anchor._internal.index_spark_columns[0]), IntegralType
        ):
            instance = object.__new__(Int64Index)
        elif isinstance(
            anchor._internal.spark_type_for(anchor._internal.index_spark_columns[0]), FractionalType
        ):
            instance = object.__new__(Float64Index)
        elif isinstance(
            anchor._internal.spark_type_for(anchor._internal.index_spark_columns[0]),
            (TimestampType, TimestampNTZType),
        ):
            instance = object.__new__(DatetimeIndex)
        else:
            instance = object.__new__(Index)

        instance._anchor = anchor
        return instance

    @property
    def _psdf(self) -> DataFrame:
        return self._anchor

    @property
    def _internal(self) -> InternalFrame:
        internal = self._psdf._internal
        return internal.copy(
            column_labels=internal.index_names,
            data_spark_columns=internal.index_spark_columns,
            data_fields=internal.index_fields,
            column_label_names=None,
        )

    @property
    def _column_label(self) -> Optional[Label]:
        return self._psdf._internal.index_names[0]

    def _with_new_scol(self, scol: Column, *, field: Optional[InternalField] = None) -> "Index":
        """
        Copy pandas-on-Spark Index with the new Spark Column.

        :param scol: the new Spark Column
        :return: the copied Index
        """
        internal = self._internal.copy(
            index_spark_columns=[scol.alias(SPARK_DEFAULT_INDEX_NAME)],
            index_fields=[
                field
                if field is None or field.struct_field is None
                else field.copy(name=SPARK_DEFAULT_INDEX_NAME)
            ],
            column_labels=[],
            data_spark_columns=[],
            data_fields=[],
        )
        return DataFrame(internal).index

    spark = CachedAccessor("spark", SparkIndexMethods)

    # This method is used via `DataFrame.info` API internally.
    def _summary(self, name: Optional[str] = None) -> str:
        """
        Return a summarized representation.

        Parameters
        ----------
        name : str
            name to use in the summary representation

        Returns
        -------
        String with a summarized representation of the index
        """
        head, tail, total_count = tuple(
            cast(
                pd.DataFrame,
                self._internal.spark_frame.select(
                    F.first(self.spark.column), F.last(self.spark.column), F.count(F.expr("*"))
                ).toPandas(),
            ).iloc[0]
        )

        if total_count > 0:
            index_summary = ", %s to %s" % (pprint_thing(head), pprint_thing(tail))
        else:
            index_summary = ""

        if name is None:
            name = type(self).__name__
        return "%s: %s entries%s" % (name, total_count, index_summary)

    @property
    def size(self) -> int:
        """
        Return an int representing the number of elements in this object.

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'],
        ...                   index=list('abcd'))
        >>> df.index.size
        4

        >>> df.set_index('dogs', append=True).index.size
        4
        """
        return len(self)

    @property
    def shape(self) -> tuple:
        """
        Return a tuple of the shape of the underlying data.

        Examples
        --------
        >>> idx = ps.Index(['a', 'b', 'c'])
        >>> idx
        Index(['a', 'b', 'c'], dtype='object')
        >>> idx.shape
        (3,)

        >>> midx = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y'), ('c', 'z')])
        >>> midx  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y'),
                    ('c', 'z')],
                   )
        >>> midx.shape
        (3,)
        """
        return (len(self._psdf),)

    def identical(self, other: "Index") -> bool:
        """
        Similar to equals, but check that other comparable attributes are
        also equal.

        Returns
        -------
        bool
            If two Index objects have equal elements and same type True,
            otherwise False.

        Examples
        --------

        >>> from pyspark.pandas.config import option_context
        >>> idx = ps.Index(['a', 'b', 'c'])
        >>> midx = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y'), ('c', 'z')])

        For Index

        >>> idx.identical(idx)
        True
        >>> with option_context('compute.ops_on_diff_frames', True):
        ...     idx.identical(ps.Index(['a', 'b', 'c']))
        True
        >>> with option_context('compute.ops_on_diff_frames', True):
        ...     idx.identical(ps.Index(['b', 'b', 'a']))
        False
        >>> idx.identical(midx)
        False

        For MultiIndex

        >>> midx.identical(midx)
        True
        >>> with option_context('compute.ops_on_diff_frames', True):
        ...     midx.identical(ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y'), ('c', 'z')]))
        True
        >>> with option_context('compute.ops_on_diff_frames', True):
        ...     midx.identical(ps.MultiIndex.from_tuples([('c', 'z'), ('b', 'y'), ('a', 'x')]))
        False
        >>> midx.identical(idx)
        False
        """
        from pyspark.pandas.indexes.multi import MultiIndex

        self_name = self.names if isinstance(self, MultiIndex) else self.name
        other_name = other.names if isinstance(other, MultiIndex) else other.name

        return (
            self_name == other_name  # to support non-index comparison by short-circuiting.
            and self.equals(other)
        )

    def equals(self, other: "Index") -> bool:
        """
        Determine if two Index objects contain the same elements.

        Returns
        -------
        bool
            True if "other" is an Index and it has the same elements as calling
            index; False otherwise.

        Examples
        --------

        >>> from pyspark.pandas.config import option_context
        >>> idx = ps.Index(['a', 'b', 'c'])
        >>> idx.name = "name"
        >>> midx = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y'), ('c', 'z')])
        >>> midx.names = ("nameA", "nameB")

        For Index

        >>> idx.equals(idx)
        True
        >>> with option_context('compute.ops_on_diff_frames', True):
        ...     idx.equals(ps.Index(['a', 'b', 'c']))
        True
        >>> with option_context('compute.ops_on_diff_frames', True):
        ...     idx.equals(ps.Index(['b', 'b', 'a']))
        False
        >>> idx.equals(midx)
        False

        For MultiIndex

        >>> midx.equals(midx)
        True
        >>> with option_context('compute.ops_on_diff_frames', True):
        ...     midx.equals(ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y'), ('c', 'z')]))
        True
        >>> with option_context('compute.ops_on_diff_frames', True):
        ...     midx.equals(ps.MultiIndex.from_tuples([('c', 'z'), ('b', 'y'), ('a', 'x')]))
        False
        >>> midx.equals(idx)
        False
        """
        if same_anchor(self, other):
            return True
        elif type(self) == type(other):
            if get_option("compute.ops_on_diff_frames"):
                # TODO: avoid using default index?
                with option_context("compute.default_index_type", "distributed-sequence"):
                    # Directly using Series from both self and other seems causing
                    # some exceptions when 'compute.ops_on_diff_frames' is enabled.
                    # Working around for now via using frame.
                    return (
                        cast(Series, self.to_series("self").reset_index(drop=True))
                        == cast(Series, other.to_series("other").reset_index(drop=True))
                    ).all()
            else:
                raise ValueError(ERROR_MESSAGE_CANNOT_COMBINE)
        else:
            return False

    def transpose(self) -> "Index":
        """
        Return the transpose, For index, It will be index itself.

        Examples
        --------
        >>> idx = ps.Index(['a', 'b', 'c'])
        >>> idx
        Index(['a', 'b', 'c'], dtype='object')

        >>> idx.transpose()
        Index(['a', 'b', 'c'], dtype='object')

        For MultiIndex

        >>> midx = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y'), ('c', 'z')])
        >>> midx  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y'),
                    ('c', 'z')],
                   )

        >>> midx.transpose()  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y'),
                    ('c', 'z')],
                   )
        """
        return self

    T = property(transpose)

    def _to_internal_pandas(self) -> pd.Index:
        """
        Return a pandas Index directly from _internal to avoid overhead of copy.

        This method is for internal use only.
        """
        return self._psdf._internal.to_pandas_frame.index

    def to_pandas(self) -> pd.Index:
        """
        Return a pandas Index.

        .. note:: This method should only be used if the resulting pandas object is expected
                  to be small, as all the data is loaded into the driver's memory.

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'],
        ...                   index=list('abcd'))
        >>> df['dogs'].index.to_pandas()
        Index(['a', 'b', 'c', 'd'], dtype='object')
        """
        return self._to_internal_pandas().copy()

    def to_numpy(self, dtype: Optional[Union[str, Dtype]] = None, copy: bool = False) -> np.ndarray:
        """
        A NumPy ndarray representing the values in this Index or MultiIndex.

        .. note:: This method should only be used if the resulting NumPy ndarray is expected
            to be small, as all the data is loaded into the driver's memory.

        Parameters
        ----------
        dtype : str or numpy.dtype, optional
            The dtype to pass to :meth:`numpy.asarray`
        copy : bool, default False
            Whether to ensure that the returned value is a not a view on
            another array. Note that ``copy=False`` does not *ensure* that
            ``to_numpy()`` is no-copy. Rather, ``copy=True`` ensure that
            a copy is made, even if not strictly necessary.

        Returns
        -------
        numpy.ndarray

        Examples
        --------
        >>> ps.Series([1, 2, 3, 4]).index.to_numpy()
        array([0, 1, 2, 3])
        >>> ps.DataFrame({'a': ['a', 'b', 'c']}, index=[[1, 2, 3], [4, 5, 6]]).index.to_numpy()
        array([(1, 4), (2, 5), (3, 6)], dtype=object)
        """
        result = np.asarray(self._to_internal_pandas()._values, dtype=dtype)
        if copy:
            result = result.copy()
        return result

    def map(
        self, mapper: Union[dict, Callable[[Any], Any], pd.Series], na_action: Optional[str] = None
    ) -> "Index":
        """
        Map values using input correspondence (a dict, Series, or function).

        Parameters
        ----------
        mapper : function, dict, or pd.Series
            Mapping correspondence.
        na_action : {None, 'ignore'}
            If ‘ignore’, propagate NA values, without passing them to the mapping correspondence.

        Returns
        -------
        applied : Index, inferred
            The output of the mapping function applied to the index.

        Examples
        --------
        >>> psidx = ps.Index([1, 2, 3])

        >>> psidx.map({1: "one", 2: "two", 3: "three"})
        Index(['one', 'two', 'three'], dtype='object')

        >>> psidx.map(lambda id: "{id} + 1".format(id=id))
        Index(['1 + 1', '2 + 1', '3 + 1'], dtype='object')

        >>> pser = pd.Series(["one", "two", "three"], index=[1, 2, 3])
        >>> psidx.map(pser)
        Index(['one', 'two', 'three'], dtype='object')
        """
        if isinstance(mapper, dict):
            if len(set(type(k) for k in mapper.values())) > 1:
                raise TypeError(
                    "If the mapper is a dictionary, its values must be of the same type"
                )

        return Index(
            self.to_series().pandas_on_spark.transform_batch(
                lambda pser: pser.map(mapper, na_action)
            )
        ).rename(self.name)

    @property
    def values(self) -> np.ndarray:
        """
        Return an array representing the data in the Index.

        .. warning:: We recommend using `Index.to_numpy()` instead.

        .. note:: This method should only be used if the resulting NumPy ndarray is expected
            to be small, as all the data is loaded into the driver's memory.

        Returns
        -------
        numpy.ndarray

        Examples
        --------
        >>> ps.Series([1, 2, 3, 4]).index.values
        array([0, 1, 2, 3])
        >>> ps.DataFrame({'a': ['a', 'b', 'c']}, index=[[1, 2, 3], [4, 5, 6]]).index.values
        array([(1, 4), (2, 5), (3, 6)], dtype=object)
        """
        warnings.warn("We recommend using `{}.to_numpy()` instead.".format(type(self).__name__))
        return self.to_numpy()

    @property
    def asi8(self) -> np.ndarray:
        """
        Integer representation of the values.

        .. warning:: We recommend using `Index.to_numpy()` instead.

        .. note:: This method should only be used if the resulting NumPy ndarray is expected
            to be small, as all the data is loaded into the driver's memory.

        Returns
        -------
        numpy.ndarray
            An ndarray with int64 dtype.

        Examples
        --------
        >>> ps.Index([1, 2, 3]).asi8
        array([1, 2, 3])

        Returns None for non-int64 dtype

        >>> ps.Index(['a', 'b', 'c']).asi8 is None
        True
        """
        warnings.warn("We recommend using `{}.to_numpy()` instead.".format(type(self).__name__))
        if isinstance(self.spark.data_type, IntegralType):
            return self.to_numpy()
        elif isinstance(self.spark.data_type, (TimestampType, TimestampNTZType)):
            return np.array(list(map(lambda x: x.astype(np.int64), self.to_numpy())))
        else:
            return None

    @property
    def has_duplicates(self) -> bool:
        """
        If index has duplicates, return True, otherwise False.

        Examples
        --------
        >>> idx = ps.Index([1, 5, 7, 7])
        >>> idx.has_duplicates
        True

        >>> idx = ps.Index([1, 5, 7])
        >>> idx.has_duplicates
        False

        >>> idx = ps.Index(["Watermelon", "Orange", "Apple",
        ...                 "Watermelon"])
        >>> idx.has_duplicates
        True

        >>> idx = ps.Index(["Orange", "Apple",
        ...                 "Watermelon"])
        >>> idx.has_duplicates
        False
        """
        sdf = self._internal.spark_frame.select(self.spark.column)
        scol = scol_for(sdf, sdf.columns[0])

        return sdf.select(F.count(scol) != F.countDistinct(scol)).first()[0]

    @property
    def is_unique(self) -> bool:
        """
        Return if the index has unique values.

        Examples
        --------
        >>> idx = ps.Index([1, 5, 7, 7])
        >>> idx.is_unique
        False

        >>> idx = ps.Index([1, 5, 7])
        >>> idx.is_unique
        True

        >>> idx = ps.Index(["Watermelon", "Orange", "Apple",
        ...                 "Watermelon"])
        >>> idx.is_unique
        False

        >>> idx = ps.Index(["Orange", "Apple",
        ...                 "Watermelon"])
        >>> idx.is_unique
        True
        """
        return not self.has_duplicates

    @property
    def name(self) -> Name:
        """Return name of the Index."""
        return self.names[0]

    @name.setter
    def name(self, name: Name) -> None:
        self.names = [name]

    @property
    def names(self) -> List[Name]:
        """Return names of the Index."""
        return [
            name if name is None or len(name) > 1 else name[0]
            for name in self._internal.index_names
        ]

    @names.setter
    def names(self, names: List[Name]) -> None:
        if not is_list_like(names):
            raise ValueError("Names must be a list-like")
        if self._internal.index_level != len(names):
            raise ValueError(
                "Length of new names must be {}, got {}".format(
                    self._internal.index_level, len(names)
                )
            )
        if self._internal.index_level == 1:
            self.rename(names[0], inplace=True)
        else:
            self.rename(names, inplace=True)

    @property
    def nlevels(self) -> int:
        """
        Number of levels in Index & MultiIndex.

        Examples
        --------
        >>> psdf = ps.DataFrame({"a": [1, 2, 3]}, index=pd.Index(['a', 'b', 'c'], name="idx"))
        >>> psdf.index.nlevels
        1

        >>> psdf = ps.DataFrame({'a': [1, 2, 3]}, index=[list('abc'), list('def')])
        >>> psdf.index.nlevels
        2
        """
        return self._internal.index_level

    def rename(self, name: Union[Name, List[Name]], inplace: bool = False) -> Optional["Index"]:
        """
        Alter Index or MultiIndex name.
        Able to set new names without level. Defaults to returning new index.

        Parameters
        ----------
        name : label or list of labels
            Name(s) to set.
        inplace : boolean, default False
            Modifies the object directly, instead of creating a new Index or MultiIndex.

        Returns
        -------
        Index or MultiIndex
            The same type as the caller or None if inplace is True.

        Examples
        --------
        >>> df = ps.DataFrame({'a': ['A', 'C'], 'b': ['A', 'B']}, columns=['a', 'b'])
        >>> df.index.rename("c")
        Int64Index([0, 1], dtype='int64', name='c')

        >>> df.set_index("a", inplace=True)
        >>> df.index.rename("d")
        Index(['A', 'C'], dtype='object', name='d')

        You can also change the index name in place.

        >>> df.index.rename("e", inplace=True)
        >>> df.index
        Index(['A', 'C'], dtype='object', name='e')

        >>> df  # doctest: +NORMALIZE_WHITESPACE
           b
        e
        A  A
        C  B

        Support for MultiIndex

        >>> psidx = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y')])
        >>> psidx.names = ['hello', 'pandas-on-Spark']
        >>> psidx  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y')],
                   names=['hello', 'pandas-on-Spark'])

        >>> psidx.rename(['aloha', 'databricks'])  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y')],
                   names=['aloha', 'databricks'])
        """
        names = self._verify_for_rename(name)

        internal = self._psdf._internal.copy(index_names=names)

        if inplace:
            self._psdf._update_internal_frame(internal)
            return None
        else:
            return DataFrame(internal).index

    def _verify_for_rename(self, name: Name) -> List[Label]:
        if is_hashable(name):
            if is_name_like_tuple(name):
                return [name]
            elif is_name_like_value(name):
                return [(name,)]
        raise TypeError("Index.name must be a hashable type")

    # TODO: add downcast parameter for fillna function
    def fillna(self, value: Scalar) -> "Index":
        """
        Fill NA/NaN values with the specified value.

        Parameters
        ----------
        value : scalar
            Scalar value to use to fill holes (example: 0). This value cannot be a list-likes.

        Returns
        -------
        Index :
            filled with value

        Examples
        --------
        >>> idx = ps.Index([1, 2, None])
        >>> idx
        Float64Index([1.0, 2.0, nan], dtype='float64')

        >>> idx.fillna(0)
        Float64Index([1.0, 2.0, 0.0], dtype='float64')
        """
        if not isinstance(value, (float, int, str, bool)):
            raise TypeError("Unsupported type %s" % type(value).__name__)
        sdf = self._internal.spark_frame.fillna(value)

        internal = InternalFrame(  # TODO: dtypes?
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
        )
        return DataFrame(internal).index

    # TODO: ADD keep parameter
    def drop_duplicates(self) -> "Index":
        """
        Return Index with duplicate values removed.

        Returns
        -------
        deduplicated : Index

        See Also
        --------
        Series.drop_duplicates : Equivalent method on Series.
        DataFrame.drop_duplicates : Equivalent method on DataFrame.

        Examples
        --------
        Generate an pandas.Index with duplicate values.

        >>> idx = ps.Index(['lama', 'cow', 'lama', 'beetle', 'lama', 'hippo'])

        >>> idx.drop_duplicates().sort_values()
        Index(['beetle', 'cow', 'hippo', 'lama'], dtype='object')
        """
        sdf = self._internal.spark_frame.select(
            self._internal.index_spark_columns
        ).drop_duplicates()
        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )
        return DataFrame(internal).index

    def to_series(self, name: Optional[Name] = None) -> Series:
        """
        Create a Series with both index and values equal to the index keys
        useful with map for returning an indexer based on an index.

        Parameters
        ----------
        name : string, optional
            name of resulting Series. If None, defaults to name of original
            index

        Returns
        -------
        Series : dtype will be based on the type of the Index values.

        Examples
        --------
        >>> df = ps.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'],
        ...                   index=list('abcd'))
        >>> df['dogs'].index.to_series()
        a    a
        b    b
        c    c
        d    d
        dtype: object
        """
        if not is_hashable(name):
            raise TypeError("Series.name must be a hashable type")
        scol = self.spark.column
        field = self._internal.data_fields[0]
        if name is not None:
            scol = scol.alias(name_like_string(name))
            field = field.copy(name=name_like_string(name))
        elif self._internal.index_level == 1:
            name = self.name
        column_labels = [
            name if is_name_like_tuple(name) else (name,)
        ]  # type: List[Optional[Label]]
        internal = self._internal.copy(
            column_labels=column_labels,
            data_spark_columns=[scol],
            data_fields=[field],
            column_label_names=None,
        )
        return first_series(DataFrame(internal))

    def to_frame(self, index: bool = True, name: Optional[Name] = None) -> DataFrame:
        """
        Create a DataFrame with a column containing the Index.

        Parameters
        ----------
        index : boolean, default True
            Set the index of the returned DataFrame as the original Index.
        name : object, default None
            The passed name should substitute for the index name (if it has
            one).

        Returns
        -------
        DataFrame
            DataFrame containing the original Index data.

        See Also
        --------
        Index.to_series : Convert an Index to a Series.
        Series.to_frame : Convert Series to DataFrame.

        Examples
        --------
        >>> idx = ps.Index(['Ant', 'Bear', 'Cow'], name='animal')
        >>> idx.to_frame()  # doctest: +NORMALIZE_WHITESPACE
               animal
        animal
        Ant       Ant
        Bear     Bear
        Cow       Cow

        By default, the original Index is reused. To enforce a new Index:

        >>> idx.to_frame(index=False)
          animal
        0    Ant
        1   Bear
        2    Cow

        To override the name of the resulting column, specify `name`:

        >>> idx.to_frame(name='zoo')  # doctest: +NORMALIZE_WHITESPACE
                 zoo
        animal
        Ant      Ant
        Bear    Bear
        Cow      Cow
        """
        if name is None:
            if self._internal.index_names[0] is None:
                name = (DEFAULT_SERIES_NAME,)
            else:
                name = self._internal.index_names[0]
        elif not is_name_like_tuple(name):
            if is_name_like_value(name):
                name = (name,)
            else:
                raise TypeError("unhashable type: '{}'".format(type(name).__name__))

        return self._to_frame(index=index, names=[name])

    def _to_frame(self, index: bool, names: List[Label]) -> DataFrame:
        if index:
            index_spark_columns = self._internal.index_spark_columns
            index_names = self._internal.index_names
            index_fields = self._internal.index_fields
        else:
            index_spark_columns = []
            index_names = []
            index_fields = []

        internal = InternalFrame(
            spark_frame=self._internal.spark_frame,
            index_spark_columns=index_spark_columns,
            index_names=index_names,
            index_fields=index_fields,
            column_labels=names,
            data_spark_columns=self._internal.index_spark_columns,
            data_fields=self._internal.index_fields,
        )
        return DataFrame(internal)

    def is_boolean(self) -> bool:
        """
        Return if the current index type is a boolean type.

        Examples
        --------
        >>> ps.DataFrame({'a': [1]}, index=[True]).index.is_boolean()
        True
        """
        return is_bool_dtype(self.dtype)

    def is_categorical(self) -> bool:
        """
        Return if the current index type is a categorical type.

        Examples
        --------
        >>> ps.DataFrame({'a': [1]}, index=[1]).index.is_categorical()
        False
        """
        return is_categorical_dtype(self.dtype)

    def is_floating(self) -> bool:
        """
        Return if the current index type is a floating type.

        Examples
        --------
        >>> ps.DataFrame({'a': [1]}, index=[1]).index.is_floating()
        False
        """
        return is_float_dtype(self.dtype)

    def is_integer(self) -> bool:
        """
        Return if the current index type is a integer type.

        Examples
        --------
        >>> ps.DataFrame({'a': [1]}, index=[1]).index.is_integer()
        True
        """
        return is_integer_dtype(self.dtype)

    def is_interval(self) -> bool:
        """
        Return if the current index type is an interval type.

        Examples
        --------
        >>> ps.DataFrame({'a': [1]}, index=[1]).index.is_interval()
        False
        """
        return is_interval_dtype(self.dtype)

    def is_numeric(self) -> bool:
        """
        Return if the current index type is a numeric type.

        Examples
        --------
        >>> ps.DataFrame({'a': [1]}, index=[1]).index.is_numeric()
        True
        """
        return is_numeric_dtype(self.dtype)

    def is_object(self) -> bool:
        """
        Return if the current index type is a object type.

        Examples
        --------
        >>> ps.DataFrame({'a': [1]}, index=["a"]).index.is_object()
        True
        """
        return is_object_dtype(self.dtype)

    def is_type_compatible(self, kind: str) -> bool:
        """
        Whether the index type is compatible with the provided type.

        Examples
        --------
        >>> psidx = ps.Index([1, 2, 3])
        >>> psidx.is_type_compatible('integer')
        True

        >>> psidx = ps.Index([1.0, 2.0, 3.0])
        >>> psidx.is_type_compatible('integer')
        False
        >>> psidx.is_type_compatible('floating')
        True
        """
        return kind == self.inferred_type

    def dropna(self) -> "Index":
        """
        Return Index or MultiIndex without NA/NaN values

        Examples
        --------

        >>> df = ps.DataFrame([[1, 2], [4, 5], [7, 8]],
        ...                   index=['cobra', 'viper', None],
        ...                   columns=['max_speed', 'shield'])
        >>> df
               max_speed  shield
        cobra          1       2
        viper          4       5
        NaN            7       8

        >>> df.index.dropna()
        Index(['cobra', 'viper'], dtype='object')

        Also support for MultiIndex

        >>> midx = pd.MultiIndex([['lama', 'cow', 'falcon'],
        ...                       [None, 'weight', 'length']],
        ...                      [[0, 1, 1, 1, 1, 1, 2, 2, 2],
        ...                       [0, 1, 1, 0, 1, 2, 1, 1, 2]])
        >>> s = ps.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, None],
        ...               index=midx)
        >>> s
        lama    NaN        45.0
        cow     weight    200.0
                weight      1.2
                NaN        30.0
                weight    250.0
                length      1.5
        falcon  weight    320.0
                weight      1.0
                length      NaN
        dtype: float64

        >>> s.index.dropna()  # doctest: +SKIP
        MultiIndex([(   'cow', 'weight'),
                    (   'cow', 'weight'),
                    (   'cow', 'weight'),
                    (   'cow', 'length'),
                    ('falcon', 'weight'),
                    ('falcon', 'weight'),
                    ('falcon', 'length')],
                   )
        """
        sdf = self._internal.spark_frame.select(self._internal.index_spark_columns).dropna()
        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )
        return DataFrame(internal).index

    def unique(self, level: Optional[Union[int, Name]] = None) -> "Index":
        """
        Return unique values in the index.

        Be aware the order of unique values might be different than pandas.Index.unique

        Parameters
        ----------
        level : int or str, optional, default is None

        Returns
        -------
        Index without duplicates

        See Also
        --------
        Series.unique
        groupby.SeriesGroupBy.unique

        Examples
        --------
        >>> ps.DataFrame({'a': ['a', 'b', 'c']}, index=[1, 1, 3]).index.unique().sort_values()
        Int64Index([1, 3], dtype='int64')

        >>> ps.DataFrame({'a': ['a', 'b', 'c']}, index=['d', 'e', 'e']).index.unique().sort_values()
        Index(['d', 'e'], dtype='object')

        MultiIndex

        >>> ps.MultiIndex.from_tuples([("A", "X"), ("A", "Y"), ("A", "X")]).unique()
        ... # doctest: +SKIP
        MultiIndex([('A', 'X'),
                    ('A', 'Y')],
                   )
        """
        if level is not None:
            self._validate_index_level(level)
        scols = self._internal.index_spark_columns
        sdf = self._psdf._internal.spark_frame.select(scols).distinct()
        return DataFrame(
            InternalFrame(
                spark_frame=sdf,
                index_spark_columns=[
                    scol_for(sdf, col) for col in self._internal.index_spark_column_names
                ],
                index_names=self._internal.index_names,
                index_fields=self._internal.index_fields,
            )
        ).index

    # TODO: add error parameter
    def drop(self, labels: List[Any]) -> "Index":
        """
        Make new Index with passed list of labels deleted.

        Parameters
        ----------
        labels : array-like

        Returns
        -------
        dropped : Index

        Examples
        --------
        >>> index = ps.Index([1, 2, 3])
        >>> index
        Int64Index([1, 2, 3], dtype='int64')

        >>> index.drop([1])
        Int64Index([2, 3], dtype='int64')
        """
        internal = self._internal.resolved_copy
        sdf = internal.spark_frame[~internal.index_spark_columns[0].isin(labels)]

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
            column_labels=[],
            data_spark_columns=[],
            data_fields=[],
        )
        return DataFrame(internal).index

    def _validate_index_level(self, level: Union[int, Name]) -> None:
        """
        Validate index level.
        For single-level Index getting level number is a no-op, but some
        verification must be done like in MultiIndex.
        """
        if isinstance(level, int):
            if level < 0 and level != -1:
                raise IndexError(
                    "Too many levels: Index has only 1 level,"
                    " %d is not a valid level number" % (level,)
                )
            elif level > 0:
                raise IndexError("Too many levels:" " Index has only 1 level, not %d" % (level + 1))
        elif level != self.name:
            raise KeyError(
                "Requested level ({}) does not match index name ({})".format(level, self.name)
            )

    def get_level_values(self, level: Union[int, Name]) -> "Index":
        """
        Return Index if a valid level is given.

        Examples:
        --------
        >>> psidx = ps.Index(['a', 'b', 'c'], name='ks')
        >>> psidx.get_level_values(0)
        Index(['a', 'b', 'c'], dtype='object', name='ks')

        >>> psidx.get_level_values('ks')
        Index(['a', 'b', 'c'], dtype='object', name='ks')
        """
        self._validate_index_level(level)
        return self

    def copy(self, name: Optional[Name] = None, deep: Optional[bool] = None) -> "Index":
        """
        Make a copy of this object. name sets those attributes on the new object.

        Parameters
        ----------
        name : string, optional
            to set name of index
        deep : None
            this parameter is not supported but just dummy parameter to match pandas.

        Examples
        --------
        >>> df = ps.DataFrame([[1, 2], [4, 5], [7, 8]],
        ...                   index=['cobra', 'viper', 'sidewinder'],
        ...                   columns=['max_speed', 'shield'])
        >>> df
                    max_speed  shield
        cobra               1       2
        viper               4       5
        sidewinder          7       8
        >>> df.index
        Index(['cobra', 'viper', 'sidewinder'], dtype='object')

        Copy index

        >>> df.index.copy()
        Index(['cobra', 'viper', 'sidewinder'], dtype='object')

        Copy index with name

        >>> df.index.copy(name='snake')
        Index(['cobra', 'viper', 'sidewinder'], dtype='object', name='snake')
        """
        result = self._psdf[[]].index
        if name:
            result.name = name
        return result

    def droplevel(self, level: Union[int, Name, List[Union[int, Name]]]) -> "Index":
        """
        Return index with requested level(s) removed.
        If resulting index has only 1 level left, the result will be
        of Index type, not MultiIndex.

        Parameters
        ----------
        level : int, str, tuple, or list-like, default 0
            If a string is given, must be the name of a level
            If list-like, elements must be names or indexes of levels.

        Returns
        -------
        Index or MultiIndex

        Examples
        --------
        >>> midx = ps.DataFrame({'a': ['a', 'b']}, index=[['a', 'x'], ['b', 'y'], [1, 2]]).index
        >>> midx  # doctest: +SKIP
        MultiIndex([('a', 'b', 1),
                    ('x', 'y', 2)],
                   )
        >>> midx.droplevel([0, 1])  # doctest: +SKIP
        Int64Index([1, 2], dtype='int64')
        >>> midx.droplevel(0)  # doctest: +SKIP
        MultiIndex([('b', 1),
                    ('y', 2)],
                   )
        >>> midx.names = [("a", "b"), "b", "c"]
        >>> midx.droplevel([('a', 'b')])  # doctest: +SKIP
        MultiIndex([('b', 1),
                    ('y', 2)],
                   names=['b', 'c'])
        """
        names = self.names
        nlevels = self.nlevels
        if not is_list_like(level):
            levels = [cast(Union[int, Name], level)]
        else:
            levels = cast(List[Union[int, Name]], level)

        int_level = set()
        for n in levels:
            if isinstance(n, int):
                if n < 0:
                    n = n + nlevels
                    if n < 0:
                        raise IndexError(
                            "Too many levels: Index has only {} levels, "
                            "{} is not a valid level number".format(nlevels, (n - nlevels))
                        )
                if n >= nlevels:
                    raise IndexError(
                        "Too many levels: Index has only {} levels, not {}".format(nlevels, n + 1)
                    )
            else:
                if n not in names:
                    raise KeyError("Level {} not found".format(n))
                n = names.index(n)
            int_level.add(n)

        if len(levels) >= nlevels:
            raise ValueError(
                "Cannot remove {} levels from an index with {} "
                "levels: at least one level must be "
                "left.".format(len(levels), nlevels)
            )

        index_spark_columns, index_names, index_fields = zip(
            *[
                item
                for i, item in enumerate(
                    zip(
                        self._internal.index_spark_columns,
                        self._internal.index_names,
                        self._internal.index_fields,
                    )
                )
                if i not in int_level
            ]
        )

        internal = self._internal.copy(
            index_spark_columns=list(index_spark_columns),
            index_names=list(index_names),
            index_fields=list(index_fields),
            column_labels=[],
            data_spark_columns=[],
            data_fields=[],
        )
        return DataFrame(internal).index

    def symmetric_difference(
        self,
        other: "Index",
        result_name: Optional[Name] = None,
        sort: Optional[bool] = None,
    ) -> "Index":
        """
        Compute the symmetric difference of two Index objects.

        Parameters
        ----------
        other : Index or array-like
        result_name : str
        sort : True or None, default None
            Whether to sort the resulting index.
            * True : Attempt to sort the result.
            * None : Do not sort the result.

        Returns
        -------
        symmetric_difference : Index

        Notes
        -----
        ``symmetric_difference`` contains elements that appear in either
        ``idx1`` or ``idx2`` but not both. Equivalent to the Index created by
        ``idx1.difference(idx2) | idx2.difference(idx1)`` with duplicates
        dropped.

        Examples
        --------
        >>> s1 = ps.Series([1, 2, 3, 4], index=[1, 2, 3, 4])
        >>> s2 = ps.Series([1, 2, 3, 4], index=[2, 3, 4, 5])

        >>> s1.index.symmetric_difference(s2.index)  # doctest: +SKIP
        Int64Index([5, 1], dtype='int64')

        You can set name of result Index.

        >>> s1.index.symmetric_difference(s2.index, result_name='pandas-on-Spark')  # doctest: +SKIP
        Int64Index([5, 1], dtype='int64', name='pandas-on-Spark')

        You can set sort to `True`, if you want to sort the resulting index.

        >>> s1.index.symmetric_difference(s2.index, sort=True)
        Int64Index([1, 5], dtype='int64')

        You can also use the ``^`` operator:

        >>> s1.index ^ s2.index  # doctest: +SKIP
        Int64Index([5, 1], dtype='int64')
        """
        if type(self) != type(other):
            raise NotImplementedError(
                "Doesn't support symmetric_difference between Index & MultiIndex for now"
            )

        sdf_self = self._psdf._internal.spark_frame.select(self._internal.index_spark_columns)
        sdf_other = other._psdf._internal.spark_frame.select(other._internal.index_spark_columns)

        sdf_symdiff = sdf_self.union(sdf_other).subtract(sdf_self.intersect(sdf_other))

        if sort:
            sdf_symdiff = sdf_symdiff.sort(*self._internal.index_spark_column_names)

        internal = InternalFrame(
            spark_frame=sdf_symdiff,
            index_spark_columns=[
                scol_for(sdf_symdiff, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )
        result = DataFrame(internal).index

        if result_name:
            result.name = result_name

        return result

    # TODO: return_indexer
    def sort_values(self, ascending: bool = True) -> "Index":
        """
        Return a sorted copy of the index.

        .. note:: This method is not supported for pandas when index has NaN value.
                  pandas raises unexpected TypeError, but we support treating NaN
                  as the smallest value.

        Parameters
        ----------
        ascending : bool, default True
            Should the index values be sorted in an ascending order.

        Returns
        -------
        sorted_index : ps.Index or ps.MultiIndex
            Sorted copy of the index.

        See Also
        --------
        Series.sort_values : Sort values of a Series.
        DataFrame.sort_values : Sort values in a DataFrame.

        Examples
        --------
        >>> idx = ps.Index([10, 100, 1, 1000])
        >>> idx
        Int64Index([10, 100, 1, 1000], dtype='int64')

        Sort values in ascending order (default behavior).

        >>> idx.sort_values()
        Int64Index([1, 10, 100, 1000], dtype='int64')

        Sort values in descending order.

        >>> idx.sort_values(ascending=False)
        Int64Index([1000, 100, 10, 1], dtype='int64')

        Support for MultiIndex.

        >>> psidx = ps.MultiIndex.from_tuples([('a', 'x', 1), ('c', 'y', 2), ('b', 'z', 3)])
        >>> psidx  # doctest: +SKIP
        MultiIndex([('a', 'x', 1),
                    ('c', 'y', 2),
                    ('b', 'z', 3)],
                   )

        >>> psidx.sort_values()  # doctest: +SKIP
        MultiIndex([('a', 'x', 1),
                    ('b', 'z', 3),
                    ('c', 'y', 2)],
                   )

        >>> psidx.sort_values(ascending=False)  # doctest: +SKIP
        MultiIndex([('c', 'y', 2),
                    ('b', 'z', 3),
                    ('a', 'x', 1)],
                   )
        """
        sdf = self._internal.spark_frame
        sdf = sdf.orderBy(*self._internal.index_spark_columns, ascending=ascending).select(
            self._internal.index_spark_columns
        )

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )
        return DataFrame(internal).index

    @no_type_check
    def sort(self, *args, **kwargs) -> None:
        """
        Use sort_values instead.
        """
        raise TypeError("cannot sort an Index object in-place, use sort_values instead")

    def min(self) -> Union[Scalar, Tuple[Scalar, ...]]:
        """
        Return the minimum value of the Index.

        Returns
        -------
        scalar
            Minimum value.

        See Also
        --------
        Index.max : Return the maximum value of the object.
        Series.min : Return the minimum value in a Series.
        DataFrame.min : Return the minimum values in a DataFrame.

        Examples
        --------
        >>> idx = ps.Index([3, 2, 1])
        >>> idx.min()
        1

        >>> idx = ps.Index(['c', 'b', 'a'])
        >>> idx.min()
        'a'

        For a MultiIndex, the maximum is determined lexicographically.

        >>> idx = ps.MultiIndex.from_tuples([('a', 'x', 1), ('b', 'y', 2)])
        >>> idx.min()
        ('a', 'x', 1)
        """
        sdf = self._internal.spark_frame
        min_row = cast(
            pd.DataFrame,
            sdf.select(F.min(F.struct(*self._internal.index_spark_columns)).alias("min_row"))
            .select("min_row.*")
            .toPandas(),
        )
        result = tuple(min_row.iloc[0])

        return result if len(result) > 1 else result[0]

    def max(self) -> Union[Scalar, Tuple[Scalar, ...]]:
        """
        Return the maximum value of the Index.

        Returns
        -------
        scalar
            Maximum value.

        See Also
        --------
        Index.min : Return the minimum value in an Index.
        Series.max : Return the maximum value in a Series.
        DataFrame.max : Return the maximum values in a DataFrame.

        Examples
        --------
        >>> idx = ps.Index([3, 2, 1])
        >>> idx.max()
        3

        >>> idx = ps.Index(['c', 'b', 'a'])
        >>> idx.max()
        'c'

        For a MultiIndex, the maximum is determined lexicographically.

        >>> idx = ps.MultiIndex.from_tuples([('a', 'x', 1), ('b', 'y', 2)])
        >>> idx.max()
        ('b', 'y', 2)
        """
        sdf = self._internal.spark_frame
        max_row = cast(
            pd.DataFrame,
            sdf.select(F.max(F.struct(*self._internal.index_spark_columns)).alias("max_row"))
            .select("max_row.*")
            .toPandas(),
        )
        result = tuple(max_row.iloc[0])

        return result if len(result) > 1 else result[0]

    def delete(self, loc: Union[int, List[int]]) -> "Index":
        """
        Make new Index with passed location(-s) deleted.

        .. note:: this API can be pretty expensive since it is based on
             a global sequence internally.

        Returns
        -------
        new_index : Index

        Examples
        --------
        >>> psidx = ps.Index([10, 10, 9, 8, 4, 2, 4, 4, 2, 2, 10, 10])
        >>> psidx
        Int64Index([10, 10, 9, 8, 4, 2, 4, 4, 2, 2, 10, 10], dtype='int64')

        >>> psidx.delete(0).sort_values()
        Int64Index([2, 2, 2, 4, 4, 4, 8, 9, 10, 10, 10], dtype='int64')

        >>> psidx.delete([0, 1, 2, 3, 10, 11]).sort_values()
        Int64Index([2, 2, 2, 4, 4, 4], dtype='int64')

        MultiIndex

        >>> psidx = ps.MultiIndex.from_tuples([('a', 'x', 1), ('b', 'y', 2), ('c', 'z', 3)])
        >>> psidx  # doctest: +SKIP
        MultiIndex([('a', 'x', 1),
                    ('b', 'y', 2),
                    ('c', 'z', 3)],
                   )

        >>> psidx.delete([0, 2]).sort_values()  # doctest: +SKIP
        MultiIndex([('b', 'y', 2)],
                   )
        """
        length = len(self)

        def is_len_exceeded(index: int) -> bool:
            """Check if the given index is exceeded the length or not"""
            return index >= length if index >= 0 else abs(index) > length

        if not is_list_like(loc):
            if is_len_exceeded(cast(int, loc)):
                raise IndexError(
                    "index {} is out of bounds for axis 0 with size {}".format(loc, length)
                )
            locs = [cast(int, loc)]
        else:
            for index in cast(List[int], loc):
                if is_len_exceeded(index):
                    raise IndexError(
                        "index {} is out of bounds for axis 0 with size {}".format(index, length)
                    )
            locs = cast(List[int], loc)

        locs = [int(item) for item in locs]
        locs = [item if item >= 0 else length + item for item in locs]

        # we need a temporary column such as '__index_value_0__'
        # since 'InternalFrame.attach_default_index' will be failed
        # when self._scol has name of '__index_level_0__'
        index_value_column_format = "__index_value_{}__"

        sdf = self._internal._sdf
        index_value_column_names = [
            verify_temp_column_name(sdf, index_value_column_format.format(i))
            for i in range(self._internal.index_level)
        ]
        index_value_columns = [
            index_scol.alias(index_vcol_name)
            for index_scol, index_vcol_name in zip(
                self._internal.index_spark_columns, index_value_column_names
            )
        ]
        sdf = sdf.select(index_value_columns)

        sdf = InternalFrame.attach_default_index(sdf, default_index_type="distributed-sequence")
        # sdf here looks as below
        # +-----------------+-----------------+-----------------+-----------------+
        # |__index_level_0__|__index_value_0__|__index_value_1__|__index_value_2__|
        # +-----------------+-----------------+-----------------+-----------------+
        # |                0|                a|                x|                1|
        # |                1|                b|                y|                2|
        # |                2|                c|                z|                3|
        # +-----------------+-----------------+-----------------+-----------------+

        # delete rows which are matched with given `loc`
        sdf = sdf.where(~F.col(SPARK_INDEX_NAME_FORMAT(0)).isin(locs))
        sdf = sdf.select(index_value_column_names)
        # sdf here looks as below, we should alias them back to origin spark column names
        # +-----------------+-----------------+-----------------+
        # |__index_value_0__|__index_value_1__|__index_value_2__|
        # +-----------------+-----------------+-----------------+
        # |                c|                z|                3|
        # +-----------------+-----------------+-----------------+
        index_origin_columns = [
            F.col(index_vcol_name).alias(index_scol_name)
            for index_vcol_name, index_scol_name in zip(
                index_value_column_names, self._internal.index_spark_column_names
            )
        ]
        sdf = sdf.select(index_origin_columns)

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )

        return DataFrame(internal).index

    def append(self, other: "Index") -> "Index":
        """
        Append a collection of Index options together.

        Parameters
        ----------
        other : Index

        Returns
        -------
        appended : Index

        Examples
        --------
        >>> psidx = ps.Index([10, 5, 0, 5, 10, 5, 0, 10])
        >>> psidx
        Int64Index([10, 5, 0, 5, 10, 5, 0, 10], dtype='int64')

        >>> psidx.append(psidx)
        Int64Index([10, 5, 0, 5, 10, 5, 0, 10, 10, 5, 0, 5, 10, 5, 0, 10], dtype='int64')

        Support for MiltiIndex

        >>> psidx = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y')])
        >>> psidx  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y')],
                   )

        >>> psidx.append(psidx)  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y'),
                    ('a', 'x'),
                    ('b', 'y')],
                   )
        """
        from pyspark.pandas.indexes.multi import MultiIndex

        if isinstance(self, MultiIndex) != isinstance(other, MultiIndex):
            raise NotImplementedError(
                "append() between Index & MultiIndex is currently not supported"
            )
        if self._internal.index_level != other._internal.index_level:
            raise NotImplementedError(
                "append() between MultiIndexs with different levels is currently not supported"
            )

        index_fields = self._index_fields_for_union_like(other, func_name="append")

        sdf_self = self._internal.spark_frame.select(self._internal.index_spark_columns)
        sdf_other = other._internal.spark_frame.select(other._internal.index_spark_columns)
        sdf_appended = sdf_self.union(sdf_other)

        # names should be kept when MultiIndex, but Index wouldn't keep its name.
        if isinstance(self, MultiIndex):
            index_names = self._internal.index_names
        else:
            index_names = None

        internal = InternalFrame(
            spark_frame=sdf_appended,
            index_spark_columns=[
                scol_for(sdf_appended, col) for col in self._internal.index_spark_column_names
            ],
            index_names=index_names,
            index_fields=index_fields,
        )

        return DataFrame(internal).index

    def argmax(self) -> int:
        """
        Return a maximum argument indexer.

        Parameters
        ----------
        skipna : bool, default True

        Returns
        -------
        maximum argument indexer

        Examples
        --------
        >>> psidx = ps.Index([10, 9, 8, 7, 100, 5, 4, 3, 100, 3])
        >>> psidx
        Int64Index([10, 9, 8, 7, 100, 5, 4, 3, 100, 3], dtype='int64')

        >>> psidx.argmax()
        4
        """
        sdf = self._internal.spark_frame.select(self.spark.column)
        sequence_col = verify_temp_column_name(sdf, "__distributed_sequence_column__")
        sdf = InternalFrame.attach_distributed_sequence_column(sdf, column_name=sequence_col)
        # spark_frame here looks like below
        # +-----------------+---------------+
        # |__index_level_0__|__index_value__|
        # +-----------------+---------------+
        # |                0|             10|
        # |                4|            100|
        # |                2|              8|
        # |                3|              7|
        # |                6|              4|
        # |                5|              5|
        # |                7|              3|
        # |                8|            100|
        # |                1|              9|
        # +-----------------+---------------+

        return (
            sdf.orderBy(
                scol_for(sdf, self._internal.data_spark_column_names[0]).desc(),
                F.col(sequence_col).asc(),
            )
            .select(sequence_col)
            .first()[0]
        )

    def argmin(self) -> int:
        """
        Return a minimum argument indexer.

        Parameters
        ----------
        skipna : bool, default True

        Returns
        -------
        minimum argument indexer

        Examples
        --------
        >>> psidx = ps.Index([10, 9, 8, 7, 100, 5, 4, 3, 100, 3])
        >>> psidx
        Int64Index([10, 9, 8, 7, 100, 5, 4, 3, 100, 3], dtype='int64')

        >>> psidx.argmin()
        7
        """
        sdf = self._internal.spark_frame.select(self.spark.column)
        sequence_col = verify_temp_column_name(sdf, "__distributed_sequence_column__")
        sdf = InternalFrame.attach_distributed_sequence_column(sdf, column_name=sequence_col)

        return (
            sdf.orderBy(
                scol_for(sdf, self._internal.data_spark_column_names[0]).asc(),
                F.col(sequence_col).asc(),
            )
            .select(sequence_col)
            .first()[0]
        )

    def set_names(
        self,
        names: Union[Name, List[Name]],
        level: Optional[Union[int, Name, List[Union[int, Name]]]] = None,
        inplace: bool = False,
    ) -> Optional["Index"]:
        """
        Set Index or MultiIndex name.
        Able to set new names partially and by level.

        Parameters
        ----------
        names : label or list of label
            Name(s) to set.
        level : int, label or list of int or label, optional
            If the index is a MultiIndex, level(s) to set (None for all
            levels). Otherwise level must be None.
        inplace : bool, default False
            Modifies the object directly, instead of creating a new Index or
            MultiIndex.

        Returns
        -------
        Index
            The same type as the caller or None if inplace is True.

        See Also
        --------
        Index.rename : Able to set new names without level.

        Examples
        --------
        >>> idx = ps.Index([1, 2, 3, 4])
        >>> idx
        Int64Index([1, 2, 3, 4], dtype='int64')

        >>> idx.set_names('quarter')
        Int64Index([1, 2, 3, 4], dtype='int64', name='quarter')

        For MultiIndex

        >>> idx = ps.MultiIndex.from_tuples([('a', 'x'), ('b', 'y')])
        >>> idx  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y')],
                   )

        >>> idx.set_names(['kind', 'year'], inplace=True)
        >>> idx  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y')],
                   names=['kind', 'year'])

        >>> idx.set_names('species', level=0)  # doctest: +SKIP
        MultiIndex([('a', 'x'),
                    ('b', 'y')],
                   names=['species', 'year'])
        """
        from pyspark.pandas.indexes.multi import MultiIndex

        if isinstance(self, MultiIndex):
            if level is not None:
                self_names = self.names
                self_names[level] = names  # type: ignore
                names = self_names
        return self.rename(name=names, inplace=inplace)

    def difference(self, other: "Index", sort: Optional[bool] = None) -> "Index":
        """
        Return a new Index with elements from the index that are not in
        `other`.

        This is the set difference of two Index objects.

        Parameters
        ----------
        other : Index or array-like
        sort : True or None, default None
            Whether to sort the resulting index.
            * True : Attempt to sort the result.
            * None : Do not sort the result.

        Returns
        -------
        difference : Index

        Examples
        --------

        >>> idx1 = ps.Index([2, 1, 3, 4])
        >>> idx2 = ps.Index([3, 4, 5, 6])
        >>> idx1.difference(idx2, sort=True)
        Int64Index([1, 2], dtype='int64')

        MultiIndex

        >>> midx1 = ps.MultiIndex.from_tuples([('a', 'x', 1), ('b', 'y', 2), ('c', 'z', 3)])
        >>> midx2 = ps.MultiIndex.from_tuples([('a', 'x', 1), ('b', 'z', 2), ('k', 'z', 3)])
        >>> midx1.difference(midx2)  # doctest: +SKIP
        MultiIndex([('b', 'y', 2),
                    ('c', 'z', 3)],
                   )
        """
        from pyspark.pandas.indexes.multi import MultiIndex

        # Check if the `self` and `other` have different index types.
        # 1. `self` is Index, `other` is MultiIndex
        # 2. `self` is MultiIndex, `other` is Index
        is_index_types_different = isinstance(other, Index) and not isinstance(self, type(other))
        if is_index_types_different:
            if isinstance(self, MultiIndex):
                # In case `self` is MultiIndex and `other` is Index,
                # return MultiIndex without its names.
                return self.rename([None] * len(self))
            elif isinstance(self, Index):
                # In case `self` is Index and `other` is MultiIndex,
                # return Index without its name.
                return self.rename(None)

        if not isinstance(other, (Index, Series, tuple, list, set, dict)):
            raise TypeError("Input must be Index or array-like")
        if not isinstance(sort, (type(None), type(True))):
            raise ValueError(
                "The 'sort' keyword only takes the values of None or True; {} was passed.".format(
                    sort
                )
            )
        # Handling MultiIndex when `other` is not MultiIndex.
        if isinstance(self, MultiIndex) and not isinstance(other, MultiIndex):
            is_other_list_of_tuples = isinstance(other, (list, set, dict)) and all(
                [isinstance(item, tuple) for item in other]
            )
            if is_other_list_of_tuples:
                other = MultiIndex.from_tuples(other)  # type: ignore
            else:
                raise TypeError("other must be a MultiIndex or a list of tuples")

        if not isinstance(other, Index):
            other = Index(other)

        sdf_self = self._internal.spark_frame
        sdf_other = other._internal.spark_frame
        idx_self = self._internal.index_spark_columns
        idx_other = other._internal.index_spark_columns
        sdf_diff = sdf_self.select(idx_self).subtract(sdf_other.select(idx_other))
        internal = InternalFrame(
            spark_frame=sdf_diff,
            index_spark_columns=[
                scol_for(sdf_diff, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=self._internal.index_fields,
        )
        result = DataFrame(internal).index
        # Name(s) will be kept when only name(s) of (Multi)Index are the same.
        if isinstance(self, type(other)) and isinstance(self, MultiIndex):
            if self.names == other.names:
                result.names = self.names
        elif isinstance(self, type(other)) and not isinstance(self, MultiIndex):
            if self.name == other.name:
                result.name = self.name
        return result if sort is None else result.sort_values()

    @property
    def is_all_dates(self) -> bool:
        """
        Return if all data types of the index are datetime.
        remember that since pandas-on-Spark does not support multiple data types in an index,
        so it returns True if any type of data is datetime.

        Examples
        --------
        >>> from datetime import datetime

        >>> idx = ps.Index([datetime(2019, 1, 1, 0, 0, 0), datetime(2019, 2, 3, 0, 0, 0)])
        >>> idx
        DatetimeIndex(['2019-01-01', '2019-02-03'], dtype='datetime64[ns]', freq=None)

        >>> idx.is_all_dates
        True

        >>> idx = ps.Index([datetime(2019, 1, 1, 0, 0, 0), None])
        >>> idx
        DatetimeIndex(['2019-01-01', 'NaT'], dtype='datetime64[ns]', freq=None)

        >>> idx.is_all_dates
        True

        >>> idx = ps.Index([0, 1, 2])
        >>> idx
        Int64Index([0, 1, 2], dtype='int64')

        >>> idx.is_all_dates
        False
        """
        return isinstance(self.spark.data_type, (TimestampType, TimestampNTZType))

    def repeat(self, repeats: int) -> "Index":
        """
        Repeat elements of a Index/MultiIndex.

        Returns a new Index/MultiIndex where each element of the current Index/MultiIndex
        is repeated consecutively a given number of times.

        Parameters
        ----------
        repeats : int
            The number of repetitions for each element. This should be a
            non-negative integer. Repeating 0 times will return an empty
            Index.

        Returns
        -------
        repeated_index : Index/MultiIndex
            Newly created Index/MultiIndex with repeated elements.

        See Also
        --------
        Series.repeat : Equivalent function for Series.

        Examples
        --------
        >>> idx = ps.Index(['a', 'b', 'c'])
        >>> idx
        Index(['a', 'b', 'c'], dtype='object')
        >>> idx.repeat(2)
        Index(['a', 'b', 'c', 'a', 'b', 'c'], dtype='object')

        For MultiIndex,

        >>> midx = ps.MultiIndex.from_tuples([('x', 'a'), ('x', 'b'), ('y', 'c')])
        >>> midx  # doctest: +SKIP
        MultiIndex([('x', 'a'),
                    ('x', 'b'),
                    ('y', 'c')],
                   )
        >>> midx.repeat(2)  # doctest: +SKIP
        MultiIndex([('x', 'a'),
                    ('x', 'b'),
                    ('y', 'c'),
                    ('x', 'a'),
                    ('x', 'b'),
                    ('y', 'c')],
                   )
        >>> midx.repeat(0)  # doctest: +SKIP
        MultiIndex([], )
        """
        if not isinstance(repeats, int):
            raise TypeError(
                "`repeats` argument must be integer, but got {}".format(type(repeats).__name__)
            )
        elif repeats < 0:
            raise ValueError("negative dimensions are not allowed")

        psdf = DataFrame(self._internal.resolved_copy)  # type: DataFrame
        if repeats == 0:
            return DataFrame(psdf._internal.with_filter(SF.lit(False))).index
        else:
            return ps.concat([psdf] * repeats).index

    def asof(self, label: Any) -> Scalar:
        """
        Return the label from the index, or, if not present, the previous one.

        Assuming that the index is sorted, return the passed index label if it
        is in the index, or return the previous index label if the passed one
        is not in the index.

        .. note:: This API is dependent on :meth:`Index.is_monotonic_increasing`
            which can be expensive.

        Parameters
        ----------
        label : object
            The label up to which the method returns the latest index label.

        Returns
        -------
        object
            The passed label if it is in the index. The previous label if the
            passed label is not in the sorted index or `NaN` if there is no
            such label.

        Examples
        --------
        `Index.asof` returns the latest index label up to the passed label.

        >>> idx = ps.Index(['2013-12-31', '2014-01-02', '2014-01-03'])
        >>> idx.asof('2014-01-01')
        '2013-12-31'

        If the label is in the index, the method returns the passed label.

        >>> idx.asof('2014-01-02')
        '2014-01-02'

        If all of the labels in the index are later than the passed label,
        NaN is returned.

        >>> idx.asof('1999-01-02')
        nan
        """
        sdf = self._internal.spark_frame
        if self.is_monotonic_increasing:
            sdf = sdf.where(self.spark.column <= SF.lit(label).cast(self.spark.data_type)).select(
                F.max(self.spark.column)
            )
        elif self.is_monotonic_decreasing:
            sdf = sdf.where(self.spark.column >= SF.lit(label).cast(self.spark.data_type)).select(
                F.min(self.spark.column)
            )
        else:
            raise ValueError("index must be monotonic increasing or decreasing")

        result = cast(pd.DataFrame, sdf.toPandas()).iloc[0, 0]
        return result if result is not None else np.nan

    def _index_fields_for_union_like(
        self: "Index", other: "Index", func_name: str
    ) -> Optional[List[InternalField]]:
        if self._internal.index_fields == other._internal.index_fields:
            return self._internal.index_fields
        elif all(
            left.dtype == right.dtype
            and (isinstance(left.dtype, CategoricalDtype) or left.spark_type == right.spark_type)
            for left, right in zip(self._internal.index_fields, other._internal.index_fields)
        ):
            return [
                left.copy(nullable=left.nullable or right.nullable)
                if left.spark_type == right.spark_type
                else InternalField(dtype=left.dtype)
                for left, right in zip(self._internal.index_fields, other._internal.index_fields)
            ]
        elif any(
            isinstance(field.dtype, CategoricalDtype)
            for field in self._internal.index_fields + other._internal.index_fields
        ):
            # TODO: non-categorical or categorical with different categories
            raise NotImplementedError(
                "{}() between CategoricalIndex and non-categorical or "
                "categorical with different categories is currently not supported".format(func_name)
            )
        else:
            return None

    def union(
        self, other: Union[DataFrame, Series, "Index", List], sort: Optional[bool] = None
    ) -> "Index":
        """
        Form the union of two Index objects.

        Parameters
        ----------
        other : Index or array-like
        sort : bool or None, default None
            Whether to sort the resulting Index.

        Returns
        -------
        union : Index

        Examples
        --------

        Index

        >>> idx1 = ps.Index([1, 2, 3, 4])
        >>> idx2 = ps.Index([3, 4, 5, 6])
        >>> idx1.union(idx2).sort_values()
        Int64Index([1, 2, 3, 4, 5, 6], dtype='int64')

        MultiIndex

        >>> midx1 = ps.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")])
        >>> midx2 = ps.MultiIndex.from_tuples([("x", "c"), ("x", "d"), ("x", "e"), ("x", "f")])
        >>> midx1.union(midx2).sort_values()  # doctest: +SKIP
        MultiIndex([('x', 'a'),
                    ('x', 'b'),
                    ('x', 'c'),
                    ('x', 'd'),
                    ('x', 'e'),
                    ('x', 'f')],
                   )
        """
        from pyspark.pandas.indexes.multi import MultiIndex

        sort = True if sort is None else sort
        sort = validate_bool_kwarg(sort, "sort")
        if isinstance(self, MultiIndex):
            if isinstance(other, MultiIndex):
                other_idx = other  # type: Index
            elif isinstance(other, list) and all(isinstance(item, tuple) for item in other):
                other_idx = MultiIndex.from_tuples(other)
            else:
                raise TypeError("other must be a MultiIndex or a list of tuples")
        else:
            if isinstance(other, MultiIndex):
                # TODO: We can't support different type of values in a single column for now.
                raise NotImplementedError("Union between Index and MultiIndex is not yet supported")
            elif isinstance(other, DataFrame):
                raise ValueError("Index data must be 1-dimensional")
            else:
                other_idx = Index(other)

        index_fields = self._index_fields_for_union_like(other_idx, func_name="union")

        sdf_self = self._internal.spark_frame.select(self._internal.index_spark_columns)
        sdf_other = other_idx._internal.spark_frame.select(other_idx._internal.index_spark_columns)
        sdf = sdf_self.unionAll(sdf_other).exceptAll(sdf_self.intersectAll(sdf_other))
        if sort:
            sdf = sdf.sort(*self._internal.index_spark_column_names)

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[
                scol_for(sdf, col) for col in self._internal.index_spark_column_names
            ],
            index_names=self._internal.index_names,
            index_fields=index_fields,
        )

        return DataFrame(internal).index

    def holds_integer(self) -> bool:
        """
        Whether the type is an integer type.
        Always return False for MultiIndex.

        Notes
        -----
        When Index contains null values the result can be different with pandas
        since pandas-on-Spark cast integer to float when Index contains null values.

        >>> ps.Index([1, 2, 3, None])
        Float64Index([1.0, 2.0, 3.0, nan], dtype='float64')

        Examples
        --------
        >>> psidx = ps.Index([1, 2, 3, 4])
        >>> psidx.holds_integer()
        True

        Returns False for string type.

        >>> psidx = ps.Index(["A", "B", "C", "D"])
        >>> psidx.holds_integer()
        False

        Returns False for float type.

        >>> psidx = ps.Index([1.1, 2.2, 3.3, 4.4])
        >>> psidx.holds_integer()
        False
        """
        return isinstance(self.spark.data_type, IntegralType)

    def intersection(self, other: Union[DataFrame, Series, "Index", List]) -> "Index":
        """
        Form the intersection of two Index objects.

        This returns a new Index with elements common to the index and `other`.

        Parameters
        ----------
        other : Index or array-like

        Returns
        -------
        intersection : Index

        Examples
        --------
        >>> idx1 = ps.Index([1, 2, 3, 4])
        >>> idx2 = ps.Index([3, 4, 5, 6])
        >>> idx1.intersection(idx2).sort_values()
        Int64Index([3, 4], dtype='int64')
        """
        from pyspark.pandas.indexes.multi import MultiIndex

        if isinstance(other, DataFrame):
            raise ValueError("Index data must be 1-dimensional")
        elif isinstance(other, MultiIndex):
            # Always returns a no-named empty Index if `other` is MultiIndex.
            return self._psdf.head(0).index.rename(None)
        elif isinstance(other, Index):
            spark_frame_other = other.to_frame().to_spark()
            keep_name = self.name == other.name
        elif isinstance(other, Series):
            spark_frame_other = other.to_frame().to_spark()
            keep_name = True
        elif is_list_like(other):
            other = Index(other)
            if isinstance(other, MultiIndex):
                return other.to_frame().head(0).index
            spark_frame_other = other.to_frame().to_spark()
            keep_name = True
        else:
            raise TypeError("Input must be Index or array-like")

        index_fields = self._index_fields_for_union_like(other, func_name="intersection")

        spark_frame_self = self.to_frame(name=SPARK_DEFAULT_INDEX_NAME).to_spark()
        spark_frame_intersected = spark_frame_self.intersect(spark_frame_other)
        if keep_name:
            index_names = self._internal.index_names
        else:
            index_names = None

        internal = InternalFrame(
            spark_frame=spark_frame_intersected,
            index_spark_columns=[scol_for(spark_frame_intersected, SPARK_DEFAULT_INDEX_NAME)],
            index_names=index_names,
            index_fields=index_fields,
        )

        return DataFrame(internal).index

    def item(self) -> Union[Scalar, Tuple[Scalar, ...]]:
        """
        Return the first element of the underlying data as a python scalar.

        Returns
        -------
        scalar
            The first element of Index.

        Raises
        ------
        ValueError
            If the data is not length-1.

        Examples
        --------
        >>> psidx = ps.Index([10])
        >>> psidx.item()
        10
        """
        return self.to_series().item()

    def insert(self, loc: int, item: Any) -> "Index":
        """
        Make new Index inserting new item at location.

        Follows Python list.append semantics for negative values.

        Parameters
        ----------
        loc : int
        item : object

        Returns
        -------
        new_index : Index

        Examples
        --------
        >>> psidx = ps.Index([1, 2, 3, 4, 5])
        >>> psidx.insert(3, 100)
        Int64Index([1, 2, 3, 100, 4, 5], dtype='int64')

        For negative values

        >>> psidx = ps.Index([1, 2, 3, 4, 5])
        >>> psidx.insert(-3, 100)
        Int64Index([1, 2, 100, 3, 4, 5], dtype='int64')
        """
        if loc < 0:
            length = len(self)
            loc = loc + length
            loc = 0 if loc < 0 else loc

        index_name = self._internal.index_spark_column_names[0]
        sdf_before = self.to_frame(name=index_name)[:loc].to_spark()
        sdf_middle = Index([item], dtype=self.dtype).to_frame(name=index_name).to_spark()
        sdf_after = self.to_frame(name=index_name)[loc:].to_spark()
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

    def view(self) -> "Index":
        """
        this is defined as a copy with the same identity
        """
        return self.copy()

    def to_list(self) -> List:
        """
        Return a list of the values.

        These are each a scalar type, which is a Python scalar
        (for str, int, float) or a pandas scalar
        (for Timestamp/Timedelta/Interval/Period)

        .. note:: This method should only be used if the resulting list is expected
            to be small, as all the data is loaded into the driver's memory.

        Examples
        --------
        Index

        >>> idx = ps.Index([1, 2, 3, 4, 5])
        >>> idx.to_list()
        [1, 2, 3, 4, 5]

        MultiIndex

        >>> tuples = [(1, 'red'), (1, 'blue'), (2, 'red'), (2, 'green')]
        >>> midx = ps.MultiIndex.from_tuples(tuples)
        >>> midx.to_list()
        [(1, 'red'), (1, 'blue'), (2, 'red'), (2, 'green')]
        """
        return self._to_internal_pandas().tolist()

    tolist = to_list

    @property
    def inferred_type(self) -> str:
        """
        Return a string of the type inferred from the values.

        Examples
        --------
        >>> from datetime import datetime
        >>> ps.Index([1, 2, 3]).inferred_type
        'integer'

        >>> ps.Index([1.0, 2.0, 3.0]).inferred_type
        'floating'

        >>> ps.Index(['a', 'b', 'c']).inferred_type
        'string'

        >>> ps.Index([True, False, True, False]).inferred_type
        'boolean'
        """
        return lib.infer_dtype([self.to_series().head(1).item()])

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeIndex, item):
            property_or_func = getattr(MissingPandasLikeIndex, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)  # type: ignore
            else:
                return partial(property_or_func, self)
        raise AttributeError("'{}' object has no attribute '{}'".format(type(self).__name__, item))

    def __repr__(self) -> str:
        max_display_count = get_option("display.max_rows")
        if max_display_count is None:
            return repr(self._to_internal_pandas())

        pindex = self._psdf._get_or_create_repr_pandas_cache(max_display_count).index

        pindex_length = len(pindex)
        repr_string = repr(pindex[:max_display_count])

        if pindex_length > max_display_count:
            footer = "\nShowing only the first {}".format(max_display_count)
            return repr_string + footer
        return repr_string

    def __iter__(self) -> Iterator:
        return MissingPandasLikeIndex.__iter__(self)

    def __and__(self, other: "Index") -> "Index":
        warnings.warn(
            "Index.__and__ operating as a set operation is deprecated, "
            "in the future this will be a logical operation matching Series.__and__.  "
            "Use index.intersection(other) instead",
            FutureWarning,
        )
        return self.intersection(other)

    def __or__(self, other: "Index") -> "Index":
        warnings.warn(
            "Index.__or__ operating as a set operation is deprecated, "
            "in the future this will be a logical operation matching Series.__or__.  "
            "Use index.union(other) instead",
            FutureWarning,
        )
        return self.union(other)

    def __xor__(self, other: "Index") -> "Index":
        warnings.warn(
            "Index.__xor__ operating as a set operation is deprecated, "
            "in the future this will be a logical operation matching Series.__xor__.  "
            "Use index.symmetric_difference(other) instead",
            FutureWarning,
        )
        return self.symmetric_difference(other)

    def __rxor__(self, other: Any) -> "Index":
        return NotImplemented

    def __bool__(self) -> bool:
        raise ValueError(
            "The truth value of a {0} is ambiguous. "
            "Use a.empty, a.bool(), a.item(), a.any() or a.all().".format(self.__class__.__name__)
        )


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.indexes.base

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.indexes.base.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.indexes.base tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.indexes.base,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
