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
from typing import Any, Optional, cast, no_type_check

import pandas as pd
from pandas.api.types import is_hashable, CategoricalDtype

from pyspark import pandas as ps
from pyspark.pandas.indexes.base import Index
from pyspark.pandas.internal import InternalField
from pyspark.pandas.missing.indexes import MissingPandasLikeCategoricalIndex
from pyspark.pandas.series import Series
from pyspark.sql.types import StructField


class CategoricalIndex(Index):
    """
    Index based on an underlying `Categorical`.

    CategoricalIndex can only take on a limited,
    and usually fixed, number of possible values (`categories`). Also,
    it might have an order, but numerical operations
    (additions, divisions, ...) are not possible.

    Parameters
    ----------
    data : array-like (1-dimensional)
        The values of the categorical. If `categories` are given, values not in
        `categories` will be replaced with NaN.
    categories : index-like, optional
        The categories for the categorical. Items need to be unique.
        If the categories are not given here (and also not in `dtype`), they
        will be inferred from the `data`.
    ordered : bool, optional
        Whether or not this categorical is treated as an ordered
        categorical. If not given here or in `dtype`, the resulting
        categorical will be unordered.
    dtype : CategoricalDtype or "category", optional
        If :class:`CategoricalDtype`, cannot be used together with
        `categories` or `ordered`.
    copy : bool, default False
        Make a copy of input ndarray.
    name : object, optional
        Name to be stored in the index.

    See Also
    --------
    Index : The base pandas-on-Spark Index type.

    Examples
    --------
    >>> ps.CategoricalIndex(["a", "b", "c", "a", "b", "c"])  # doctest: +NORMALIZE_WHITESPACE
    CategoricalIndex(['a', 'b', 'c', 'a', 'b', 'c'],
                     categories=['a', 'b', 'c'], ordered=False, dtype='category')

    ``CategoricalIndex`` can also be instantiated from a ``Categorical``:

    >>> c = pd.Categorical(["a", "b", "c", "a", "b", "c"])
    >>> ps.CategoricalIndex(c)  # doctest: +NORMALIZE_WHITESPACE
    CategoricalIndex(['a', 'b', 'c', 'a', 'b', 'c'],
                     categories=['a', 'b', 'c'], ordered=False, dtype='category')

    Ordered ``CategoricalIndex`` can have a min and max value.

    >>> ci = ps.CategoricalIndex(
    ...     ["a", "b", "c", "a", "b", "c"], ordered=True, categories=["c", "b", "a"]
    ... )
    >>> ci  # doctest: +NORMALIZE_WHITESPACE
    CategoricalIndex(['a', 'b', 'c', 'a', 'b', 'c'],
                     categories=['c', 'b', 'a'], ordered=True, dtype='category')

    From a Series:

    >>> s = ps.Series(["a", "b", "c", "a", "b", "c"], index=[10, 20, 30, 40, 50, 60])
    >>> ps.CategoricalIndex(s)  # doctest: +NORMALIZE_WHITESPACE
    CategoricalIndex(['a', 'b', 'c', 'a', 'b', 'c'],
                     categories=['a', 'b', 'c'], ordered=False, dtype='category')

    From an Index:

    >>> idx = ps.Index(["a", "b", "c", "a", "b", "c"])
    >>> ps.CategoricalIndex(idx)  # doctest: +NORMALIZE_WHITESPACE
    CategoricalIndex(['a', 'b', 'c', 'a', 'b', 'c'],
                     categories=['a', 'b', 'c'], ordered=False, dtype='category')
    """

    @no_type_check
    def __new__(cls, data=None, categories=None, ordered=None, dtype=None, copy=False, name=None):
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, (Series, Index)):
            if dtype is None:
                dtype = "category"
            return Index(data, dtype=dtype, copy=copy, name=name)

        return ps.from_pandas(
            pd.CategoricalIndex(
                data=data, categories=categories, ordered=ordered, dtype=dtype, name=name
            )
        )

    @property
    def dtype(self) -> CategoricalDtype:
        return cast(CategoricalDtype, super().dtype)

    @property
    def codes(self) -> Index:
        """
        The category codes of this categorical.

        Codes are an Index of integers which are the positions of the actual
        values in the categories Index.

        There is no setter, use the other categorical methods and the normal item
        setter to change values in the categorical.

        Returns
        -------
        Index
            A non-writable view of the `codes` Index.

        Examples
        --------
        >>> idx = ps.CategoricalIndex(list("abbccc"))
        >>> idx  # doctest: +NORMALIZE_WHITESPACE
        CategoricalIndex(['a', 'b', 'b', 'c', 'c', 'c'],
                         categories=['a', 'b', 'c'], ordered=False, dtype='category')

        >>> idx.codes
        Int64Index([0, 1, 1, 2, 2, 2], dtype='int64')
        """
        return self._with_new_scol(
            self.spark.column,
            field=InternalField.from_struct_field(
                StructField(
                    name=self._internal.index_spark_column_names[0],
                    dataType=self.spark.data_type,
                    nullable=self.spark.nullable,
                )
            ),
        ).rename(None)

    @property
    def categories(self) -> pd.Index:
        """
        The categories of this categorical.

        Examples
        --------
        >>> idx = ps.CategoricalIndex(list("abbccc"))
        >>> idx  # doctest: +NORMALIZE_WHITESPACE
        CategoricalIndex(['a', 'b', 'b', 'c', 'c', 'c'],
                         categories=['a', 'b', 'c'], ordered=False, dtype='category')

        >>> idx.categories
        Index(['a', 'b', 'c'], dtype='object')
        """
        return self.dtype.categories

    @categories.setter
    def categories(self, categories: pd.Index) -> None:
        raise NotImplementedError()

    @property
    def ordered(self) -> bool:
        """
        Whether the categories have an ordered relationship.

        Examples
        --------
        >>> idx = ps.CategoricalIndex(list("abbccc"))
        >>> idx  # doctest: +NORMALIZE_WHITESPACE
        CategoricalIndex(['a', 'b', 'b', 'c', 'c', 'c'],
                         categories=['a', 'b', 'c'], ordered=False, dtype='category')

        >>> idx.ordered
        False
        """
        return self.dtype.ordered

    def as_ordered(self, inplace: bool = False) -> Optional["CategoricalIndex"]:
        """
        Set the Categorical to be ordered.

        Parameters
        ----------
        inplace : bool, default False
           Whether or not to set the ordered attribute in-place or return
           a copy of this categorical with ordered set to True.

        Returns
        -------
        CategoricalIndex or None
            Ordered Categorical or None if ``inplace=True``.

        Examples
        --------
        >>> idx = ps.CategoricalIndex(list("abbccc"))
        >>> idx  # doctest: +NORMALIZE_WHITESPACE
        CategoricalIndex(['a', 'b', 'b', 'c', 'c', 'c'],
                         categories=['a', 'b', 'c'], ordered=False, dtype='category')

        >>> idx.as_ordered()  # doctest: +NORMALIZE_WHITESPACE
        CategoricalIndex(['a', 'b', 'b', 'c', 'c', 'c'],
                         categories=['a', 'b', 'c'], ordered=True, dtype='category')
        """
        if inplace:
            raise ValueError("cannot use inplace with CategoricalIndex")

        return CategoricalIndex(self.to_series().cat.as_ordered()).rename(self.name)

    def as_unordered(self, inplace: bool = False) -> Optional["CategoricalIndex"]:
        """
        Set the Categorical to be unordered.

        Parameters
        ----------
        inplace : bool, default False
           Whether or not to set the ordered attribute in-place or return
           a copy of this categorical with ordered set to False.

        Returns
        -------
        CategoricalIndex or None
            Unordered Categorical or None if ``inplace=True``.

        Examples
        --------
        >>> idx = ps.CategoricalIndex(list("abbccc")).as_ordered()
        >>> idx  # doctest: +NORMALIZE_WHITESPACE
        CategoricalIndex(['a', 'b', 'b', 'c', 'c', 'c'],
                         categories=['a', 'b', 'c'], ordered=True, dtype='category')

        >>> idx.as_unordered()  # doctest: +NORMALIZE_WHITESPACE
        CategoricalIndex(['a', 'b', 'b', 'c', 'c', 'c'],
                         categories=['a', 'b', 'c'], ordered=False, dtype='category')
        """
        if inplace:
            raise ValueError("cannot use inplace with CategoricalIndex")

        return CategoricalIndex(self.to_series().cat.as_unordered()).rename(self.name)

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeCategoricalIndex, item):
            property_or_func = getattr(MissingPandasLikeCategoricalIndex, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)  # type: ignore
            else:
                return partial(property_or_func, self)
        raise AttributeError("'CategoricalIndex' object has no attribute '{}'".format(item))


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.indexes.category

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.indexes.category.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.indexes.category tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.indexes.category,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
