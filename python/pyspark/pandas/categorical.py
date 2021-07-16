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
from typing import Optional, TYPE_CHECKING, cast

import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.pandas.internal import InternalField
from pyspark.sql.types import StructField

if TYPE_CHECKING:
    import pyspark.pandas as ps  # noqa: F401 (SPARK-34943)


class CategoricalAccessor(object):
    """
    Accessor object for categorical properties of the Series values.

    Examples
    --------
    >>> s = ps.Series(list("abbccc"), dtype="category")
    >>> s  # doctest: +SKIP
    0    a
    1    b
    2    b
    3    c
    4    c
    5    c
    dtype: category
    Categories (3, object): ['a', 'b', 'c']

    >>> s.cat.categories
    Index(['a', 'b', 'c'], dtype='object')

    >>> s.cat.codes
    0    0
    1    1
    2    1
    3    2
    4    2
    5    2
    dtype: int8
    """

    def __init__(self, series: "ps.Series"):
        if not isinstance(series.dtype, CategoricalDtype):
            raise ValueError("Cannot call CategoricalAccessor on type {}".format(series.dtype))
        self._data = series

    @property
    def _dtype(self) -> CategoricalDtype:
        return cast(CategoricalDtype, self._data.dtype)

    @property
    def categories(self) -> pd.Index:
        """
        The categories of this categorical.

        Examples
        --------
        >>> s = ps.Series(list("abbccc"), dtype="category")
        >>> s  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['a', 'b', 'c']

        >>> s.cat.categories
        Index(['a', 'b', 'c'], dtype='object')
        """
        return self._dtype.categories

    @categories.setter
    def categories(self, categories: pd.Index) -> None:
        raise NotImplementedError()

    @property
    def ordered(self) -> bool:
        """
        Whether the categories have an ordered relationship.

        Examples
        --------
        >>> s = ps.Series(list("abbccc"), dtype="category")
        >>> s  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['a', 'b', 'c']

        >>> s.cat.ordered
        False
        """
        return self._dtype.ordered

    @property
    def codes(self) -> "ps.Series":
        """
        Return Series of codes as well as the index.

        Examples
        --------
        >>> s = ps.Series(list("abbccc"), dtype="category")
        >>> s  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['a', 'b', 'c']

        >>> s.cat.codes
        0    0
        1    1
        2    1
        3    2
        4    2
        5    2
        dtype: int8
        """
        return self._data._with_new_scol(
            self._data.spark.column,
            field=InternalField.from_struct_field(
                StructField(
                    name=self._data._internal.data_spark_column_names[0],
                    dataType=self._data.spark.data_type,
                    nullable=self._data.spark.nullable,
                )
            ),
        ).rename()

    def add_categories(self, new_categories: pd.Index, inplace: bool = False) -> "ps.Series":
        raise NotImplementedError()

    def _set_ordered(self, *, ordered: bool, inplace: bool) -> Optional["ps.Series"]:
        from pyspark.pandas.frame import DataFrame

        if self.ordered == ordered:
            if inplace:
                return None
            else:
                psser = self._data
        else:
            internal = self._data._psdf._internal.with_new_spark_column(
                self._data._column_label,
                self._data.spark.column,
                field=self._data._internal.data_fields[0].copy(
                    dtype=CategoricalDtype(categories=self.categories, ordered=ordered)
                ),
            )
            if inplace:
                self._data._psdf._update_internal_frame(internal)
                return None
            else:
                psser = DataFrame(internal)._psser_for(self._data._column_label)

        return psser._with_new_scol(psser.spark.column, field=psser._internal.data_fields[0])

    def as_ordered(self, inplace: bool = False) -> Optional["ps.Series"]:
        """
        Set the Categorical to be ordered.

        Parameters
        ----------
        inplace : bool, default False
           Whether or not to set the ordered attribute in-place or return
           a copy of this categorical with ordered set to True.

        Returns
        -------
        Series or None
            Ordered Categorical or None if ``inplace=True``.

        Examples
        --------
        >>> s = ps.Series(list("abbccc"), dtype="category")
        >>> s  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['a', 'b', 'c']

        >>> s.cat.as_ordered()  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['a' < 'b' < 'c']
        """
        return self._set_ordered(ordered=True, inplace=inplace)

    def as_unordered(self, inplace: bool = False) -> Optional["ps.Series"]:
        """
        Set the Categorical to be unordered.

        Parameters
        ----------
        inplace : bool, default False
           Whether or not to set the ordered attribute in-place or return
           a copy of this categorical with ordered set to False.

        Returns
        -------
        Series or None
            Unordered Categorical or None if ``inplace=True``.

        Examples
        --------
        >>> s = ps.Series(list("abbccc"), dtype="category").cat.as_ordered()
        >>> s  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['a' < 'b' < 'c']

        >>> s.cat.as_unordered()  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['a', 'b', 'c']
        """
        return self._set_ordered(ordered=False, inplace=inplace)

    def remove_categories(self, removals: pd.Index, inplace: bool = False) -> "ps.Series":
        raise NotImplementedError()

    def remove_unused_categories(self) -> "ps.Series":
        raise NotImplementedError()

    def rename_categories(self, new_categories: pd.Index, inplace: bool = False) -> "ps.Series":
        raise NotImplementedError()

    def reorder_categories(
        self, new_categories: pd.Index, ordered: bool = None, inplace: bool = False
    ) -> "ps.Series":
        raise NotImplementedError()

    def set_categories(
        self,
        new_categories: pd.Index,
        ordered: bool = None,
        rename: bool = False,
        inplace: bool = False,
    ) -> "ps.Series":
        raise NotImplementedError()


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.categorical

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.categorical.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.categorical tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.categorical,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
