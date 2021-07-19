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
from typing import TYPE_CHECKING, cast

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
        return cast(CategoricalDtype, self._data.dtype).categories

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
        return cast(CategoricalDtype, self._data.dtype).ordered

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

    def as_ordered(self, inplace: bool = False) -> "ps.Series":
        raise NotImplementedError()

    def as_unordered(self, inplace: bool = False) -> "ps.Series":
        raise NotImplementedError()

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
