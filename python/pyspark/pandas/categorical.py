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
from typing import Any, Callable, List, Optional, Union, TYPE_CHECKING, cast
import warnings

import pandas as pd
from pandas.api.types import (  # type: ignore[attr-defined]
    CategoricalDtype,
    is_dict_like,
    is_list_like,
)

from pyspark.pandas.internal import InternalField
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.data_type_ops.categorical_ops import _to_cat
from pyspark.sql import functions as F
from pyspark.sql.types import StructField

if TYPE_CHECKING:
    import pyspark.pandas as ps


class CategoricalAccessor:
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
    def categories(self, categories: Union[pd.Index, List]) -> None:
        dtype = CategoricalDtype(categories, ordered=self.ordered)

        if len(self.categories) != len(dtype.categories):
            raise ValueError(
                "new categories need to have the same number of items as the old categories!"
            )

        internal = self._data._psdf._internal.with_new_spark_column(
            self._data._column_label,
            self._data.spark.column,
            field=self._data._internal.data_fields[0].copy(dtype=dtype),
        )
        self._data._psdf._update_internal_frame(internal)

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

    def add_categories(
        self, new_categories: Union[pd.Index, Any, List], inplace: bool = False
    ) -> Optional["ps.Series"]:
        """
        Add new categories.

        `new_categories` will be included at the last/highest place in the
        categories and will be unused directly after this call.

        Parameters
        ----------
        new_categories : category or list-like of category
           The new categories to be included.
        inplace : bool, default False
           Whether or not to add the categories inplace or return a copy of
           this categorical with added categories.

           .. deprecated:: 3.2.0

        Returns
        -------
        Series or None
            Categorical with new categories added or None if ``inplace=True``.

        Raises
        ------
        ValueError
            If the new categories include old categories or do not validate as
            categories

        See Also
        --------
        rename_categories : Rename categories.
        reorder_categories : Reorder categories.
        remove_categories : Remove the specified categories.
        remove_unused_categories : Remove categories which are not used.
        set_categories : Set the categories to the specified ones.

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

        >>> s.cat.add_categories('x')  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (4, object): ['a', 'b', 'c', 'x']
        """
        from pyspark.pandas.frame import DataFrame

        if inplace:
            warnings.warn(
                "The `inplace` parameter in add_categories is deprecated "
                "and will be removed in a future version.",
                FutureWarning,
            )

        categories: List[Any]
        if is_list_like(new_categories):
            categories = list(new_categories)
        else:
            categories = [new_categories]

        if any(cat in self.categories for cat in categories):
            raise ValueError(
                "new categories must not include old categories: {{{cats}}}".format(
                    cats=", ".join(set(str(cat) for cat in categories if cat in self.categories))
                )
            )

        internal = self._data._psdf._internal.with_new_spark_column(
            self._data._column_label,
            self._data.spark.column,
            field=self._data._internal.data_fields[0].copy(
                dtype=CategoricalDtype(list(self.categories) + categories, ordered=self.ordered)
            ),
        )
        if inplace:
            self._data._psdf._update_internal_frame(internal)
            return None
        else:
            return DataFrame(internal)._psser_for(self._data._column_label).copy()

    def _set_ordered(self, *, ordered: bool, inplace: bool) -> Optional["ps.Series"]:
        from pyspark.pandas.frame import DataFrame

        if self.ordered == ordered:
            if inplace:
                return None
            else:
                return self._data.copy()
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
                return DataFrame(internal)._psser_for(self._data._column_label).copy()

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

    def remove_categories(
        self, removals: Union[pd.Index, Any, List], inplace: bool = False
    ) -> Optional["ps.Series"]:
        """
        Remove the specified categories.

        `removals` must be included in the old categories. Values which were in
        the removed categories will be set to NaN

        Parameters
        ----------
        removals : category or list of categories
           The categories which should be removed.
        inplace : bool, default False
           Whether or not to remove the categories inplace or return a copy of
           this categorical with removed categories.

           .. deprecated:: 3.2.0

        Returns
        -------
        Series or None
            Categorical with removed categories or None if ``inplace=True``.

        Raises
        ------
        ValueError
            If the removals are not contained in the categories

        See Also
        --------
        rename_categories : Rename categories.
        reorder_categories : Reorder categories.
        add_categories : Add new categories.
        remove_unused_categories : Remove categories which are not used.
        set_categories : Set the categories to the specified ones.

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

        >>> s.cat.remove_categories('b')  # doctest: +SKIP
        0      a
        1    NaN
        2    NaN
        3      c
        4      c
        5      c
        dtype: category
        Categories (2, object): ['a', 'c']
        """
        if inplace:
            warnings.warn(
                "The `inplace` parameter in remove_categories is deprecated "
                "and will be removed in a future version.",
                FutureWarning,
            )

        categories: List[Any]
        if is_list_like(removals):
            categories = [cat for cat in removals if cat is not None]
        elif removals is None:
            categories = []
        else:
            categories = [removals]

        if any(cat not in self.categories for cat in categories):
            raise ValueError(
                "removals must all be in old categories: {{{cats}}}".format(
                    cats=", ".join(
                        set(str(cat) for cat in categories if cat not in self.categories)
                    )
                )
            )

        if len(categories) == 0:
            if inplace:
                return None
            else:
                return self._data.copy()
        else:
            dtype = CategoricalDtype(
                [cat for cat in self.categories if cat not in categories], ordered=self.ordered
            )
            psser = self._data.astype(dtype)

            if inplace:
                internal = self._data._psdf._internal.with_new_spark_column(
                    self._data._column_label,
                    psser.spark.column,
                    field=psser._internal.data_fields[0],
                )
                self._data._psdf._update_internal_frame(internal)
                return None
            else:
                return psser

    def remove_unused_categories(self, inplace: bool = False) -> Optional["ps.Series"]:
        """
        Remove categories which are not used.

        Parameters
        ----------
        inplace : bool, default False
           Whether or not to drop unused categories inplace or return a copy of
           this categorical with unused categories dropped.

           .. deprecated:: 3.2.0

        Returns
        -------
        cat : Series or None
            Categorical with unused categories dropped or None if ``inplace=True``.

        See Also
        --------
        rename_categories : Rename categories.
        reorder_categories : Reorder categories.
        add_categories : Add new categories.
        remove_categories : Remove the specified categories.
        set_categories : Set the categories to the specified ones.

        Examples
        --------
        >>> s = ps.Series(pd.Categorical(list("abbccc"), categories=['a', 'b', 'c', 'd']))
        >>> s  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (4, object): ['a', 'b', 'c', 'd']

        >>> s.cat.remove_unused_categories()  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['a', 'b', 'c']
        """
        if inplace:
            warnings.warn(
                "The `inplace` parameter in remove_unused_categories is deprecated "
                "and will be removed in a future version.",
                FutureWarning,
            )

        categories = set(self._data.drop_duplicates()._to_pandas())
        removals = [cat for cat in self.categories if cat not in categories]
        return self.remove_categories(removals=removals, inplace=inplace)

    def rename_categories(
        self, new_categories: Union[list, dict, Callable], inplace: bool = False
    ) -> Optional["ps.Series"]:
        """
        Rename categories.

        Parameters
        ----------
        new_categories : list-like, dict-like or callable

            New categories which will replace old categories.

            * list-like: all items must be unique and the number of items in
              the new categories must match the existing number of categories.

            * dict-like: specifies a mapping from
              old categories to new. Categories not contained in the mapping
              are passed through and extra categories in the mapping are
              ignored.

            * callable : a callable that is called on all items in the old
              categories and whose return values comprise the new categories.

        inplace : bool, default False
            Whether or not to rename the categories inplace or return a copy of
            this categorical with renamed categories.

            .. deprecated:: 3.2.0

        Returns
        -------
        cat : Series or None
            Categorical with removed categories or None if ``inplace=True``.

        Raises
        ------
        ValueError
            If new categories are list-like and do not have the same number of
            items than the current categories or do not validate as categories

        See Also
        --------
        reorder_categories : Reorder categories.
        add_categories : Add new categories.
        remove_categories : Remove the specified categories.
        remove_unused_categories : Remove categories which are not used.
        set_categories : Set the categories to the specified ones.

        Examples
        --------
        >>> s = ps.Series(["a", "a", "b"], dtype="category")
        >>> s.cat.rename_categories([0, 1])  # doctest: +SKIP
        0    0
        1    0
        2    1
        dtype: category
        Categories (2, int64): [0, 1]

        For dict-like ``new_categories``, extra keys are ignored and
        categories not in the dictionary are passed through

        >>> s.cat.rename_categories({'a': 'A', 'c': 'C'})  # doctest: +SKIP
        0    A
        1    A
        2    b
        dtype: category
        Categories (2, object): ['A', 'b']

        You may also provide a callable to create the new categories

        >>> s.cat.rename_categories(lambda x: x.upper())  # doctest: +SKIP
        0    A
        1    A
        2    B
        dtype: category
        Categories (2, object): ['A', 'B']
        """
        from pyspark.pandas.frame import DataFrame

        if inplace:
            warnings.warn(
                "The `inplace` parameter in rename_categories is deprecated "
                "and will be removed in a future version.",
                FutureWarning,
            )

        if is_dict_like(new_categories):
            categories = [cast(dict, new_categories).get(item, item) for item in self.categories]
        elif callable(new_categories):
            categories = [new_categories(item) for item in self.categories]
        elif is_list_like(new_categories):
            if len(self.categories) != len(new_categories):
                raise ValueError(
                    "new categories need to have the same number of items as the old categories!"
                )
            categories = cast(list, new_categories)
        else:
            raise TypeError("new_categories must be list-like, dict-like or callable.")

        internal = self._data._psdf._internal.with_new_spark_column(
            self._data._column_label,
            self._data.spark.column,
            field=self._data._internal.data_fields[0].copy(
                dtype=CategoricalDtype(categories=categories, ordered=self.ordered)
            ),
        )

        if inplace:
            self._data._psdf._update_internal_frame(internal)
            return None
        else:
            return DataFrame(internal)._psser_for(self._data._column_label).copy()

    def reorder_categories(
        self,
        new_categories: Union[pd.Index, List],
        ordered: Optional[bool] = None,
        inplace: bool = False,
    ) -> Optional["ps.Series"]:
        """
        Reorder categories as specified in new_categories.

        `new_categories` need to include all old categories and no new category
        items.

        Parameters
        ----------
        new_categories : Index-like
           The categories in new order.
        ordered : bool, optional
           Whether or not the categorical is treated as a ordered categorical.
           If not given, do not change the ordered information.
        inplace : bool, default False
           Whether or not to reorder the categories inplace or return a copy of
           this categorical with reordered categories.

           .. deprecated:: 3.2.0

        Returns
        -------
        cat : Series or None
            Categorical with removed categories or None if ``inplace=True``.

        Raises
        ------
        ValueError
            If the new categories do not contain all old category items or any
            new ones

        See Also
        --------
        rename_categories : Rename categories.
        add_categories : Add new categories.
        remove_categories : Remove the specified categories.
        remove_unused_categories : Remove categories which are not used.
        set_categories : Set the categories to the specified ones.

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

        >>> s.cat.reorder_categories(['c', 'b', 'a'], ordered=True)  # doctest: +SKIP
        0    a
        1    b
        2    b
        3    c
        4    c
        5    c
        dtype: category
        Categories (3, object): ['c' < 'b' < 'a']
        """
        if inplace:
            warnings.warn(
                "The `inplace` parameter in reorder_categories is deprecated "
                "and will be removed in a future version.",
                FutureWarning,
            )

        if not is_list_like(new_categories):
            raise TypeError(
                "Parameter 'new_categories' must be list-like, was '{}'".format(new_categories)
            )
        elif len(set(new_categories)) != len(set(self.categories)) or any(
            cat not in self.categories for cat in new_categories
        ):
            raise ValueError("items in new_categories are not the same as in old categories")

        if ordered is None:
            ordered = self.ordered

        if new_categories == list(self.categories) and ordered == self.ordered:
            if inplace:
                return None
            else:
                return self._data.copy()
        else:
            dtype = CategoricalDtype(categories=new_categories, ordered=ordered)
            psser = _to_cat(self._data).astype(dtype)

            if inplace:
                internal = self._data._psdf._internal.with_new_spark_column(
                    self._data._column_label,
                    psser.spark.column,
                    field=psser._internal.data_fields[0],
                )
                self._data._psdf._update_internal_frame(internal)
                return None
            else:
                return psser

    def set_categories(
        self,
        new_categories: Union[pd.Index, List],
        ordered: Optional[bool] = None,
        rename: bool = False,
        inplace: bool = False,
    ) -> Optional["ps.Series"]:
        """
        Set the categories to the specified new_categories.

        `new_categories` can include new categories (which will result in
        unused categories) or remove old categories (which results in values
        set to NaN). If `rename==True`, the categories will simple be renamed
        (less or more items than in old categories will result in values set to
        NaN or in unused categories respectively).

        This method can be used to perform more than one action of adding,
        removing, and reordering simultaneously and is therefore faster than
        performing the individual steps via the more specialised methods.

        On the other hand this methods does not do checks (e.g., whether the
        old categories are included in the new categories on a reorder), which
        can result in surprising changes, for example when using special string
        dtypes, which does not considers a S1 string equal to a single char
        python string.

        Parameters
        ----------
        new_categories : Index-like
           The categories in new order.
        ordered : bool, default False
           Whether or not the categorical is treated as a ordered categorical.
           If not given, do not change the ordered information.
        rename : bool, default False
           Whether or not the new_categories should be considered as a rename
           of the old categories or as reordered categories.
        inplace : bool, default False
           Whether or not to reorder the categories in-place or return a copy
           of this categorical with reordered categories.

           .. deprecated:: 3.2.0

        Returns
        -------
        Series with reordered categories or None if inplace.

        Raises
        ------
        ValueError
            If new_categories does not validate as categories

        See Also
        --------
        rename_categories : Rename categories.
        reorder_categories : Reorder categories.
        add_categories : Add new categories.
        remove_categories : Remove the specified categories.
        remove_unused_categories : Remove categories which are not used.

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

        >>> s.cat.set_categories(['b', 'c'])  # doctest: +SKIP
        0    NaN
        1      b
        2      b
        3      c
        4      c
        5      c
        dtype: category
        Categories (2, object): ['b', 'c']

        >>> s.cat.set_categories([1, 2, 3], rename=True)  # doctest: +SKIP
        0    1
        1    2
        2    2
        3    3
        4    3
        5    3
        dtype: category
        Categories (3, int64): [1, 2, 3]

        >>> s.cat.set_categories([1, 2, 3], rename=True, ordered=True)  # doctest: +SKIP
        0    1
        1    2
        2    2
        3    3
        4    3
        5    3
        dtype: category
        Categories (3, int64): [1 < 2 < 3]
        """
        from pyspark.pandas.frame import DataFrame

        if inplace:
            warnings.warn(
                "The `inplace` parameter in set_categories is deprecated "
                "and will be removed in a future version.",
                FutureWarning,
            )

        if not is_list_like(new_categories):
            raise TypeError(
                "Parameter 'new_categories' must be list-like, was '{}'".format(new_categories)
            )

        if ordered is None:
            ordered = self.ordered

        new_dtype = CategoricalDtype(new_categories, ordered=ordered)
        scol = self._data.spark.column

        if rename:
            new_scol = (
                F.when(scol >= len(new_categories), SF.lit(-1).cast(self._data.spark.data_type))
                .otherwise(scol)
                .alias(self._data._internal.data_spark_column_names[0])
            )

            internal = self._data._psdf._internal.with_new_spark_column(
                self._data._column_label,
                new_scol,
                field=self._data._internal.data_fields[0].copy(dtype=new_dtype),
            )

            if inplace:
                self._data._psdf._update_internal_frame(internal)
                return None
            else:
                return DataFrame(internal)._psser_for(self._data._column_label).copy()
        else:
            psser = self._data.astype(new_dtype)
            if inplace:
                internal = self._data._psdf._internal.with_new_spark_column(
                    self._data._column_label,
                    psser.spark.column,
                    field=psser._internal.data_fields[0],
                )
                self._data._psdf._update_internal_frame(internal)
                return None
            else:
                return psser


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
