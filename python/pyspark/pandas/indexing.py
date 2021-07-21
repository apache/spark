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
A loc indexer for pandas-on-Spark DataFrame/Series.
"""
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from functools import reduce
from typing import Any, Optional, List, Tuple, TYPE_CHECKING, Union, cast, Sized

import pandas as pd
from pandas.api.types import is_list_like
from pyspark.sql import functions as F, Column
from pyspark.sql.types import BooleanType, LongType
from pyspark.sql.utils import AnalysisException
import numpy as np

from pyspark import pandas as ps  # noqa: F401
from pyspark.pandas._typing import Label, Name, Scalar
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_DEFAULT_SERIES_NAME,
)
from pyspark.pandas.exceptions import SparkPandasIndexingError, SparkPandasNotImplementedError
from pyspark.pandas.spark import functions as SF
from pyspark.pandas.utils import (
    is_name_like_tuple,
    is_name_like_value,
    lazy_property,
    name_like_string,
    same_anchor,
    scol_for,
    spark_column_equals,
    verify_temp_column_name,
)

if TYPE_CHECKING:
    from pyspark.pandas.frame import DataFrame  # noqa: F401 (SPARK-34943)
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)


class IndexerLike(object):
    def __init__(self, psdf_or_psser: Union["Series", "DataFrame"]):
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import Series

        assert isinstance(
            psdf_or_psser, (DataFrame, Series)
        ), "unexpected argument type: {}".format(type(psdf_or_psser))
        self._psdf_or_psser = psdf_or_psser

    @property
    def _is_df(self) -> bool:
        from pyspark.pandas.frame import DataFrame

        return isinstance(self._psdf_or_psser, DataFrame)

    @property
    def _is_series(self) -> bool:
        from pyspark.pandas.series import Series

        return isinstance(self._psdf_or_psser, Series)

    @property
    def _psdf(self) -> "DataFrame":
        if self._is_df:
            return cast("DataFrame", self._psdf_or_psser)
        else:
            assert self._is_series
            return self._psdf_or_psser._psdf

    @property
    def _internal(self) -> InternalFrame:
        return self._psdf._internal


class AtIndexer(IndexerLike):
    """
    Access a single value for a row/column label pair.
    If the index is not unique, all matching pairs are returned as an array.
    Similar to ``loc``, in that both provide label-based lookups. Use ``at`` if you only need to
    get a single value in a DataFrame or Series.

    .. note:: Unlike pandas, pandas-on-Spark only allows using ``at`` to get values but not to
        set them.

    .. note:: Warning: If ``row_index`` matches a lot of rows, large amounts of data will be
        fetched, potentially causing your machine to run out of memory.

    Raises
    ------
    KeyError
        When label does not exist in DataFrame

    Examples
    --------
    >>> psdf = ps.DataFrame([[0, 2, 3], [0, 4, 1], [10, 20, 30]],
    ...                    index=[4, 5, 5], columns=['A', 'B', 'C'])
    >>> psdf
        A   B   C
    4   0   2   3
    5   0   4   1
    5  10  20  30

    Get value at specified row/column pair

    >>> psdf.at[4, 'B']
    2

    Get array if an index occurs multiple times

    >>> psdf.at[5, 'B']
    array([ 4, 20])
    """

    def __getitem__(self, key: Any) -> Union["Series", "DataFrame", Scalar]:
        if self._is_df:
            if not isinstance(key, tuple) or len(key) != 2:
                raise TypeError("Use DataFrame.at like .at[row_index, column_name]")
            row_sel, col_sel = key
        else:
            assert self._is_series, type(self._psdf_or_psser)
            if isinstance(key, tuple) and len(key) != 1:
                raise TypeError("Use Series.at like .at[row_index]")
            row_sel = key
            col_sel = self._psdf_or_psser._column_label

        if self._internal.index_level == 1:
            if not is_name_like_value(row_sel, allow_none=False, allow_tuple=False):
                raise ValueError("At based indexing on a single index can only have a single value")
            row_sel = (row_sel,)
        else:
            if not is_name_like_tuple(row_sel, allow_none=False):
                raise ValueError("At based indexing on multi-index can only have tuple values")

        if col_sel is not None:
            if not is_name_like_value(col_sel, allow_none=False):
                raise ValueError("At based indexing on multi-index can only have tuple values")
            if not is_name_like_tuple(col_sel):
                col_sel = (col_sel,)

        cond = reduce(
            lambda x, y: x & y,
            [scol == row for scol, row in zip(self._internal.index_spark_columns, row_sel)],
        )
        pdf = (
            self._internal.spark_frame.drop(NATURAL_ORDER_COLUMN_NAME)
            .filter(cond)
            .select(self._internal.spark_column_for(col_sel))
            .toPandas()
        )

        if len(pdf) < 1:
            raise KeyError(name_like_string(row_sel))

        values = cast(pd.DataFrame, pdf).iloc[:, 0].values
        return (
            values if (len(row_sel) < self._internal.index_level or len(values) > 1) else values[0]
        )


class iAtIndexer(IndexerLike):
    """
    Access a single value for a row/column pair by integer position.

    Similar to ``iloc``, in that both provide integer-based lookups. Use
    ``iat`` if you only need to get or set a single value in a DataFrame
    or Series.

    Raises
    ------
    KeyError
        When label does not exist in DataFrame

    Examples
    --------
    >>> df = ps.DataFrame([[0, 2, 3], [0, 4, 1], [10, 20, 30]],
    ...                   columns=['A', 'B', 'C'])
    >>> df
        A   B   C
    0   0   2   3
    1   0   4   1
    2  10  20  30

    Get value at specified row/column pair

    >>> df.iat[1, 2]
    1

    Get value within a series

    >>> psser = ps.Series([1, 2, 3], index=[10, 20, 30])
    >>> psser
    10    1
    20    2
    30    3
    dtype: int64

    >>> psser.iat[1]
    2
    """

    def __getitem__(self, key: Any) -> Union["Series", "DataFrame", Scalar]:
        if self._is_df:
            if not isinstance(key, tuple) or len(key) != 2:
                raise TypeError(
                    "Use DataFrame.iat like .iat[row_integer_position, column_integer_position]"
                )
            row_sel, col_sel = key
            if not isinstance(row_sel, int) or not isinstance(col_sel, int):
                raise ValueError("iAt based indexing can only have integer indexers")
            return self._psdf_or_psser.iloc[row_sel, col_sel]
        else:
            assert self._is_series, type(self._psdf_or_psser)
            if not isinstance(key, int) and len(key) != 1:
                raise TypeError("Use Series.iat like .iat[row_integer_position]")
            if not isinstance(key, int):
                raise ValueError("iAt based indexing can only have integer indexers")
            return self._psdf_or_psser.iloc[key]


class LocIndexerLike(IndexerLike, metaclass=ABCMeta):
    def _select_rows(self, rows_sel: Any) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        """
        Dispatch the logic for select rows to more specific methods by `rows_sel` argument types.

        Parameters
        ----------
        rows_sel : the key specified to select rows.

        Returns
        -------
        Tuple of Spark column, int, int:

            * The Spark column for the condition to filter the rows.
            * The number of rows when the selection can be simplified by limit.
            * The remaining index rows if the result index size is shrunk.
        """
        from pyspark.pandas.series import Series

        if rows_sel is None:
            return None, None, None
        elif isinstance(rows_sel, Series):
            return self._select_rows_by_series(rows_sel)
        elif isinstance(rows_sel, Column):
            return self._select_rows_by_spark_column(rows_sel)
        elif isinstance(rows_sel, slice):
            if rows_sel == slice(None):
                # If slice is None - select everything, so nothing to do
                return None, None, None
            return self._select_rows_by_slice(rows_sel)
        elif isinstance(rows_sel, tuple):
            return self._select_rows_else(rows_sel)
        elif is_list_like(rows_sel):
            return self._select_rows_by_iterable(rows_sel)
        else:
            return self._select_rows_else(rows_sel)

    def _select_cols(
        self, cols_sel: Any, missing_keys: Optional[List[Name]] = None
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        """
        Dispatch the logic for select columns to more specific methods by `cols_sel` argument types.

        Parameters
        ----------
        cols_sel : the key specified to select columns.

        Returns
        -------
        Tuple of list of column label, list of Spark columns, list of dtypes, bool:

            * The column labels selected.
            * The Spark columns selected.
            * The field metadata selected.
            * The boolean value whether Series should be returned or not.
            * The Series name if needed.
        """
        from pyspark.pandas.series import Series

        if cols_sel is None:
            column_labels = self._internal.column_labels
            data_spark_columns = self._internal.data_spark_columns
            data_fields = self._internal.data_fields
            return column_labels, data_spark_columns, data_fields, False, None
        elif isinstance(cols_sel, Series):
            return self._select_cols_by_series(cols_sel, missing_keys)
        elif isinstance(cols_sel, Column):
            return self._select_cols_by_spark_column(cols_sel, missing_keys)
        elif isinstance(cols_sel, slice):
            if cols_sel == slice(None):
                # If slice is None - select everything, so nothing to do
                column_labels = self._internal.column_labels
                data_spark_columns = self._internal.data_spark_columns
                data_fields = self._internal.data_fields
                return column_labels, data_spark_columns, data_fields, False, None
            return self._select_cols_by_slice(cols_sel, missing_keys)
        elif isinstance(cols_sel, tuple):
            return self._select_cols_else(cols_sel, missing_keys)
        elif is_list_like(cols_sel):
            return self._select_cols_by_iterable(cols_sel, missing_keys)
        else:
            return self._select_cols_else(cols_sel, missing_keys)

    # Methods for row selection

    @abstractmethod
    def _select_rows_by_series(
        self, rows_sel: "Series"
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        """Select rows by `Series` type key."""
        pass

    @abstractmethod
    def _select_rows_by_spark_column(
        self, rows_sel: Column
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        """Select rows by Spark `Column` type key."""
        pass

    @abstractmethod
    def _select_rows_by_slice(
        self, rows_sel: slice
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        """Select rows by `slice` type key."""
        pass

    @abstractmethod
    def _select_rows_by_iterable(
        self, rows_sel: Iterable
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        """Select rows by `Iterable` type key."""
        pass

    @abstractmethod
    def _select_rows_else(
        self, rows_sel: Any
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        """Select rows by other type key."""
        pass

    # Methods for col selection

    @abstractmethod
    def _select_cols_by_series(
        self, cols_sel: "Series", missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        """Select columns by `Series` type key."""
        pass

    @abstractmethod
    def _select_cols_by_spark_column(
        self, cols_sel: Column, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        """Select columns by Spark `Column` type key."""
        pass

    @abstractmethod
    def _select_cols_by_slice(
        self, cols_sel: slice, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        """Select columns by `slice` type key."""
        pass

    @abstractmethod
    def _select_cols_by_iterable(
        self, cols_sel: Iterable, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        """Select columns by `Iterable` type key."""
        pass

    @abstractmethod
    def _select_cols_else(
        self, cols_sel: Any, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        """Select columns by other type key."""
        pass

    def __getitem__(self, key: Any) -> Union["Series", "DataFrame"]:
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import Series, first_series

        if self._is_series:
            if isinstance(key, Series) and not same_anchor(key, self._psdf_or_psser):
                psdf = self._psdf_or_psser.to_frame()
                temp_col = verify_temp_column_name(psdf, "__temp_col__")

                psdf[temp_col] = key
                return type(self)(psdf[self._psdf_or_psser.name])[psdf[temp_col]]

            cond, limit, remaining_index = self._select_rows(key)
            if cond is None and limit is None:
                return self._psdf_or_psser

            column_label = self._psdf_or_psser._column_label
            column_labels = [column_label]
            data_spark_columns = [self._internal.spark_column_for(column_label)]
            data_fields = [self._internal.field_for(column_label)]
            returns_series = True
            series_name = self._psdf_or_psser.name
        else:
            assert self._is_df
            if isinstance(key, tuple):
                if len(key) != 2:
                    raise SparkPandasIndexingError("Only accepts pairs of candidates")
                rows_sel, cols_sel = key
            else:
                rows_sel = key
                cols_sel = None

            if isinstance(rows_sel, Series) and not same_anchor(rows_sel, self._psdf_or_psser):
                psdf = self._psdf_or_psser.copy()
                temp_col = verify_temp_column_name(cast("DataFrame", psdf), "__temp_col__")

                psdf[temp_col] = rows_sel
                return type(self)(psdf)[psdf[temp_col], cols_sel][list(self._psdf_or_psser.columns)]

            cond, limit, remaining_index = self._select_rows(rows_sel)
            (
                column_labels,
                data_spark_columns,
                data_fields,
                returns_series,
                series_name,
            ) = self._select_cols(cols_sel)

            if cond is None and limit is None and returns_series:
                psser = self._psdf_or_psser._psser_for(column_labels[0])
                if series_name is not None and series_name != psser.name:
                    psser = psser.rename(series_name)
                return psser

        if remaining_index is not None:
            index_spark_columns = self._internal.index_spark_columns[-remaining_index:]
            index_names = self._internal.index_names[-remaining_index:]
            index_fields = self._internal.index_fields[-remaining_index:]
        else:
            index_spark_columns = self._internal.index_spark_columns
            index_names = self._internal.index_names
            index_fields = self._internal.index_fields

        if len(column_labels) > 0:
            column_labels = column_labels.copy()
            column_labels_level = max(
                len(label) if label is not None else 1 for label in column_labels
            )
            none_column = 0
            for i, label in enumerate(column_labels):
                if label is None:
                    label = (none_column,)
                    none_column += 1
                if len(label) < column_labels_level:
                    label = tuple(list(label) + ([""]) * (column_labels_level - len(label)))
                column_labels[i] = label

            if i == 0 and none_column == 1:
                column_labels = [None]

            column_label_names = self._internal.column_label_names[-column_labels_level:]
        else:
            column_label_names = self._internal.column_label_names

        try:
            sdf = self._internal.spark_frame

            if cond is not None:
                index_columns = sdf.select(index_spark_columns).columns
                data_columns = sdf.select(data_spark_columns).columns
                sdf = sdf.filter(cond).select(index_spark_columns + data_spark_columns)
                index_spark_columns = [scol_for(sdf, col) for col in index_columns]
                data_spark_columns = [scol_for(sdf, col) for col in data_columns]

            if limit is not None:
                if limit >= 0:
                    sdf = sdf.limit(limit)
                else:
                    sdf = sdf.limit(sdf.count() + limit)
                sdf = sdf.drop(NATURAL_ORDER_COLUMN_NAME)
        except AnalysisException:
            raise KeyError(
                "[{}] don't exist in columns".format(
                    [col._jc.toString() for col in data_spark_columns]  # type: ignore
                )
            )

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=index_spark_columns,
            index_names=index_names,
            index_fields=index_fields,
            column_labels=column_labels,
            data_spark_columns=data_spark_columns,
            data_fields=data_fields,
            column_label_names=column_label_names,
        )
        psdf = DataFrame(internal)

        if returns_series:
            psdf_or_psser = first_series(psdf)
            if series_name is not None and series_name != psdf_or_psser.name:
                psdf_or_psser = psdf_or_psser.rename(series_name)
        else:
            psdf_or_psser = psdf

        if remaining_index is not None and remaining_index == 0:
            pdf_or_pser = psdf_or_psser.head(2).to_pandas()
            length = len(pdf_or_pser)
            if length == 0:
                raise KeyError(name_like_string(key))
            elif length == 1:
                return pdf_or_pser.iloc[0]
            else:
                return psdf_or_psser
        else:
            return psdf_or_psser

    def __setitem__(self, key: Any, value: Any) -> None:
        from pyspark.pandas.frame import DataFrame
        from pyspark.pandas.series import Series, first_series

        if self._is_series:
            if (
                isinstance(key, Series)
                and (isinstance(self, iLocIndexer) or not same_anchor(key, self._psdf_or_psser))
            ) or (
                isinstance(value, Series)
                and (isinstance(self, iLocIndexer) or not same_anchor(value, self._psdf_or_psser))
            ):
                if self._psdf_or_psser.name is None:
                    psdf = self._psdf_or_psser.to_frame()
                    column_label = psdf._internal.column_labels[0]
                else:
                    psdf = self._psdf_or_psser._psdf.copy()
                    column_label = self._psdf_or_psser._column_label
                temp_natural_order = verify_temp_column_name(psdf, "__temp_natural_order__")
                temp_key_col = verify_temp_column_name(psdf, "__temp_key_col__")
                temp_value_col = verify_temp_column_name(psdf, "__temp_value_col__")

                psdf[temp_natural_order] = F.monotonically_increasing_id()
                if isinstance(key, Series):
                    psdf[temp_key_col] = key
                if isinstance(value, Series):
                    psdf[temp_value_col] = value
                psdf = psdf.sort_values(temp_natural_order).drop(temp_natural_order)

                psser = psdf._psser_for(column_label)
                if isinstance(key, Series):
                    key = F.col(
                        "`{}`".format(psdf[temp_key_col]._internal.data_spark_column_names[0])
                    )
                if isinstance(value, Series):
                    value = F.col(
                        "`{}`".format(psdf[temp_value_col]._internal.data_spark_column_names[0])
                    )

                type(self)(psser)[key] = value

                if self._psdf_or_psser.name is None:
                    psser = psser.rename()

                self._psdf_or_psser._psdf._update_internal_frame(
                    psser._psdf[
                        self._psdf_or_psser._psdf._internal.column_labels
                    ]._internal.resolved_copy,
                    requires_same_anchor=False,
                )
                return

            if isinstance(value, DataFrame):
                raise ValueError("Incompatible indexer with DataFrame")

            cond, limit, remaining_index = self._select_rows(key)
            if cond is None:
                cond = SF.lit(True)
            if limit is not None:
                cond = cond & (
                    self._internal.spark_frame[cast(iLocIndexer, self)._sequence_col]
                    < SF.lit(limit)
                )

            if isinstance(value, (Series, Column)):
                if remaining_index is not None and remaining_index == 0:
                    raise ValueError(
                        "No axis named {} for object type {}".format(key, type(value).__name__)
                    )
                if isinstance(value, Series):
                    value = value.spark.column
            else:
                value = SF.lit(value)
            scol = (
                F.when(cond, value)
                .otherwise(self._internal.spark_column_for(self._psdf_or_psser._column_label))
                .alias(name_like_string(self._psdf_or_psser.name or SPARK_DEFAULT_SERIES_NAME))
            )

            internal = self._internal.with_new_spark_column(
                self._psdf_or_psser._column_label, scol  # TODO: dtype?
            )
            self._psdf_or_psser._psdf._update_internal_frame(internal, requires_same_anchor=False)
        else:
            assert self._is_df

            if isinstance(key, tuple):
                if len(key) != 2:
                    raise SparkPandasIndexingError("Only accepts pairs of candidates")
                rows_sel, cols_sel = key
            else:
                rows_sel = key
                cols_sel = None

            if isinstance(value, DataFrame):
                if len(value.columns) == 1:
                    value = first_series(value)
                else:
                    raise ValueError("Only a dataframe with one column can be assigned")

            if (
                isinstance(rows_sel, Series)
                and (
                    isinstance(self, iLocIndexer) or not same_anchor(rows_sel, self._psdf_or_psser)
                )
            ) or (
                isinstance(value, Series)
                and (isinstance(self, iLocIndexer) or not same_anchor(value, self._psdf_or_psser))
            ):
                psdf = cast(DataFrame, self._psdf_or_psser.copy())
                temp_natural_order = verify_temp_column_name(psdf, "__temp_natural_order__")
                temp_key_col = verify_temp_column_name(psdf, "__temp_key_col__")
                temp_value_col = verify_temp_column_name(psdf, "__temp_value_col__")

                psdf[temp_natural_order] = F.monotonically_increasing_id()
                if isinstance(rows_sel, Series):
                    psdf[temp_key_col] = rows_sel
                if isinstance(value, Series):
                    psdf[temp_value_col] = value
                psdf = psdf.sort_values(temp_natural_order).drop(temp_natural_order)

                if isinstance(rows_sel, Series):
                    rows_sel = F.col(
                        "`{}`".format(psdf[temp_key_col]._internal.data_spark_column_names[0])
                    )
                if isinstance(value, Series):
                    value = F.col(
                        "`{}`".format(psdf[temp_value_col]._internal.data_spark_column_names[0])
                    )

                type(self)(psdf)[rows_sel, cols_sel] = value

                self._psdf_or_psser._update_internal_frame(
                    psdf[list(self._psdf_or_psser.columns)]._internal.resolved_copy,
                    requires_same_anchor=False,
                )
                return

            cond, limit, remaining_index = self._select_rows(rows_sel)
            missing_keys = []  # type: Optional[List[Name]]
            _, data_spark_columns, _, _, _ = self._select_cols(cols_sel, missing_keys=missing_keys)

            if cond is None:
                cond = SF.lit(True)
            if limit is not None:
                cond = cond & (
                    self._internal.spark_frame[cast(iLocIndexer, self)._sequence_col]
                    < SF.lit(limit)
                )

            if isinstance(value, (Series, Column)):
                if remaining_index is not None and remaining_index == 0:
                    raise ValueError("Incompatible indexer with Series")
                if len(data_spark_columns) > 1:
                    raise ValueError("shape mismatch")
                if isinstance(value, Series):
                    value = value.spark.column
            else:
                value = SF.lit(value)

            new_data_spark_columns = []
            new_fields = []
            for new_scol, spark_column_name, new_field in zip(
                self._internal.data_spark_columns,
                self._internal.data_spark_column_names,
                self._internal.data_fields,
            ):
                for scol in data_spark_columns:
                    if spark_column_equals(new_scol, scol):
                        new_scol = F.when(cond, value).otherwise(scol).alias(spark_column_name)
                        new_field = InternalField.from_struct_field(
                            self._internal.spark_frame.select(new_scol).schema[0],
                            use_extension_dtypes=new_field.is_extension_dtype,
                        )
                        break
                new_data_spark_columns.append(new_scol)
                new_fields.append(new_field)

            column_labels = self._internal.column_labels.copy()
            for missing in missing_keys:
                if is_name_like_tuple(missing):
                    label = cast(Label, missing)
                else:
                    label = cast(Label, (missing,))
                if len(label) < self._internal.column_labels_level:
                    label = tuple(
                        list(label) + ([""] * (self._internal.column_labels_level - len(label)))
                    )
                elif len(label) > self._internal.column_labels_level:
                    raise KeyError(
                        "Key length ({}) exceeds index depth ({})".format(
                            len(label), self._internal.column_labels_level
                        )
                    )
                column_labels.append(label)
                new_data_spark_columns.append(F.when(cond, value).alias(name_like_string(label)))
                new_fields.append(None)

            internal = self._internal.with_new_columns(
                new_data_spark_columns, column_labels=column_labels, data_fields=new_fields
            )
            self._psdf_or_psser._update_internal_frame(internal, requires_same_anchor=False)


class LocIndexer(LocIndexerLike):
    """
    Access a group of rows and columns by label(s) or a boolean Series.

    ``.loc[]`` is primarily label based, but may also be used with a
    conditional boolean Series derived from the DataFrame or Series.

    Allowed inputs are:

    - A single label, e.g. ``5`` or ``'a'``, (note that ``5`` is
      interpreted as a *label* of the index, and **never** as an
      integer position along the index) for column selection.

    - A list or array of labels, e.g. ``['a', 'b', 'c']``.

    - A slice object with labels, e.g. ``'a':'f'``.

    - A conditional boolean Series derived from the DataFrame or Series

    - A boolean array of the same length as the column axis being sliced,
      e.g. ``[True, False, True]``.

    - An alignable boolean pandas Series to the column axis being sliced.
      The index of the key will be aligned before masking.

    Not allowed inputs which pandas allows are:

    - A boolean array of the same length as the row axis being sliced,
      e.g. ``[True, False, True]``.
    - A ``callable`` function with one argument (the calling Series, DataFrame
      or Panel) and that returns valid output for indexing (one of the above)

    .. note:: MultiIndex is not supported yet.

    .. note:: Note that contrary to usual python slices, **both** the
        start and the stop are included, and the step of the slice is not allowed.

    .. note:: With a list or array of labels for row selection,
        pandas-on-Spark behaves as a filter without reordering by the labels.

    See Also
    --------
    Series.loc : Access group of values using labels.

    Examples
    --------
    **Getting values**

    >>> df = ps.DataFrame([[1, 2], [4, 5], [7, 8]],
    ...                   index=['cobra', 'viper', 'sidewinder'],
    ...                   columns=['max_speed', 'shield'])
    >>> df
                max_speed  shield
    cobra               1       2
    viper               4       5
    sidewinder          7       8

    Single label. Note this returns the row as a Series.

    >>> df.loc['viper']
    max_speed    4
    shield       5
    Name: viper, dtype: int64

    List of labels. Note using ``[[]]`` returns a DataFrame.
    Also note that pandas-on-Spark behaves just a filter without reordering by the labels.

    >>> df.loc[['viper', 'sidewinder']]
                max_speed  shield
    viper               4       5
    sidewinder          7       8

    >>> df.loc[['sidewinder', 'viper']]
                max_speed  shield
    viper               4       5
    sidewinder          7       8

    Single label for column.

    >>> df.loc['cobra', 'shield']
    2

    List of labels for row.

    >>> df.loc[['cobra'], 'shield']
    cobra    2
    Name: shield, dtype: int64

    List of labels for column.

    >>> df.loc['cobra', ['shield']]
    shield    2
    Name: cobra, dtype: int64

    List of labels for both row and column.

    >>> df.loc[['cobra'], ['shield']]
           shield
    cobra       2

    Slice with labels for row and single label for column. As mentioned
    above, note that both the start and stop of the slice are included.

    >>> df.loc['cobra':'viper', 'max_speed']
    cobra    1
    viper    4
    Name: max_speed, dtype: int64

    Conditional that returns a boolean Series

    >>> df.loc[df['shield'] > 6]
                max_speed  shield
    sidewinder          7       8

    Conditional that returns a boolean Series with column labels specified

    >>> df.loc[df['shield'] > 6, ['max_speed']]
                max_speed
    sidewinder          7

    A boolean array of the same length as the column axis being sliced.

    >>> df.loc[:, [False, True]]
                shield
    cobra            2
    viper            5
    sidewinder       8

    An alignable boolean Series to the column axis being sliced.

    >>> df.loc[:, pd.Series([False, True], index=['max_speed', 'shield'])]
                shield
    cobra            2
    viper            5
    sidewinder       8

    **Setting values**

    Setting value for all items matching the list of labels.

    >>> df.loc[['viper', 'sidewinder'], ['shield']] = 50
    >>> df
                max_speed  shield
    cobra               1       2
    viper               4      50
    sidewinder          7      50

    Setting value for an entire row

    >>> df.loc['cobra'] = 10
    >>> df
                max_speed  shield
    cobra              10      10
    viper               4      50
    sidewinder          7      50

    Set value for an entire column

    >>> df.loc[:, 'max_speed'] = 30
    >>> df
                max_speed  shield
    cobra              30      10
    viper              30      50
    sidewinder         30      50

    Set value for an entire list of columns

    >>> df.loc[:, ['max_speed', 'shield']] = 100
    >>> df
                max_speed  shield
    cobra             100     100
    viper             100     100
    sidewinder        100     100

    Set value with Series

    >>> df.loc[:, 'shield'] = df['shield'] * 2
    >>> df
                max_speed  shield
    cobra             100     200
    viper             100     200
    sidewinder        100     200

    **Getting values on a DataFrame with an index that has integer labels**

    Another example using integers for the index

    >>> df = ps.DataFrame([[1, 2], [4, 5], [7, 8]],
    ...                   index=[7, 8, 9],
    ...                   columns=['max_speed', 'shield'])
    >>> df
       max_speed  shield
    7          1       2
    8          4       5
    9          7       8

    Slice with integer labels for rows. As mentioned above, note that both
    the start and stop of the slice are included.

    >>> df.loc[7:9]
       max_speed  shield
    7          1       2
    8          4       5
    9          7       8
    """

    @staticmethod
    def _NotImplemented(description: str) -> SparkPandasNotImplementedError:
        return SparkPandasNotImplementedError(
            description=description,
            pandas_function=".loc[..., ...]",
            spark_target_function="select, where",
        )

    def _select_rows_by_series(
        self, rows_sel: "Series"
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        assert isinstance(rows_sel.spark.data_type, BooleanType), rows_sel.spark.data_type
        return rows_sel.spark.column, None, None

    def _select_rows_by_spark_column(
        self, rows_sel: Column
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        spark_type = self._internal.spark_frame.select(rows_sel).schema[0].dataType
        assert isinstance(spark_type, BooleanType), spark_type
        return rows_sel, None, None

    def _select_rows_by_slice(
        self, rows_sel: slice
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        from pyspark.pandas.indexes import MultiIndex

        if rows_sel.step is not None:
            raise LocIndexer._NotImplemented("Cannot use step with Spark.")
        elif self._internal.index_level == 1:
            sdf = self._internal.spark_frame
            index = self._psdf_or_psser.index
            index_column = index.to_series()
            index_data_type = index_column.spark.data_type
            start = rows_sel.start
            stop = rows_sel.stop

            # get natural order from '__natural_order__' from start to stop
            # to keep natural order.
            start_and_stop = (
                sdf.select(index_column.spark.column, NATURAL_ORDER_COLUMN_NAME)
                .where(
                    (index_column.spark.column == SF.lit(start).cast(index_data_type))
                    | (index_column.spark.column == SF.lit(stop).cast(index_data_type))
                )
                .collect()
            )

            start = [row[1] for row in start_and_stop if row[0] == start]
            start = start[0] if len(start) > 0 else None

            stop = [row[1] for row in start_and_stop if row[0] == stop]
            stop = stop[-1] if len(stop) > 0 else None

            conds = []  # type: List[Column]
            if start is not None:
                conds.append(F.col(NATURAL_ORDER_COLUMN_NAME) >= SF.lit(start).cast(LongType()))
            if stop is not None:
                conds.append(F.col(NATURAL_ORDER_COLUMN_NAME) <= SF.lit(stop).cast(LongType()))

            # if index order is not monotonic increasing or decreasing
            # and specified values don't exist in index, raise KeyError
            if (start is None and rows_sel.start is not None) or (
                stop is None and rows_sel.stop is not None
            ):

                inc = index_column.is_monotonic_increasing
                if inc is False:
                    dec = index_column.is_monotonic_decreasing

                if start is None and rows_sel.start is not None:
                    start = rows_sel.start
                    if inc is not False:
                        conds.append(
                            index_column.spark.column >= SF.lit(start).cast(index_data_type)
                        )
                    elif dec is not False:
                        conds.append(
                            index_column.spark.column <= SF.lit(start).cast(index_data_type)
                        )
                    else:
                        raise KeyError(rows_sel.start)
                if stop is None and rows_sel.stop is not None:
                    stop = rows_sel.stop
                    if inc is not False:
                        conds.append(
                            index_column.spark.column <= SF.lit(stop).cast(index_data_type)
                        )
                    elif dec is not False:
                        conds.append(
                            index_column.spark.column >= SF.lit(stop).cast(index_data_type)
                        )
                    else:
                        raise KeyError(rows_sel.stop)

            return reduce(lambda x, y: x & y, conds), None, None
        else:
            index = self._psdf_or_psser.index
            index_data_type = [f.dataType for f in index.to_series().spark.data_type]

            start = rows_sel.start
            if start is not None:
                if not isinstance(start, tuple):
                    start = (start,)
                if len(start) == 0:
                    start = None
            stop = rows_sel.stop
            if stop is not None:
                if not isinstance(stop, tuple):
                    stop = (stop,)
                if len(stop) == 0:
                    stop = None

            depth = max(
                len(start) if start is not None else 0, len(stop) if stop is not None else 0
            )
            if depth == 0:
                return None, None, None
            elif (
                depth > self._internal.index_level
                or not index.droplevel(list(range(self._internal.index_level)[depth:])).is_monotonic
            ):
                raise KeyError(
                    "Key length ({}) was greater than MultiIndex sort depth".format(depth)
                )

            conds = []
            if start is not None:
                cond = SF.lit(True)
                for scol, value, dt in list(
                    zip(self._internal.index_spark_columns, start, index_data_type)
                )[::-1]:
                    compare = MultiIndex._comparator_for_monotonic_increasing(dt)
                    cond = F.when(scol.eqNullSafe(SF.lit(value).cast(dt)), cond).otherwise(
                        compare(scol, SF.lit(value).cast(dt), Column.__gt__)
                    )
                conds.append(cond)
            if stop is not None:
                cond = SF.lit(True)
                for scol, value, dt in list(
                    zip(self._internal.index_spark_columns, stop, index_data_type)
                )[::-1]:
                    compare = MultiIndex._comparator_for_monotonic_increasing(dt)
                    cond = F.when(scol.eqNullSafe(SF.lit(value).cast(dt)), cond).otherwise(
                        compare(scol, SF.lit(value).cast(dt), Column.__lt__)
                    )
                conds.append(cond)

            return reduce(lambda x, y: x & y, conds), None, None

    def _select_rows_by_iterable(
        self, rows_sel: Iterable
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        rows_sel = list(rows_sel)
        if len(rows_sel) == 0:
            return SF.lit(False), None, None
        elif self._internal.index_level == 1:
            index_column = self._psdf_or_psser.index.to_series()
            index_data_type = index_column.spark.data_type
            if len(rows_sel) == 1:
                return (
                    index_column.spark.column == SF.lit(rows_sel[0]).cast(index_data_type),
                    None,
                    None,
                )
            else:
                return (
                    index_column.spark.column.isin(
                        [SF.lit(r).cast(index_data_type) for r in rows_sel]
                    ),
                    None,
                    None,
                )
        else:
            raise LocIndexer._NotImplemented("Cannot select with MultiIndex with Spark.")

    def _select_rows_else(
        self, rows_sel: Any
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        if not isinstance(rows_sel, tuple):
            rows_sel = (rows_sel,)
        if len(rows_sel) > self._internal.index_level:
            raise SparkPandasIndexingError("Too many indexers")

        rows = [scol == value for scol, value in zip(self._internal.index_spark_columns, rows_sel)]
        return (
            reduce(lambda x, y: x & y, rows),
            None,
            self._internal.index_level - len(rows_sel),
        )

    def _get_from_multiindex_column(
        self,
        key: Optional[Label],
        missing_keys: Optional[List[Name]],
        labels: Optional[List[Tuple[Label, Label]]] = None,
        recursed: int = 0,
    ) -> Tuple[List[Label], Optional[List[Column]], List[InternalField], bool, Optional[Name]]:
        """Select columns from multi-index columns."""
        assert isinstance(key, tuple)
        if labels is None:
            labels = [(label, label) for label in self._internal.column_labels]
        for k in key:
            labels = [
                (label, None if lbl is None else lbl[1:])
                for label, lbl in labels
                if (lbl is None and k is None) or (lbl is not None and lbl[0] == k)
            ]
            if len(labels) == 0:
                if missing_keys is None:
                    raise KeyError(k)
                else:
                    missing_keys.append(key)
                    return [], [], [], False, None

        if all(lbl is not None and len(lbl) > 0 and lbl[0] == "" for _, lbl in labels):
            # If the head is '', drill down recursively.
            labels = [(label, tuple([str(key), *lbl[1:]])) for i, (label, lbl) in enumerate(labels)]
            return self._get_from_multiindex_column((str(key),), missing_keys, labels, recursed + 1)
        else:
            returns_series = all(lbl is None or len(lbl) == 0 for _, lbl in labels)
            if returns_series:
                label_set = set(label for label, _ in labels)
                assert len(label_set) == 1
                label = list(label_set)[0]
                column_labels = [label]
                data_spark_columns = [self._internal.spark_column_for(label)]
                data_fields = [self._internal.field_for(label)]
                if label is None:
                    series_name = None  # type: Name
                else:
                    if recursed > 0:
                        label = label[:-recursed]
                    series_name = label if len(label) > 1 else label[0]
            else:
                column_labels = [
                    None if lbl is None or lbl == (None,) else lbl for _, lbl in labels
                ]
                data_spark_columns = [self._internal.spark_column_for(label) for label, _ in labels]
                data_fields = [self._internal.field_for(label) for label, _ in labels]
                series_name = None

            return column_labels, data_spark_columns, data_fields, returns_series, series_name

    def _select_cols_by_series(
        self, cols_sel: "Series", missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        column_labels = cols_sel._internal.column_labels
        data_spark_columns = cols_sel._internal.data_spark_columns
        data_fields = cols_sel._internal.data_fields
        return column_labels, data_spark_columns, data_fields, True, None

    def _select_cols_by_spark_column(
        self, cols_sel: Column, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        column_labels = [
            (self._internal.spark_frame.select(cols_sel).columns[0],)
        ]  # type: List[Label]
        data_spark_columns = [cols_sel]
        return column_labels, data_spark_columns, None, True, None

    def _select_cols_by_slice(
        self, cols_sel: slice, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        start, stop = self._psdf_or_psser.columns.slice_locs(
            start=cols_sel.start, end=cols_sel.stop
        )
        column_labels = self._internal.column_labels[start:stop]
        data_spark_columns = self._internal.data_spark_columns[start:stop]
        data_fields = self._internal.data_fields[start:stop]
        return column_labels, data_spark_columns, data_fields, False, None

    def _select_cols_by_iterable(
        self, cols_sel: Iterable, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        from pyspark.pandas.series import Series

        if all(isinstance(key, Series) for key in cols_sel):
            column_labels = [key._column_label for key in cols_sel]
            data_spark_columns = [key.spark.column for key in cols_sel]
            data_fields = [key._internal.data_fields[0] for key in cols_sel]
        elif all(isinstance(key, Column) for key in cols_sel):
            column_labels = [
                (self._internal.spark_frame.select(col).columns[0],) for col in cols_sel
            ]
            data_spark_columns = list(cols_sel)
            data_fields = None
        elif all(isinstance(key, bool) for key in cols_sel) or all(
            isinstance(key, np.bool_) for key in cols_sel
        ):
            if len(cast(Sized, cols_sel)) != len(self._internal.column_labels):
                raise IndexError(
                    "Boolean index has wrong length: %s instead of %s"
                    % (len(cast(Sized, cols_sel)), len(self._internal.column_labels))
                )
            if isinstance(cols_sel, pd.Series):
                if not cols_sel.index.sort_values().equals(self._psdf.columns.sort_values()):
                    raise SparkPandasIndexingError(
                        "Unalignable boolean Series provided as indexer "
                        "(index of the boolean Series and of the indexed object do not match)"
                    )
                else:
                    column_labels = [
                        column_label
                        for column_label in self._internal.column_labels
                        if cols_sel[column_label if len(column_label) > 1 else column_label[0]]
                    ]
                    data_spark_columns = [
                        self._internal.spark_column_for(column_label)
                        for column_label in column_labels
                    ]
                    data_fields = [
                        self._internal.field_for(column_label) for column_label in column_labels
                    ]
            else:
                column_labels = [
                    self._internal.column_labels[i] for i, col in enumerate(cols_sel) if col
                ]
                data_spark_columns = [
                    self._internal.data_spark_columns[i] for i, col in enumerate(cols_sel) if col
                ]
                data_fields = [
                    self._internal.data_fields[i] for i, col in enumerate(cols_sel) if col
                ]
        elif any(isinstance(key, tuple) for key in cols_sel) and any(
            not is_name_like_tuple(key) for key in cols_sel
        ):
            raise TypeError(
                "Expected tuple, got {}".format(
                    type(set(key for key in cols_sel if not is_name_like_tuple(key)).pop())
                )
            )
        else:
            if missing_keys is None and all(isinstance(key, tuple) for key in cols_sel):
                level = self._internal.column_labels_level
                if any(len(key) != level for key in cols_sel):
                    raise ValueError("All the key level should be the same as column index level.")

            column_labels = []
            data_spark_columns = []
            data_fields = []
            for key in cols_sel:
                found = False
                for label in self._internal.column_labels:
                    if label == key or label[0] == key:
                        column_labels.append(label)
                        data_spark_columns.append(self._internal.spark_column_for(label))
                        data_fields.append(self._internal.field_for(label))
                        found = True
                if not found:
                    if missing_keys is None:
                        raise KeyError("['{}'] not in index".format(name_like_string(key)))
                    else:
                        missing_keys.append(key)

        return column_labels, data_spark_columns, data_fields, False, None

    def _select_cols_else(
        self, cols_sel: Any, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        if not is_name_like_tuple(cols_sel):
            cols_sel = (cols_sel,)
        return self._get_from_multiindex_column(cols_sel, missing_keys)


class iLocIndexer(LocIndexerLike):
    """
    Purely integer-location based indexing for selection by position.

    ``.iloc[]`` is primarily integer position based (from ``0`` to
    ``length-1`` of the axis), but may also be used with a conditional boolean Series.

    Allowed inputs are:

    - An integer for column selection, e.g. ``5``.
    - A list or array of integers for row selection with distinct index values,
      e.g. ``[3, 4, 0]``
    - A list or array of integers for column selection, e.g. ``[4, 3, 0]``.
    - A boolean array for column selection.
    - A slice object with ints for row and column selection, e.g. ``1:7``.

    Not allowed inputs which pandas allows are:

    - A list or array of integers for row selection with duplicated indexes,
      e.g. ``[4, 4, 0]``.
    - A boolean array for row selection.
    - A ``callable`` function with one argument (the calling Series, DataFrame
      or Panel) and that returns valid output for indexing (one of the above).
      This is useful in method chains, when you don't have a reference to the
      calling object, but would like to base your selection on some value.

    ``.iloc`` will raise ``IndexError`` if a requested indexer is
    out-of-bounds, except *slice* indexers which allow out-of-bounds
    indexing (this conforms with python/numpy *slice* semantics).

    See Also
    --------
    DataFrame.loc : Purely label-location based indexer for selection by label.
    Series.iloc : Purely integer-location based indexing for
                   selection by position.

    Examples
    --------

    >>> mydict = [{'a': 1, 'b': 2, 'c': 3, 'd': 4},
    ...           {'a': 100, 'b': 200, 'c': 300, 'd': 400},
    ...           {'a': 1000, 'b': 2000, 'c': 3000, 'd': 4000 }]
    >>> df = ps.DataFrame(mydict, columns=['a', 'b', 'c', 'd'])
    >>> df
          a     b     c     d
    0     1     2     3     4
    1   100   200   300   400
    2  1000  2000  3000  4000

    **Indexing just the rows**

    A scalar integer for row selection.

    >>> df.iloc[1]
    a    100
    b    200
    c    300
    d    400
    Name: 1, dtype: int64

    >>> df.iloc[[0]]
       a  b  c  d
    0  1  2  3  4

    With a `slice` object.

    >>> df.iloc[:3]
          a     b     c     d
    0     1     2     3     4
    1   100   200   300   400
    2  1000  2000  3000  4000

    **Indexing both axes**

    You can mix the indexer types for the index and columns. Use ``:`` to
    select the entire axis.

    With scalar integers.

    >>> df.iloc[:1, 1]
    0    2
    Name: b, dtype: int64

    With lists of integers.

    >>> df.iloc[:2, [1, 3]]
         b    d
    0    2    4
    1  200  400

    With `slice` objects.

    >>> df.iloc[:2, 0:3]
         a    b    c
    0    1    2    3
    1  100  200  300

    With a boolean array whose length matches the columns.

    >>> df.iloc[:, [True, False, True, False]]
          a     c
    0     1     3
    1   100   300
    2  1000  3000

    **Setting values**

    Setting value for all items matching the list of labels.

    >>> df.iloc[[1, 2], [1]] = 50
    >>> df
          a   b     c     d
    0     1   2     3     4
    1   100  50   300   400
    2  1000  50  3000  4000

    Setting value for an entire row

    >>> df.iloc[0] = 10
    >>> df
          a   b     c     d
    0    10  10    10    10
    1   100  50   300   400
    2  1000  50  3000  4000

    Set value for an entire column

    >>> df.iloc[:, 2] = 30
    >>> df
          a   b   c     d
    0    10  10  30    10
    1   100  50  30   400
    2  1000  50  30  4000

    Set value for an entire list of columns

    >>> df.iloc[:, [2, 3]] = 100
    >>> df
          a   b    c    d
    0    10  10  100  100
    1   100  50  100  100
    2  1000  50  100  100

    Set value with Series

    >>> df.iloc[:, 3] = df.iloc[:, 3] * 2
    >>> df
          a   b    c    d
    0    10  10  100  200
    1   100  50  100  200
    2  1000  50  100  200
    """

    @staticmethod
    def _NotImplemented(description: str) -> SparkPandasNotImplementedError:
        return SparkPandasNotImplementedError(
            description=description,
            pandas_function=".iloc[..., ...]",
            spark_target_function="select, where",
        )

    @lazy_property
    def _internal(self) -> "InternalFrame":
        # Use resolved_copy to fix the natural order.
        internal = super()._internal.resolved_copy
        sdf, force_nullable = InternalFrame.attach_distributed_sequence_column(
            internal.spark_frame, column_name=self._sequence_col
        )
        return internal.with_new_sdf(
            spark_frame=sdf.orderBy(NATURAL_ORDER_COLUMN_NAME),
            index_fields=(
                [field.copy(nullable=True) for field in internal.index_fields]
                if force_nullable
                else internal.index_fields
            ),
            data_fields=(
                [field.copy(nullable=True) for field in internal.data_fields]
                if force_nullable
                else internal.data_fields
            ),
        )

    @lazy_property
    def _sequence_col(self) -> str:
        # Use resolved_copy to fix the natural order.
        internal = super()._internal.resolved_copy
        return verify_temp_column_name(internal.spark_frame, "__distributed_sequence_column__")

    def _select_rows_by_series(
        self, rows_sel: "Series"
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        raise iLocIndexer._NotImplemented(
            ".iloc requires numeric slice, conditional "
            "boolean Index or a sequence of positions as int, "
            "got {}".format(type(rows_sel))
        )

    def _select_rows_by_spark_column(
        self, rows_sel: Column
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        raise iLocIndexer._NotImplemented(
            ".iloc requires numeric slice, conditional "
            "boolean Index or a sequence of positions as int, "
            "got {}".format(type(rows_sel))
        )

    def _select_rows_by_slice(
        self, rows_sel: slice
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        def verify_type(i: int) -> None:
            if not isinstance(i, int):
                raise TypeError(
                    "cannot do slice indexing with these indexers [{}] of {}".format(i, type(i))
                )

        has_negative = False
        start = rows_sel.start
        if start is not None:
            verify_type(start)
            if start == 0:
                start = None
            elif start < 0:
                has_negative = True
        stop = rows_sel.stop
        if stop is not None:
            verify_type(stop)
            if stop < 0:
                has_negative = True

        step = rows_sel.step
        if step is not None:
            verify_type(step)
            if step == 0:
                raise ValueError("slice step cannot be zero")
        else:
            step = 1

        if start is None and step == 1:
            return None, stop, None

        sdf = self._internal.spark_frame
        sequence_scol = sdf[self._sequence_col]

        if has_negative or (step < 0 and start is None):
            cnt = sdf.count()

        cond = []
        if start is not None:
            if start < 0:
                start = start + cnt
            if step >= 0:
                cond.append(sequence_scol >= SF.lit(start).cast(LongType()))
            else:
                cond.append(sequence_scol <= SF.lit(start).cast(LongType()))
        if stop is not None:
            if stop < 0:
                stop = stop + cnt
            if step >= 0:
                cond.append(sequence_scol < SF.lit(stop).cast(LongType()))
            else:
                cond.append(sequence_scol > SF.lit(stop).cast(LongType()))
        if step != 1:
            if step > 0:
                start = start or 0
            else:
                start = start or (cnt - 1)
            cond.append(((sequence_scol - start) % SF.lit(step).cast(LongType())) == SF.lit(0))

        return reduce(lambda x, y: x & y, cond), None, None

    def _select_rows_by_iterable(
        self, rows_sel: Iterable
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        sdf = self._internal.spark_frame

        if any(isinstance(key, (int, np.int64, np.int32)) and key < 0 for key in rows_sel):
            offset = sdf.count()
        else:
            offset = 0

        new_rows_sel = []
        for key in list(rows_sel):
            if not isinstance(key, (int, np.int64, np.int32)):
                raise TypeError(
                    "cannot do positional indexing with these indexers [{}] of {}".format(
                        key, type(key)
                    )
                )
            if key < 0:
                key = key + offset
            new_rows_sel.append(key)

        if len(new_rows_sel) != len(set(new_rows_sel)):
            raise NotImplementedError(
                "Duplicated row selection is not currently supported; "
                "however, normalised index was [%s]" % new_rows_sel
            )

        sequence_scol = sdf[self._sequence_col]
        cond = []
        for key in new_rows_sel:
            cond.append(sequence_scol == SF.lit(int(key)).cast(LongType()))

        if len(cond) == 0:
            cond = [SF.lit(False)]
        return reduce(lambda x, y: x | y, cond), None, None

    def _select_rows_else(
        self, rows_sel: Any
    ) -> Tuple[Optional[Column], Optional[int], Optional[int]]:
        if isinstance(rows_sel, int):
            sdf = self._internal.spark_frame
            return (sdf[self._sequence_col] == rows_sel), None, 0
        elif isinstance(rows_sel, tuple):
            raise SparkPandasIndexingError("Too many indexers")
        else:
            raise iLocIndexer._NotImplemented(
                ".iloc requires numeric slice, conditional "
                "boolean Index or a sequence of positions as int, "
                "got {}".format(type(rows_sel))
            )

    def _select_cols_by_series(
        self, cols_sel: "Series", missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        raise ValueError(
            "Location based indexing can only have [integer, integer slice, "
            "listlike of integers, boolean array] types, got {}".format(cols_sel)
        )

    def _select_cols_by_spark_column(
        self, cols_sel: Column, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        raise ValueError(
            "Location based indexing can only have [integer, integer slice, "
            "listlike of integers, boolean array] types, got {}".format(cols_sel)
        )

    def _select_cols_by_slice(
        self, cols_sel: slice, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        if all(
            s is None or isinstance(s, int) for s in (cols_sel.start, cols_sel.stop, cols_sel.step)
        ):
            column_labels = self._internal.column_labels[cols_sel]
            data_spark_columns = self._internal.data_spark_columns[cols_sel]
            data_fields = self._internal.data_fields[cols_sel]
            return column_labels, data_spark_columns, data_fields, False, None
        else:
            not_none = (
                cols_sel.start
                if cols_sel.start is not None
                else cols_sel.stop
                if cols_sel.stop is not None
                else cols_sel.step
            )
            raise TypeError(
                "cannot do slice indexing with these indexers {} of {}".format(
                    not_none, type(not_none)
                )
            )

    def _select_cols_by_iterable(
        self, cols_sel: Iterable, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        if all(isinstance(s, bool) for s in cols_sel):
            cols_sel = [i for i, s in enumerate(cols_sel) if s]
        if all(isinstance(s, int) for s in cols_sel):
            column_labels = [self._internal.column_labels[s] for s in cols_sel]
            data_spark_columns = [self._internal.data_spark_columns[s] for s in cols_sel]
            data_fields = [self._internal.data_fields[s] for s in cols_sel]
            return column_labels, data_spark_columns, data_fields, False, None
        else:
            raise TypeError("cannot perform reduce with flexible type")

    def _select_cols_else(
        self, cols_sel: Any, missing_keys: Optional[List[Name]]
    ) -> Tuple[
        List[Label],
        Optional[List[Column]],
        Optional[List[InternalField]],
        bool,
        Optional[Name],
    ]:
        if isinstance(cols_sel, int):
            if cols_sel > len(self._internal.column_labels):
                raise KeyError(cols_sel)
            column_labels = [self._internal.column_labels[cols_sel]]
            data_spark_columns = [self._internal.data_spark_columns[cols_sel]]
            data_fields = [self._internal.data_fields[cols_sel]]
            return column_labels, data_spark_columns, data_fields, True, None
        else:
            raise ValueError(
                "Location based indexing can only have [integer, integer slice, "
                "listlike of integers, boolean array] types, got {}".format(cols_sel)
            )

    def __setitem__(self, key: Any, value: Any) -> None:
        if is_list_like(value) and not isinstance(value, Column):
            iloc_item = self[key]
            if not is_list_like(key) or not is_list_like(iloc_item):
                raise ValueError("setting an array element with a sequence.")
            else:
                shape_iloc_item = iloc_item.shape
                len_iloc_item = shape_iloc_item[0]
                len_value = len(value)
                if len_iloc_item != len_value:
                    if self._is_series:
                        raise ValueError(
                            "cannot set using a list-like indexer with a different length than "
                            "the value"
                        )
                    else:
                        raise ValueError(
                            "shape mismatch: value array of shape ({},) could not be broadcast "
                            "to indexing result of shape {}".format(len_value, shape_iloc_item)
                        )
        super().__setitem__(key, value)
        # Update again with resolved_copy to drop extra columns.
        self._psdf._update_internal_frame(
            self._psdf._internal.resolved_copy, requires_same_anchor=False
        )

        # Clean up implicitly cached properties to be able to reuse the indexer.
        del self._internal
        del self._sequence_col


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.indexing

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.indexing.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.indexing tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.indexing,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
