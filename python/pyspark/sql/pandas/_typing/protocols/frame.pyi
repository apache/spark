#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This Protocol reuses core Pandas annotation.
# Overall pipeline looks as follows
# - Stubgen pandas.core.frame
# - Add Protocol as a base class
# - Replace imports with Any

import numpy as np
from typing import Any, Hashable, IO, Iterable, List, Optional, Sequence, Tuple, Union
from typing_extensions import Protocol
from .series import SeriesLike

Axes = Any
Dtype = Any
Index = Any
Renamer = Any
Axis = Any
Level = Any

class DataFrameLike(Protocol):
    columns: Axes
    dtypes: List[Any]
    def __init__(
        self,
        data: Any = ...,
        index: Optional[Axes] = ...,
        columns: Optional[Axes] = ...,
        dtype: Optional[Dtype] = ...,
        copy: bool = ...,
    ) -> None: ...
    @property
    def axes(self) -> List[Index]: ...
    @property
    def shape(self) -> Tuple[int, int]: ...
    @property
    def style(self) -> Any: ...
    def items(self) -> Iterable[Tuple[Optional[Hashable], SeriesLike]]: ...
    def iteritems(self) -> Iterable[Tuple[Optional[Hashable], SeriesLike]]: ...
    def iterrows(self) -> Iterable[Tuple[Optional[Hashable], SeriesLike]]: ...
    def itertuples(self, index: bool = ..., name: str = ...): ...
    def __len__(self) -> int: ...
    def dot(self, other: Any): ...
    def __matmul__(self, other: Any): ...
    def __rmatmul__(self, other: Any): ...
    @classmethod
    def from_dict(
        cls: Any, data: Any, orient: Any = ..., dtype: Any = ..., columns: Any = ...
    ) -> DataFrameLike: ...
    def to_numpy(self, dtype: Any = ..., copy: Any = ...) -> np.ndarray: ...
    def to_dict(self, orient: str = ..., into: Any = ...): ...
    def to_gbq(
        self,
        destination_table: Any,
        project_id: Any = ...,
        chunksize: Any = ...,
        reauth: Any = ...,
        if_exists: Any = ...,
        auth_local_webserver: Any = ...,
        table_schema: Any = ...,
        location: Any = ...,
        progress_bar: Any = ...,
        credentials: Any = ...,
    ) -> None: ...
    @classmethod
    def from_records(
        cls: Any,
        data: Any,
        index: Any = ...,
        exclude: Any = ...,
        columns: Any = ...,
        coerce_float: Any = ...,
        nrows: Any = ...,
    ) -> DataFrameLike: ...
    def to_records(
        self, index: Any = ..., column_dtypes: Any = ..., index_dtypes: Any = ...
    ) -> np.recarray: ...
    def to_stata(
        self,
        path: Any,
        convert_dates: Optional[Any] = ...,
        write_index: bool = ...,
        byteorder: Optional[Any] = ...,
        time_stamp: Optional[Any] = ...,
        data_label: Optional[Any] = ...,
        variable_labels: Optional[Any] = ...,
        version: int = ...,
        convert_strl: Optional[Any] = ...,
    ) -> None: ...
    def to_feather(self, path: Any) -> None: ...
    def to_markdown(
        self, buf: Optional[IO[str]] = ..., mode: Optional[str] = ..., **kwargs: Any
    ) -> Optional[str]: ...
    def to_parquet(
        self,
        path: Any,
        engine: Any = ...,
        compression: Any = ...,
        index: Any = ...,
        partition_cols: Any = ...,
        **kwargs: Any
    ) -> None: ...
    def to_html(
        self,
        buf: Optional[Any] = ...,
        columns: Optional[Any] = ...,
        col_space: Optional[Any] = ...,
        header: bool = ...,
        index: bool = ...,
        na_rep: str = ...,
        formatters: Optional[Any] = ...,
        float_format: Optional[Any] = ...,
        sparsify: Optional[Any] = ...,
        index_names: bool = ...,
        justify: Optional[Any] = ...,
        max_rows: Optional[Any] = ...,
        max_cols: Optional[Any] = ...,
        show_dimensions: bool = ...,
        decimal: str = ...,
        bold_rows: bool = ...,
        classes: Optional[Any] = ...,
        escape: bool = ...,
        notebook: bool = ...,
        border: Optional[Any] = ...,
        table_id: Optional[Any] = ...,
        render_links: bool = ...,
        encoding: Optional[Any] = ...,
    ): ...
    def info(
        self,
        verbose: Any = ...,
        buf: Any = ...,
        max_cols: Any = ...,
        memory_usage: Any = ...,
        null_counts: Any = ...,
    ) -> None: ...
    def memory_usage(self, index: Any = ..., deep: Any = ...) -> SeriesLike: ...
    def transpose(self, *args: Any, copy: bool = ...) -> DataFrameLike: ...
    T: Any = ...
    def __getitem__(self, key: Any): ...
    def __setitem__(self, key: Any, value: Any): ...
    def query(self, expr: Any, inplace: bool = ..., **kwargs: Any): ...
    def eval(self, expr: Any, inplace: bool = ..., **kwargs: Any): ...
    def select_dtypes(
        self, include: Any = ..., exclude: Any = ...
    ) -> DataFrameLike: ...
    def insert(
        self, loc: Any, column: Any, value: Any, allow_duplicates: Any = ...
    ) -> None: ...
    def assign(self, **kwargs: Any) -> DataFrameLike: ...
    def lookup(self, row_labels: Any, col_labels: Any) -> np.ndarray: ...
    def align(
        self,
        other: Any,
        join: Any = ...,
        axis: Any = ...,
        level: Any = ...,
        copy: Any = ...,
        fill_value: Any = ...,
        method: Any = ...,
        limit: Any = ...,
        fill_axis: Any = ...,
        broadcast_axis: Any = ...,
    ) -> DataFrameLike: ...
    def reindex(self, *args: Any, **kwargs: Any) -> DataFrameLike: ...
    def drop(
        self,
        labels: Optional[Any] = ...,
        axis: int = ...,
        index: Optional[Any] = ...,
        columns: Optional[Any] = ...,
        level: Optional[Any] = ...,
        inplace: bool = ...,
        errors: str = ...,
    ): ...
    def rename(
        self,
        mapper: Optional[Renamer] = ...,
        *,
        index: Optional[Renamer] = ...,
        columns: Optional[Renamer] = ...,
        axis: Optional[Axis] = ...,
        copy: bool = ...,
        inplace: bool = ...,
        level: Optional[Level] = ...,
        errors: str = ...
    ) -> Optional[DataFrameLike]: ...
    def fillna(
        self,
        value: Any = ...,
        method: Any = ...,
        axis: Any = ...,
        inplace: Any = ...,
        limit: Any = ...,
        downcast: Any = ...,
    ) -> Optional[DataFrameLike]: ...
    def replace(
        self,
        to_replace: Optional[Any] = ...,
        value: Optional[Any] = ...,
        inplace: bool = ...,
        limit: Optional[Any] = ...,
        regex: bool = ...,
        method: str = ...,
    ): ...
    def shift(
        self,
        periods: Any = ...,
        freq: Any = ...,
        axis: Any = ...,
        fill_value: Any = ...,
    ) -> DataFrameLike: ...
    def set_index(
        self,
        keys: Any,
        drop: bool = ...,
        append: bool = ...,
        inplace: bool = ...,
        verify_integrity: bool = ...,
    ): ...
    def reset_index(
        self,
        level: Optional[Union[Hashable, Sequence[Hashable]]] = ...,
        drop: bool = ...,
        inplace: bool = ...,
        col_level: Hashable = ...,
        col_fill: Optional[Hashable] = ...,
    ) -> Optional[DataFrameLike]: ...
    def isna(self) -> DataFrameLike: ...
    def isnull(self) -> DataFrameLike: ...
    def notna(self) -> DataFrameLike: ...
    def notnull(self) -> DataFrameLike: ...
    def dropna(
        self,
        axis: int = ...,
        how: str = ...,
        thresh: Optional[Any] = ...,
        subset: Optional[Any] = ...,
        inplace: bool = ...,
    ): ...
    def drop_duplicates(
        self,
        subset: Optional[Union[Hashable, Sequence[Hashable]]] = ...,
        keep: Union[str, bool] = ...,
        inplace: bool = ...,
        ignore_index: bool = ...,
    ) -> Optional[DataFrameLike]: ...
    def duplicated(
        self,
        subset: Optional[Union[Hashable, Sequence[Hashable]]] = ...,
        keep: Union[str, bool] = ...,
    ) -> SeriesLike: ...
    def sort_values(
        self,
        by: Any,
        axis: int = ...,
        ascending: bool = ...,
        inplace: bool = ...,
        kind: str = ...,
        na_position: str = ...,
        ignore_index: bool = ...,
    ): ...
    def sort_index(
        self,
        axis: Any = ...,
        level: Any = ...,
        ascending: Any = ...,
        inplace: Any = ...,
        kind: Any = ...,
        na_position: Any = ...,
        sort_remaining: Any = ...,
        ignore_index: bool = ...,
    ) -> Any: ...
    def nlargest(self, n: Any, columns: Any, keep: Any = ...) -> DataFrameLike: ...
    def nsmallest(self, n: Any, columns: Any, keep: Any = ...) -> DataFrameLike: ...
    def swaplevel(
        self, i: Any = ..., j: Any = ..., axis: Any = ...
    ) -> DataFrameLike: ...
    def reorder_levels(self, order: Any, axis: Any = ...) -> DataFrameLike: ...
    def combine(
        self,
        other: DataFrameLike,
        func: Any,
        fill_value: Any = ...,
        overwrite: Any = ...,
    ) -> DataFrameLike: ...
    def combine_first(self, other: DataFrameLike) -> DataFrameLike: ...
    def update(
        self,
        other: Any,
        join: Any = ...,
        overwrite: Any = ...,
        filter_func: Any = ...,
        errors: Any = ...,
    ) -> None: ...
    def groupby(
        self,
        by: Any = ...,
        axis: Any = ...,
        level: Any = ...,
        as_index: bool = ...,
        sort: bool = ...,
        group_keys: bool = ...,
        squeeze: bool = ...,
        observed: bool = ...,
    ) -> Any: ...
    def pivot(
        self, index: Any = ..., columns: Any = ..., values: Any = ...
    ) -> DataFrameLike: ...
    def pivot_table(
        self,
        values: Any = ...,
        index: Any = ...,
        columns: Any = ...,
        aggfunc: Any = ...,
        fill_value: Any = ...,
        margins: Any = ...,
        dropna: Any = ...,
        margins_name: Any = ...,
        observed: Any = ...,
    ) -> DataFrameLike: ...
    def stack(self, level: int = ..., dropna: bool = ...): ...
    def explode(self, column: Union[str, Tuple]) -> DataFrameLike: ...
    def unstack(self, level: int = ..., fill_value: Optional[Any] = ...): ...
    def melt(
        self,
        id_vars: Any = ...,
        value_vars: Any = ...,
        var_name: Any = ...,
        value_name: Any = ...,
        col_level: Any = ...,
    ) -> DataFrameLike: ...
    def diff(self, periods: Any = ..., axis: Any = ...) -> DataFrameLike: ...
    def aggregate(self, func: Any, axis: int = ..., *args: Any, **kwargs: Any): ...
    agg: Any = ...
    def transform(
        self, func: Any, axis: Any = ..., *args: Any, **kwargs: Any
    ) -> DataFrameLike: ...
    def apply(
        self,
        func: Any,
        axis: int = ...,
        raw: bool = ...,
        result_type: Optional[Any] = ...,
        args: Any = ...,
        **kwds: Any
    ): ...
    def applymap(self, func: Any) -> DataFrameLike: ...
    def append(
        self,
        other: Any,
        ignore_index: Any = ...,
        verify_integrity: Any = ...,
        sort: Any = ...,
    ) -> DataFrameLike: ...
    def join(
        self,
        other: Any,
        on: Any = ...,
        how: Any = ...,
        lsuffix: Any = ...,
        rsuffix: Any = ...,
        sort: Any = ...,
    ) -> DataFrameLike: ...
    def merge(
        self,
        right: Any,
        how: Any = ...,
        on: Any = ...,
        left_on: Any = ...,
        right_on: Any = ...,
        left_index: Any = ...,
        right_index: Any = ...,
        sort: Any = ...,
        suffixes: Any = ...,
        copy: Any = ...,
        indicator: Any = ...,
        validate: Any = ...,
    ) -> DataFrameLike: ...
    def round(
        self, decimals: Any = ..., *args: Any, **kwargs: Any
    ) -> DataFrameLike: ...
    def corr(self, method: Any = ..., min_periods: Any = ...) -> DataFrameLike: ...
    def cov(self, min_periods: Any = ...) -> DataFrameLike: ...
    def corrwith(
        self, other: Any, axis: Any = ..., drop: Any = ..., method: Any = ...
    ) -> SeriesLike: ...
    def count(
        self, axis: int = ..., level: Optional[Any] = ..., numeric_only: bool = ...
    ): ...
    def nunique(self, axis: Any = ..., dropna: Any = ...) -> SeriesLike: ...
    def idxmin(self, axis: Any = ..., skipna: Any = ...) -> SeriesLike: ...
    def idxmax(self, axis: Any = ..., skipna: Any = ...) -> SeriesLike: ...
    def mode(
        self, axis: Any = ..., numeric_only: Any = ..., dropna: Any = ...
    ) -> DataFrameLike: ...
    def quantile(
        self,
        q: float = ...,
        axis: int = ...,
        numeric_only: bool = ...,
        interpolation: str = ...,
    ): ...
    def to_timestamp(
        self, freq: Any = ..., how: Any = ..., axis: Any = ..., copy: Any = ...
    ) -> DataFrameLike: ...
    def to_period(
        self, freq: Any = ..., axis: Any = ..., copy: Any = ...
    ) -> DataFrameLike: ...
    def isin(self, values: Any) -> DataFrameLike: ...
    def copy(self) -> DataFrameLike: ...
    plot: Any = ...
    hist: Any = ...
    boxplot: Any = ...
    sparse: Any = ...
    loc: Any = ...
    iloc: Any = ...
