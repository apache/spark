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
A wrapper for ResampledData to behave similar to pandas Resampler.
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

import numpy as np

import pandas as pd
from pandas.api.types import is_hashable, is_list_like  # type: ignore[attr-defined]

if LooseVersion(pd.__version__) >= LooseVersion("1.3.0"):
    from pandas.core.common import _builtin_table  # type: ignore[attr-defined]
else:
    from pandas.core.base import SelectionMixin

    _builtin_table = SelectionMixin._builtin_table  # type: ignore[attr-defined]

from pyspark import SparkContext
from pyspark.sql import Column, Window, functions as F
from pyspark.sql.types import (
    BooleanType,
    NumericType,
    StructField,
    StructType,
    StringType,
    TimestampType,
)

from pyspark import pandas as ps  # For running doctests and reference resolution in PyCharm.
from pyspark.pandas._typing import Axis, FrameLike, Label, Name
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    HIDDEN_COLUMNS,
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_INDEX_NAME_FORMAT,
    SPARK_DEFAULT_INDEX_NAME,
    SPARK_DEFAULT_SERIES_NAME,
    SPARK_INDEX_NAME_PATTERN,
)
from pyspark.pandas.series import Series, first_series
from pyspark.pandas.spark import functions as SF
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


class Resampler(Generic[FrameLike], metaclass=ABCMeta):
    def __init__(
        self,
        psdf: DataFrame,
        resamplekey: Optional[Series],
        rule: str,
        closed: Optional[str] = None,
        label: Optional[str] = None,
        agg_columns: List[Series] = [],
    ):
        self._psdf = psdf
        self._resamplekey = resamplekey
        self._rule = rule
        self._closed = closed
        self._label = label
        self._agg_columns = agg_columns

    @property
    def _resamplekey_scol(self) -> Column:
        if self._resamplekey is None:
            return self._psdf.index.spark.column
        else:
            return self._resamplekey.spark.column

    @property
    def _agg_columns_scols(self) -> List[Column]:
        return [s.spark.column for s in self._agg_columns]

    def _downsample(self, method_name: str) -> FrameLike:
        """
        Downsample the defined function.

        Parameters
        ----------
        how : string / mapped function
        **kwargs : kw args passed to how function
        """
        range_pdf = self._psdf._internal.spark_frame.select(
            F.min(self._resamplekey_scol), F.max(self._resamplekey_scol)
        ).toPandas()
        ts_min, ts_max = range_pdf.iloc[0, 0], range_pdf.iloc[0, 1]

        ts_base = (
            pd.Series([0], index=[ts_min])
            .resample(self._rule, closed=self._closed, label="left")
            .sum()
            .index[0]
        )
        assert ts_base <= ts_min

        sql_utils = SparkContext._active_spark_context._jvm.PythonSQLUtils
        bin_col_name = "__tmp_resample_bin_col__"
        bin_col_label = verify_temp_column_name(self._psdf, bin_col_name)
        bin_col_field = InternalField(
            dtype=np.dtype("datetime64[ns]"),
            struct_field=StructField(bin_col_name, TimestampType(), False),
        )
        bin_scol = Column(
            sql_utils.binTimeStamp(
                F.lit(ts_base)._jc,
                int(self._rule[0]),
                self._rule[1],
                self._closed == "left",
                self._label == "left",
                self._resamplekey_scol._jc,
            )
        ).alias(bin_col_name)

        agg_columns = [
            psser for psser in self._agg_columns if (isinstance(psser.spark.data_type, NumericType))
        ]

        sdf = self._psdf._internal.spark_frame.select(
            F.col(SPARK_DEFAULT_INDEX_NAME),
            bin_scol,
            *[psser.spark.column for psser in agg_columns],
        )

        # ts_pad_start = (
        #     pd.Series([0], index=[ts_min])
        #     .resample(self._rule, closed=self._closed, label=self._label)
        #     .sum()
        #     .index[0]
        # )
        # ts_pad_end = (
        #     pd.Series([0], index=[ts_max])
        #     .resample(self._rule, closed=self._closed, label=self._label)
        #     .sum()
        #     .index[0]
        # )
        # ts_pad_index = [
        #     t
        #     for t in pd.date_range(start=ts_pad_start, end=ts_pad_end, freq=self._rule).values
        #     if ts_min < t < ts_max
        # ]
        # pad_sdf = ps.Series(ts_pad_index)._internal.spark_frame.select(
        #     F.col(SPARK_DEFAULT_INDEX_NAME), F.col("0").alias(bin_col_name)
        # )

        # pad_index = ps.date_range(start=ts_start, end=ts_end, freq=self._rule)
        # pad_sdf = ps.from_pandas(ts_pad_index)._internal.spark_frame.select(
        #     F.col(SPARK_DEFAULT_INDEX_NAME), F.col("__index_level_0__").alias(bin_col_name)
        # )

        pad_sdf = (
            ps.date_range(start=ts_min, end=ts_max, freq=self._rule)
            ._internal.spark_frame.select(
                F.col(SPARK_DEFAULT_INDEX_NAME), F.col(SPARK_DEFAULT_INDEX_NAME).alias(bin_col_name)
            )
            .where((ts_min < F.col(bin_col_name)) & (F.col(bin_col_name) < ts_max))
        )

        sdf = sdf.unionByName(pad_sdf, allowMissingColumns=True)
        # sdf = sdf.withColumn(SPARK_DEFAULT_INDEX_NAME, SF.lit(None).cast(TimestampType()))

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, SPARK_DEFAULT_INDEX_NAME)],
            data_spark_columns=[bin_scol]
            + [scol_for(sdf, psser._internal.data_spark_column_names[0]) for psser in agg_columns],
            column_labels=[bin_col_label] + [psser._column_label for psser in agg_columns],
            data_fields=[bin_col_field] + [psser._internal.data_fields[0] for psser in agg_columns],
            column_label_names=self._psdf._internal.column_label_names,
        )
        psdf: DataFrame = DataFrame(internal)
        # return psdf

        groupby = psdf.groupby(psdf._psser_for(bin_col_label), dropna=False)
        downsampled = getattr(groupby, method_name)()
        downsampled.index.name = None

        return downsampled

    def sum(self) -> FrameLike:
        return self._downsample("sum").fillna(0.0)

    def mean(self) -> FrameLike:
        return self._downsample("mean")

    def max(self) -> FrameLike:
        return self._downsample("max")

    def min(self) -> FrameLike:
        return self._downsample("min")
