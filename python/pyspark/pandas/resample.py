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
from abc import ABCMeta
from distutils.version import LooseVersion
from functools import partial
from typing import (
    Any,
    Generic,
    List,
    Optional,
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
from pyspark.pandas.missing.resample import (
    MissingPandasLikeDataFrameResampler,
    MissingPandasLikeSeriesResampler,
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
    """
    Class for resampling datetimelike data, a groupby-like operation.

    It's easiest to use obj.resample(...) to use Resampler.

    Parameters
    ----------
    psdf : DataFrame

    Returns
    -------
    a Resampler of the appropriate type

    Notes
    -----
    After resampling, see aggregate, apply, and transform functions.
    """

    def __init__(
        self,
        psdf: DataFrame,
        resamplekey: Optional[Series],
        rule: str,
        closed: Optional[str],
        label: Optional[str],
        agg_columns: List[Series],
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

    def _downsample(self, f: str) -> FrameLike:
        """
        Downsample the defined function.

        Parameters
        ----------
        how : string / mapped function
        **kwargs : kw args passed to how function
        """
        # a pass is needed to obtain the range, in the future we may cache it in the index.
        range_pdf = self._psdf._internal.spark_frame.select(
            F.min(self._resamplekey_scol), F.max(self._resamplekey_scol)
        ).toPandas()
        ts_min, ts_max = range_pdf.iloc[0]

        # the logical to obtain a base to bin the timestamps is too complex to follow,
        # here just use Pandas' resample on a 1-length series to get it.
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
            struct_field=StructField(bin_col_name, TimestampType(), True),
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
        assert len(agg_columns) > 0

        # some intervals maybe too large for the down sampling,
        # so we need to pad the inputs (with null values) to avoid missing some results.
        # for example:
        #   dates = [
        #         datetime.datetime(2011, 12, 31),
        #         datetime.datetime(2012, 1, 2),
        #         datetime.datetime(2013, 5, 3),
        #         datetime.datetime(2022, 5, 3),
        #   ]
        #   pdf = pd.DataFrame(np.ones(4), index=pd.DatetimeIndex(dates), columns=['A'])
        #   pdf.resample('3Y').mean()
        #                 A
        #   2011-12-31  1.0
        #   2014-12-31  1.0
        #   2017-12-31  NaN
        #   2020-12-31  NaN
        #   2023-12-31  1.0

        # label the timestamps according to the base and the freq(rule) in the binning side
        sdf = self._psdf._internal.spark_frame.select(
            F.col(SPARK_DEFAULT_INDEX_NAME),
            bin_scol,
            *[psser.spark.column for psser in agg_columns],
        )

        # apply ps.date_range to insert necessary points in the padding side
        pad_sdf = (
            ps.date_range(start=ts_base, end=ts_max, freq=self._rule)
            ._internal.spark_frame.select(
                F.col(SPARK_DEFAULT_INDEX_NAME), F.col(SPARK_DEFAULT_INDEX_NAME).alias(bin_col_name)
            )
            .where((ts_min < F.col(bin_col_name)) & (F.col(bin_col_name) < ts_max))
        )

        sdf = sdf.unionByName(pad_sdf, allowMissingColumns=True)

        # something goes wrong and mess the computation logical in the padding side,
        # which will result in wrong results.
        # the conversion to/from rdd here is to work around this wrong optimization.
        spark = sdf.sparkSession
        sdf = spark.createDataFrame(sdf.rdd, sdf.schema).withColumn(
            bin_col_name,
            F.when(F.isnull(F.col(bin_col_name)), SF.lit(None).cast(TimestampType())).otherwise(
                F.col(bin_col_name)
            ),
        )

        internal = InternalFrame(
            spark_frame=sdf,
            index_spark_columns=[scol_for(sdf, SPARK_DEFAULT_INDEX_NAME)],
            data_spark_columns=[F.col(bin_col_name)]
            + [scol_for(sdf, psser._internal.data_spark_column_names[0]) for psser in agg_columns],
            column_labels=[bin_col_label] + [psser._column_label for psser in agg_columns],
            data_fields=[bin_col_field]
            + [psser._internal.data_fields[0].copy(nullable=True) for psser in agg_columns],
            column_label_names=self._psdf._internal.column_label_names,
        )
        psdf: DataFrame = DataFrame(internal)

        groupby = psdf.groupby(psdf._psser_for(bin_col_label), dropna=False)
        down_sampled = getattr(groupby, f)()
        down_sampled.index.name = None

        return down_sampled

    def min(self) -> FrameLike:
        return self._downsample("min")

    def max(self) -> FrameLike:
        return self._downsample("max")

    def sum(self) -> FrameLike:
        return self._downsample("sum").fillna(0.0)

    def mean(self) -> FrameLike:
        return self._downsample("mean")

    def std(self) -> FrameLike:
        return self._downsample("std")

    def var(self) -> FrameLike:
        return self._downsample("var")


class DataFrameResampler(Resampler[DataFrame]):
    def __init__(
        self,
        psdf: DataFrame,
        rule: str,
        closed: Optional[str],
        label: Optional[str],
        on: Optional[Series],
    ):
        agg_columns: List[Series] = []
        for column_label in psdf._internal.column_labels:
            if isinstance(psdf._internal.spark_type_for(column_label), (NumericType, BooleanType)):
                agg_columns.append(psdf._psser_for(column_label))
        assert len(agg_columns) > 0

        super().__init__(
            psdf=psdf,
            resamplekey=on,
            rule=rule,
            closed=closed,
            label=label,
            agg_columns=agg_columns,
        )

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeDataFrameResampler, item):
            property_or_func = getattr(MissingPandasLikeDataFrameResampler, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)


class SeriesResampler(Resampler[Series]):
    def __init__(
        self,
        psser: Series,
        rule: str,
        closed: Optional[str],
        label: Optional[str],
        on: Optional[Series],
    ):
        super().__init__(
            psdf=psser._psdf,
            resamplekey=on,
            rule=rule,
            closed=closed,
            label=label,
            agg_columns=[psser],
        )

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeDataFrameResampler, item):
            property_or_func = getattr(MissingPandasLikeDataFrameResampler, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)
