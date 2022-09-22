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
from pandas.tseries.frequencies import to_offset

if LooseVersion(pd.__version__) >= LooseVersion("1.3.0"):
    from pandas.core.common import _builtin_table  # type: ignore[attr-defined]
else:
    from pandas.core.base import SelectionMixin

    _builtin_table = SelectionMixin._builtin_table  # type: ignore[attr-defined]

from pyspark import SparkContext
from pyspark.sql import Column, functions as F
from pyspark.sql.types import (
    NumericType,
    StructField,
    TimestampType,
)

from pyspark import pandas as ps  # For running doctests and reference resolution in PyCharm.
from pyspark.pandas._typing import FrameLike
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    SPARK_DEFAULT_INDEX_NAME,
)
from pyspark.pandas.missing.resample import (
    MissingPandasLikeDataFrameResampler,
    MissingPandasLikeSeriesResampler,
)
from pyspark.pandas.series import Series, first_series
from pyspark.pandas.utils import (
    scol_for,
    verify_temp_column_name,
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
        closed: Optional[str] = None,
        label: Optional[str] = None,
        agg_columns: List[Series] = [],
    ):
        self._psdf = psdf
        self._resamplekey = resamplekey

        self._offset = to_offset(rule)
        if self._offset.rule_code not in ["A-DEC", "M", "D", "H", "T", "S"]:
            raise ValueError("rule code {} is not supported".format(self._offset.rule_code))
        if not self._offset.n > 0:  # type: ignore[attr-defined]
            raise ValueError("rule offset must be positive")

        if closed is None:
            self._closed = "right" if self._offset.rule_code in ["A-DEC", "M"] else "left"
        elif closed in ["left", "right"]:
            self._closed = closed
        else:
            raise ValueError("invalid closed: '{}'".format(closed))

        if label is None:
            self._label = "right" if self._offset.rule_code in ["A-DEC", "M"] else "left"
        elif label in ["left", "right"]:
            self._label = label
        else:
            raise ValueError("invalid label: '{}'".format(label))

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

    def _bin_time_stamp(self, origin: pd.Timestamp, ts_scol: Column) -> Column:
        sql_utils = SparkContext._active_spark_context._jvm.PythonSQLUtils
        origin_scol = F.lit(origin)
        (rule_code, n) = (self._offset.rule_code, self._offset.n)  # type: ignore[attr-defined]
        left_closed, right_closed = (self._closed == "left", self._closed == "right")
        left_labeled, right_labeled = (self._label == "left", self._label == "right")

        if rule_code == "A-DEC":
            assert (
                origin.month == 12
                and origin.day == 31
                and origin.hour == 0
                and origin.minute == 0
                and origin.second == 0
            )

            diff = F.year(ts_scol) - F.year(origin_scol)
            mod = F.lit(0) if n == 1 else (diff % n)
            edge_cond = (mod == 0) & (F.month(ts_scol) == 12) & (F.dayofmonth(ts_scol) == 31)

            edge_label = F.year(ts_scol)
            if left_closed and right_labeled:
                edge_label += n
            elif right_closed and left_labeled:
                edge_label -= n

            if left_labeled:
                non_edge_label = F.when(mod == 0, F.year(ts_scol) - n).otherwise(
                    F.year(ts_scol) - mod
                )
            else:
                non_edge_label = F.when(mod == 0, F.year(ts_scol)).otherwise(
                    F.year(ts_scol) - (mod - n)
                )

            return F.to_timestamp(
                F.make_date(
                    F.when(edge_cond, edge_label).otherwise(non_edge_label), F.lit(12), F.lit(31)
                )
            )

        elif rule_code == "M":
            assert (
                origin.is_month_end
                and origin.hour == 0
                and origin.minute == 0
                and origin.second == 0
            )

            diff = (
                (F.year(ts_scol) - F.year(origin_scol)) * 12
                + F.month(ts_scol)
                - F.month(origin_scol)
            )
            mod = F.lit(0) if n == 1 else (diff % n)
            edge_cond = (mod == 0) & (F.dayofmonth(ts_scol) == F.dayofmonth(F.last_day(ts_scol)))

            truncated_ts_scol = F.date_trunc("MONTH", ts_scol)
            edge_label = truncated_ts_scol
            if left_closed and right_labeled:
                edge_label += sql_utils.makeInterval("MONTH", F.lit(n)._jc)
            elif right_closed and left_labeled:
                edge_label -= sql_utils.makeInterval("MONTH", F.lit(n)._jc)

            if left_labeled:
                non_edge_label = F.when(
                    mod == 0,
                    truncated_ts_scol - sql_utils.makeInterval("MONTH", F.lit(n)._jc),
                ).otherwise(truncated_ts_scol - sql_utils.makeInterval("MONTH", mod._jc))
            else:
                non_edge_label = F.when(mod == 0, truncated_ts_scol).otherwise(
                    truncated_ts_scol - sql_utils.makeInterval("MONTH", (mod - n)._jc)
                )

            return F.to_timestamp(
                F.last_day(F.when(edge_cond, edge_label).otherwise(non_edge_label))
            )

        elif rule_code == "D":
            assert origin.hour == 0 and origin.minute == 0 and origin.second == 0

            if n == 1:
                # NOTE: the logic to process '1D' is different from the cases with n>1,
                # since hour/minute/second parts are taken into account to determine edges!
                edge_cond = (
                    (F.hour(ts_scol) == 0) & (F.minute(ts_scol) == 0) & (F.second(ts_scol) == 0)
                )

                if left_closed and left_labeled:
                    return F.date_trunc("DAY", ts_scol)
                elif left_closed and right_labeled:
                    return F.date_trunc("DAY", F.date_add(ts_scol, 1))
                elif right_closed and left_labeled:
                    return F.when(edge_cond, F.date_trunc("DAY", F.date_sub(ts_scol, 1))).otherwise(
                        F.date_trunc("DAY", ts_scol)
                    )
                else:
                    return F.when(edge_cond, F.date_trunc("DAY", ts_scol)).otherwise(
                        F.date_trunc("DAY", F.date_add(ts_scol, 1))
                    )

            else:
                diff = F.datediff(end=ts_scol, start=origin_scol)
                mod = diff % n

                edge_cond = mod == 0

                truncated_ts_scol = F.date_trunc("DAY", ts_scol)
                edge_label = truncated_ts_scol
                if left_closed and right_labeled:
                    edge_label = F.date_add(truncated_ts_scol, n)
                elif right_closed and left_labeled:
                    edge_label = F.date_sub(truncated_ts_scol, n)

                if left_labeled:
                    non_edge_label = F.date_sub(truncated_ts_scol, mod)
                else:
                    non_edge_label = F.date_sub(truncated_ts_scol, mod - n)

                return F.when(edge_cond, edge_label).otherwise(non_edge_label)

        elif rule_code in ["H", "T", "S"]:
            unit_mapping = {"H": "HOUR", "T": "MINUTE", "S": "SECOND"}
            unit_str = unit_mapping[rule_code]

            truncated_ts_scol = F.date_trunc(unit_str, ts_scol)
            diff = sql_utils.timestampDiff(unit_str, origin_scol._jc, truncated_ts_scol._jc)
            mod = F.lit(0) if n == 1 else (diff % F.lit(n))

            if rule_code == "H":
                assert origin.minute == 0 and origin.second == 0
                edge_cond = (mod == 0) & (F.minute(ts_scol) == 0) & (F.second(ts_scol) == 0)
            elif rule_code == "T":
                assert origin.second == 0
                edge_cond = (mod == 0) & (F.second(ts_scol) == 0)
            else:
                edge_cond = mod == 0

            edge_label = truncated_ts_scol
            if left_closed and right_labeled:
                edge_label += sql_utils.makeInterval(unit_str, F.lit(n)._jc)
            elif right_closed and left_labeled:
                edge_label -= sql_utils.makeInterval(unit_str, F.lit(n)._jc)

            if left_labeled:
                non_edge_label = F.when(mod == 0, truncated_ts_scol).otherwise(
                    truncated_ts_scol - sql_utils.makeInterval(unit_str, mod._jc)
                )
            else:
                non_edge_label = F.when(
                    mod == 0,
                    truncated_ts_scol + sql_utils.makeInterval(unit_str, F.lit(n)._jc),
                ).otherwise(truncated_ts_scol - sql_utils.makeInterval(unit_str, (mod - n)._jc))

            return F.when(edge_cond, edge_label).otherwise(non_edge_label)

        else:
            raise ValueError("Got the unexpected unit {}".format(rule_code))

    def _downsample(self, f: str) -> DataFrame:
        """
        Downsample the defined function.

        Parameters
        ----------
        how : string / mapped function
        **kwargs : kw args passed to how function
        """

        # a simple example to illustrate the computation:
        #   dates = [
        #         datetime(2012, 1, 2),
        #         datetime(2012, 5, 3),
        #         datetime(2022, 5, 3),
        #   ]
        #   index = pd.DatetimeIndex(dates)
        #   pdf = pd.DataFrame(np.array([1,2,3]), index=index, columns=['A'])
        #   pdf.resample('3Y').max()
        #                 A
        #   2012-12-31  2.0
        #   2015-12-31  NaN
        #   2018-12-31  NaN
        #   2021-12-31  NaN
        #   2024-12-31  3.0
        #
        # in this case:
        # 1, obtain one origin point to bin all timestamps, we can get one (2009-12-31)
        # from the minimum timestamp (2012-01-02);
        # 2, the default intervals for 'Y' are right-closed, so intervals are:
        # (2009-12-31, 2012-12-31], (2012-12-31, 2015-12-31], (2015-12-31, 2018-12-31], ...
        # 3, bin all timestamps, for example, 2022-05-03 belongs to interval
        # (2021-12-31, 2024-12-31], since the default label is 'right', label it with the right
        # edge 2024-12-31;
        # 4, some intervals maybe too large for this down sampling, so we need to pad the dataframe
        # to avoid missing some results, like: 2015-12-31, 2018-12-31 and 2021-12-31;
        # 5, union the binned dataframe and padded dataframe, and apply aggregation 'max' to get
        # the final results;

        # one action to obtain the range, in the future we may cache it in the index.
        ts_min, ts_max = (
            self._psdf._internal.spark_frame.select(
                F.min(self._resamplekey_scol), F.max(self._resamplekey_scol)
            )
            .toPandas()
            .iloc[0]
        )

        # the logic to obtain an origin point to bin the timestamps is too complex to follow,
        # here just use Pandas' resample on a 1-length series to get it.
        ts_origin = (
            pd.Series([0], index=[ts_min])
            .resample(rule=self._offset.freqstr, closed=self._closed, label="left")
            .sum()
            .index[0]
        )
        assert ts_origin <= ts_min

        bin_col_name = "__tmp_resample_bin_col__"
        bin_col_label = verify_temp_column_name(self._psdf, bin_col_name)
        bin_col_field = InternalField(
            dtype=np.dtype("datetime64[ns]"),
            struct_field=StructField(bin_col_name, TimestampType(), True),
        )
        bin_scol = self._bin_time_stamp(
            ts_origin,
            self._resamplekey_scol,
        )

        agg_columns = [
            psser for psser in self._agg_columns if (isinstance(psser.spark.data_type, NumericType))
        ]
        assert len(agg_columns) > 0

        # in the binning side, label the timestamps according to the origin and the freq(rule)
        bin_sdf = self._psdf._internal.spark_frame.select(
            F.col(SPARK_DEFAULT_INDEX_NAME),
            bin_scol.alias(bin_col_name),
            *[psser.spark.column for psser in agg_columns],
        )

        # in the padding side, insert necessary points
        # again, directly apply Pandas' resample on a 2-length series to obtain the indices
        pad_sdf = (
            ps.from_pandas(
                pd.Series([0, 0], index=[ts_min, ts_max])
                .resample(rule=self._offset.freqstr, closed=self._closed, label=self._label)
                .sum()
                .index
            )
            ._internal.spark_frame.select(F.col(SPARK_DEFAULT_INDEX_NAME).alias(bin_col_name))
            .where((ts_min <= F.col(bin_col_name)) & (F.col(bin_col_name) <= ts_max))
        )

        # union the above two spark dataframes.
        sdf = bin_sdf.unionByName(pad_sdf, allowMissingColumns=True).where(
            ~F.isnull(F.col(bin_col_name))
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
        downsampled = getattr(groupby, f)()
        downsampled.index.name = None

        return downsampled

    @abstractmethod
    def _handle_output(self, psdf: DataFrame) -> FrameLike:
        pass

    def min(self) -> FrameLike:
        """
        Compute min of resampled values.

        .. versionadded:: 3.4.0

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> import numpy as np
        >>> from datetime import datetime
        >>> np.random.seed(22)
        >>> dates = [
        ...    datetime(2022, 5, 1, 4, 5, 6),
        ...    datetime(2022, 5, 3),
        ...    datetime(2022, 5, 3, 23, 59, 59),
        ...    datetime(2022, 5, 4),
        ...    pd.NaT,
        ...    datetime(2022, 5, 4, 0, 0, 1),
        ...    datetime(2022, 5, 11),
        ... ]
        >>> df = ps.DataFrame(
        ...    np.random.rand(len(dates), 2), index=pd.DatetimeIndex(dates), columns=["A", "B"]
        ... )
        >>> df
                                    A         B
        2022-05-01 04:05:06  0.208461  0.481681
        2022-05-03 00:00:00  0.420538  0.859182
        2022-05-03 23:59:59  0.171162  0.338864
        2022-05-04 00:00:00  0.270533  0.691041
        NaT                  0.220405  0.811951
        2022-05-04 00:00:01  0.010527  0.561204
        2022-05-11 00:00:00  0.813726  0.745100
        >>> df.resample("3D").min().sort_index()
                           A         B
        2022-05-01  0.171162  0.338864
        2022-05-04  0.010527  0.561204
        2022-05-07       NaN       NaN
        2022-05-10  0.813726  0.745100
        """
        return self._handle_output(self._downsample("min"))

    def max(self) -> FrameLike:
        """
        Compute max of resampled values.

        .. versionadded:: 3.4.0

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> import numpy as np
        >>> from datetime import datetime
        >>> np.random.seed(22)
        >>> dates = [
        ...    datetime(2022, 5, 1, 4, 5, 6),
        ...    datetime(2022, 5, 3),
        ...    datetime(2022, 5, 3, 23, 59, 59),
        ...    datetime(2022, 5, 4),
        ...    pd.NaT,
        ...    datetime(2022, 5, 4, 0, 0, 1),
        ...    datetime(2022, 5, 11),
        ... ]
        >>> df = ps.DataFrame(
        ...    np.random.rand(len(dates), 2), index=pd.DatetimeIndex(dates), columns=["A", "B"]
        ... )
        >>> df
                                    A         B
        2022-05-01 04:05:06  0.208461  0.481681
        2022-05-03 00:00:00  0.420538  0.859182
        2022-05-03 23:59:59  0.171162  0.338864
        2022-05-04 00:00:00  0.270533  0.691041
        NaT                  0.220405  0.811951
        2022-05-04 00:00:01  0.010527  0.561204
        2022-05-11 00:00:00  0.813726  0.745100
        >>> df.resample("3D").max().sort_index()
                           A         B
        2022-05-01  0.420538  0.859182
        2022-05-04  0.270533  0.691041
        2022-05-07       NaN       NaN
        2022-05-10  0.813726  0.745100
        """
        return self._handle_output(self._downsample("max"))

    def sum(self) -> FrameLike:
        """
        Compute sum of resampled values.

        .. versionadded:: 3.4.0

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> import numpy as np
        >>> from datetime import datetime
        >>> np.random.seed(22)
        >>> dates = [
        ...    datetime(2022, 5, 1, 4, 5, 6),
        ...    datetime(2022, 5, 3),
        ...    datetime(2022, 5, 3, 23, 59, 59),
        ...    datetime(2022, 5, 4),
        ...    pd.NaT,
        ...    datetime(2022, 5, 4, 0, 0, 1),
        ...    datetime(2022, 5, 11),
        ... ]
        >>> df = ps.DataFrame(
        ...    np.random.rand(len(dates), 2), index=pd.DatetimeIndex(dates), columns=["A", "B"]
        ... )
        >>> df
                                    A         B
        2022-05-01 04:05:06  0.208461  0.481681
        2022-05-03 00:00:00  0.420538  0.859182
        2022-05-03 23:59:59  0.171162  0.338864
        2022-05-04 00:00:00  0.270533  0.691041
        NaT                  0.220405  0.811951
        2022-05-04 00:00:01  0.010527  0.561204
        2022-05-11 00:00:00  0.813726  0.745100
        >>> df.resample("3D").sum().sort_index()
                           A         B
        2022-05-01  0.800160  1.679727
        2022-05-04  0.281060  1.252245
        2022-05-07  0.000000  0.000000
        2022-05-10  0.813726  0.745100
        """
        return self._handle_output(self._downsample("sum").fillna(0.0))

    def mean(self) -> FrameLike:
        """
        Compute mean of resampled values.

        .. versionadded:: 3.4.0

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> import numpy as np
        >>> from datetime import datetime
        >>> np.random.seed(22)
        >>> dates = [
        ...    datetime(2022, 5, 1, 4, 5, 6),
        ...    datetime(2022, 5, 3),
        ...    datetime(2022, 5, 3, 23, 59, 59),
        ...    datetime(2022, 5, 4),
        ...    pd.NaT,
        ...    datetime(2022, 5, 4, 0, 0, 1),
        ...    datetime(2022, 5, 11),
        ... ]
        >>> df = ps.DataFrame(
        ...    np.random.rand(len(dates), 2), index=pd.DatetimeIndex(dates), columns=["A", "B"]
        ... )
        >>> df
                                    A         B
        2022-05-01 04:05:06  0.208461  0.481681
        2022-05-03 00:00:00  0.420538  0.859182
        2022-05-03 23:59:59  0.171162  0.338864
        2022-05-04 00:00:00  0.270533  0.691041
        NaT                  0.220405  0.811951
        2022-05-04 00:00:01  0.010527  0.561204
        2022-05-11 00:00:00  0.813726  0.745100
        >>> df.resample("3D").mean().sort_index()
                           A         B
        2022-05-01  0.266720  0.559909
        2022-05-04  0.140530  0.626123
        2022-05-07       NaN       NaN
        2022-05-10  0.813726  0.745100
        """
        return self._handle_output(self._downsample("mean"))

    def std(self) -> FrameLike:
        """
        Compute std of resampled values.

        .. versionadded:: 3.4.0

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> import numpy as np
        >>> from datetime import datetime
        >>> np.random.seed(22)
        >>> dates = [
        ...    datetime(2022, 5, 1, 4, 5, 6),
        ...    datetime(2022, 5, 3),
        ...    datetime(2022, 5, 3, 23, 59, 59),
        ...    datetime(2022, 5, 4),
        ...    pd.NaT,
        ...    datetime(2022, 5, 4, 0, 0, 1),
        ...    datetime(2022, 5, 11),
        ... ]
        >>> df = ps.DataFrame(
        ...    np.random.rand(len(dates), 2), index=pd.DatetimeIndex(dates), columns=["A", "B"]
        ... )
        >>> df
                                    A         B
        2022-05-01 04:05:06  0.208461  0.481681
        2022-05-03 00:00:00  0.420538  0.859182
        2022-05-03 23:59:59  0.171162  0.338864
        2022-05-04 00:00:00  0.270533  0.691041
        NaT                  0.220405  0.811951
        2022-05-04 00:00:01  0.010527  0.561204
        2022-05-11 00:00:00  0.813726  0.745100
        >>> df.resample("3D").std().sort_index()
                           A         B
        2022-05-01  0.134509  0.268835
        2022-05-04  0.183852  0.091809
        2022-05-07       NaN       NaN
        2022-05-10       NaN       NaN
        """
        return self._handle_output(self._downsample("std"))

    def var(self) -> FrameLike:
        """
        Compute var of resampled values.

        .. versionadded:: 3.4.0

        See Also
        --------
        pyspark.pandas.Series.groupby
        pyspark.pandas.DataFrame.groupby

        Examples
        --------
        >>> import numpy as np
        >>> from datetime import datetime
        >>> np.random.seed(22)
        >>> dates = [
        ...    datetime(2022, 5, 1, 4, 5, 6),
        ...    datetime(2022, 5, 3),
        ...    datetime(2022, 5, 3, 23, 59, 59),
        ...    datetime(2022, 5, 4),
        ...    pd.NaT,
        ...    datetime(2022, 5, 4, 0, 0, 1),
        ...    datetime(2022, 5, 11),
        ... ]
        >>> df = ps.DataFrame(
        ...    np.random.rand(len(dates), 2), index=pd.DatetimeIndex(dates), columns=["A", "B"]
        ... )
        >>> df
                                    A         B
        2022-05-01 04:05:06  0.208461  0.481681
        2022-05-03 00:00:00  0.420538  0.859182
        2022-05-03 23:59:59  0.171162  0.338864
        2022-05-04 00:00:00  0.270533  0.691041
        NaT                  0.220405  0.811951
        2022-05-04 00:00:01  0.010527  0.561204
        2022-05-11 00:00:00  0.813726  0.745100
        >>> df.resample("3D").var().sort_index()
                           A         B
        2022-05-01  0.018093  0.072272
        2022-05-04  0.033802  0.008429
        2022-05-07       NaN       NaN
        2022-05-10       NaN       NaN
        """
        return self._handle_output(self._downsample("var"))


class DataFrameResampler(Resampler[DataFrame]):
    def __init__(
        self,
        psdf: DataFrame,
        resamplekey: Optional[Series],
        rule: str,
        closed: Optional[str] = None,
        label: Optional[str] = None,
        agg_columns: List[Series] = [],
    ):
        super().__init__(
            psdf=psdf,
            resamplekey=resamplekey,
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

    def _handle_output(self, psdf: DataFrame) -> DataFrame:
        return psdf


class SeriesResampler(Resampler[Series]):
    def __init__(
        self,
        psser: Series,
        resamplekey: Optional[Series],
        rule: str,
        closed: Optional[str] = None,
        label: Optional[str] = None,
        agg_columns: List[Series] = [],
    ):
        super().__init__(
            psdf=psser._psdf,
            resamplekey=resamplekey,
            rule=rule,
            closed=closed,
            label=label,
            agg_columns=agg_columns,
        )
        self._psser = psser

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeSeriesResampler, item):
            property_or_func = getattr(MissingPandasLikeSeriesResampler, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)

    def _handle_output(self, psdf: DataFrame) -> Series:
        return first_series(psdf).rename(self._psser.name)


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.resample

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.resample.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.resample tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.resample,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
