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

from types import ModuleType
from typing import Union

import pandas as pd

from pyspark.pandas import config, namespace, sql_formatter
from pyspark.pandas.accessors import PandasOnSparkFrameMethods
from pyspark.pandas.frame import DataFrame
from pyspark.pandas.datetimes import DatetimeMethods
from pyspark.pandas.groupby import DataFrameGroupBy, SeriesGroupBy
from pyspark.pandas.indexes.base import Index
from pyspark.pandas.indexes.category import CategoricalIndex
from pyspark.pandas.indexes.datetimes import DatetimeIndex
from pyspark.pandas.indexes.multi import MultiIndex
from pyspark.pandas.indexes.numeric import Float64Index, Int64Index
from pyspark.pandas.missing.frame import _MissingPandasLikeDataFrame
from pyspark.pandas.missing.general_functions import _MissingPandasLikeGeneralFunctions
from pyspark.pandas.missing.groupby import (
    MissingPandasLikeDataFrameGroupBy,
    MissingPandasLikeSeriesGroupBy,
)
from pyspark.pandas.missing.indexes import (
    MissingPandasLikeDatetimeIndex,
    MissingPandasLikeIndex,
    MissingPandasLikeMultiIndex,
)
from pyspark.pandas.missing.series import MissingPandasLikeSeries
from pyspark.pandas.missing.window import (
    MissingPandasLikeExpanding,
    MissingPandasLikeRolling,
    MissingPandasLikeExpandingGroupby,
    MissingPandasLikeRollingGroupby,
)
from pyspark.pandas.series import Series
from pyspark.pandas.spark.accessors import (
    CachedSparkFrameMethods,
    SparkFrameMethods,
    SparkIndexOpsMethods,
)
from pyspark.pandas.strings import StringMethods
from pyspark.pandas.window import Expanding, ExpandingGroupby, Rolling, RollingGroupby
from pyspark.instrumentation_utils import _attach


def attach(logger_module: Union[str, ModuleType]) -> None:
    """
    Attach the usage logger.

    Parameters
    ----------
    logger_module : the module or module name contains the usage logger.
        The module needs to provide `get_logger` function as an entry point of the plug-in
        returning the usage logger.

    See Also
    --------
    usage_logger : the reference implementation of the usage logger.
    """

    modules = [config, namespace]
    classes = [
        DataFrame,
        Series,
        Index,
        MultiIndex,
        Int64Index,
        Float64Index,
        CategoricalIndex,
        DatetimeIndex,
        DataFrameGroupBy,
        SeriesGroupBy,
        DatetimeMethods,
        StringMethods,
        Expanding,
        ExpandingGroupby,
        Rolling,
        RollingGroupby,
        CachedSparkFrameMethods,
        SparkFrameMethods,
        SparkIndexOpsMethods,
        PandasOnSparkFrameMethods,
    ]

    try:
        from pyspark.pandas import mlflow

        modules.append(mlflow)
        classes.append(mlflow.PythonModelWrapper)
    except ImportError:
        pass

    sql_formatter._CAPTURE_SCOPES = 4
    modules.append(sql_formatter)

    missings = [
        (pd, _MissingPandasLikeGeneralFunctions),
        (pd.DataFrame, _MissingPandasLikeDataFrame),
        (pd.Series, MissingPandasLikeSeries),
        (pd.Index, MissingPandasLikeIndex),
        (pd.MultiIndex, MissingPandasLikeMultiIndex),
        (pd.DatetimeIndex, MissingPandasLikeDatetimeIndex),
        (pd.core.groupby.DataFrameGroupBy, MissingPandasLikeDataFrameGroupBy),
        (pd.core.groupby.SeriesGroupBy, MissingPandasLikeSeriesGroupBy),
        (pd.core.window.Expanding, MissingPandasLikeExpanding),
        (pd.core.window.Rolling, MissingPandasLikeRolling),
        (pd.core.window.ExpandingGroupby, MissingPandasLikeExpandingGroupby),
        (pd.core.window.RollingGroupby, MissingPandasLikeRollingGroupby),
    ]

    _attach(logger_module, modules, classes, missings)  # type: ignore[arg-type]
