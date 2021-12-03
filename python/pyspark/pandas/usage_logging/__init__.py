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

import functools
import importlib
import inspect
import threading
import time
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
    if isinstance(logger_module, str):
        logger_module = importlib.import_module(logger_module)

    logger = getattr(logger_module, "get_logger")()

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

    # Modules
    for target_module in modules:
        target_name = target_module.__name__.split(".")[-1]
        for name in getattr(target_module, "__all__"):
            func = getattr(target_module, name)
            if not inspect.isfunction(func):
                continue
            setattr(target_module, name, _wrap_function(target_name, name, func, logger))

    special_functions = set(
        [
            "__init__",
            "__repr__",
            "__str__",
            "_repr_html_",
            "__len__",
            "__getitem__",
            "__setitem__",
            "__getattr__",
        ]
    )

    # Classes
    for target_class in classes:
        for name, func in inspect.getmembers(target_class, inspect.isfunction):
            if name.startswith("_") and name not in special_functions:
                continue
            setattr(target_class, name, _wrap_function(target_class.__name__, name, func, logger))

        for name, prop in inspect.getmembers(target_class, lambda o: isinstance(o, property)):
            if name.startswith("_"):
                continue
            setattr(target_class, name, _wrap_property(target_class.__name__, name, prop, logger))

    # Missings
    for original, missing in [
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
    ]:
        for name, func in inspect.getmembers(missing, inspect.isfunction):
            setattr(
                missing,
                name,
                _wrap_missing_function(original.__name__, name, func, original, logger),
            )

        for name, prop in inspect.getmembers(missing, lambda o: isinstance(o, property)):
            setattr(missing, name, _wrap_missing_property(original.__name__, name, prop, logger))


_local = threading.local()


def _wrap_function(class_name, function_name, func, logger):

    signature = inspect.signature(func)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if hasattr(_local, "logging") and _local.logging:
            # no need to log since this should be internal call.
            return func(*args, **kwargs)
        _local.logging = True
        try:
            start = time.perf_counter()
            try:
                res = func(*args, **kwargs)
                logger.log_success(
                    class_name, function_name, time.perf_counter() - start, signature
                )
                return res
            except Exception as ex:
                logger.log_failure(
                    class_name, function_name, ex, time.perf_counter() - start, signature
                )
                raise
        finally:
            _local.logging = False

    return wrapper


def _wrap_property(class_name, property_name, prop, logger):
    @property
    def wrapper(self):
        if hasattr(_local, "logging") and _local.logging:
            # no need to log since this should be internal call.
            return prop.fget(self)
        _local.logging = True
        try:
            start = time.perf_counter()
            try:
                res = prop.fget(self)
                logger.log_success(class_name, property_name, time.perf_counter() - start)
                return res
            except Exception as ex:
                logger.log_failure(class_name, property_name, ex, time.perf_counter() - start)
                raise
        finally:
            _local.logging = False

    wrapper.__doc__ = prop.__doc__

    if prop.fset is not None:
        wrapper = wrapper.setter(_wrap_function(class_name, prop.fset.__name__, prop.fset, logger))

    return wrapper


def _wrap_missing_function(class_name, function_name, func, original, logger):

    if not hasattr(original, function_name):
        return func

    signature = inspect.signature(getattr(original, function_name))

    is_deprecated = func.__name__ == "deprecated_function"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        finally:
            logger.log_missing(class_name, function_name, is_deprecated, signature)

    return wrapper


def _wrap_missing_property(class_name, property_name, prop, logger):

    is_deprecated = prop.fget.__name__ == "deprecated_property"

    @property
    def wrapper(self):
        try:
            return prop.fget(self)
        finally:
            logger.log_missing(class_name, property_name, is_deprecated)

    return wrapper
