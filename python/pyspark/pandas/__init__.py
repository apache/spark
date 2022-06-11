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
.. versionadded:: 3.2.0
    pandas API on Spark
"""

import os
import sys
import warnings
from distutils.version import LooseVersion
from typing import Any

from pyspark.pandas.missing.general_functions import _MissingPandasLikeGeneralFunctions
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version

try:
    require_minimum_pandas_version()
    require_minimum_pyarrow_version()
except ImportError as e:
    if os.environ.get("SPARK_TESTING"):
        warnings.warn(str(e))
        sys.exit()
    else:
        raise


import pyarrow

if (
    LooseVersion(pyarrow.__version__) >= LooseVersion("2.0.0")
    and "PYARROW_IGNORE_TIMEZONE" not in os.environ
):
    import logging

    logging.warning(
        "'PYARROW_IGNORE_TIMEZONE' environment variable was not set. It is required to "
        "set this environment variable to '1' in both driver and executor sides if you use "
        "pyarrow>=2.0.0. "
        "pandas-on-Spark will set it for you but it does not work if there is a Spark context "
        "already launched."
    )
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

from pyspark.pandas.frame import DataFrame
from pyspark.pandas.indexes.base import Index
from pyspark.pandas.indexes.category import CategoricalIndex
from pyspark.pandas.indexes.datetimes import DatetimeIndex
from pyspark.pandas.indexes.multi import MultiIndex
from pyspark.pandas.indexes.numeric import Float64Index, Int64Index
from pyspark.pandas.indexes.timedelta import TimedeltaIndex
from pyspark.pandas.series import Series
from pyspark.pandas.groupby import NamedAgg

__all__ = [  # noqa: F405
    "read_csv",
    "read_parquet",
    "to_datetime",
    "date_range",
    "from_pandas",
    "get_dummies",
    "DataFrame",
    "Series",
    "Index",
    "MultiIndex",
    "Int64Index",
    "Float64Index",
    "CategoricalIndex",
    "DatetimeIndex",
    "TimedeltaIndex",
    "sql",
    "range",
    "concat",
    "melt",
    "get_option",
    "set_option",
    "reset_option",
    "read_sql_table",
    "read_sql_query",
    "read_sql",
    "options",
    "option_context",
    "NamedAgg",
]


def _auto_patch_spark() -> None:
    import os
    import logging

    # Attach a usage logger. 'KOALAS_USAGE_LOGGER' is legacy, and it's for compatibility.
    logger_module = os.getenv("PYSPARK_PANDAS_USAGE_LOGGER", os.getenv("KOALAS_USAGE_LOGGER", ""))
    if logger_module != "":
        try:
            from pyspark.pandas import usage_logging

            usage_logging.attach(logger_module)
        except Exception as e:
            logger = logging.getLogger("pyspark.pandas.usage_logger")
            logger.warning(
                "Tried to attach usage logger `{}`, but an exception was raised: {}".format(
                    logger_module, str(e)
                )
            )


_frame_has_class_getitem = False
_series_has_class_getitem = False


def _auto_patch_pandas() -> None:
    import pandas as pd

    # In order to use it in test cases.
    global _frame_has_class_getitem
    global _series_has_class_getitem

    _frame_has_class_getitem = hasattr(pd.DataFrame, "__class_getitem__")
    _series_has_class_getitem = hasattr(pd.Series, "__class_getitem__")

    if sys.version_info >= (3, 7):
        # Just in case pandas implements '__class_getitem__' later.
        if not _frame_has_class_getitem:
            pd.DataFrame.__class_getitem__ = (  # type: ignore[assignment,attr-defined]
                lambda params: DataFrame.__class_getitem__(params)
            )

        if not _series_has_class_getitem:
            pd.Series.__class_getitem__ = (  # type: ignore[assignment,attr-defined]
                lambda params: Series.__class_getitem__(params)
            )


_auto_patch_spark()
_auto_patch_pandas()

# Import after the usage logger is attached.
from pyspark.pandas.config import get_option, options, option_context, reset_option, set_option
from pyspark.pandas.namespace import *  # noqa: F403
from pyspark.pandas.sql_formatter import sql


def __getattr__(key: str) -> Any:
    if key.startswith("__"):
        raise AttributeError(key)
    if hasattr(_MissingPandasLikeGeneralFunctions, key):
        return getattr(_MissingPandasLikeGeneralFunctions, key)
    else:
        raise AttributeError("module 'pyspark.pandas' has no attribute '%s'" % (key))
