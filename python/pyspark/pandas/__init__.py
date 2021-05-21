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
import os
import sys
from distutils.version import LooseVersion
import warnings

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

from pyspark.pandas.version import __version__  # noqa: F401


def assert_python_version() -> None:
    major = 3
    minor = 5
    deprecated_version = (major, minor)
    min_supported_version = (major, minor + 1)

    if sys.version_info[:2] <= deprecated_version:
        warnings.warn(
            "pandas-on-Spark support for Python {dep_ver} is deprecated and will be dropped in "
            "the future release. At that point, existing Python {dep_ver} workflows "
            "that use pandas-on-Spark will continue to work without modification, but "
            "Python {dep_ver} users will no longer get access to the latest pandas-on-Spark "
            "features and bugfixes. We recommend that you upgrade to Python {min_ver} or "
            "newer.".format(
                dep_ver=".".join(map(str, deprecated_version)),
                min_ver=".".join(map(str, min_supported_version)),
            ),
            FutureWarning,
        )


assert_python_version()

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

    # Attach a usage logger.
    logger_module = os.getenv("KOALAS_USAGE_LOGGER", "")
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

    # Autopatching is on by default.
    x = os.getenv("SPARK_KOALAS_AUTOPATCH", "true")
    if x.lower() in ("true", "1", "enabled"):
        logger = logging.getLogger("spark")
        logger.info(
            "Patching spark automatically. You can disable it by setting "
            "SPARK_KOALAS_AUTOPATCH=false in your environment"
        )

        from pyspark.sql import dataframe as df

        df.DataFrame.to_pandas_on_spark = DataFrame.to_pandas_on_spark  # type: ignore


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
            pd.DataFrame.__class_getitem__ = lambda params: DataFrame.__class_getitem__(params)

        if not _series_has_class_getitem:
            pd.Series.__class_getitem__ = lambda params: Series.__class_getitem__(params)


_auto_patch_spark()
_auto_patch_pandas()

# Import after the usage logger is attached.
from pyspark.pandas.config import get_option, options, option_context, reset_option, set_option
from pyspark.pandas.namespace import *  # F405
from pyspark.pandas.sql_processor import sql
