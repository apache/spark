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
import os
from typing import Any, Callable, Optional, Sequence, TYPE_CHECKING, cast, TypeVar

from py4j.java_collections import JavaArray
from py4j.java_gateway import (
    JavaClass,
    JavaGateway,
    JavaObject,
)

from pyspark import SparkContext

# For backward compatibility.
from pyspark.errors import (  # noqa: F401
    AnalysisException,
    ParseException,
    IllegalArgumentException,
    StreamingQueryException,
    QueryExecutionException,
    PythonException,
    UnknownException,
    SparkUpgradeException,
)
from pyspark.find_spark_home import _find_spark_home

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession
    from pyspark.sql.dataframe import DataFrame

has_numpy = False
try:
    import numpy as np  # noqa: F401

    has_numpy = True
except ImportError:
    pass


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def toJArray(gateway: JavaGateway, jtype: JavaClass, arr: Sequence[Any]) -> JavaArray:
    """
    Convert python list to java type array

    Parameters
    ----------
    gateway :
        Py4j Gateway
    jtype :
        java type of element in array
    arr :
        python type list
    """
    jarray: JavaArray = gateway.new_array(jtype, len(arr))
    for i in range(0, len(arr)):
        jarray[i] = arr[i]
    return jarray


def require_test_compiled() -> None:
    """Raise Exception if test classes are not compiled"""
    import os
    import glob

    test_class_path = os.path.join(_find_spark_home(), "sql", "core", "target", "*", "test-classes")
    paths = glob.glob(test_class_path)

    if len(paths) == 0:
        raise RuntimeError(
            "%s doesn't exist. Spark sql test classes are not compiled." % test_class_path
        )


class ForeachBatchFunction:
    """
    This is the Python implementation of Java interface 'ForeachBatchFunction'. This wraps
    the user-defined 'foreachBatch' function such that it can be called from the JVM when
    the query is active.
    """

    def __init__(self, session: "SparkSession", func: Callable[["DataFrame", int], None]):
        self.func = func
        self.session = session

    def call(self, jdf: JavaObject, batch_id: int) -> None:
        from pyspark.sql.dataframe import DataFrame
        from pyspark.sql.session import SparkSession

        try:
            session_jdf = jdf.sparkSession()
            # assuming that spark context is still the same between JVM and PySpark
            wrapped_session_jdf = SparkSession(self.session.sparkContext, session_jdf)
            self.func(DataFrame(jdf, wrapped_session_jdf), batch_id)
        except Exception as e:
            self.error = e
            raise e

    class Java:
        implements = ["org.apache.spark.sql.execution.streaming.sources.PythonForeachBatchFunction"]


def to_str(value: Any) -> Optional[str]:
    """
    A wrapper over str(), but converts bool values to lower case strings.
    If None is given, just returns None, instead of converting it to string "None".
    """
    if isinstance(value, bool):
        return str(value).lower()
    elif value is None:
        return value
    else:
        return str(value)


def is_timestamp_ntz_preferred() -> bool:
    """
    Return a bool if TimestampNTZType is preferred according to the SQL configuration set.
    """
    jvm = SparkContext._jvm
    return jvm is not None and jvm.PythonSQLUtils.isTimestampNTZPreferred()


def is_remote() -> bool:
    """
    Returns if the current running environment is for Spark Connect.
    """
    return "SPARK_REMOTE" in os.environ


def try_remote_functions(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:

        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect import functions

            return getattr(functions, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_window(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:

        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect.window import Window

            return getattr(Window, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_windowspec(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:

        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect.window import WindowSpec

            return getattr(WindowSpec, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def get_active_spark_context() -> SparkContext:
    """Raise RuntimeError if SparkContext is not initialized,
    otherwise, returns the active SparkContext."""
    sc = SparkContext._active_spark_context
    if sc is None or sc._jvm is None:
        raise RuntimeError("SparkContext or SparkSession should be created first.")
    return sc


def try_remote_observation(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        # TODO(SPARK-41527): Add the support of Observation.
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            raise NotImplementedError()
        return f(*args, **kwargs)

    return cast(FuncT, wrapped)
