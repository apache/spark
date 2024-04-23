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
import inspect
import functools
import os
from typing import Any, Callable, Optional, Sequence, TYPE_CHECKING, cast, TypeVar, Union, Type

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
    PySparkNotImplementedError,
    PySparkRuntimeError,
)
from pyspark.util import is_remote_only
from pyspark.errors.exceptions.captured import CapturedException  # noqa: F401
from pyspark.find_spark_home import _find_spark_home

if TYPE_CHECKING:
    from py4j.java_collections import JavaArray
    from py4j.java_gateway import (
        JavaClass,
        JavaGateway,
        JavaObject,
    )
    from pyspark import SparkContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.column import Column
    from pyspark.sql.window import Window
    from pyspark.pandas._typing import IndexOpsLike, SeriesOrIndex

has_numpy: bool = False
try:
    import numpy as np  # noqa: F401

    has_numpy = True
except ImportError:
    pass


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def toJArray(gateway: "JavaGateway", jtype: "JavaClass", arr: Sequence[Any]) -> "JavaArray":
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
    jarray: "JavaArray" = gateway.new_array(jtype, len(arr))
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
        raise PySparkRuntimeError(
            error_class="TEST_CLASS_NOT_COMPILED",
            message_parameters={"test_class_path": test_class_path},
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

    def call(self, jdf: "JavaObject", batch_id: int) -> None:
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
    if is_remote():
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        session = ConnectSparkSession.getActiveSession()
        if session is None:
            return False
        else:
            return session.conf.get("spark.sql.timestampType", None) == "TIMESTAMP_NTZ"
    else:
        from pyspark import SparkContext

        jvm = SparkContext._jvm
        return jvm is not None and jvm.PythonSQLUtils.isTimestampNTZPreferred()


def is_remote() -> bool:
    """
    Returns if the current running environment is for Spark Connect.

    .. versionadded:: 4.0.0

    Notes
    -----
    This will only return ``True`` if there is a remote session running.
    Otherwise, it returns ``False``.

    This API is unstable, and for developers.

    Returns
    -------
    bool

    Examples
    --------
    >>> from pyspark.sql import is_remote
    >>> is_remote()
    False
    """
    return ("SPARK_CONNECT_MODE_ENABLED" in os.environ) or is_remote_only()


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


def try_partitioning_remote_functions(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect.functions import partitioning

            return getattr(partitioning, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_avro_functions(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect.avro import functions

            return getattr(functions, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_protobuf_functions(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect.protobuf import functions

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


def get_active_spark_context() -> "SparkContext":
    """Raise RuntimeError if SparkContext is not initialized,
    otherwise, returns the active SparkContext."""
    from pyspark import SparkContext

    sc = SparkContext._active_spark_context
    if sc is None or sc._jvm is None:
        raise PySparkRuntimeError(
            error_class="SESSION_OR_CONTEXT_NOT_EXISTS",
            message_parameters={},
        )
    return sc


def try_remote_session_classmethod(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect.session import SparkSession

            assert inspect.isclass(args[0])
            return getattr(SparkSession, f.__name__)(*args[1:], **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)


def dispatch_df_method(f: FuncT) -> FuncT:
    """
    For the usecases of direct DataFrame.union(df, ...), it checks if self
    is a Connect DataFrame or Classic DataFrame, and dispatches.
    """

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

            if isinstance(args[0], ConnectDataFrame):
                return getattr(ConnectDataFrame, f.__name__)(*args, **kwargs)
        else:
            from pyspark.sql.classic.dataframe import DataFrame as ClassicDataFrame

            if isinstance(args[0], ClassicDataFrame):
                return getattr(ClassicDataFrame, f.__name__)(*args, **kwargs)

        raise PySparkNotImplementedError(
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": f"DataFrame.{f.__name__}"},
        )

    return cast(FuncT, wrapped)


def pyspark_column_op(
    func_name: str, left: "IndexOpsLike", right: Any, fillna: Any = None
) -> Union["SeriesOrIndex", None]:
    """
    Wrapper function for column_op to get proper Column class.
    """
    from pyspark.pandas.base import column_op
    from pyspark.sql.column import Column as PySparkColumn
    from pyspark.pandas.data_type_ops.base import _is_extension_dtypes

    if is_remote():
        from pyspark.sql.connect.column import Column as ConnectColumn

        Column = ConnectColumn
    else:
        Column = PySparkColumn  # type: ignore[assignment]
    result = column_op(getattr(Column, func_name))(left, right)
    # It works as expected on extension dtype, so we don't need to call `fillna` for this case.
    if (fillna is not None) and (_is_extension_dtypes(left) or _is_extension_dtypes(right)):
        fillna = None
    # TODO(SPARK-43877): Fix behavior difference for compare binary functions.
    return result.fillna(fillna) if fillna is not None else result


def get_column_class() -> Type["Column"]:
    from pyspark.sql.column import Column as PySparkColumn

    if is_remote():
        from pyspark.sql.connect.column import Column as ConnectColumn

        return ConnectColumn  # type: ignore[return-value]
    else:
        return PySparkColumn


def get_dataframe_class() -> Type["DataFrame"]:
    from pyspark.sql.dataframe import DataFrame as PySparkDataFrame

    if is_remote():
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        return ConnectDataFrame
    else:
        return PySparkDataFrame


def get_window_class() -> Type["Window"]:
    from pyspark.sql.window import Window as PySparkWindow

    if is_remote():
        from pyspark.sql.connect.window import Window as ConnectWindow

        return ConnectWindow  # type: ignore[return-value]
    else:
        return PySparkWindow


def get_lit_sql_str(val: str) -> str:
    # Equivalent to `lit(val)._jc.expr().sql()` for string typed val
    # See `sql` definition in `sql/catalyst/src/main/scala/org/apache/spark/
    # sql/catalyst/expressions/literals.scala`
    return "'" + val.replace("\\", "\\\\").replace("'", "\\'") + "'"
