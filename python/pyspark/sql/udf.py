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
User-defined function related classes and functions
"""
from inspect import getfullargspec

import functools
import inspect
import sys
import warnings
from typing import Callable, Any, TYPE_CHECKING, Optional, cast, Union

from py4j.java_gateway import JavaObject

from pyspark import SparkContext
from pyspark.profiler import Profiler
from pyspark.rdd import _prepare_for_python_RDD, PythonEvalType
from pyspark.sql.column import Column, _to_java_column, _to_java_expr, _to_seq
from pyspark.sql.types import (
    DataType,
    StringType,
    StructType,
    _parse_datatype_string,
)
from pyspark.sql.utils import get_active_spark_context
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from pyspark.errors import PySparkTypeError, PySparkNotImplementedError

if TYPE_CHECKING:
    from pyspark.sql._typing import DataTypeOrString, ColumnOrName, UserDefinedFunctionLike
    from pyspark.sql.session import SparkSession

__all__ = ["UDFRegistration"]


def _wrap_function(
    sc: SparkContext, func: Callable[..., Any], returnType: Optional[DataType] = None
) -> JavaObject:
    command: Any
    if returnType is None:
        command = func
    else:
        command = (func, returnType)
    pickled_command, broadcast_vars, env, includes = _prepare_for_python_RDD(sc, command)
    assert sc._jvm is not None
    return sc._jvm.SimplePythonFunction(
        bytearray(pickled_command),
        env,
        includes,
        sc.pythonExec,
        sc.pythonVer,
        broadcast_vars,
        sc._javaAccumulator,
    )


def _create_udf(
    f: Callable[..., Any],
    returnType: "DataTypeOrString",
    evalType: int,
    name: Optional[str] = None,
    deterministic: bool = True,
) -> "UserDefinedFunctionLike":
    """Create a regular(non-Arrow-optimized) Python UDF."""
    # Set the name of the UserDefinedFunction object to be the name of function f
    udf_obj = UserDefinedFunction(
        f, returnType=returnType, name=name, evalType=evalType, deterministic=deterministic
    )
    return udf_obj._wrapped()


def _create_py_udf(
    f: Callable[..., Any],
    returnType: "DataTypeOrString",
    useArrow: Optional[bool] = None,
) -> "UserDefinedFunctionLike":
    """Create a regular/Arrow-optimized Python UDF."""
    # The following table shows the results when the type coercion in Arrow is needed, that is,
    # when the user-specified return type(SQL Type) of the UDF and the actual instance(Python
    # Value(Type)) that the UDF returns are different.
    # Arrow and Pickle have different type coercion rules, so a UDF might have a different result
    # with/without Arrow optimization. That's the main reason the Arrow optimization for Python
    # UDFs is disabled by default.
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+  # noqa
    # |SQL Type \ Python Value(Type)|None(NoneType)|True(bool)|1(int)|         a(str)|    1970-01-01(date)|1970-01-01 00:00:00(datetime)|1.0(float)|array('i', [1])(array)|[1](list)|         (1,)(tuple)|bytearray(b'ABC')(bytearray)|  1(Decimal)|{'a': 1}(dict)|  # noqa
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+  # noqa
    # |                      boolean|          None|      True|  None|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                      tinyint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                     smallint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                          int|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                       bigint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                       string|          None|    'true'|   '1'|            'a'|'java.util.Gregor...|         'java.util.Gregor...|     '1.0'|         '[I@120d813a'|    '[1]'|'[Ljava.lang.Obje...|               '[B@48571878'|         '1'|       '{a=1}'|  # noqa
    # |                         date|          None|         X|     X|              X|datetime.date(197...|         datetime.date(197...|         X|                     X|        X|                   X|                           X|           X|             X|  # noqa
    # |                    timestamp|          None|         X|     X|              X|                   X|         datetime.datetime...|         X|                     X|        X|                   X|                           X|           X|             X|  # noqa
    # |                        float|          None|      None|  None|           None|                None|                         None|       1.0|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                       double|          None|      None|  None|           None|                None|                         None|       1.0|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                       binary|          None|      None|  None|bytearray(b'a')|                None|                         None|      None|                  None|     None|                None|           bytearray(b'ABC')|        None|          None|  # noqa
    # |                decimal(10,0)|          None|      None|  None|           None|                None|                         None|      None|                  None|     None|                None|                        None|Decimal('1')|          None|  # noqa
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+  # noqa
    # Note: Python 3.9.15, Pandas 1.5.2 and PyArrow 10.0.1 are used.
    # Note: The values of 'SQL Type' are DDL formatted strings, which can be used as `returnType`s.
    # Note: The values inside the table are generated by `repr`. X' means it throws an exception
    # during the conversion.

    if useArrow is None:
        from pyspark.sql import SparkSession

        session = SparkSession._instantiatedSession
        is_arrow_enabled = (
            False
            if session is None
            else session.conf.get("spark.sql.execution.pythonUDF.arrow.enabled") == "true"
        )
    else:
        is_arrow_enabled = useArrow

    eval_type: int = PythonEvalType.SQL_BATCHED_UDF

    if is_arrow_enabled:
        try:
            is_func_with_args = len(getfullargspec(f).args) > 0
        except TypeError:
            is_func_with_args = False
        if is_func_with_args:
            require_minimum_pandas_version()
            require_minimum_pyarrow_version()
            eval_type = PythonEvalType.SQL_ARROW_BATCHED_UDF
        else:
            warnings.warn(
                "Arrow optimization for Python UDFs cannot be enabled.",
                UserWarning,
            )

    return _create_udf(f, returnType, eval_type)


class UserDefinedFunction:
    """
    User defined function in Python

    .. versionadded:: 1.3

    Notes
    -----
    The constructor of this class is not supposed to be directly called.
    Use :meth:`pyspark.sql.functions.udf` or :meth:`pyspark.sql.functions.pandas_udf`
    to create this instance.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        returnType: "DataTypeOrString" = StringType(),
        name: Optional[str] = None,
        evalType: int = PythonEvalType.SQL_BATCHED_UDF,
        deterministic: bool = True,
    ):
        if not callable(func):
            raise PySparkTypeError(
                error_class="NOT_CALLABLE",
                message_parameters={"arg_name": "func", "arg_type": type(func).__name__},
            )

        if not isinstance(returnType, (DataType, str)):
            raise PySparkTypeError(
                error_class="NOT_DATATYPE_OR_STR",
                message_parameters={
                    "arg_name": "returnType",
                    "arg_type": type(returnType).__name__,
                },
            )

        if not isinstance(evalType, int):
            raise PySparkTypeError(
                error_class="NOT_INT",
                message_parameters={"arg_name": "evalType", "arg_type": type(evalType).__name__},
            )

        self.func = func
        self._returnType = returnType
        # Stores UserDefinedPythonFunctions jobj, once initialized
        self._returnType_placeholder: Optional[DataType] = None
        self._judf_placeholder = None
        self._name = name or (
            func.__name__ if hasattr(func, "__name__") else func.__class__.__name__
        )
        self.evalType = evalType
        self.deterministic = deterministic

    @property
    def returnType(self) -> DataType:
        # This makes sure this is called after SparkContext is initialized.
        # ``_parse_datatype_string`` accesses to JVM for parsing a DDL formatted string.
        # TODO: PythonEvalType.SQL_BATCHED_UDF
        if self._returnType_placeholder is None:
            if isinstance(self._returnType, DataType):
                self._returnType_placeholder = self._returnType
            else:
                self._returnType_placeholder = _parse_datatype_string(self._returnType)

        if (
            self.evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF
            or self.evalType == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
        ):
            try:
                to_arrow_type(self._returnType_placeholder)
            except TypeError:
                raise PySparkNotImplementedError(
                    error_class="NOT_IMPLEMENTED",
                    message_parameters={
                        "feature": f"Invalid return type with scalar Pandas UDFs: "
                        f"{self._returnType_placeholder}"
                    },
                )
        elif (
            self.evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF
            or self.evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE
        ):
            if isinstance(self._returnType_placeholder, StructType):
                try:
                    to_arrow_type(self._returnType_placeholder)
                except TypeError:
                    raise PySparkNotImplementedError(
                        error_class="NOT_IMPLEMENTED",
                        message_parameters={
                            "feature": f"Invalid return type with grouped map Pandas UDFs or "
                            f"at groupby.applyInPandas(WithState): {self._returnType_placeholder}"
                        },
                    )
            else:
                raise PySparkTypeError(
                    error_class="INVALID_RETURN_TYPE_FOR_PANDAS_UDF",
                    message_parameters={
                        "eval_type": "SQL_GROUPED_MAP_PANDAS_UDF or "
                        "SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE",
                        "return_type": str(self._returnType_placeholder),
                    },
                )
        elif (
            self.evalType == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
            or self.evalType == PythonEvalType.SQL_MAP_ARROW_ITER_UDF
        ):
            if isinstance(self._returnType_placeholder, StructType):
                try:
                    to_arrow_type(self._returnType_placeholder)
                except TypeError:
                    raise PySparkNotImplementedError(
                        error_class="NOT_IMPLEMENTED",
                        message_parameters={
                            "feature": f"Invalid return type in mapInPandas: "
                            f"{self._returnType_placeholder}"
                        },
                    )
            else:
                raise PySparkTypeError(
                    error_class="INVALID_RETURN_TYPE_FOR_PANDAS_UDF",
                    message_parameters={
                        "eval_type": "SQL_MAP_PANDAS_ITER_UDF or SQL_MAP_ARROW_ITER_UDF",
                        "return_type": str(self._returnType_placeholder),
                    },
                )
        elif self.evalType == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
            if isinstance(self._returnType_placeholder, StructType):
                try:
                    to_arrow_type(self._returnType_placeholder)
                except TypeError:
                    raise PySparkNotImplementedError(
                        error_class="NOT_IMPLEMENTED",
                        message_parameters={
                            "feature": f"Invalid return type in cogroup.applyInPandas: "
                            f"{self._returnType_placeholder}"
                        },
                    )
            else:
                raise PySparkTypeError(
                    error_class="INVALID_RETURN_TYPE_FOR_PANDAS_UDF",
                    message_parameters={
                        "eval_type": "SQL_COGROUPED_MAP_PANDAS_UDF",
                        "return_type": str(self._returnType_placeholder),
                    },
                )
        elif self.evalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF:
            try:
                # StructType is not yet allowed as a return type, explicitly check here to fail fast
                if isinstance(self._returnType_placeholder, StructType):
                    raise TypeError
                to_arrow_type(self._returnType_placeholder)
            except TypeError:
                raise PySparkNotImplementedError(
                    error_class="NOT_IMPLEMENTED",
                    message_parameters={
                        "feature": f"Invalid return type with grouped aggregate Pandas UDFs: "
                        f"{self._returnType_placeholder}"
                    },
                )

        return self._returnType_placeholder

    @property
    def _judf(self) -> JavaObject:
        # It is possible that concurrent access, to newly created UDF,
        # will initialize multiple UserDefinedPythonFunctions.
        # This is unlikely, doesn't affect correctness,
        # and should have a minimal performance impact.
        if self._judf_placeholder is None:
            self._judf_placeholder = self._create_judf(self.func)
        return self._judf_placeholder

    def _create_judf(self, func: Callable[..., Any]) -> JavaObject:
        from pyspark.sql import SparkSession

        spark = SparkSession._getActiveSessionOrCreate()
        sc = spark.sparkContext

        wrapped_func = _wrap_function(sc, func, self.returnType)
        jdt = spark._jsparkSession.parseDataType(self.returnType.json())
        assert sc._jvm is not None
        judf = sc._jvm.org.apache.spark.sql.execution.python.UserDefinedPythonFunction(
            self._name, wrapped_func, jdt, self.evalType, self.deterministic
        )
        return judf

    def __call__(self, *cols: "ColumnOrName") -> Column:
        sc = get_active_spark_context()
        profiler: Optional[Profiler] = None
        memory_profiler: Optional[Profiler] = None
        if sc.profiler_collector:
            profiler_enabled = sc._conf.get("spark.python.profile", "false") == "true"
            memory_profiler_enabled = sc._conf.get("spark.python.profile.memory", "false") == "true"

            # Disable profiling Pandas UDFs with iterators as input/output.
            if profiler_enabled or memory_profiler_enabled:
                if self.evalType in [
                    PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
                    PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
                    PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
                ]:
                    profiler_enabled = memory_profiler_enabled = False
                    warnings.warn(
                        "Profiling UDFs with iterators input/output is not supported.",
                        UserWarning,
                    )

            # Disallow enabling two profilers at the same time.
            if profiler_enabled and memory_profiler_enabled:
                # When both profilers are enabled, they interfere with each other,
                # that makes the result profile misleading.
                raise RuntimeError(
                    "'spark.python.profile' and 'spark.python.profile.memory' configuration"
                    " cannot be enabled together."
                )
            elif profiler_enabled:
                f = self.func
                profiler = sc.profiler_collector.new_udf_profiler(sc)

                @functools.wraps(f)
                def func(*args: Any, **kwargs: Any) -> Any:
                    assert profiler is not None
                    return profiler.profile(f, *args, **kwargs)

                func.__signature__ = inspect.signature(f)  # type: ignore[attr-defined]
                judf = self._create_judf(func)
                jUDFExpr = judf.builder(_to_seq(sc, cols, _to_java_expr))
                jPythonUDF = judf.fromUDFExpr(jUDFExpr)
                id = jUDFExpr.resultId().id()
                sc.profiler_collector.add_profiler(id, profiler)
            else:  # memory_profiler_enabled
                f = self.func
                memory_profiler = sc.profiler_collector.new_memory_profiler(sc)
                (sub_lines, start_line) = inspect.getsourcelines(f.__code__)

                @functools.wraps(f)
                def func(*args: Any, **kwargs: Any) -> Any:
                    assert memory_profiler is not None
                    return memory_profiler.profile(
                        sub_lines, start_line, f, *args, **kwargs  # type: ignore[arg-type]
                    )

                func.__signature__ = inspect.signature(f)  # type: ignore[attr-defined]
                judf = self._create_judf(func)
                jUDFExpr = judf.builder(_to_seq(sc, cols, _to_java_expr))
                jPythonUDF = judf.fromUDFExpr(jUDFExpr)
                id = jUDFExpr.resultId().id()
                sc.profiler_collector.add_profiler(id, memory_profiler)
        else:
            judf = self._judf
            jPythonUDF = judf.apply(_to_seq(sc, cols, _to_java_column))
        return Column(jPythonUDF)

    # This function is for improving the online help system in the interactive interpreter.
    # For example, the built-in help / pydoc.help. It wraps the UDF with the docstring and
    # argument annotation. (See: SPARK-19161)
    def _wrapped(self) -> "UserDefinedFunctionLike":
        """
        Wrap this udf with a function and attach docstring from func
        """

        # It is possible for a callable instance without __name__ attribute or/and
        # __module__ attribute to be wrapped here. For example, functools.partial. In this case,
        # we should avoid wrapping the attributes from the wrapped function to the wrapper
        # function. So, we take out these attribute names from the default names to set and
        # then manually assign it after being wrapped.
        assignments = tuple(
            a for a in functools.WRAPPER_ASSIGNMENTS if a != "__name__" and a != "__module__"
        )

        @functools.wraps(self.func, assigned=assignments)
        def wrapper(*args: "ColumnOrName") -> Column:
            return self(*args)

        wrapper.__name__ = self._name
        wrapper.__module__ = (
            self.func.__module__
            if hasattr(self.func, "__module__")
            else self.func.__class__.__module__
        )

        wrapper.func = self.func  # type: ignore[attr-defined]
        wrapper.returnType = self.returnType  # type: ignore[attr-defined]
        wrapper.evalType = self.evalType  # type: ignore[attr-defined]
        wrapper.deterministic = self.deterministic  # type: ignore[attr-defined]
        wrapper.asNondeterministic = functools.wraps(  # type: ignore[attr-defined]
            self.asNondeterministic
        )(lambda: self.asNondeterministic()._wrapped())
        wrapper._unwrapped = self  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    def asNondeterministic(self) -> "UserDefinedFunction":
        """
        Updates UserDefinedFunction to nondeterministic.

        .. versionadded:: 2.3
        """
        # Here, we explicitly clean the cache to create a JVM UDF instance
        # with 'deterministic' updated. See SPARK-23233.
        self._judf_placeholder = None
        self.deterministic = False
        return self


class UDFRegistration:
    """
    Wrapper for user-defined function registration. This instance can be accessed by
    :attr:`spark.udf` or :attr:`sqlContext.udf`.

    .. versionadded:: 1.3.1
    """

    def __init__(self, sparkSession: "SparkSession"):
        self.sparkSession = sparkSession

    def register(
        self,
        name: str,
        f: Union[Callable[..., Any], "UserDefinedFunctionLike"],
        returnType: Optional["DataTypeOrString"] = None,
    ) -> "UserDefinedFunctionLike":
        """Register a Python function (including lambda function) or a user-defined function
        as a SQL function.

        .. versionadded:: 1.3.1

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name : str,
            name of the user-defined function in SQL statements.
        f : function, :meth:`pyspark.sql.functions.udf` or :meth:`pyspark.sql.functions.pandas_udf`
            a Python function, or a user-defined function. The user-defined function can
            be either row-at-a-time or vectorized. See :meth:`pyspark.sql.functions.udf` and
            :meth:`pyspark.sql.functions.pandas_udf`.
        returnType : :class:`pyspark.sql.types.DataType` or str, optional
            the return type of the registered user-defined function. The value can
            be either a :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
            `returnType` can be optionally specified when `f` is a Python function but not
            when `f` is a user-defined function. Please see the examples below.

        Returns
        -------
        function
            a user-defined function

        Notes
        -----
        To register a nondeterministic Python function, users need to first build
        a nondeterministic user-defined function for the Python function and then register it
        as a SQL function.

        Examples
        --------
        1. When `f` is a Python function:

            `returnType` defaults to string type and can be optionally specified. The produced
            object must match the specified type. In this case, this API works as if
            `register(name, f, returnType=StringType())`.

            >>> strlen = spark.udf.register("stringLengthString", lambda x: len(x))
            >>> spark.sql("SELECT stringLengthString('test')").collect()
            [Row(stringLengthString(test)='4')]

            >>> spark.sql("SELECT 'foo' AS text").select(strlen("text")).collect()
            [Row(stringLengthString(text)='3')]

            >>> from pyspark.sql.types import IntegerType
            >>> _ = spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
            >>> spark.sql("SELECT stringLengthInt('test')").collect()
            [Row(stringLengthInt(test)=4)]

            >>> from pyspark.sql.types import IntegerType
            >>> _ = spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
            >>> spark.sql("SELECT stringLengthInt('test')").collect()
            [Row(stringLengthInt(test)=4)]

        2. When `f` is a user-defined function (from Spark 2.3.0):

            Spark uses the return type of the given user-defined function as the return type of
            the registered user-defined function. `returnType` should not be specified.
            In this case, this API works as if `register(name, f)`.

            >>> from pyspark.sql.types import IntegerType
            >>> from pyspark.sql.functions import udf
            >>> slen = udf(lambda s: len(s), IntegerType())
            >>> _ = spark.udf.register("slen", slen)
            >>> spark.sql("SELECT slen('test')").collect()
            [Row(slen(test)=4)]

            >>> import random
            >>> from pyspark.sql.functions import udf
            >>> from pyspark.sql.types import IntegerType
            >>> random_udf = udf(lambda: random.randint(0, 100), IntegerType()).asNondeterministic()
            >>> new_random_udf = spark.udf.register("random_udf", random_udf)
            >>> spark.sql("SELECT random_udf()").collect()  # doctest: +SKIP
            [Row(random_udf()=82)]

            >>> import pandas as pd  # doctest: +SKIP
            >>> from pyspark.sql.functions import pandas_udf
            >>> @pandas_udf("integer")  # doctest: +SKIP
            ... def add_one(s: pd.Series) -> pd.Series:
            ...     return s + 1
            ...
            >>> _ = spark.udf.register("add_one", add_one)  # doctest: +SKIP
            >>> spark.sql("SELECT add_one(id) FROM range(3)").collect()  # doctest: +SKIP
            [Row(add_one(id)=1), Row(add_one(id)=2), Row(add_one(id)=3)]

            >>> @pandas_udf("integer")  # doctest: +SKIP
            ... def sum_udf(v: pd.Series) -> int:
            ...     return v.sum()
            ...
            >>> _ = spark.udf.register("sum_udf", sum_udf)  # doctest: +SKIP
            >>> q = "SELECT sum_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
            >>> spark.sql(q).collect()  # doctest: +SKIP
            [Row(sum_udf(v1)=1), Row(sum_udf(v1)=5)]

        """

        # This is to check whether the input function is from a user-defined function or
        # Python function.
        if hasattr(f, "asNondeterministic"):
            if returnType is not None:
                raise PySparkTypeError(
                    error_class="CANNOT_SPECIFY_RETURN_TYPE_FOR_UDF",
                    message_parameters={"arg_name": "f", "return_type": str(returnType)},
                )
            f = cast("UserDefinedFunctionLike", f)
            if f.evalType not in [
                PythonEvalType.SQL_BATCHED_UDF,
                PythonEvalType.SQL_ARROW_BATCHED_UDF,
                PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
                PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
            ]:
                raise PySparkTypeError(
                    error_class="INVALID_UDF_EVAL_TYPE",
                    message_parameters={
                        "eval_type": "SQL_BATCHED_UDF, SQL_ARROW_BATCHED_UDF, "
                        "SQL_SCALAR_PANDAS_UDF, SQL_SCALAR_PANDAS_ITER_UDF or "
                        "SQL_GROUPED_AGG_PANDAS_UDF"
                    },
                )
            source_udf = _create_udf(
                f.func,
                returnType=f.returnType,
                name=name,
                evalType=f.evalType,
                deterministic=f.deterministic,
            )
            register_udf = source_udf._unwrapped  # type: ignore[attr-defined]
            return_udf = register_udf
        else:
            if returnType is None:
                returnType = StringType()
            return_udf = _create_udf(
                f, returnType=returnType, evalType=PythonEvalType.SQL_BATCHED_UDF, name=name
            )
            register_udf = return_udf._unwrapped
        self.sparkSession._jsparkSession.udf().registerPython(name, register_udf._judf)
        return return_udf

    def registerJavaFunction(
        self,
        name: str,
        javaClassName: str,
        returnType: Optional["DataTypeOrString"] = None,
    ) -> None:
        """Register a Java user-defined function as a SQL function.

        In addition to a name and the function itself, the return type can be optionally specified.
        When the return type is not specified we would infer it via reflection.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name : str
            name of the user-defined function
        javaClassName : str
            fully qualified name of java class
        returnType : :class:`pyspark.sql.types.DataType` or str, optional
            the return type of the registered Java function. The value can be either
            a :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        Examples
        --------
        >>> from pyspark.sql.types import IntegerType
        >>> spark.udf.registerJavaFunction(
        ...     "javaStringLength", "test.org.apache.spark.sql.JavaStringLength", IntegerType())
        ... # doctest: +SKIP
        >>> spark.sql("SELECT javaStringLength('test')").collect()  # doctest: +SKIP
        [Row(javaStringLength(test)=4)]

        >>> spark.udf.registerJavaFunction(
        ...     "javaStringLength2", "test.org.apache.spark.sql.JavaStringLength")
        ... # doctest: +SKIP
        >>> spark.sql("SELECT javaStringLength2('test')").collect()  # doctest: +SKIP
        [Row(javaStringLength2(test)=4)]

        >>> spark.udf.registerJavaFunction(
        ...     "javaStringLength3", "test.org.apache.spark.sql.JavaStringLength", "integer")
        ... # doctest: +SKIP
        >>> spark.sql("SELECT javaStringLength3('test')").collect()  # doctest: +SKIP
        [Row(javaStringLength3(test)=4)]
        """

        jdt = None
        if returnType is not None:
            if not isinstance(returnType, DataType):
                returnType = _parse_datatype_string(returnType)
            jdt = self.sparkSession._jsparkSession.parseDataType(returnType.json())
        self.sparkSession._jsparkSession.udf().registerJava(name, javaClassName, jdt)

    def registerJavaUDAF(self, name: str, javaClassName: str) -> None:
        """Register a Java user-defined aggregate function as a SQL function.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        name : str
            name of the user-defined aggregate function
        javaClassName : str
            fully qualified name of java class

        Examples
        --------
        >>> spark.udf.registerJavaUDAF("javaUDAF", "test.org.apache.spark.sql.MyDoubleAvg")
        ... # doctest: +SKIP
        >>> df = spark.createDataFrame([(1, "a"),(2, "b"), (3, "a")],["id", "name"])
        >>> df.createOrReplaceTempView("df")
        >>> q = "SELECT name, javaUDAF(id) as avg from df group by name order by name desc"
        >>> spark.sql(q).collect()  # doctest: +SKIP
        [Row(name='b', avg=102.0), Row(name='a', avg=102.0)]
        """

        self.sparkSession._jsparkSession.udf().registerJavaUDAF(name, javaClassName)


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.udf

    globs = pyspark.sql.udf.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.udf tests").getOrCreate()
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.udf, globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
