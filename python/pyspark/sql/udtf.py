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
User-defined table function related classes and functions
"""
from dataclasses import dataclass
import inspect
import sys
import warnings
from typing import Any, Iterator, Type, TYPE_CHECKING, Optional, Union

from py4j.java_gateway import JavaObject

from pyspark.errors import PySparkAttributeError, PySparkTypeError
from pyspark.rdd import PythonEvalType
from pyspark.sql.column import _to_java_column, _to_seq
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from pyspark.sql.types import DataType, StructType, _parse_datatype_string
from pyspark.sql.udf import _wrap_function

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession

__all__ = ["AnalyzeArgument", "AnalyzeResult", "UDTFRegistration"]


@dataclass(frozen=True)
class AnalyzeArgument:
    """
    The argument for Python UDTF's analyze static method.

    Parameters
    ----------
    data_type : :class:`DataType`
        The argument's data type
    value : Optional[Any]
        The calculated value if the argument is foldable; otherwise None
    is_table : bool
        If True, the argument is a table argument.
    """

    data_type: DataType
    value: Optional[Any]
    is_table: bool


@dataclass(frozen=True)
class AnalyzeResult:
    """
    The return of Python UDTF's analyze static method.

    Parameters
    ----------
    schema : :class:`StructType`
        The schema that the Python UDTF will return.
    """

    schema: StructType


def _create_udtf(
    cls: Type,
    returnType: Optional[Union[StructType, str]],
    name: Optional[str] = None,
    evalType: int = PythonEvalType.SQL_TABLE_UDF,
    deterministic: bool = True,
) -> "UserDefinedTableFunction":
    """Create a Python UDTF with the given eval type."""
    udtf_obj = UserDefinedTableFunction(
        cls, returnType=returnType, name=name, evalType=evalType, deterministic=deterministic
    )

    return udtf_obj


def _create_py_udtf(
    cls: Type,
    returnType: Optional[Union[StructType, str]],
    name: Optional[str] = None,
    deterministic: bool = True,
    useArrow: Optional[bool] = None,
) -> "UserDefinedTableFunction":
    """Create a regular or an Arrow-optimized Python UDTF."""
    # Determine whether to create Arrow-optimized UDTFs.
    if useArrow is not None:
        arrow_enabled = useArrow
    else:
        from pyspark.sql import SparkSession

        session = SparkSession._instantiatedSession
        arrow_enabled = (
            session.conf.get("spark.sql.execution.pythonUDTF.arrow.enabled") == "true"
            if session is not None
            else True
        )

    # Create a regular Python UDTF and check for invalid handler class.
    regular_udtf = _create_udtf(cls, returnType, name, PythonEvalType.SQL_TABLE_UDF, deterministic)

    if not arrow_enabled:
        return regular_udtf

    # Return the regular UDTF if the required dependencies are not satisfied.
    try:
        require_minimum_pandas_version()
        require_minimum_pyarrow_version()
    except ImportError as e:
        warnings.warn(
            f"Arrow optimization for Python UDTFs cannot be enabled: {str(e)}. "
            f"Falling back to using regular Python UDTFs.",
            UserWarning,
        )
        return regular_udtf

    # Return the vectorized UDTF.
    vectorized_udtf = _vectorize_udtf(cls)
    return _create_udtf(
        cls=vectorized_udtf,
        returnType=returnType,
        name=name,
        evalType=PythonEvalType.SQL_ARROW_TABLE_UDF,
        deterministic=regular_udtf.deterministic,
    )


def _vectorize_udtf(cls: Type) -> Type:
    """Vectorize a Python UDTF handler class."""
    import pandas as pd

    class VectorizedUDTF:
        def __init__(self) -> None:
            self.func = cls()

        if hasattr(cls, "analyze") and isinstance(
            inspect.getattr_static(cls, "analyze"), staticmethod
        ):

            @staticmethod
            def analyze(*args: AnalyzeArgument) -> AnalyzeResult:
                return cls.analyze(*args)

        def eval(self, *args: pd.Series) -> Iterator[pd.DataFrame]:
            if len(args) == 0:
                yield pd.DataFrame(self.func.eval())
            else:
                # Create tuples from the input pandas Series, each tuple
                # represents a row across all Series.
                row_tuples = zip(*args)
                for row in row_tuples:
                    yield pd.DataFrame(self.func.eval(*row))

        def terminate(self) -> Iterator[pd.DataFrame]:
            if hasattr(self.func, "terminate"):
                yield pd.DataFrame(self.func.terminate())

    vectorized_udtf = VectorizedUDTF
    vectorized_udtf.__name__ = cls.__name__
    vectorized_udtf.__module__ = cls.__module__
    vectorized_udtf.__doc__ = cls.__doc__
    vectorized_udtf.__init__.__doc__ = cls.__init__.__doc__
    vectorized_udtf.eval.__doc__ = getattr(cls, "eval").__doc__
    if hasattr(cls, "terminate"):
        getattr(vectorized_udtf, "terminate").__doc__ = getattr(cls, "terminate").__doc__

    if hasattr(vectorized_udtf, "analyze"):
        getattr(vectorized_udtf, "analyze").__doc__ = getattr(cls, "analyze").__doc__

    return vectorized_udtf


def _validate_udtf_handler(cls: Any, returnType: Optional[Union[StructType, str]]) -> None:
    """Validate the handler class of a UDTF."""

    if not isinstance(cls, type):
        raise PySparkTypeError(
            error_class="INVALID_UDTF_HANDLER_TYPE", message_parameters={"type": type(cls).__name__}
        )

    if not hasattr(cls, "eval"):
        raise PySparkAttributeError(
            error_class="INVALID_UDTF_NO_EVAL", message_parameters={"name": cls.__name__}
        )

    has_analyze = hasattr(cls, "analyze")
    has_analyze_staticmethod = has_analyze and isinstance(
        inspect.getattr_static(cls, "analyze"), staticmethod
    )
    if returnType is None and not has_analyze_staticmethod:
        raise PySparkAttributeError(
            error_class="INVALID_UDTF_RETURN_TYPE", message_parameters={"name": cls.__name__}
        )
    if returnType is not None and has_analyze:
        raise PySparkAttributeError(
            error_class="INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE",
            message_parameters={"name": cls.__name__},
        )


class UserDefinedTableFunction:
    """
    User-defined table function in Python

    .. versionadded:: 3.5.0

    Notes
    -----
    The constructor of this class is not supposed to be directly called.
    Use :meth:`pyspark.sql.functions.udtf` to create this instance.

    This API is evolving.
    """

    def __init__(
        self,
        func: Type,
        returnType: Optional[Union[StructType, str]],
        name: Optional[str] = None,
        evalType: int = PythonEvalType.SQL_TABLE_UDF,
        deterministic: bool = True,
    ):
        _validate_udtf_handler(func, returnType)

        self.func = func
        self._returnType = returnType
        self._returnType_placeholder: Optional[StructType] = None
        self._inputTypes_placeholder = None
        self._judtf_placeholder = None
        self._name = name or func.__name__
        self.evalType = evalType
        self.deterministic = deterministic

    @property
    def returnType(self) -> Optional[StructType]:
        if self._returnType is None:
            return None
        # `_parse_datatype_string` accesses to JVM for parsing a DDL formatted string.
        # This makes sure this is called after SparkContext is initialized.
        if self._returnType_placeholder is None:
            if isinstance(self._returnType, str):
                parsed = _parse_datatype_string(self._returnType)
            else:
                parsed = self._returnType
            if not isinstance(parsed, StructType):
                raise PySparkTypeError(
                    error_class="UDTF_RETURN_TYPE_MISMATCH",
                    message_parameters={
                        "name": self._name,
                        "return_type": f"{parsed}",
                    },
                )
            self._returnType_placeholder = parsed
        return self._returnType_placeholder

    @property
    def _judtf(self) -> JavaObject:
        if self._judtf_placeholder is None:
            self._judtf_placeholder = self._create_judtf(self.func)
        return self._judtf_placeholder

    def _create_judtf(self, func: Type) -> JavaObject:
        from pyspark.sql import SparkSession

        spark = SparkSession._getActiveSessionOrCreate()
        sc = spark.sparkContext

        wrapped_func = _wrap_function(sc, func)
        assert sc._jvm is not None
        if self.returnType is None:
            judtf = sc._jvm.org.apache.spark.sql.execution.python.UserDefinedPythonTableFunction(
                self._name, wrapped_func, self.evalType, self.deterministic
            )
        else:
            jdt = spark._jsparkSession.parseDataType(self.returnType.json())
            judtf = sc._jvm.org.apache.spark.sql.execution.python.UserDefinedPythonTableFunction(
                self._name, wrapped_func, jdt, self.evalType, self.deterministic
            )
        return judtf

    def __call__(self, *cols: "ColumnOrName") -> "DataFrame":
        from pyspark.sql import DataFrame, SparkSession

        spark = SparkSession._getActiveSessionOrCreate()
        sc = spark.sparkContext

        judtf = self._judtf
        jPythonUDTF = judtf.apply(spark._jsparkSession, _to_seq(sc, cols, _to_java_column))
        return DataFrame(jPythonUDTF, spark)

    def asNondeterministic(self) -> "UserDefinedTableFunction":
        """
        Updates UserDefinedTableFunction to nondeterministic.
        """
        # Explicitly clean the cache to create a JVM UDTF instance.
        self._judtf_placeholder = None
        self.deterministic = False
        return self


class UDTFRegistration:
    """
    Wrapper for user-defined table function registration. This instance can be accessed by
    :attr:`spark.udtf` or :attr:`sqlContext.udtf`.

    .. versionadded:: 3.5.0
    """

    def __init__(self, sparkSession: "SparkSession"):
        self.sparkSession = sparkSession

    def register(
        self,
        name: str,
        f: "UserDefinedTableFunction",
    ) -> "UserDefinedTableFunction":
        """Register a Python user-defined table function as a SQL table function.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        name : str
            The name of the user-defined table function in SQL statements.
        f : function or :meth:`pyspark.sql.functions.udtf`
            The user-defined table function.

        Returns
        -------
        function
            The registered user-defined table function.

        Notes
        -----
        Spark uses the return type of the given user-defined table function as the return
        type of the registered user-defined function.

        To register a nondeterministic Python table function, users need to first build
        a nondeterministic user-defined table function and then register it as a SQL function.

        Examples
        --------
        >>> from pyspark.sql.functions import udtf
        >>> @udtf(returnType="c1: int, c2: int")
        ... class PlusOne:
        ...     def eval(self, x: int):
        ...         yield x, x + 1
        ...
        >>> _ = spark.udtf.register(name="plus_one", f=PlusOne)
        >>> spark.sql("SELECT * FROM plus_one(1)").collect()
        [Row(c1=1, c2=2)]

        Use it with lateral join

        >>> spark.sql("SELECT * FROM VALUES (0, 1), (1, 2) t(x, y), LATERAL plus_one(x)").collect()
        [Row(x=0, y=1, c1=0, c2=1), Row(x=1, y=2, c1=1, c2=2)]
        """
        if f.evalType not in [PythonEvalType.SQL_TABLE_UDF, PythonEvalType.SQL_ARROW_TABLE_UDF]:
            raise PySparkTypeError(
                error_class="INVALID_UDTF_EVAL_TYPE",
                message_parameters={
                    "name": name,
                    "eval_type": "SQL_TABLE_UDF, SQL_ARROW_TABLE_UDF",
                },
            )

        register_udtf = _create_udtf(
            cls=f.func,
            returnType=f.returnType,
            name=name,
            evalType=f.evalType,
            deterministic=f.deterministic,
        )
        self.sparkSession._jsparkSession.udtf().registerPython(name, register_udtf._judtf)
        return register_udtf


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.udf

    globs = pyspark.sql.udtf.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.udtf tests").getOrCreate()
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.udtf, globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
