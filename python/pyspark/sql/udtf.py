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
import sys
import warnings
from typing import Iterator, Type, TYPE_CHECKING, Optional, Union

from py4j.java_gateway import JavaObject

from pyspark.errors import PySparkAttributeError, PySparkTypeError
from pyspark.sql.column import _to_java_column, _to_seq
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from pyspark.rdd import PythonEvalType
from pyspark.sql.types import StructType, _parse_datatype_string
from pyspark.sql.udf import _wrap_function

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession

__all__ = ["UDTFRegistration"]


def _create_udtf(
    cls: Type,
    returnType: Union[StructType, str],
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
    returnType: Union[StructType, str],
    name: Optional[str] = None,
    deterministic: bool = True,
    useArrow: Optional[bool] = None,
) -> "UserDefinedTableFunction":
    """Create a regular or an Arrow-optimized Python UDTF."""
    from pyspark.sql import SparkSession

    # Determine whether to create Arrow-optimized UDTFs.
    # Use arrow-optimization by default.
    session = SparkSession._instantiatedSession
    if session is None:
        arrow_enabled = useArrow or True
    else:
        arrow_enabled = (
            useArrow or session.conf.get("spark.sql.execution.pythonUDTF.arrow.enabled") == "true"
        )

    # Create a regular Python UDTF and check for invalid handler class.
    regular_udtf = _create_udtf(cls, returnType, name, PythonEvalType.SQL_TABLE_UDF, deterministic)

    if arrow_enabled:
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
        return _create_arrow_udtf(regular_udtf)
    else:
        return regular_udtf


def _create_arrow_udtf(regular_udtf: "UserDefinedTableFunction") -> "UserDefinedTableFunction":
    """Create an Arrow-optimized Python UDTF."""
    import pandas as pd

    cls = regular_udtf.func

    class VectorizedUDTF:
        def __init__(self):
            self.func = cls()

        def eval(self, *args: pd.Series) -> Iterator[pd.DataFrame]:
            # TODO: check the signature of the `eval` method and make sure
            # it does not use pd.Series or pd.DataFrame.
            if len(args) == 0:
                yield pd.DataFrame(self.func.eval())
            else:
                # Create tuples from the input pandas Series, each tuple
                # represents a row across all Series.
                row_tuples = zip(*args)
                for row in row_tuples:
                    yield pd.DataFrame(self.func.eval(*row))

        def terminate(self) -> Iterator[pd.DataFrame]:
            if hasattr(cls, "terminate"):
                yield pd.DataFrame(self.func.terminate())

    vectorized_udtf = VectorizedUDTF
    vectorized_udtf.__name__ = cls.__name__
    vectorized_udtf.__module__ = cls.__module__
    vectorized_udtf.__doc__ = cls.__doc__
    vectorized_udtf.eval.__doc__ = getattr(cls, "eval").__doc__
    if hasattr(cls, "terminate"):
        getattr(vectorized_udtf, "terminate").__doc__ = getattr(cls, "terminate").__doc__

    return _create_udtf(
        cls=vectorized_udtf,
        returnType=regular_udtf.returnType,
        name=regular_udtf.name,
        evalType=PythonEvalType.SQL_ARROW_TABLE_UDF,
        deterministic=regular_udtf.deterministic,
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
        returnType: Union[StructType, str],
        name: Optional[str] = None,
        evalType: int = PythonEvalType.SQL_TABLE_UDF,
        deterministic: bool = True,
    ):

        if not isinstance(func, type):
            raise PySparkTypeError(
                f"Invalid user defined table function: the function handler "
                f"must be a class, but got {type(func).__name__}. Please provide "
                "a class as the handler."
            )

        # TODO(SPARK-43968): add more compile time checks for UDTFs
        self.func = func
        self._returnType = returnType
        self._returnType_placeholder: Optional[StructType] = None
        self._inputTypes_placeholder = None
        self._judtf_placeholder = None
        self.name = name or func.__name__
        self.evalType = evalType
        self.deterministic = deterministic

        if not hasattr(func, "eval"):
            raise PySparkAttributeError(
                error_class="INVALID_UDTF_NO_EVAL", message_parameters={"name": self.name}
            )

    @property
    def returnType(self) -> StructType:
        # `_parse_datatype_string` accesses to JVM for parsing a DDL formatted string.
        # This makes sure this is called after SparkContext is initialized.
        if self._returnType_placeholder is None:
            if isinstance(self._returnType, StructType):
                self._returnType_placeholder = self._returnType
            else:
                assert isinstance(self._returnType, str)
                parsed = _parse_datatype_string(self._returnType)
                if not isinstance(parsed, StructType):
                    raise PySparkTypeError(
                        f"Invalid return type for the user defined table function "
                        f"'{self.name}': {self._returnType}. The return type of a "
                        f"UDTF must be a 'StructType'. Please ensure the return "
                        "type is a correctly formatted 'StructType' string."
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
        jdt = spark._jsparkSession.parseDataType(self.returnType.json())
        assert sc._jvm is not None
        judtf = sc._jvm.org.apache.spark.sql.execution.python.UserDefinedPythonTableFunction(
            self.name, wrapped_func, jdt, self.evalType, self.deterministic
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
        f: UserDefinedTableFunction,
    ) -> UserDefinedTableFunction:
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
