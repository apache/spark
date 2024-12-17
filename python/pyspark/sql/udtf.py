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
import pickle
from dataclasses import dataclass, field
import inspect
import sys
import warnings
from typing import Any, Type, TYPE_CHECKING, Optional, Sequence, Union

from pyspark.errors import PySparkAttributeError, PySparkPicklingError, PySparkTypeError
from pyspark.util import PythonEvalType
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from pyspark.sql.types import DataType, StructType, _parse_datatype_string
from pyspark.sql.udf import _wrap_function

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql._typing import ColumnOrName
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession

__all__ = [
    "AnalyzeArgument",
    "AnalyzeResult",
    "PartitioningColumn",
    "OrderingColumn",
    "SelectedColumn",
    "SkipRestOfInputTableException",
    "UDTFRegistration",
]


@dataclass(frozen=True)
class AnalyzeArgument:
    """
    The argument for Python UDTF's analyze static method.

    Parameters
    ----------
    dataType : :class:`DataType`
        The argument's data type
    value : any, optional
        The calculated value if the argument is foldable; otherwise None
    isTable : bool
        If True, the argument is a table argument.
    isConstantExpression : bool
        If True, the argument is a constant-foldable scalar expression. Then the 'value' field
        contains None if the argument is a NULL literal, or a non-None value if the argument is a
        non-NULL literal. In this way, we can distinguish between a literal NULL argument and other
        types of arguments such as complex expression trees or table arguments where the 'value'
        field is always None.
    """

    dataType: DataType
    value: Optional[Any]
    isTable: bool
    isConstantExpression: bool


@dataclass(frozen=True)
class PartitioningColumn:
    """
    Represents an expression that the UDTF is specifying for Catalyst to partition the input table
    by. This can be either the name of a single column from the input table (such as "columnA"), or
    a SQL expression based on the column names of the input table (such as "columnA + columnB").

    Parameters
    ----------
    name : str
        The contents of the partitioning column name or expression represented as a SQL string.
    """

    name: str


@dataclass(frozen=True)
class OrderingColumn:
    """
    Represents an expression that the UDTF is specifying for Catalyst to order the input partition
    by. This can be either the name of a single column from the input table (such as "columnA"),
    or a SQL expression based on the column names of the input table (such as "columnA + columnB").

    Parameters
    ----------
    name : str
        The contents of the ordering column name or expression represented as a SQL string.
    ascending : bool, default True
        This is if this expression specifies an ascending sorting order.
    overrideNullsFirst : str, optional
        If this is None, use the default behavior to sort NULL values first when sorting in
        ascending order, or last when sorting in descending order. Otherwise, if this is
        True or False, we override the default behavior accordingly.
    """

    name: str
    ascending: bool = True
    overrideNullsFirst: Optional[bool] = None


@dataclass(frozen=True)
class SelectedColumn:
    """
    Represents an expression that the UDTF is specifying for Catalyst to evaluate against the
    columns in the input TABLE argument. The UDTF then receives one input column for each expression
    in the list, in the order they are listed.

    Parameters
    ----------
    name : str
        The contents of the selected column name or expression represented as a SQL string.
    alias : str, default ''
        If non-empty, this is the alias for the column or expression as visible from the UDTF's
        'eval' method. This is required if the expression is not a simple column reference.
    """

    name: str
    alias: str = ""


# Note: this class is a "dataclass" for purposes of convenience, but it is not marked "frozen"
# because the intention is that users may create subclasses of it for purposes of returning custom
# information from the "analyze" method.
@dataclass
class AnalyzeResult:
    """
    The return of Python UDTF's analyze static method.

    Parameters
    ----------
    schema: :class:`StructType`
        The schema that the Python UDTF will return.
    withSinglePartition: bool
        If true, the UDTF is specifying for Catalyst to repartition all rows of the input TABLE
        argument to one collection for consumption by exactly one instance of the corresponding
        UDTF class.
    partitionBy: sequence of :class:`PartitioningColumn`
        If non-empty, this is a sequence of expressions that the UDTF is specifying for Catalyst to
        partition the input TABLE argument by. In this case, calls to the UDTF may not include any
        explicit PARTITION BY clause, in which case Catalyst will return an error. This option is
        mutually exclusive with 'withSinglePartition'.
    orderBy: sequence of :class:`OrderingColumn`
        If non-empty, this is a sequence of expressions that the UDTF is specifying for Catalyst to
        sort the input TABLE argument by. Note that the 'partitionBy' list must also be non-empty
        in this case.
    select: sequence of :class:`SelectedColumn`
        If non-empty, this is a sequence of expressions that the UDTF is specifying for Catalyst to
        evaluate against the columns in the input TABLE argument. The UDTF then receives one input
        attribute for each name in the list, in the order they are listed.
    """

    schema: StructType
    withSinglePartition: bool = False
    partitionBy: Sequence[PartitioningColumn] = field(default_factory=tuple)
    orderBy: Sequence[OrderingColumn] = field(default_factory=tuple)
    select: Sequence[SelectedColumn] = field(default_factory=tuple)


class SkipRestOfInputTableException(Exception):
    """
    This represents an exception that the 'eval' method may raise to indicate that it is done
    consuming rows from the current partition of the input table. Then the UDTF's 'terminate'
    method runs (if any).
    """

    pass


def _create_udtf(
    cls: Type,
    returnType: Optional[Union[StructType, str]],
    name: Optional[str] = None,
    evalType: int = PythonEvalType.SQL_TABLE_UDF,
    deterministic: bool = False,
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
    deterministic: bool = False,
    useArrow: Optional[bool] = None,
) -> "UserDefinedTableFunction":
    """Create a regular or an Arrow-optimized Python UDTF."""
    # Determine whether to create Arrow-optimized UDTFs.
    if useArrow is not None:
        arrow_enabled = useArrow
    else:
        from pyspark.sql import SparkSession

        session = SparkSession._instantiatedSession
        arrow_enabled = False
        if session is not None:
            value = session.conf.get("spark.sql.execution.pythonUDTF.arrow.enabled")
            if isinstance(value, str) and value.lower() == "true":
                arrow_enabled = True

    eval_type: int = PythonEvalType.SQL_TABLE_UDF

    if arrow_enabled:
        # Return the regular UDTF if the required dependencies are not satisfied.
        try:
            require_minimum_pandas_version()
            require_minimum_pyarrow_version()
            eval_type = PythonEvalType.SQL_ARROW_TABLE_UDF
        except ImportError as e:
            warnings.warn(
                f"Arrow optimization for Python UDTFs cannot be enabled: {str(e)}. "
                f"Falling back to using regular Python UDTFs.",
                UserWarning,
            )

    return _create_udtf(
        cls=cls,
        returnType=returnType,
        name=name,
        evalType=eval_type,
        deterministic=deterministic,
    )


def _validate_udtf_handler(cls: Any, returnType: Optional[Union[StructType, str]]) -> None:
    """Validate the handler class of a UDTF."""

    if not isinstance(cls, type):
        raise PySparkTypeError(
            errorClass="INVALID_UDTF_HANDLER_TYPE", messageParameters={"type": type(cls).__name__}
        )

    if not hasattr(cls, "eval"):
        raise PySparkAttributeError(
            errorClass="INVALID_UDTF_NO_EVAL", messageParameters={"name": cls.__name__}
        )

    has_analyze = hasattr(cls, "analyze")
    has_analyze_staticmethod = has_analyze and isinstance(
        inspect.getattr_static(cls, "analyze"), staticmethod
    )
    if returnType is None and not has_analyze_staticmethod:
        raise PySparkAttributeError(
            errorClass="INVALID_UDTF_RETURN_TYPE", messageParameters={"name": cls.__name__}
        )
    if returnType is not None and has_analyze:
        raise PySparkAttributeError(
            errorClass="INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE",
            messageParameters={"name": cls.__name__},
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
        deterministic: bool = False,
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
                    errorClass="UDTF_RETURN_TYPE_MISMATCH",
                    messageParameters={
                        "name": self._name,
                        "return_type": f"{parsed}",
                    },
                )
            self._returnType_placeholder = parsed
        return self._returnType_placeholder

    @property
    def _judtf(self) -> "JavaObject":
        if self._judtf_placeholder is None:
            self._judtf_placeholder = self._create_judtf(self.func)
        return self._judtf_placeholder

    def _create_judtf(self, func: Type) -> "JavaObject":
        from pyspark.sql import SparkSession

        spark = SparkSession._getActiveSessionOrCreate()
        sc = spark.sparkContext

        try:
            wrapped_func = _wrap_function(sc, func)
        except pickle.PicklingError as e:
            if "CONTEXT_ONLY_VALID_ON_DRIVER" in str(e):
                raise PySparkPicklingError(
                    errorClass="UDTF_SERIALIZATION_ERROR",
                    messageParameters={
                        "name": self._name,
                        "message": "it appears that you are attempting to reference SparkSession "
                        "inside a UDTF. SparkSession can only be used on the driver, "
                        "not in code that runs on workers. Please remove the reference "
                        "and try again.",
                    },
                ) from None
            raise PySparkPicklingError(
                errorClass="UDTF_SERIALIZATION_ERROR",
                messageParameters={
                    "name": self._name,
                    "message": "Please check the stack trace and make sure the "
                    "function is serializable.",
                },
            )

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

    def __call__(self, *args: "ColumnOrName", **kwargs: "ColumnOrName") -> "DataFrame":
        from pyspark.sql.classic.column import _to_java_column, _to_seq

        from pyspark.sql import DataFrame, SparkSession

        spark = SparkSession._getActiveSessionOrCreate()
        sc = spark.sparkContext

        assert sc._jvm is not None
        jcols = [_to_java_column(arg) for arg in args] + [
            sc._jvm.PythonSQLUtils.namedArgumentExpression(key, _to_java_column(value))
            for key, value in kwargs.items()
        ]

        judtf = self._judtf
        jPythonUDTF = judtf.apply(spark._jsparkSession, _to_seq(sc, jcols))
        return DataFrame(jPythonUDTF, spark)

    def asDeterministic(self) -> "UserDefinedTableFunction":
        """
        Updates UserDefinedTableFunction to deterministic.
        """
        # Explicitly clean the cache to create a JVM UDTF instance.
        self._judtf_placeholder = None
        self.deterministic = True
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
        if not isinstance(f, UserDefinedTableFunction):
            raise PySparkTypeError(
                errorClass="CANNOT_REGISTER_UDTF",
                messageParameters={
                    "name": name,
                },
            )

        if f.evalType not in [PythonEvalType.SQL_TABLE_UDF, PythonEvalType.SQL_ARROW_TABLE_UDF]:
            raise PySparkTypeError(
                errorClass="INVALID_UDTF_EVAL_TYPE",
                messageParameters={
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
