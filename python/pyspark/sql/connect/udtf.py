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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

import warnings
from typing import List, Type, TYPE_CHECKING, Optional, Union, Any

from pyspark.util import PythonEvalType
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import ColumnReference, Expression, NamedArgumentExpression
from pyspark.sql.connect.plan import (
    CommonInlineUserDefinedTableFunction,
    PythonUDTF,
)
from pyspark.sql.connect.table_arg import TableArg
from pyspark.sql.connect.types import UnparsedDataType
from pyspark.sql.connect.utils import get_python_ver
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version, require_minimum_pandas_version
from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult  # noqa: F401
from pyspark.sql.udtf import UDTFRegistration as PySparkUDTFRegistration, _validate_udtf_handler
from pyspark.sql.types import DataType, StructType
from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkAttributeError


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.session import SparkSession


def _create_udtf(
    cls: Type,
    returnType: Optional[Union[StructType, str]],
    name: Optional[str] = None,
    evalType: int = PythonEvalType.SQL_TABLE_UDF,
    deterministic: bool = False,
) -> "UserDefinedTableFunction":
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
    if useArrow is not None:
        arrow_enabled = useArrow
    else:
        from pyspark.sql.connect.session import SparkSession

        arrow_enabled = False
        try:
            session = SparkSession.active()
            arrow_enabled = (
                str(session.conf.get("spark.sql.execution.pythonUDTF.arrow.enabled")).lower()
                == "true"
            )
        except PySparkRuntimeError as e:
            if e.getCondition() == "NO_ACTIVE_OR_DEFAULT_SESSION":
                pass  # Just uses the default if no session found.
            else:
                raise e

    eval_type: int = PythonEvalType.SQL_TABLE_UDF

    if arrow_enabled:
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

    return _create_udtf(cls, returnType, name, eval_type, deterministic)


def _create_pyarrow_udtf(
    cls: Type,
    returnType: Optional[Union[StructType, str]],
    name: Optional[str] = None,
    deterministic: bool = False,
) -> "UserDefinedTableFunction":
    """Create a PyArrow-native Python UDTF."""
    # Validate PyArrow dependencies
    require_minimum_pyarrow_version()

    # Validate the handler class with PyArrow-specific checks
    _validate_arrow_udtf_handler(cls, returnType)

    return _create_udtf(
        cls=cls,
        returnType=returnType,
        name=name,
        evalType=PythonEvalType.SQL_ARROW_UDTF,
        deterministic=deterministic,
    )


def _validate_arrow_udtf_handler(cls: Any, returnType: Optional[Union[StructType, str]]) -> None:
    """Validate the handler class of a PyArrow UDTF."""
    # First run standard UDTF validation
    _validate_udtf_handler(cls, returnType)

    # Block analyze method usage in arrow UDTFs
    # TODO(SPARK-53286): Support analyze method for Arrow UDTFs to enable dynamic return types
    has_analyze = hasattr(cls, "analyze")
    if has_analyze:
        raise PySparkAttributeError(
            errorClass="INVALID_ARROW_UDTF_WITH_ANALYZE",
            messageParameters={"name": cls.__name__},
        )


class UserDefinedTableFunction:
    """
    User defined function in Python

    Notes
    -----
    The constructor of this class is not supposed to be directly called.
    Use :meth:`pyspark.sql.functions.udtf` to create this instance.
    """

    def __init__(
        self,
        func: Type,
        returnType: Optional[Union[StructType, str]],
        name: Optional[str] = None,
        evalType: int = PythonEvalType.SQL_TABLE_UDF,
        deterministic: bool = False,
    ) -> None:
        _validate_udtf_handler(func, returnType)

        self.func = func
        self.returnType: Optional[DataType] = (
            None
            if returnType is None
            else UnparsedDataType(returnType)
            if isinstance(returnType, str)
            else returnType
        )
        self._name = name or func.__name__
        self.evalType = evalType
        self.deterministic = deterministic

    def _build_common_inline_user_defined_table_function(
        self, *args: "ColumnOrName", **kwargs: "ColumnOrName"
    ) -> CommonInlineUserDefinedTableFunction:
        def to_expr(col: "ColumnOrName") -> Expression:
            if isinstance(col, Column):
                return col._expr
            elif isinstance(col, TableArg):
                return col._subquery_expr
            else:
                return ColumnReference(col)  # type: ignore[arg-type]

        arg_exprs: List[Expression] = [to_expr(arg) for arg in args] + [
            NamedArgumentExpression(key, to_expr(value)) for key, value in kwargs.items()
        ]

        udtf = PythonUDTF(
            func=self.func,
            return_type=self.returnType,
            eval_type=self.evalType,
            python_ver=get_python_ver(),
        )
        return CommonInlineUserDefinedTableFunction(
            function_name=self._name,
            function=udtf,
            deterministic=self.deterministic,
            arguments=arg_exprs,
        )

    def __call__(self, *args: "ColumnOrName", **kwargs: "ColumnOrName") -> "DataFrame":
        from pyspark.sql.connect.session import SparkSession
        from pyspark.sql.connect.dataframe import DataFrame

        session = SparkSession.active()

        plan = self._build_common_inline_user_defined_table_function(*args, **kwargs)
        return DataFrame(plan, session)

    def asDeterministic(self) -> "UserDefinedTableFunction":
        self.deterministic = True
        return self


class UDTFRegistration:
    """
    Wrapper for user-defined table function registration.

    .. versionadded:: 3.5.0
    """

    def __init__(self, sparkSession: "SparkSession"):
        self.sparkSession = sparkSession

    def register(
        self,
        name: str,
        f: "UserDefinedTableFunction",
    ) -> "UserDefinedTableFunction":
        if not isinstance(f, UserDefinedTableFunction):
            raise PySparkTypeError(
                errorClass="CANNOT_REGISTER_UDTF",
                messageParameters={
                    "name": name,
                },
            )

        if f.evalType not in [
            PythonEvalType.SQL_TABLE_UDF,
            PythonEvalType.SQL_ARROW_TABLE_UDF,
            PythonEvalType.SQL_ARROW_UDTF,
        ]:
            raise PySparkTypeError(
                errorClass="INVALID_UDTF_EVAL_TYPE",
                messageParameters={
                    "name": name,
                    "eval_type": "SQL_TABLE_UDF, SQL_ARROW_TABLE_UDF, SQL_ARROW_UDTF",
                },
            )

        self.sparkSession._client.register_udtf(
            f.func, f.returnType, name, f.evalType, f.deterministic
        )
        return f

    register.__doc__ = PySparkUDTFRegistration.register.__doc__
