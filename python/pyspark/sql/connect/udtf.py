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

import sys
from typing import Type, TYPE_CHECKING, Optional, Union

from pyspark.rdd import PythonEvalType
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import ColumnReference
from pyspark.sql.connect.plan import (
    CommonInlineUserDefinedTableFunction,
    PythonUDTF,
)
from pyspark.sql.connect.types import UnparsedDataType
from pyspark.sql.udtf import UDTFRegistration as PySparkUDTFRegistration
from pyspark.sql.types import DataType, StructType


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.session import SparkSession


def _create_udtf(
    cls: Type,
    returnType: Union[StructType, str],
    name: Optional[str] = None,
    deterministic: bool = True,
) -> "UserDefinedTableFunction":
    udtf_obj = UserDefinedTableFunction(
        cls, returnType=returnType, name=name, deterministic=deterministic
    )
    return udtf_obj


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
        returnType: Union[StructType, str],
        name: Optional[str] = None,
        evalType: int = PythonEvalType.SQL_TABLE_UDF,
        deterministic: bool = True,
    ) -> None:
        # TODO: add checks here

        self.func = func
        self.returnType: DataType = (
            UnparsedDataType(returnType) if isinstance(returnType, str) else returnType
        )
        self._name = name or func.__name__
        self.evalType = evalType
        self.deterministic = deterministic

    def _build_common_inline_user_defined_table_function(
        self, *cols: "ColumnOrName"
    ) -> CommonInlineUserDefinedTableFunction:
        arg_cols = [
            col if isinstance(col, Column) else Column(ColumnReference(col)) for col in cols
        ]
        arg_exprs = [col._expr for col in arg_cols]

        udtf = PythonUDTF(
            func=self.func,
            return_type=self.returnType,  # TODO: handle this type
            eval_type=self.evalType,
            python_ver="%d.%d" % sys.version_info[:2],
        )
        return CommonInlineUserDefinedTableFunction(
            function_name=self._name,
            function=udtf,
            deterministic=self.deterministic,
            arguments=arg_exprs,
        )

    def __call__(self, *cols: "ColumnOrName") -> "DataFrame":
        from pyspark.sql.connect.dataframe import DataFrame
        from pyspark.sql.connect.session import _active_spark_session

        if _active_spark_session is None:
            # TODO: can we create this session?
            raise Exception("No active spark session")

        plan = self._build_common_inline_user_defined_table_function(*cols)
        return DataFrame.withPlan(plan, _active_spark_session)

    def asNondeterministic(self) -> "UserDefinedTableFunction":
        self.deterministic = False
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
        f: UserDefinedTableFunction,
    ) -> "UserDefinedTableFunction":
        # TODO: add eval type check
        self.sparkSession._client.register_udtf(
            f.func, f.returnType, name, f.evalType, f.deterministic
        )
        return f

    register.__doc__ = PySparkUDTFRegistration.register.__doc__
