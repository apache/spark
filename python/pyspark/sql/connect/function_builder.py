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
from typing import TYPE_CHECKING, Optional, Any, Iterable, Union

import pyspark.sql.types

import pyspark.sql.connect.proto as proto
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import Expression, UnresolvedFunction
from pyspark.sql.connect.functions import col


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import (
        ColumnOrName,
        FunctionBuilderCallable,
        UserDefinedFunctionCallable,
    )
    from pyspark.sql.connect.client import SparkConnectClient


def _build(name: str, *args: "ColumnOrName") -> Column:
    """
    Simple wrapper function that converts the arguments into the appropriate types.
    Parameters
    ----------
    name Name of the function to be called.
    args The list of arguments.

    Returns
    -------
    :class:`UnresolvedFunction`
    """
    cols = [arg if isinstance(arg, Column) else col(arg) for arg in args]
    return Column(UnresolvedFunction(name, [col._expr for col in cols]))


class FunctionBuilder:
    """This class is used to build arbitrary functions used in expressions"""

    def __getattr__(self, name: str) -> "FunctionBuilderCallable":
        def _(*args: "ColumnOrName") -> Column:
            return _build(name, *args)

        _.__doc__ = f"""Function to apply {name}"""
        return _


functions = FunctionBuilder()


class UserDefinedFunction(Expression):
    """A user defied function is an expression that has a reference to the actual
    Python callable attached. During plan generation, the client sends a command to
    the server to register the UDF before execution. The expression object can be
    reused and is not attached to a specific execution. If the internal name of
    the temporary function is set, it is assumed that the registration has already
    happened."""

    def __init__(
        self,
        func: Any,
        return_type: Union[str, pyspark.sql.types.DataType] = pyspark.sql.types.StringType(),
        args: Optional[Iterable[Any]] = None,
    ) -> None:
        super().__init__()

        self._func_ref = func
        self._return_type = return_type
        if args is not None:
            self._args = list(args)
        else:
            self._args = []
        self._func_name = None

    def to_plan(self, session: "SparkConnectClient") -> proto.Expression:
        if session is None:
            raise Exception("CAnnot create UDF without remote Session.")
        # Needs to materialize the UDF to the server
        # Only do this once per session
        func_name = session.register_udf(self._func_ref, self._return_type)
        # Func name is used for the actual reference
        return _build(func_name, *self._args).to_plan(session)

    def __str__(self) -> str:
        return f"UserDefinedFunction({self._func_name})"


def _create_udf(
    function: Any, return_type: Union[str, pyspark.sql.types.DataType]
) -> "UserDefinedFunctionCallable":
    def wrapper(*cols: "ColumnOrName") -> "Column":
        return Column(UserDefinedFunction(func=function, return_type=return_type, args=cols))

    return wrapper


def udf(
    function: Any, return_type: pyspark.sql.types.DataType = pyspark.sql.types.StringType()
) -> Any:
    """
    Returns a callable that represents the column once arguments are applied

    Parameters
    ----------
    function
    return_type

    Returns
    -------

    """
    # This is when @udf / @udf(DataType()) is used
    if function is None or isinstance(function, (str, pyspark.sql.types.DataType)):
        actual_return_type = function or return_type
        # Overload with
        if actual_return_type is None:
            actual_return_type = pyspark.sql.types.StringType()
        return functools.partial(_create_udf, return_type=actual_return_type)
    else:
        return _create_udf(function, return_type)
