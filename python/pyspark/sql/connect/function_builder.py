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
from typing import TYPE_CHECKING

import pyspark.sql.types
from pyspark.sql.connect.column import (
    ColumnOrString,
    ColumnRef,
    Expression,
    ExpressionOrString,
    ScalarFunctionExpression,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.client import RemoteSparkSession


def _build(name: str, *args: ExpressionOrString) -> ScalarFunctionExpression:
    """
    Simple wrapper function that converts the arguments into the appropriate types.
    Parameters
    ----------
    name Name of the function to be called.
    args The list of arguments.

    Returns
    -------
    :class:`ScalarFunctionExpression`
    """
    cols = [x if isinstance(x, Expression) else ColumnRef.from_qualified_name(x) for x in args]
    return ScalarFunctionExpression(name, *cols)


class FunctionBuilder:
    """This class is used to build arbitrary functions used in expressions"""

    def __getattr__(self, name):
        def _(*args: ExpressionOrString) -> ScalarFunctionExpression:
            return _build(name, *args)

        _.__doc__ = f"""Function to apply {name}"""
        return _


functions = FunctionBuilder()


class UserDefinedFunction(Expression):
    """A user defied function is an expresison that has a reference to the actual
    Python callable attached. During plan generation, the client sends a command to
    the server to register the UDF before execution. The expression object can be
    reused and is not attached to a specific execution. If the internal name of
    the temporary function is set, it is assumed that the registration has already
    happened."""

    def __init__(self, func, return_type=pyspark.sql.types.StringType(), args=None):
        super().__init__()

        self._func_ref = func
        self._return_type = return_type
        self._args = list(args)
        self._func_name = None

    def to_plan(self, session: "RemoteSparkSession") -> Expression:
        # Needs to materialize the UDF to the server
        # Only do this once per session
        func_name = session.register_udf(self._func_ref, self._return_type)
        # Func name is used for the actual reference
        return _build(func_name, *self._args).to_plan(session)

    def __str__(self):
        return f"UserDefinedFunction({self._func_name})"


def _create_udf(function, return_type):
    def wrapper(*cols: "ColumnOrString"):
        return UserDefinedFunction(func=function, return_type=return_type, args=cols)

    return wrapper


def udf(function, return_type=pyspark.sql.types.StringType()):
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
        return_type = function or return_type
        # Overload with
        if return_type is None:
            return_type = pyspark.sql.types.StringType()
        return functools.partial(_create_udf, return_type=return_type)
    else:
        return _create_udf(function, return_type)
