#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import overload
from typing import Union, Callable

from pyspark.sql._typing import (
    AtomicDataTypeOrString,
    UserDefinedFunctionLike,
)
from pyspark.sql.pandas._typing import (
    GroupedMapPandasUserDefinedFunction,
    PandasGroupedAggFunction,
    PandasGroupedAggUDFType,
    PandasGroupedMapFunction,
    PandasGroupedMapUDFType,
    PandasScalarIterFunction,
    PandasScalarIterUDFType,
    PandasScalarToScalarFunction,
    PandasScalarToStructFunction,
    PandasScalarUDFType,
)

from pyspark import since as since  # noqa: F401
from pyspark.rdd import PythonEvalType as PythonEvalType  # noqa: F401
from pyspark.sql.types import ArrayType, StructType

class PandasUDFType:
    SCALAR: PandasScalarUDFType
    SCALAR_ITER: PandasScalarIterUDFType
    GROUPED_MAP: PandasGroupedMapUDFType
    GROUPED_AGG: PandasGroupedAggUDFType

@overload
def pandas_udf(
    f: PandasScalarToScalarFunction,
    returnType: Union[AtomicDataTypeOrString, ArrayType],
    functionType: PandasScalarUDFType,
) -> UserDefinedFunctionLike: ...
@overload
def pandas_udf(f: Union[AtomicDataTypeOrString, ArrayType], returnType: PandasScalarUDFType) -> Callable[[PandasScalarToScalarFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(f: Union[AtomicDataTypeOrString, ArrayType], *, functionType: PandasScalarUDFType) -> Callable[[PandasScalarToScalarFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(*, returnType: Union[AtomicDataTypeOrString, ArrayType], functionType: PandasScalarUDFType) -> Callable[[PandasScalarToScalarFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(
    f: PandasScalarToStructFunction,
    returnType: Union[StructType, str],
    functionType: PandasScalarUDFType,
) -> UserDefinedFunctionLike: ...
@overload
def pandas_udf(f: Union[StructType, str], returnType: PandasScalarUDFType) -> Callable[[PandasScalarToStructFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(f: Union[StructType, str], *, functionType: PandasScalarUDFType) -> Callable[[PandasScalarToStructFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(*, returnType: Union[StructType, str], functionType: PandasScalarUDFType) -> Callable[[PandasScalarToStructFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(
    f: PandasScalarIterFunction,
    returnType: Union[AtomicDataTypeOrString, ArrayType],
    functionType: PandasScalarIterUDFType,
) -> UserDefinedFunctionLike: ...
@overload
def pandas_udf(
    f: Union[AtomicDataTypeOrString, ArrayType], returnType: PandasScalarIterUDFType
) -> Callable[[PandasScalarIterFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    *, returnType: Union[AtomicDataTypeOrString, ArrayType], functionType: PandasScalarIterUDFType
) -> Callable[[PandasScalarIterFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    f: Union[AtomicDataTypeOrString, ArrayType], *, functionType: PandasScalarIterUDFType
) -> Callable[[PandasScalarIterFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    f: PandasGroupedMapFunction,
    returnType: Union[StructType, str],
    functionType: PandasGroupedMapUDFType,
) -> GroupedMapPandasUserDefinedFunction: ...
@overload
def pandas_udf(
    f: Union[StructType, str], returnType: PandasGroupedMapUDFType
) -> Callable[[PandasGroupedMapFunction], GroupedMapPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    *, returnType: Union[StructType, str], functionType: PandasGroupedMapUDFType
) -> Callable[[PandasGroupedMapFunction], GroupedMapPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    f: Union[StructType, str], *, functionType: PandasGroupedMapUDFType
) -> Callable[[PandasGroupedMapFunction], GroupedMapPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    f: PandasGroupedAggFunction,
    returnType: Union[AtomicDataTypeOrString, ArrayType],
    functionType: PandasGroupedAggUDFType,
) -> UserDefinedFunctionLike: ...
@overload
def pandas_udf(
    f: Union[AtomicDataTypeOrString, ArrayType], returnType: PandasGroupedAggUDFType
) -> Callable[[PandasGroupedAggFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    *, returnType: Union[AtomicDataTypeOrString, ArrayType], functionType: PandasGroupedAggUDFType
) -> Callable[[PandasGroupedAggFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    f: Union[AtomicDataTypeOrString, ArrayType], *, functionType: PandasGroupedAggUDFType
) -> Callable[[PandasGroupedAggFunction], UserDefinedFunctionLike]: ...
