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

from typing import Any, Callable, Optional

from pyspark.sql._typing import ColumnOrName, DataTypeOrString, UserDefinedFunctionLike
from pyspark.sql.column import Column
from pyspark.sql.types import DataType
import pyspark.sql.session

class UserDefinedFunction:
    func: Callable[..., Any]
    evalType: int
    deterministic: bool
    def __init__(
        self,
        func: Callable[..., Any],
        returnType: DataTypeOrString = ...,
        name: Optional[str] = ...,
        evalType: int = ...,
        deterministic: bool = ...,
    ) -> None: ...
    @property
    def returnType(self) -> DataType: ...
    def __call__(self, *cols: ColumnOrName) -> Column: ...
    def asNondeterministic(self) -> UserDefinedFunction: ...

class UDFRegistration:
    sparkSession: pyspark.sql.session.SparkSession
    def __init__(self, sparkSession: pyspark.sql.session.SparkSession) -> None: ...
    def register(
        self,
        name: str,
        f: Callable[..., Any],
        returnType: Optional[DataTypeOrString] = ...,
    ) -> UserDefinedFunctionLike: ...
    def registerJavaFunction(
        self,
        name: str,
        javaClassName: str,
        returnType: Optional[DataTypeOrString] = ...,
    ) -> None: ...
    def registerJavaUDAF(self, name: str, javaClassName: str) -> None: ...
