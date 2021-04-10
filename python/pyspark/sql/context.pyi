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
from pyspark.sql._typing import UserDefinedFunctionLike

from typing import overload
from typing import Any, Callable, Iterable, List, Optional, Tuple, TypeVar, Union

from py4j.java_gateway import JavaObject  # type: ignore[import]

from pyspark.sql._typing import (
    DateTimeLiteral,
    LiteralType,
    DecimalLiteral,
    RowLike,
)
from pyspark.sql.pandas._typing import DataFrameLike
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import AtomicType, DataType, StructType
from pyspark.sql.udf import UDFRegistration as UDFRegistration
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.streaming import DataStreamReader, StreamingQueryManager

T = TypeVar("T")

class SQLContext:
    sparkSession: SparkSession
    def __init__(
        self,
        sparkContext: SparkContext,
        sparkSession: Optional[SparkSession] = ...,
        jsqlContext: Optional[JavaObject] = ...,
    ) -> None: ...
    @classmethod
    def getOrCreate(cls: type, sc: SparkContext) -> SQLContext: ...
    def newSession(self) -> SQLContext: ...
    def setConf(self, key: str, value: Union[bool, int, str]) -> None: ...
    def getConf(self, key: str, defaultValue: Optional[str] = ...) -> str: ...
    @property
    def udf(self) -> UDFRegistration: ...
    def range(
        self,
        start: int,
        end: Optional[int] = ...,
        step: int = ...,
        numPartitions: Optional[int] = ...,
    ) -> DataFrame: ...
    def registerFunction(
        self, name: str, f: Callable[..., Any], returnType: DataType = ...
    ) -> UserDefinedFunctionLike: ...
    def registerJavaFunction(
        self, name: str, javaClassName: str, returnType: Optional[DataType] = ...
    ) -> None: ...
    @overload
    def createDataFrame(
        self,
        data: Union[RDD[RowLike], Iterable[RowLike]],
        samplingRatio: Optional[float] = ...,
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: Union[RDD[RowLike], Iterable[RowLike]],
        schema: Union[List[str], Tuple[str, ...]] = ...,
        verifySchema: bool = ...,
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: Union[
            RDD[Union[DateTimeLiteral, LiteralType, DecimalLiteral]],
            Iterable[Union[DateTimeLiteral, LiteralType, DecimalLiteral]],
        ],
        schema: Union[AtomicType, str],
        verifySchema: bool = ...,
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: Union[RDD[RowLike], Iterable[RowLike]],
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self, data: DataFrameLike, samplingRatio: Optional[float] = ...
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: DataFrameLike,
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> DataFrame: ...
    def registerDataFrameAsTable(self, df: DataFrame, tableName: str) -> None: ...
    def dropTempTable(self, tableName: str) -> None: ...
    def createExternalTable(
        self,
        tableName: str,
        path: Optional[str] = ...,
        source: Optional[str] = ...,
        schema: Optional[StructType] = ...,
        **options: str
    ) -> DataFrame: ...
    def sql(self, sqlQuery: str) -> DataFrame: ...
    def table(self, tableName: str) -> DataFrame: ...
    def tables(self, dbName: Optional[str] = ...) -> DataFrame: ...
    def tableNames(self, dbName: Optional[str] = ...) -> List[str]: ...
    def cacheTable(self, tableName: str) -> None: ...
    def uncacheTable(self, tableName: str) -> None: ...
    def clearCache(self) -> None: ...
    @property
    def read(self) -> DataFrameReader: ...
    @property
    def readStream(self) -> DataStreamReader: ...
    @property
    def streams(self) -> StreamingQueryManager: ...

class HiveContext(SQLContext):
    def __init__(
        self, sparkContext: SparkContext, jhiveContext: Optional[JavaObject] = ...
    ) -> None: ...
    def refreshTable(self, tableName: str) -> None: ...
