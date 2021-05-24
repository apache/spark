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
from typing import Any, Iterable, List, Optional, Tuple, Type, TypeVar, Union
from types import TracebackType

from py4j.java_gateway import JavaObject  # type: ignore[import]

from pyspark.sql._typing import DateTimeLiteral, LiteralType, DecimalLiteral, RowLike
from pyspark.sql.pandas._typing import DataFrameLike
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.sql.catalog import Catalog
from pyspark.sql.conf import RuntimeConfig
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.pandas.conversion import SparkConversionMixin
from pyspark.sql.types import AtomicType, StructType
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.streaming import DataStreamReader, StreamingQueryManager
from pyspark.sql.udf import UDFRegistration

T = TypeVar("T")

class SparkSession(SparkConversionMixin):
    class Builder:
        @overload
        def config(self, *, conf: SparkConf) -> SparkSession.Builder: ...
        @overload
        def config(self, key: str, value: Any) -> SparkSession.Builder: ...
        def master(self, master: str) -> SparkSession.Builder: ...
        def appName(self, name: str) -> SparkSession.Builder: ...
        def enableHiveSupport(self) -> SparkSession.Builder: ...
        def getOrCreate(self) -> SparkSession: ...
    builder: SparkSession.Builder
    def __init__(
        self, sparkContext: SparkContext, jsparkSession: Optional[JavaObject] = ...
    ) -> None: ...
    def newSession(self) -> SparkSession: ...
    @classmethod
    def getActiveSession(cls) -> SparkSession: ...
    @property
    def sparkContext(self) -> SparkContext: ...
    @property
    def version(self) -> str: ...
    @property
    def conf(self) -> RuntimeConfig: ...
    @property
    def catalog(self) -> Catalog: ...
    @property
    def udf(self) -> UDFRegistration: ...
    def range(
        self,
        start: int,
        end: Optional[int] = ...,
        step: int = ...,
        numPartitions: Optional[int] = ...,
    ) -> DataFrame: ...
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
    def sql(self, sqlQuery: str) -> DataFrame: ...
    def table(self, tableName: str) -> DataFrame: ...
    @property
    def read(self) -> DataFrameReader: ...
    @property
    def readStream(self) -> DataStreamReader: ...
    @property
    def streams(self) -> StreamingQueryManager: ...
    def stop(self) -> None: ...
    def __enter__(self) -> SparkSession: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...
