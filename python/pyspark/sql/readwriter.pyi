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
from typing import Dict, List, Optional, Tuple, Union

from pyspark.sql._typing import OptionalPrimitiveType
from pyspark.sql.dataframe import DataFrame
from pyspark.rdd import RDD
from pyspark.sql.column import Column
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType

PathOrPaths = Union[str, List[str]]
TupleOrListOfString = Union[List[str], Tuple[str, ...]]

class OptionUtils: ...

class DataFrameReader(OptionUtils):
    def __init__(self, spark: SQLContext) -> None: ...
    def format(self, source: str) -> DataFrameReader: ...
    def schema(self, schema: Union[StructType, str]) -> DataFrameReader: ...
    def option(self, key: str, value: OptionalPrimitiveType) -> DataFrameReader: ...
    def options(self, **options: OptionalPrimitiveType) -> DataFrameReader: ...
    def load(
        self,
        path: Optional[PathOrPaths] = ...,
        format: Optional[str] = ...,
        schema: Optional[Union[StructType, str]] = ...,
        **options: OptionalPrimitiveType
    ) -> DataFrame: ...
    def json(
        self,
        path: Union[str, List[str], RDD[str]],
        schema: Optional[Union[StructType, str]] = ...,
        primitivesAsString: Optional[Union[bool, str]] = ...,
        prefersDecimal: Optional[Union[bool, str]] = ...,
        allowComments: Optional[Union[bool, str]] = ...,
        allowUnquotedFieldNames: Optional[Union[bool, str]] = ...,
        allowSingleQuotes: Optional[Union[bool, str]] = ...,
        allowNumericLeadingZero: Optional[Union[bool, str]] = ...,
        allowBackslashEscapingAnyCharacter: Optional[Union[bool, str]] = ...,
        mode: Optional[str] = ...,
        columnNameOfCorruptRecord: Optional[str] = ...,
        dateFormat: Optional[str] = ...,
        timestampFormat: Optional[str] = ...,
        multiLine: Optional[Union[bool, str]] = ...,
        allowUnquotedControlChars: Optional[Union[bool, str]] = ...,
        lineSep: Optional[str] = ...,
        samplingRatio: Optional[Union[float, str]] = ...,
        dropFieldIfAllNull: Optional[Union[bool, str]] = ...,
        encoding: Optional[str] = ...,
        locale: Optional[str] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
        allowNonNumericNumbers: Optional[Union[bool, str]] = ...,
    ) -> DataFrame: ...
    def table(self, tableName: str) -> DataFrame: ...
    def parquet(self, *paths: str, **options: OptionalPrimitiveType) -> DataFrame: ...
    def text(
        self,
        paths: PathOrPaths,
        wholetext: bool = ...,
        lineSep: Optional[str] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
    ) -> DataFrame: ...
    def csv(
        self,
        path: PathOrPaths,
        schema: Optional[Union[StructType, str]] = ...,
        sep: Optional[str] = ...,
        encoding: Optional[str] = ...,
        quote: Optional[str] = ...,
        escape: Optional[str] = ...,
        comment: Optional[str] = ...,
        header: Optional[Union[bool, str]] = ...,
        inferSchema: Optional[Union[bool, str]] = ...,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = ...,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = ...,
        nullValue: Optional[str] = ...,
        nanValue: Optional[str] = ...,
        positiveInf: Optional[str] = ...,
        negativeInf: Optional[str] = ...,
        dateFormat: Optional[str] = ...,
        timestampFormat: Optional[str] = ...,
        maxColumns: Optional[Union[int, str]] = ...,
        maxCharsPerColumn: Optional[Union[int, str]] = ...,
        maxMalformedLogPerPartition: Optional[Union[int, str]] = ...,
        mode: Optional[str] = ...,
        columnNameOfCorruptRecord: Optional[str] = ...,
        multiLine: Optional[Union[bool, str]] = ...,
        charToEscapeQuoteEscaping: Optional[str] = ...,
        samplingRatio: Optional[Union[float, str]] = ...,
        enforceSchema: Optional[Union[bool, str]] = ...,
        emptyValue: Optional[str] = ...,
        locale: Optional[str] = ...,
        lineSep: Optional[str] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
        unescapedQuoteHandling: Optional[str] = ...,
    ) -> DataFrame: ...
    def orc(
        self,
        path: PathOrPaths,
        mergeSchema: Optional[bool] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
    ) -> DataFrame: ...
    @overload
    def jdbc(
        self, url: str, table: str, *, properties: Optional[Dict[str, str]] = ...
    ) -> DataFrame: ...
    @overload
    def jdbc(
        self,
        url: str,
        table: str,
        column: str,
        lowerBound: Union[int, str],
        upperBound: Union[int, str],
        numPartitions: int,
        *,
        properties: Optional[Dict[str, str]] = ...
    ) -> DataFrame: ...
    @overload
    def jdbc(
        self,
        url: str,
        table: str,
        *,
        predicates: List[str],
        properties: Optional[Dict[str, str]] = ...
    ) -> DataFrame: ...

class DataFrameWriter(OptionUtils):
    def __init__(self, df: DataFrame) -> None: ...
    def mode(self, saveMode: str) -> DataFrameWriter: ...
    def format(self, source: str) -> DataFrameWriter: ...
    def option(self, key: str, value: OptionalPrimitiveType) -> DataFrameWriter: ...
    def options(self, **options: OptionalPrimitiveType) -> DataFrameWriter: ...
    @overload
    def partitionBy(self, *cols: str) -> DataFrameWriter: ...
    @overload
    def partitionBy(self, __cols: List[str]) -> DataFrameWriter: ...
    @overload
    def bucketBy(self, numBuckets: int, col: str, *cols: str) -> DataFrameWriter: ...
    @overload
    def bucketBy(
        self, numBuckets: int, col: TupleOrListOfString
    ) -> DataFrameWriter: ...
    @overload
    def sortBy(self, col: str, *cols: str) -> DataFrameWriter: ...
    @overload
    def sortBy(self, col: TupleOrListOfString) -> DataFrameWriter: ...
    def save(
        self,
        path: Optional[str] = ...,
        format: Optional[str] = ...,
        mode: Optional[str] = ...,
        partitionBy: Optional[Union[str, List[str]]] = ...,
        **options: OptionalPrimitiveType
    ) -> None: ...
    def insertInto(self, tableName: str, overwrite: Optional[bool] = ...) -> None: ...
    def saveAsTable(
        self,
        name: str,
        format: Optional[str] = ...,
        mode: Optional[str] = ...,
        partitionBy: Optional[Union[str, List[str]]] = ...,
        **options: OptionalPrimitiveType
    ) -> None: ...
    def json(
        self,
        path: str,
        mode: Optional[str] = ...,
        compression: Optional[str] = ...,
        dateFormat: Optional[str] = ...,
        timestampFormat: Optional[str] = ...,
        lineSep: Optional[str] = ...,
        encoding: Optional[str] = ...,
        ignoreNullFields: Optional[Union[bool, str]] = ...,
    ) -> None: ...
    def parquet(
        self,
        path: str,
        mode: Optional[str] = ...,
        partitionBy: Optional[Union[str, List[str]]] = ...,
        compression: Optional[str] = ...,
    ) -> None: ...
    def text(
        self, path: str, compression: Optional[str] = ..., lineSep: Optional[str] = ...
    ) -> None: ...
    def csv(
        self,
        path: str,
        mode: Optional[str] = ...,
        compression: Optional[str] = ...,
        sep: Optional[str] = ...,
        quote: Optional[str] = ...,
        escape: Optional[str] = ...,
        header: Optional[Union[bool, str]] = ...,
        nullValue: Optional[str] = ...,
        escapeQuotes: Optional[Union[bool, str]] = ...,
        quoteAll: Optional[Union[bool, str]] = ...,
        dateFormat: Optional[str] = ...,
        timestampFormat: Optional[str] = ...,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = ...,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = ...,
        charToEscapeQuoteEscaping: Optional[str] = ...,
        encoding: Optional[str] = ...,
        emptyValue: Optional[str] = ...,
        lineSep: Optional[str] = ...,
    ) -> None: ...
    def orc(
        self,
        path: str,
        mode: Optional[str] = ...,
        partitionBy: Optional[Union[str, List[str]]] = ...,
        compression: Optional[str] = ...,
    ) -> None: ...
    def jdbc(
        self,
        url: str,
        table: str,
        mode: Optional[str] = ...,
        properties: Optional[Dict[str, str]] = ...,
    ) -> None: ...

class DataFrameWriterV2:
    def __init__(self, df: DataFrame, table: str) -> None: ...
    def using(self, provider: str) -> DataFrameWriterV2: ...
    def option(self, key: str, value: OptionalPrimitiveType) -> DataFrameWriterV2: ...
    def options(self, **options: OptionalPrimitiveType) -> DataFrameWriterV2: ...
    def tableProperty(self, property: str, value: str) -> DataFrameWriterV2: ...
    def partitionedBy(self, col: Column, *cols: Column) -> DataFrameWriterV2: ...
    def create(self) -> None: ...
    def replace(self) -> None: ...
    def createOrReplace(self) -> None: ...
    def append(self) -> None: ...
    def overwrite(self, condition: Column) -> None: ...
    def overwritePartitions(self) -> None: ...
