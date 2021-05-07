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
from typing import Any, Callable, Dict, List, Optional, Union

from pyspark.sql._typing import SupportsProcess, OptionalPrimitiveType
from pyspark.sql.context import SQLContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import OptionUtils
from pyspark.sql.types import Row, StructType
from pyspark.sql.utils import StreamingQueryException

from py4j.java_gateway import JavaObject  # type: ignore[import]

class StreamingQuery:
    def __init__(self, jsq: JavaObject) -> None: ...
    @property
    def id(self) -> str: ...
    @property
    def runId(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def isActive(self) -> bool: ...
    def awaitTermination(self, timeout: Optional[int] = ...) -> Optional[bool]: ...
    @property
    def status(self) -> Dict[str, Any]: ...
    @property
    def recentProgress(self) -> List[Dict[str, Any]]: ...
    @property
    def lastProgress(self) -> Optional[Dict[str, Any]]: ...
    def processAllAvailable(self) -> None: ...
    def stop(self) -> None: ...
    def explain(self, extended: bool = ...) -> None: ...
    def exception(self) -> Optional[StreamingQueryException]: ...

class StreamingQueryManager:
    def __init__(self, jsqm: JavaObject) -> None: ...
    @property
    def active(self) -> List[StreamingQuery]: ...
    def get(self, id: str) -> StreamingQuery: ...
    def awaitAnyTermination(self, timeout: Optional[int] = ...) -> bool: ...
    def resetTerminated(self) -> None: ...

class DataStreamReader(OptionUtils):
    def __init__(self, spark: SQLContext) -> None: ...
    def format(self, source: str) -> DataStreamReader: ...
    def schema(self, schema: Union[StructType, str]) -> DataStreamReader: ...
    def option(self, key: str, value: OptionalPrimitiveType) -> DataStreamReader: ...
    def options(self, **options: OptionalPrimitiveType) -> DataStreamReader: ...
    def load(
        self,
        path: Optional[str] = ...,
        format: Optional[str] = ...,
        schema: Optional[Union[StructType, str]] = ...,
        **options: OptionalPrimitiveType
    ) -> DataFrame: ...
    def json(
        self,
        path: str,
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
        locale: Optional[str] = ...,
        dropFieldIfAllNull: Optional[Union[bool, str]] = ...,
        encoding: Optional[str] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
        allowNonNumericNumbers: Optional[Union[bool, str]] = ...,
    ) -> DataFrame: ...
    def orc(
        self,
        path: str,
        mergeSchema: Optional[bool] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
    ) -> DataFrame: ...
    def parquet(
        self,
        path: str,
        mergeSchema: Optional[bool] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
    ) -> DataFrame: ...
    def text(
        self,
        path: str,
        wholetext: bool = ...,
        lineSep: Optional[str] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
    ) -> DataFrame: ...
    def csv(
        self,
        path: str,
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
        mode: Optional[str] = ...,
        columnNameOfCorruptRecord: Optional[str] = ...,
        multiLine: Optional[Union[bool, str]] = ...,
        charToEscapeQuoteEscaping: Optional[Union[bool, str]] = ...,
        enforceSchema: Optional[Union[bool, str]] = ...,
        emptyValue: Optional[str] = ...,
        locale: Optional[str] = ...,
        lineSep: Optional[str] = ...,
        pathGlobFilter: Optional[Union[bool, str]] = ...,
        recursiveFileLookup: Optional[Union[bool, str]] = ...,
        unescapedQuoteHandling: Optional[str] = ...,
    ) -> DataFrame: ...
    def table(self, tableName: str) -> DataFrame: ...

class DataStreamWriter:
    def __init__(self, df: DataFrame) -> None: ...
    def outputMode(self, outputMode: str) -> DataStreamWriter: ...
    def format(self, source: str) -> DataStreamWriter: ...
    def option(self, key: str, value: OptionalPrimitiveType) -> DataStreamWriter: ...
    def options(self, **options: OptionalPrimitiveType) -> DataStreamWriter: ...
    @overload
    def partitionBy(self, *cols: str) -> DataStreamWriter: ...
    @overload
    def partitionBy(self, __cols: List[str]) -> DataStreamWriter: ...
    def queryName(self, queryName: str) -> DataStreamWriter: ...
    @overload
    def trigger(self, processingTime: str) -> DataStreamWriter: ...
    @overload
    def trigger(self, once: bool) -> DataStreamWriter: ...
    @overload
    def trigger(self, continuous: bool) -> DataStreamWriter: ...
    def start(
        self,
        path: Optional[str] = ...,
        format: Optional[str] = ...,
        outputMode: Optional[str] = ...,
        partitionBy: Optional[Union[str, List[str]]] = ...,
        queryName: Optional[str] = ...,
        **options: OptionalPrimitiveType
    ) -> StreamingQuery: ...
    @overload
    def foreach(self, f: Callable[[Row], None]) -> DataStreamWriter: ...
    @overload
    def foreach(self, f: SupportsProcess) -> DataStreamWriter: ...
    def foreachBatch(
        self, func: Callable[[DataFrame, int], None]
    ) -> DataStreamWriter: ...
    def toTable(
        self,
        tableName: str,
        format: Optional[str] = ...,
        outputMode: Optional[str] = ...,
        partitionBy: Optional[Union[str, List[str]]] = ...,
        queryName: Optional[str] = ...,
        **options: OptionalPrimitiveType
    ) -> StreamingQuery: ...
