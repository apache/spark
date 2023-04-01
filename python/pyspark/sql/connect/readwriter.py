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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import Dict
from typing import Optional, Union, List, overload, Tuple, cast
from typing import TYPE_CHECKING

from pyspark.sql.connect.plan import Read, DataSource, LogicalPlan, WriteOperation, WriteOperationV2
from pyspark.sql.types import StructType
from pyspark.sql.utils import to_str
from pyspark.sql.readwriter import (
    DataFrameWriter as PySparkDataFrameWriter,
    DataFrameReader as PySparkDataFrameReader,
    DataFrameWriterV2 as PySparkDataFrameWriterV2,
)
from pyspark.errors import PySparkAttributeError

if TYPE_CHECKING:
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect._typing import ColumnOrName, OptionalPrimitiveType
    from pyspark.sql.connect.session import SparkSession

__all__ = ["DataFrameReader", "DataFrameWriter"]

PathOrPaths = Union[str, List[str]]
TupleOrListOfString = Union[List[str], Tuple[str, ...]]


class OptionUtils:
    def _set_opts(
        self,
        schema: Optional[Union[StructType, str]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """
        Set named options (filter out those the value is None)
        """
        if schema is not None:
            self.schema(schema)  # type: ignore[attr-defined]
        for k, v in options.items():
            if v is not None:
                self.option(k, v)  # type: ignore[attr-defined]


class DataFrameReader(OptionUtils):
    # TODO(SPARK-40539) Achieve parity with PySpark.

    def __init__(self, client: "SparkSession"):
        self._client = client
        self._format: Optional[str] = None
        self._schema = ""
        self._options: Dict[str, str] = {}

    def format(self, source: str) -> "DataFrameReader":
        self._format = source
        return self

    format.__doc__ = PySparkDataFrameReader.format.__doc__

    def schema(self, schema: Union[StructType, str]) -> "DataFrameReader":
        if isinstance(schema, StructType):
            self._schema = schema.json()
        elif isinstance(schema, str):
            self._schema = schema
        else:
            raise TypeError(f"schema must be a StructType or str, but got {schema}")
        return self

    schema.__doc__ = PySparkDataFrameReader.schema.__doc__

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameReader":
        self._options[key] = str(value)
        return self

    option.__doc__ = PySparkDataFrameReader.option.__doc__

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameReader":
        for k in options:
            self.option(k, to_str(options[k]))
        return self

    options.__doc__ = PySparkDataFrameReader.options.__doc__

    def load(
        self,
        path: Optional[PathOrPaths] = None,
        format: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        **options: "OptionalPrimitiveType",
    ) -> "DataFrame":
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)

        paths = path
        if isinstance(path, str):
            paths = [path]

        plan = DataSource(
            format=self._format,
            schema=self._schema,
            options=self._options,
            paths=paths,  # type: ignore[arg-type]
        )
        return self._df(plan)

    load.__doc__ = PySparkDataFrameReader.load.__doc__

    def _df(self, plan: LogicalPlan) -> "DataFrame":
        from pyspark.sql.connect.dataframe import DataFrame

        return DataFrame.withPlan(plan, self._client)

    def table(self, tableName: str) -> "DataFrame":
        return self._df(Read(tableName, self._options))

    table.__doc__ = PySparkDataFrameReader.table.__doc__

    def json(
        self,
        path: PathOrPaths,
        schema: Optional[Union[StructType, str]] = None,
        primitivesAsString: Optional[Union[bool, str]] = None,
        prefersDecimal: Optional[Union[bool, str]] = None,
        allowComments: Optional[Union[bool, str]] = None,
        allowUnquotedFieldNames: Optional[Union[bool, str]] = None,
        allowSingleQuotes: Optional[Union[bool, str]] = None,
        allowNumericLeadingZero: Optional[Union[bool, str]] = None,
        allowBackslashEscapingAnyCharacter: Optional[Union[bool, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        allowUnquotedControlChars: Optional[Union[bool, str]] = None,
        lineSep: Optional[str] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        dropFieldIfAllNull: Optional[Union[bool, str]] = None,
        encoding: Optional[str] = None,
        locale: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
        allowNonNumericNumbers: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        self._set_opts(
            primitivesAsString=primitivesAsString,
            prefersDecimal=prefersDecimal,
            allowComments=allowComments,
            allowUnquotedFieldNames=allowUnquotedFieldNames,
            allowSingleQuotes=allowSingleQuotes,
            allowNumericLeadingZero=allowNumericLeadingZero,
            allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            multiLine=multiLine,
            allowUnquotedControlChars=allowUnquotedControlChars,
            lineSep=lineSep,
            samplingRatio=samplingRatio,
            dropFieldIfAllNull=dropFieldIfAllNull,
            encoding=encoding,
            locale=locale,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            allowNonNumericNumbers=allowNonNumericNumbers,
        )
        if isinstance(path, str):
            path = [path]
        return self.load(path=path, format="json", schema=schema)

    json.__doc__ = PySparkDataFrameReader.json.__doc__

    def parquet(self, *paths: str, **options: "OptionalPrimitiveType") -> "DataFrame":
        mergeSchema = options.get("mergeSchema", None)
        pathGlobFilter = options.get("pathGlobFilter", None)
        modifiedBefore = options.get("modifiedBefore", None)
        modifiedAfter = options.get("modifiedAfter", None)
        recursiveFileLookup = options.get("recursiveFileLookup", None)
        datetimeRebaseMode = options.get("datetimeRebaseMode", None)
        int96RebaseMode = options.get("int96RebaseMode", None)
        self._set_opts(
            mergeSchema=mergeSchema,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            datetimeRebaseMode=datetimeRebaseMode,
            int96RebaseMode=int96RebaseMode,
        )

        return self.load(path=list(paths), format="parquet")

    parquet.__doc__ = PySparkDataFrameReader.parquet.__doc__

    def text(
        self,
        paths: PathOrPaths,
        wholetext: Optional[bool] = None,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        self._set_opts(
            wholetext=wholetext,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
        )

        if isinstance(paths, str):
            paths = [paths]
        return self.load(path=paths, format="text")

    text.__doc__ = PySparkDataFrameReader.text.__doc__

    def csv(
        self,
        path: PathOrPaths,
        schema: Optional[Union[StructType, str]] = None,
        sep: Optional[str] = None,
        encoding: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        comment: Optional[str] = None,
        header: Optional[Union[bool, str]] = None,
        inferSchema: Optional[Union[bool, str]] = None,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        nanValue: Optional[str] = None,
        positiveInf: Optional[str] = None,
        negativeInf: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        maxColumns: Optional[Union[int, str]] = None,
        maxCharsPerColumn: Optional[Union[int, str]] = None,
        maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        charToEscapeQuoteEscaping: Optional[str] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        enforceSchema: Optional[Union[bool, str]] = None,
        emptyValue: Optional[str] = None,
        locale: Optional[str] = None,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
        unescapedQuoteHandling: Optional[str] = None,
    ) -> "DataFrame":
        self._set_opts(
            sep=sep,
            encoding=encoding,
            quote=quote,
            escape=escape,
            comment=comment,
            header=header,
            inferSchema=inferSchema,
            ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
            nullValue=nullValue,
            nanValue=nanValue,
            positiveInf=positiveInf,
            negativeInf=negativeInf,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            multiLine=multiLine,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
            samplingRatio=samplingRatio,
            enforceSchema=enforceSchema,
            emptyValue=emptyValue,
            locale=locale,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            unescapedQuoteHandling=unescapedQuoteHandling,
        )
        if isinstance(path, str):
            path = [path]
        return self.load(path=path, format="csv", schema=schema)

    csv.__doc__ = PySparkDataFrameReader.csv.__doc__

    def orc(
        self,
        path: PathOrPaths,
        mergeSchema: Optional[bool] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        self._set_opts(
            mergeSchema=mergeSchema,
            pathGlobFilter=pathGlobFilter,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            recursiveFileLookup=recursiveFileLookup,
        )
        if isinstance(path, str):
            path = [path]
        return self.load(path=path, format="orc")

    orc.__doc__ = PySparkDataFrameReader.orc.__doc__

    @overload
    def jdbc(
        self, url: str, table: str, *, properties: Optional[Dict[str, str]] = None
    ) -> "DataFrame":
        ...

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
        properties: Optional[Dict[str, str]] = None,
    ) -> "DataFrame":
        ...

    @overload
    def jdbc(
        self,
        url: str,
        table: str,
        *,
        predicates: List[str],
        properties: Optional[Dict[str, str]] = None,
    ) -> "DataFrame":
        ...

    def jdbc(
        self,
        url: str,
        table: str,
        column: Optional[str] = None,
        lowerBound: Optional[Union[int, str]] = None,
        upperBound: Optional[Union[int, str]] = None,
        numPartitions: Optional[int] = None,
        predicates: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> "DataFrame":
        if properties is None:
            properties = dict()

        self.format("jdbc")

        if column is not None:
            assert lowerBound is not None, "lowerBound can not be None when ``column`` is specified"
            assert upperBound is not None, "upperBound can not be None when ``column`` is specified"
            assert (
                numPartitions is not None
            ), "numPartitions can not be None when ``column`` is specified"
            self.options(
                partitionColumn=column,
                lowerBound=lowerBound,
                upperBound=upperBound,
                numPartitions=numPartitions,
            )
            self.options(**properties)
            self.options(url=url, dbtable=table)
            return self.load()
        else:
            self.options(**properties)
            self.options(url=url, dbtable=table)
            if predicates is not None:
                plan = DataSource(
                    format=self._format,
                    schema=self._schema,
                    options=self._options,
                    predicates=predicates,
                )
                return self._df(plan)
            else:
                return self.load()

    jdbc.__doc__ = PySparkDataFrameReader.jdbc.__doc__

    @property
    def _jreader(self) -> None:
        raise PySparkAttributeError(
            error_class="JVM_ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": "_jreader"}
        )


DataFrameReader.__doc__ = PySparkDataFrameReader.__doc__


class DataFrameWriter(OptionUtils):
    def __init__(self, plan: "LogicalPlan", session: "SparkSession"):
        self._df: "LogicalPlan" = plan
        self._spark: "SparkSession" = session
        self._write: "WriteOperation" = WriteOperation(self._df)

    def mode(self, saveMode: Optional[str]) -> "DataFrameWriter":
        # At the JVM side, the default value of mode is already set to "error".
        # So, if the given saveMode is None, we will not call JVM-side's mode method.
        if saveMode is not None:
            self._write.mode = saveMode
        return self

    mode.__doc__ = PySparkDataFrameWriter.mode.__doc__

    def format(self, source: str) -> "DataFrameWriter":
        self._write.source = source
        return self

    format.__doc__ = PySparkDataFrameWriter.format.__doc__

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameWriter":
        self._write.options[key] = to_str(value)
        return self

    option.__doc__ = PySparkDataFrameWriter.option.__doc__

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameWriter":
        for k in options:
            self._write.options[k] = to_str(options[k])
        return self

    options.__doc__ = PySparkDataFrameWriter.options.__doc__

    @overload
    def partitionBy(self, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def partitionBy(self, *cols: List[str]) -> "DataFrameWriter":
        ...

    def partitionBy(self, *cols: Union[str, List[str]]) -> "DataFrameWriter":
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]

        self._write.partitioning_cols = cast(List[str], cols)
        return self

    partitionBy.__doc__ = PySparkDataFrameWriter.partitionBy.__doc__

    @overload
    def bucketBy(self, numBuckets: int, col: str, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def bucketBy(self, numBuckets: int, col: TupleOrListOfString) -> "DataFrameWriter":
        ...

    def bucketBy(
        self, numBuckets: int, col: Union[str, TupleOrListOfString], *cols: Optional[str]
    ) -> "DataFrameWriter":
        if not isinstance(numBuckets, int):
            raise TypeError("numBuckets should be an int, got {0}.".format(type(numBuckets)))

        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError("col is a {0} but cols are not empty".format(type(col)))

            col, cols = col[0], col[1:]  # type: ignore[assignment]

        if not all(isinstance(c, str) for c in cols) or not (isinstance(col, str)):
            raise TypeError("all names should be `str`")

        self._write.num_buckets = numBuckets
        self._write.bucket_cols = cast(List[str], [col, *cols])
        return self

    bucketBy.__doc__ = PySparkDataFrameWriter.bucketBy.__doc__

    @overload
    def sortBy(self, col: str, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def sortBy(self, col: TupleOrListOfString) -> "DataFrameWriter":
        ...

    def sortBy(
        self, col: Union[str, TupleOrListOfString], *cols: Optional[str]
    ) -> "DataFrameWriter":
        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError("col is a {0} but cols are not empty".format(type(col)))

            col, cols = col[0], col[1:]  # type: ignore[assignment]

        if not all(isinstance(c, str) for c in cols) or not (isinstance(col, str)):
            raise TypeError("all names should be `str`")

        self._write.sort_cols = cast(List[str], [col, *cols])
        return self

    sortBy.__doc__ = PySparkDataFrameWriter.sortBy.__doc__

    def save(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        self._write.path = path
        self._spark.client.execute_command(self._write.command(self._spark.client))

    save.__doc__ = PySparkDataFrameWriter.save.__doc__

    def insertInto(self, tableName: str, overwrite: Optional[bool] = None) -> None:
        if overwrite is not None:
            self.mode("overwrite" if overwrite else "append")
        self._write.table_name = tableName
        self._write.table_save_method = "insert_into"
        self._spark.client.execute_command(self._write.command(self._spark.client))

    insertInto.__doc__ = PySparkDataFrameWriter.insertInto.__doc__

    def saveAsTable(
        self,
        name: str,
        format: Optional[str] = None,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        self._write.table_name = name
        self._write.table_save_method = "save_as_table"
        self._spark.client.execute_command(self._write.command(self._spark.client))

    saveAsTable.__doc__ = PySparkDataFrameWriter.saveAsTable.__doc__

    def json(
        self,
        path: str,
        mode: Optional[str] = None,
        compression: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        lineSep: Optional[str] = None,
        encoding: Optional[str] = None,
        ignoreNullFields: Optional[Union[bool, str]] = None,
    ) -> None:
        self.mode(mode)
        self._set_opts(
            compression=compression,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            lineSep=lineSep,
            encoding=encoding,
            ignoreNullFields=ignoreNullFields,
        )
        self.format("json").save(path)

    json.__doc__ = PySparkDataFrameWriter.json.__doc__

    def parquet(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
    ) -> None:
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self.option("compression", compression)
        self.format("parquet").save(path)

    parquet.__doc__ = PySparkDataFrameWriter.parquet.__doc__

    def text(
        self, path: str, compression: Optional[str] = None, lineSep: Optional[str] = None
    ) -> None:
        self._set_opts(compression=compression, lineSep=lineSep)
        self.format("text").save(path)

    text.__doc__ = PySparkDataFrameWriter.text.__doc__

    def csv(
        self,
        path: str,
        mode: Optional[str] = None,
        compression: Optional[str] = None,
        sep: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        header: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        escapeQuotes: Optional[Union[bool, str]] = None,
        quoteAll: Optional[Union[bool, str]] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        charToEscapeQuoteEscaping: Optional[str] = None,
        encoding: Optional[str] = None,
        emptyValue: Optional[str] = None,
        lineSep: Optional[str] = None,
    ) -> None:
        self.mode(mode)
        self._set_opts(
            compression=compression,
            sep=sep,
            quote=quote,
            escape=escape,
            header=header,
            nullValue=nullValue,
            escapeQuotes=escapeQuotes,
            quoteAll=quoteAll,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
            encoding=encoding,
            emptyValue=emptyValue,
            lineSep=lineSep,
        )
        self.format("csv").save(path)

    csv.__doc__ = PySparkDataFrameWriter.csv.__doc__

    def orc(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
    ) -> None:
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self.format("orc").save(path)

    orc.__doc__ = PySparkDataFrameWriter.orc.__doc__

    def jdbc(
        self,
        url: str,
        table: str,
        mode: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> None:
        if properties is None:
            properties = dict()

        self.format("jdbc").mode(mode).options(**properties).options(url=url, dbtable=table).save()

    jdbc.__doc__ = PySparkDataFrameWriter.jdbc.__doc__


class DataFrameWriterV2(OptionUtils):
    def __init__(self, plan: "LogicalPlan", session: "SparkSession", table: str):
        self._df: "LogicalPlan" = plan
        self._spark: "SparkSession" = session
        self._table_name: str = table
        self._write: "WriteOperationV2" = WriteOperationV2(self._df, self._table_name)

    def using(self, provider: str) -> "DataFrameWriterV2":
        self._write.provider = provider
        return self

    using.__doc__ = PySparkDataFrameWriterV2.using.__doc__

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameWriterV2":
        self._write.options[key] = to_str(value)
        return self

    option.__doc__ = PySparkDataFrameWriterV2.option.__doc__

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameWriterV2":
        for k in options:
            self._write.options[k] = to_str(options[k])
        return self

    options.__doc__ = PySparkDataFrameWriterV2.options.__doc__

    def tableProperty(self, property: str, value: str) -> "DataFrameWriterV2":
        self._write.table_properties[property] = value
        return self

    tableProperty.__doc__ = PySparkDataFrameWriterV2.tableProperty.__doc__

    def partitionedBy(self, col: "ColumnOrName", *cols: "ColumnOrName") -> "DataFrameWriterV2":
        self._write.partitioning_columns = [col]
        self._write.partitioning_columns.extend(cols)
        return self

    partitionedBy.__doc__ = PySparkDataFrameWriterV2.partitionedBy.__doc__

    def create(self) -> None:
        self._write.mode = "create"
        self._spark.client.execute_command(self._write.command(self._spark.client))

    create.__doc__ = PySparkDataFrameWriterV2.create.__doc__

    def replace(self) -> None:
        self._write.mode = "replace"
        self._spark.client.execute_command(self._write.command(self._spark.client))

    replace.__doc__ = PySparkDataFrameWriterV2.replace.__doc__

    def createOrReplace(self) -> None:
        self._write.mode = "create_or_replace"
        self._spark.client.execute_command(self._write.command(self._spark.client))

    createOrReplace.__doc__ = PySparkDataFrameWriterV2.createOrReplace.__doc__

    def append(self) -> None:
        self._write.mode = "append"
        self._spark.client.execute_command(self._write.command(self._spark.client))

    append.__doc__ = PySparkDataFrameWriterV2.append.__doc__

    def overwrite(self, condition: "ColumnOrName") -> None:
        self._write.mode = "overwrite"
        self._write.overwrite_condition = condition
        self._spark.client.execute_command(self._write.command(self._spark.client))

    overwrite.__doc__ = PySparkDataFrameWriterV2.overwrite.__doc__

    def overwritePartitions(self) -> None:
        self._write.mode = "overwrite_partitions"
        self._spark.client.execute_command(self._write.command(self._spark.client))

    overwritePartitions.__doc__ = PySparkDataFrameWriterV2.overwritePartitions.__doc__


def _test() -> None:
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.readwriter

    globs = pyspark.sql.connect.readwriter.__dict__.copy()

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.readwriter tests")
        .remote("local[4]")
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.readwriter,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
