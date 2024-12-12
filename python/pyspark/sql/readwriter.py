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
import sys
from typing import cast, overload, Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING, Union

from pyspark.util import is_remote_only
from pyspark.sql.types import StructType
from pyspark.sql import utils
from pyspark.sql.utils import to_str
from pyspark.errors import PySparkTypeError, PySparkValueError

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.core.rdd import RDD
    from pyspark.sql._typing import OptionalPrimitiveType, ColumnOrName
    from pyspark.sql.session import SparkSession
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.streaming import StreamingQuery

__all__ = ["DataFrameReader", "DataFrameWriter", "DataFrameWriterV2"]

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
    """
    Interface used to load a :class:`DataFrame` from external storage systems
    (e.g. file systems, key-value stores, etc). Use :attr:`SparkSession.read`
    to access this.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, spark: "SparkSession"):
        self._jreader = spark._jsparkSession.read()
        self._spark = spark

    def _df(self, jdf: "JavaObject") -> "DataFrame":
        from pyspark.sql.dataframe import DataFrame

        return DataFrame(jdf, self._spark)

    def format(self, source: str) -> "DataFrameReader":
        """Specifies the input data source format.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> spark.read.format('json')
        <...readwriter.DataFrameReader object ...>

        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="format") as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format('json').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self._jreader = self._jreader.format(source)
        return self

    def schema(self, schema: Union[StructType, str]) -> "DataFrameReader":
        """Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        schema : :class:`pyspark.sql.types.StructType` or str
            a :class:`pyspark.sql.types.StructType` object or a DDL-formatted string
            (For example ``col0 INT, col1 DOUBLE``).

        Examples
        --------
        >>> spark.read.schema("col0 INT, col1 DOUBLE")
        <...readwriter.DataFrameReader object ...>

        Specify the schema with reading a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="schema") as d:
        ...     spark.read.schema("col0 INT, col1 DOUBLE").format("csv").load(d).printSchema()
        root
         |-- col0: integer (nullable = true)
         |-- col1: double (nullable = true)
        """
        from pyspark.sql import SparkSession

        spark = SparkSession._getActiveSessionOrCreate()
        if isinstance(schema, StructType):
            jschema = spark._jsparkSession.parseDataType(schema.json())
            self._jreader = self._jreader.schema(jschema)
        elif isinstance(schema, str):
            self._jreader = self._jreader.schema(schema)
        else:
            raise PySparkTypeError(
                errorClass="NOT_STR_OR_STRUCT",
                messageParameters={
                    "arg_name": "schema",
                    "arg_type": type(schema).__name__,
                },
            )
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameReader":
        """
        Adds an input option for the underlying data source.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        key : str
            The key for the option to set.
        value
            The value for the option to set.

        Examples
        --------
        >>> spark.read.option("key", "value")
        <...readwriter.DataFrameReader object ...>

        Specify the option 'nullValue' with reading a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="option") as d:
        ...     # Write a DataFrame into a CSV file
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     spark.read.schema(df.schema).option(
        ...         "nullValue", "Hyukjin Kwon").format('csv').load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        self._jreader = self._jreader.option(key, to_str(value))
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameReader":
        """
        Adds input options for the underlying data source.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        **options : dict
            The dictionary of string keys and primitive-type values.

        Examples
        --------
        >>> spark.read.options(key="value")
        <...readwriter.DataFrameReader object ...>

        Specify options in a dictionary.

        >>> spark.read.options(**{"k1": "v1", "k2": "v2"})
        <...readwriter.DataFrameReader object ...>

        Specify the option 'nullValue' and 'header' with reading a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="options") as d:
        ...     # Write a DataFrame into a CSV file with a header.
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.option("header", True).mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     spark.read.options(
        ...         nullValue="Hyukjin Kwon",
        ...         header=True
        ...     ).format('csv').load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        for k in options:
            self._jreader = self._jreader.option(k, to_str(options[k]))
        return self

    def load(
        self,
        path: Optional[PathOrPaths] = None,
        format: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        **options: "OptionalPrimitiveType",
    ) -> "DataFrame":
        """Loads data from a data source and returns it as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list, optional
            optional string or a list of string for file-system backed data sources.
        format : str, optional
            optional string for format of the data source. Default to 'parquet'.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        **options : dict
            all other string options

        Examples
        --------
        Load a CSV file with format, schema and options specified.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="load") as d:
        ...     # Write a DataFrame into a CSV file with a header
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.option("header", True).mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     df = spark.read.load(
        ...         d, schema=df.schema, format="csv", nullValue="Hyukjin Kwon", header=True)
        ...     df.printSchema()
        ...     df.show()
        root
         |-- age: long (nullable = true)
         |-- name: string (nullable = true)
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if isinstance(path, str):
            return self._df(self._jreader.load(path))
        elif path is not None:
            if type(path) != list:
                path = [path]  # type: ignore[list-item]
            assert self._spark._sc._jvm is not None
            return self._df(self._jreader.load(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        else:
            return self._df(self._jreader.load())

    def json(
        self,
        path: Union[str, List[str], "RDD[str]"],
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
        useUnsafeRow: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """
        Loads JSON files and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str, list or :class:`RDD`
            string represents path to the JSON dataset, or a list of paths,
            or RDD of Strings storing JSON objects.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema or
            a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Example 1: Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="json1") as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.json(d).show()
        +---+-------+
        |age|   name|
        +---+-------+
        |100|Hyukjin|
        +---+-------+

        Example 2: Read JSON from multiple files in a directory

        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory(prefix="json2") as d1, TemporaryDirectory(prefix="json3") as d2:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 30, "name": "Bob"}]
        ...     ).write.mode("overwrite").format("json").save(d1)
        ...
        ...     # Read the JSON files as a DataFrame.
        ...     spark.createDataFrame(
        ...         [{"age": 25, "name": "Alice"}]
        ...     ).write.mode("overwrite").format("json").save(d2)
        ...     spark.read.json([d1, d2]).show()
        +---+-----+
        |age| name|
        +---+-----+
        | 25|Alice|
        | 30|  Bob|
        +---+-----+

        Example 3: Read JSON with a custom schema

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="json4") as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...        [{"age": 30, "name": "Bob"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...     custom_schema = "name STRING, age INT"
        ...     spark.read.json(d, schema=custom_schema).show()
        +----+---+
        |name|age|
        +----+---+
        | Bob| 30|
        +----+---+
        """
        self._set_opts(
            schema=schema,
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
            useUnsafeRow=useUnsafeRow,
        )
        if isinstance(path, str):
            path = [path]
        if type(path) == list:
            assert self._spark._sc._jvm is not None
            return self._df(self._jreader.json(self._spark._sc._jvm.PythonUtils.toSeq(path)))

        if not is_remote_only():
            from pyspark.core.rdd import RDD  # noqa: F401

        if not is_remote_only() and isinstance(path, RDD):

            def func(iterator: Iterable) -> Iterable:
                for x in iterator:
                    if not isinstance(x, str):
                        x = str(x)
                    if isinstance(x, str):
                        x = x.encode("utf-8")
                    yield x

            keyed = path.mapPartitions(func)
            keyed._bypass_serializer = True  # type: ignore[attr-defined]
            assert self._spark._jvm is not None
            jrdd = keyed._jrdd.map(self._spark._jvm.BytesToString())
            return self._df(self._jreader.json(jrdd))
        else:
            raise PySparkTypeError(
                errorClass="NOT_STR_OR_LIST_OF_RDD",
                messageParameters={
                    "arg_name": "path",
                    "arg_type": type(path).__name__,
                },
            )

    def table(self, tableName: str) -> "DataFrame":
        """Returns the specified table as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        tableName : str
            string, name of the table.

        Examples
        --------
        >>> df = spark.range(10)
        >>> df.createOrReplaceTempView('tblA')
        >>> spark.read.table('tblA').show()
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        |  3|
        |  4|
        |  5|
        |  6|
        |  7|
        |  8|
        |  9|
        +---+
        >>> _ = spark.sql("DROP TABLE tblA")
        """
        return self._df(self._jreader.table(tableName))

    def parquet(self, *paths: str, **options: "OptionalPrimitiveType") -> "DataFrame":
        """
        Loads Parquet files, returning the result as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        paths : str
            One or more file paths to read the Parquet files from.

        Other Parameters
        ----------------
        **options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Returns
        -------
        :class:`DataFrame`
            A DataFrame containing the data from the Parquet files.

        Examples
        --------
        Create sample dataframes.

        >>> df = spark.createDataFrame(
        ...     [(10, "Alice"), (15, "Bob"), (20, "Tom")], schema=["age", "name"])
        >>> df2 = spark.createDataFrame([(70, "Alice"), (80, "Bob")], schema=["height", "name"])

        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="parquet1") as d:
        ...     # Write a DataFrame into a Parquet file.
        ...     df.write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.parquet(d).orderBy("name").show()
        +---+-----+
        |age| name|
        +---+-----+
        | 10|Alice|
        | 15|  Bob|
        | 20|  Tom|
        +---+-----+

        Read a Parquet file with a specific column.

        >>> with tempfile.TemporaryDirectory(prefix="parquet2") as d:
        ...     df.write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file with only the 'name' column.
        ...     spark.read.schema("name string").parquet(d).orderBy("name").show()
        +-----+
        | name|
        +-----+
        |Alice|
        |  Bob|
        |  Tom|
        +-----+

        Read multiple Parquet files and merge schema.

        >>> with tempfile.TemporaryDirectory(prefix="parquet3") as d1:
        ...     with tempfile.TemporaryDirectory(prefix="parquet4") as d2:
        ...         df.write.mode("overwrite").format("parquet").save(d1)
        ...         df2.write.mode("overwrite").format("parquet").save(d2)
        ...
        ...         spark.read.option(
        ...             "mergeSchema", "true"
        ...         ).parquet(d1, d2).select(
        ...             "name", "age", "height"
        ...         ).orderBy("name", "age").show()
        +-----+----+------+
        | name| age|height|
        +-----+----+------+
        |Alice|NULL|    70|
        |Alice|  10|  NULL|
        |  Bob|NULL|    80|
        |  Bob|  15|  NULL|
        |  Tom|  20|  NULL|
        +-----+----+------+
        """
        from pyspark.sql.classic.column import _to_seq

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

        return self._df(self._jreader.parquet(_to_seq(self._spark._sc, paths)))

    def text(
        self,
        paths: PathOrPaths,
        wholetext: bool = False,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """
        Loads text files and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.
        The text files must be encoded as UTF-8.

        By default, each line in the text file is a new row in the resulting DataFrame.

        .. versionadded:: 1.6.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        paths : str or list
            string, or list of strings, for input path(s).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a text file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="text") as d:
        ...     # Write a DataFrame into a text file
        ...     df = spark.createDataFrame([("a",), ("b",), ("c",)], schema=["alphabets"])
        ...     df.write.mode("overwrite").format("text").save(d)
        ...
        ...     # Read the text file as a DataFrame.
        ...     spark.read.schema(df.schema).text(d).sort("alphabets").show()
        +---------+
        |alphabets|
        +---------+
        |        a|
        |        b|
        |        c|
        +---------+
        """
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
        assert self._spark._sc._jvm is not None
        return self._df(self._jreader.text(self._spark._sc._jvm.PythonUtils.toSeq(paths)))

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
        r"""Loads a CSV file and returns the result as a  :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list
            string, or list of strings, for input path(s),
            or RDD of Strings storing CSV rows.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a CSV file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="csv") as d:
        ...     # Write a DataFrame into a CSV file
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     spark.read.csv(d, schema=df.schema, nullValue="Hyukjin Kwon").show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        self._set_opts(
            schema=schema,
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
        if type(path) == list:
            assert self._spark._sc._jvm is not None
            return self._df(self._jreader.csv(self._spark._sc._jvm.PythonUtils.toSeq(path)))

        if not is_remote_only():
            from pyspark.core.rdd import RDD  # noqa: F401

        if not is_remote_only() and isinstance(path, RDD):

            def func(iterator):
                for x in iterator:
                    if not isinstance(x, str):
                        x = str(x)
                    if isinstance(x, str):
                        x = x.encode("utf-8")
                    yield x

            keyed = path.mapPartitions(func)
            keyed._bypass_serializer = True
            jrdd = keyed._jrdd.map(self._spark._jvm.BytesToString())
            # see SPARK-22112
            # There aren't any jvm api for creating a dataframe from rdd storing csv.
            # We can do it through creating a jvm dataset firstly and using the jvm api
            # for creating a dataframe from dataset storing csv.
            jdataset = self._spark._jsparkSession.createDataset(
                jrdd.rdd(), self._spark._jvm.Encoders.STRING()
            )
            return self._df(self._jreader.csv(jdataset))
        else:
            raise PySparkTypeError(
                errorClass="NOT_STR_OR_LIST_OF_RDD",
                messageParameters={
                    "arg_name": "path",
                    "arg_type": type(path).__name__,
                },
            )

    def xml(
        self,
        path: Union[str, List[str], "RDD[str]"],
        rowTag: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        excludeAttribute: Optional[Union[bool, str]] = None,
        attributePrefix: Optional[str] = None,
        valueTag: Optional[str] = None,
        ignoreSurroundingSpaces: Optional[Union[bool, str]] = None,
        rowValidationXSDPath: Optional[str] = None,
        ignoreNamespace: Optional[Union[bool, str]] = None,
        wildcardColName: Optional[str] = None,
        encoding: Optional[str] = None,
        inferSchema: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        locale: Optional[str] = None,
    ) -> "DataFrame":
        r"""Loads a XML file and returns the result as a  :class:`DataFrame`.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        path : str, list or :class:`RDD`
            string, or list of strings, for input path(s),
            or RDD of Strings storing XML rows.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a XML file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="xml") as d:
        ...     # Write a DataFrame into a XML file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").option("rowTag", "person").format("xml").save(d)
        ...
        ...     # Read the XML file as a DataFrame.
        ...     spark.read.option("rowTag", "person").xml(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self._set_opts(
            rowTag=rowTag,
            schema=schema,
            excludeAttribute=excludeAttribute,
            attributePrefix=attributePrefix,
            valueTag=valueTag,
            ignoreSurroundingSpaces=ignoreSurroundingSpaces,
            rowValidationXSDPath=rowValidationXSDPath,
            ignoreNamespace=ignoreNamespace,
            wildcardColName=wildcardColName,
            encoding=encoding,
            inferSchema=inferSchema,
            nullValue=nullValue,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            multiLine=multiLine,
            samplingRatio=samplingRatio,
            locale=locale,
        )
        if isinstance(path, str):
            path = [path]
        if type(path) == list:
            assert self._spark._sc._jvm is not None
            return self._df(self._jreader.xml(self._spark._sc._jvm.PythonUtils.toSeq(path)))

        if not is_remote_only():
            from pyspark.core.rdd import RDD  # noqa: F401

        if not is_remote_only() and isinstance(path, RDD):

            def func(iterator: Iterable) -> Iterable:
                for x in iterator:
                    if not isinstance(x, str):
                        x = str(x)
                    if isinstance(x, str):
                        x = x.encode("utf-8")
                    yield x

            keyed = path.mapPartitions(func)
            keyed._bypass_serializer = True  # type: ignore[attr-defined]
            assert self._spark._jvm is not None
            jrdd = keyed._jrdd.map(self._spark._jvm.BytesToString())
            # There isn't any jvm api for creating a dataframe from rdd storing XML.
            # We can do it through creating a jvm dataset first and using the jvm api
            # for creating a dataframe from dataset storing XML.
            jdataset = self._spark._jsparkSession.createDataset(
                jrdd.rdd(), self._spark._jvm.Encoders.STRING()
            )
            return self._df(self._jreader.xml(jdataset))
        else:
            raise PySparkTypeError(
                errorClass="NOT_STR_OR_LIST_OF_RDD",
                messageParameters={
                    "arg_name": "path",
                    "arg_type": type(path).__name__,
                },
            )

    def orc(
        self,
        path: PathOrPaths,
        mergeSchema: Optional[bool] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """Loads ORC files, returning the result as a :class:`DataFrame`.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a ORC file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="orc") as d:
        ...     # Write a DataFrame into a ORC file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("orc").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.orc(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        from pyspark.sql.classic.column import _to_seq

        self._set_opts(
            mergeSchema=mergeSchema,
            pathGlobFilter=pathGlobFilter,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            recursiveFileLookup=recursiveFileLookup,
        )
        if isinstance(path, str):
            path = [path]
        return self._df(self._jreader.orc(_to_seq(self._spark._sc, path)))

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
        """
        Construct a :class:`DataFrame` representing the database table named ``table``
        accessible via JDBC URL ``url`` and connection ``properties``.

        Partitions of the table will be retrieved in parallel if either ``column`` or
        ``predicates`` is specified. ``lowerBound``, ``upperBound`` and ``numPartitions``
        is needed when ``column`` is specified.

        If both ``column`` and ``predicates`` are specified, ``column`` will be used.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        table : str
            the name of the table
        column : str, optional
            alias of ``partitionColumn`` option. Refer to ``partitionColumn`` in
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option>`_
            for the version you use.
        predicates : list, optional
            a list of expressions suitable for inclusion in WHERE clauses;
            each one defines one partition of the :class:`DataFrame`
        properties : dict, optional
            a dictionary of JDBC database connection arguments. Normally at
            least properties "user" and "password" with their corresponding values.
            For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Notes
        -----
        Don't create too many partitions in parallel on a large cluster;
        otherwise Spark might crash your external database systems.

        Returns
        -------
        :class:`DataFrame`
        """
        from py4j.java_gateway import JavaClass

        if properties is None:
            properties = dict()
        assert self._spark._sc._gateway is not None
        jprop = JavaClass(
            "java.util.Properties",
            self._spark._sc._gateway._gateway_client,
        )()
        for k in properties:
            jprop.setProperty(k, properties[k])
        if column is not None:
            assert lowerBound is not None, "lowerBound can not be None when ``column`` is specified"
            assert upperBound is not None, "upperBound can not be None when ``column`` is specified"
            assert (
                numPartitions is not None
            ), "numPartitions can not be None when ``column`` is specified"
            return self._df(
                self._jreader.jdbc(
                    url, table, column, int(lowerBound), int(upperBound), int(numPartitions), jprop
                )
            )
        if predicates is not None:
            gateway = self._spark._sc._gateway
            assert gateway is not None
            jpredicates = utils.to_java_array(gateway, gateway.jvm.java.lang.String, predicates)
            return self._df(self._jreader.jdbc(url, table, jpredicates, jprop))
        return self._df(self._jreader.jdbc(url, table, jprop))


class DataFrameWriter(OptionUtils):
    """
    Interface used to write a :class:`DataFrame` to external storage systems
    (e.g. file systems, key-value stores, etc). Use :attr:`DataFrame.write`
    to access this.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, df: "DataFrame"):
        self._df = df
        self._spark = df.sparkSession
        self._jwrite = df._jdf.write()

    def _sq(self, jsq: "JavaObject") -> "StreamingQuery":
        from pyspark.sql.streaming import StreamingQuery

        return StreamingQuery(jsq)

    def mode(self, saveMode: Optional[str]) -> "DataFrameWriter":
        """Specifies the behavior when data or table already exists.

        Options include:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        Raise an error when writing to an existing path.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="mode1") as d:
        ...     spark.createDataFrame(
        ...         [{"age": 80, "name": "Xinrong Meng"}]
        ...     ).write.mode("error").format("parquet").save(d) # doctest: +SKIP
        Traceback (most recent call last):
            ...
        ...AnalysisException: ...

        Write a Parquet file back with various options, and read it back.

        >>> with tempfile.TemporaryDirectory(prefix="mode2") as d:
        ...     # Overwrite the path with a new Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Append another DataFrame into the Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 120, "name": "Takuya Ueshin"}]
        ...     ).write.mode("append").format("parquet").save(d)
        ...
        ...     # Append another DataFrame into the Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 140, "name": "Haejoon Lee"}]
        ...     ).write.mode("ignore").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.parquet(d).show()
        +---+-------------+
        |age|         name|
        +---+-------------+
        |120|Takuya Ueshin|
        |100| Hyukjin Kwon|
        +---+-------------+
        """
        # At the JVM side, the default value of mode is already set to "error".
        # So, if the given saveMode is None, we will not call JVM-side's mode method.
        if saveMode is not None:
            self._jwrite = self._jwrite.mode(saveMode)
        return self

    def format(self, source: str) -> "DataFrameWriter":
        """Specifies the underlying output data source.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> spark.range(1).write.format('parquet')
        <...readwriter.DataFrameWriter object ...>

        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="format") as d:
        ...     # Write a DataFrame into a Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.format('parquet').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self._jwrite = self._jwrite.format(source)
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameWriter":
        """
        Adds an output option for the underlying data source.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        key : str
            The key for the option to set.
        value
            The value for the option to set.

        Examples
        --------
        >>> spark.range(1).write.option("key", "value")
        <...readwriter.DataFrameWriter object ...>

        Specify the option 'nullValue' with writing a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="option") as d:
        ...     # Write a DataFrame into a CSV file with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     df = spark.createDataFrame([(100, None)], "age INT, name STRING")
        ...     df.write.option("nullValue", "Hyukjin Kwon").mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame.
        ...     spark.read.schema(df.schema).format('csv').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """

        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameWriter":
        """
        Adds output options for the underlying data source.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        **options : dict
            The dictionary of string keys and primitive-type values.

        Examples
        --------
        >>> spark.range(1).write.options(key="value")
        <...readwriter.DataFrameWriter object ...>

        Specify options in a dictionary.

        >>> spark.range(1).write.options(**{"k1": "v1", "k2": "v2"})
        <...readwriter.DataFrameWriter object ...>

        Specify the option 'nullValue' and 'header' with writing a CSV file.

        >>> from pyspark.sql.types import StructType,StructField, StringType, IntegerType
        >>> schema = StructType([
        ...     StructField("age",IntegerType(),True),
        ...     StructField("name",StringType(),True),
        ... ])
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="options") as d:
        ...     # Write a DataFrame into a CSV file with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     df = spark.createDataFrame([(100, None)], schema=schema)
        ...     df.write.options(nullValue="Hyukjin Kwon", header=True).mode(
        ...         "overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame.
        ...     spark.read.option("header", True).format('csv').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, to_str(options[k]))
        return self

    @overload
    def partitionBy(self, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def partitionBy(self, *cols: List[str]) -> "DataFrameWriter":
        ...

    def partitionBy(self, *cols: Union[str, List[str]]) -> "DataFrameWriter":
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : str or list
            name of columns

        Examples
        --------
        Write a DataFrame into a Parquet file in a partitioned manner, and read it back.

        >>> import tempfile
        >>> import os
        >>> with tempfile.TemporaryDirectory(prefix="partitionBy") as d:
        ...     # Write a DataFrame into a Parquet file in a partitioned manner.
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}, {"age": 120, "name": "Ruifeng Zheng"}]
        ...     ).write.partitionBy("name").mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.parquet(d).sort("age").show()
        ...
        ...     # Read one partition as a DataFrame.
        ...     spark.read.parquet(f"{d}{os.path.sep}name=Hyukjin Kwon").show()
        +---+-------------+
        |age|         name|
        +---+-------------+
        |100| Hyukjin Kwon|
        |120|Ruifeng Zheng|
        +---+-------------+
        +---+
        |age|
        +---+
        |100|
        +---+
        """
        from pyspark.sql.classic.column import _to_seq

        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]
        self._jwrite = self._jwrite.partitionBy(
            _to_seq(self._spark._sc, cast(Iterable["ColumnOrName"], cols))
        )
        return self

    @overload
    def bucketBy(self, numBuckets: int, col: str, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def bucketBy(self, numBuckets: int, col: TupleOrListOfString) -> "DataFrameWriter":
        ...

    def bucketBy(
        self, numBuckets: int, col: Union[str, TupleOrListOfString], *cols: Optional[str]
    ) -> "DataFrameWriter":
        """Buckets the output by the given columns. If specified,
        the output is laid out on the file system similar to Hive's bucketing scheme,
        but with a different bucket hash function and is not compatible with Hive's bucketing.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        numBuckets : int
            the number of buckets to save
        col : str, list or tuple
            a name of a column, or a list of names.
        cols : str
            additional names (optional). If `col` is a list it should be empty.

        Notes
        -----
        Applicable for file-based data sources in combination with
        :py:meth:`DataFrameWriter.saveAsTable`.

        Examples
        --------
        Write a DataFrame into a Parquet file in a buckted manner, and read it back.

        >>> from pyspark.sql.functions import input_file_name
        >>> # Write a DataFrame into a Parquet file in a bucketed manner.
        ... _ = spark.sql("DROP TABLE IF EXISTS bucketed_table")
        >>> spark.createDataFrame([
        ...     (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        ...     schema=["age", "name"]
        ... ).write.bucketBy(2, "name").mode("overwrite").saveAsTable("bucketed_table")
        >>> # Read the Parquet file as a DataFrame.
        ... spark.read.table("bucketed_table").sort("age").show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |140| Haejoon Lee|
        +---+------------+
        >>> _ = spark.sql("DROP TABLE bucketed_table")
        """
        from pyspark.sql.classic.column import _to_seq

        if not isinstance(numBuckets, int):
            raise PySparkTypeError(
                errorClass="NOT_INT",
                messageParameters={
                    "arg_name": "numBuckets",
                    "arg_type": type(numBuckets).__name__,
                },
            )

        if isinstance(col, (list, tuple)):
            if cols:
                raise PySparkValueError(
                    errorClass="CANNOT_SET_TOGETHER",
                    messageParameters={
                        "arg_list": f"`col` of type {type(col).__name__} and `cols`",
                    },
                )

            col, cols = col[0], col[1:]  # type: ignore[assignment]

        for c in cols:
            if not isinstance(c, str):
                raise PySparkTypeError(
                    errorClass="NOT_LIST_OF_STR",
                    messageParameters={
                        "arg_name": "cols",
                        "arg_type": type(c).__name__,
                    },
                )
        if not isinstance(col, str):
            raise PySparkTypeError(
                errorClass="NOT_LIST_OF_STR",
                messageParameters={
                    "arg_name": "col",
                    "arg_type": type(col).__name__,
                },
            )

        self._jwrite = self._jwrite.bucketBy(
            numBuckets, col, _to_seq(self._spark._sc, cast(Iterable["ColumnOrName"], cols))
        )
        return self

    @overload
    def sortBy(self, col: str, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def sortBy(self, col: TupleOrListOfString) -> "DataFrameWriter":
        ...

    def sortBy(
        self, col: Union[str, TupleOrListOfString], *cols: Optional[str]
    ) -> "DataFrameWriter":
        """Sorts the output in each bucket by the given columns on the file system.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col : str, tuple or list
            a name of a column, or a list of names.
        cols : str
            additional names (optional). If `col` is a list it should be empty.

        Examples
        --------
        Write a DataFrame into a Parquet file in a sorted-buckted manner, and read it back.

        >>> from pyspark.sql.functions import input_file_name
        >>> # Write a DataFrame into a Parquet file in a sorted-bucketed manner.
        ... _ = spark.sql("DROP TABLE IF EXISTS sorted_bucketed_table")
        >>> spark.createDataFrame([
        ...     (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        ...     schema=["age", "name"]
        ... ).write.bucketBy(1, "name").sortBy("age").mode(
        ...     "overwrite").saveAsTable("sorted_bucketed_table")
        >>> # Read the Parquet file as a DataFrame.
        ... spark.read.table("sorted_bucketed_table").sort("age").show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |140| Haejoon Lee|
        +---+------------+
        >>> _ = spark.sql("DROP TABLE sorted_bucketed_table")
        """
        from pyspark.sql.classic.column import _to_seq

        if isinstance(col, (list, tuple)):
            if cols:
                raise PySparkValueError(
                    errorClass="CANNOT_SET_TOGETHER",
                    messageParameters={
                        "arg_list": f"`col` of type {type(col).__name__} and `cols`",
                    },
                )

            col, cols = col[0], col[1:]  # type: ignore[assignment]

        for c in cols:
            if not isinstance(c, str):
                raise PySparkTypeError(
                    errorClass="NOT_LIST_OF_STR",
                    messageParameters={
                        "arg_name": "cols",
                        "arg_type": type(c).__name__,
                    },
                )
        if not isinstance(col, str):
            raise PySparkTypeError(
                errorClass="NOT_LIST_OF_STR",
                messageParameters={
                    "arg_name": "col",
                    "arg_type": type(col).__name__,
                },
            )

        self._jwrite = self._jwrite.sortBy(
            col, _to_seq(self._spark._sc, cast(Iterable["ColumnOrName"], cols))
        )
        return self

    @overload
    def clusterBy(self, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def clusterBy(self, *cols: List[str]) -> "DataFrameWriter":
        ...

    def clusterBy(self, *cols: Union[str, List[str]]) -> "DataFrameWriter":
        """Clusters the data by the given columns to optimize query performance.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        cols : str or list
            name of columns

        Examples
        --------
        Write a DataFrame into a Parquet file with clustering.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="clusterBy") as d:
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}, {"age": 120, "name": "Ruifeng Zheng"}]
        ...     ).write.clusterBy("name").mode("overwrite").format("parquet").save(d)
        """
        from pyspark.sql.classic.column import _to_seq

        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]
        assert len(cols) > 0, "clusterBy needs one or more clustering columns."
        self._jwrite = self._jwrite.clusterBy(cols[0], _to_seq(self._spark._sc, cols[1:]))
        return self

    def save(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """Saves the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str, optional
            the path in a Hadoop supported file system
        format : str, optional
            the format used to save
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        partitionBy : list, optional
            names of partitioning columns
        **options : dict
            all other string options

        Examples
        --------
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="save") as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format('json').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if path is None:
            self._jwrite.save()
        else:
            self._jwrite.save(path)

    def insertInto(self, tableName: str, overwrite: Optional[bool] = None) -> None:
        """Inserts the content of the :class:`DataFrame` to the specified table.

        It requires that the schema of the :class:`DataFrame` is the same as the
        schema of the table.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        overwrite : bool, optional
            If true, overwrites existing data. Disabled by default

        Notes
        -----
        Unlike :meth:`DataFrameWriter.saveAsTable`, :meth:`DataFrameWriter.insertInto` ignores
        the column names and just uses position-based resolution.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tblA")
        >>> df = spark.createDataFrame([
        ...     (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        ...     schema=["age", "name"]
        ... )
        >>> df.write.saveAsTable("tblA")

        Insert the data into 'tblA' table but with different column names.

        >>> df.selectExpr("age AS col1", "name AS col2").write.insertInto("tblA")
        >>> spark.read.table("tblA").sort("age").show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        |100|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |140| Haejoon Lee|
        |140| Haejoon Lee|
        +---+------------+
        >>> _ = spark.sql("DROP TABLE tblA")
        """
        if overwrite is not None:
            self.mode("overwrite" if overwrite else "append")
        self._jwrite.insertInto(tableName)

    def saveAsTable(
        self,
        name: str,
        format: Optional[str] = None,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """Saves the content of the :class:`DataFrame` as the specified table.

        In the case the table already exists, behavior of this function depends on the
        save mode, specified by the `mode` function (default to throwing an exception).
        When `mode` is `Overwrite`, the schema of the :class:`DataFrame` does not need to be
        the same as that of the existing table.

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        When `mode` is `Append`, if there is an existing table, we will use the format and
        options of the existing table. The column order in the schema of the :class:`DataFrame`
        doesn't need to be the same as that of the existing table. Unlike
        :meth:`DataFrameWriter.insertInto`, :meth:`DataFrameWriter.saveAsTable` will use the
        column names to find the correct column positions.

        Parameters
        ----------
        name : str
            the table name
        format : str, optional
            the format used to save
        mode : str, optional
            one of `append`, `overwrite`, `error`, `errorifexists`, `ignore` \
            (default: error)
        partitionBy : str or list
            names of partitioning columns
        **options : dict
            all other string options

        Examples
        --------
        Creates a table from a DataFrame, and read it back.

        >>> _ = spark.sql("DROP TABLE IF EXISTS tblA")
        >>> spark.createDataFrame([
        ...     (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        ...     schema=["age", "name"]
        ... ).write.saveAsTable("tblA")
        >>> spark.read.table("tblA").sort("age").show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |140| Haejoon Lee|
        +---+------------+
        >>> _ = spark.sql("DROP TABLE tblA")
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        self._jwrite.saveAsTable(name)

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
        """Saves the content of the :class:`DataFrame` in JSON format
        (`JSON Lines text format or newline-delimited JSON <http://jsonlines.org/>`_) at the
        specified path.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="json") as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.json(d, mode="overwrite")
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format("json").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode)
        self._set_opts(
            compression=compression,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            lineSep=lineSep,
            encoding=encoding,
            ignoreNullFields=ignoreNullFields,
        )
        self._jwrite.json(path)

    def parquet(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        partitionBy : str or list, optional
            names of partitioning columns

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="parquet") as d:
        ...     # Write a DataFrame into a Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.parquet(d, mode="overwrite")
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.format("parquet").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self._jwrite.parquet(path)

    def text(
        self, path: str, compression: Optional[str] = None, lineSep: Optional[str] = None
    ) -> None:
        """Saves the content of the DataFrame in a text file at the specified path.
        The text files will be encoded as UTF-8.

        .. versionadded:: 1.6.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Notes
        -----
        The DataFrame must have only one column that is of string type.
        Each row becomes a new line in the output file.

        Examples
        --------
        Write a DataFrame into a text file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="text") as d:
        ...     # Write a DataFrame into a text file
        ...     df = spark.createDataFrame([("a",), ("b",), ("c",)], schema=["alphabets"])
        ...     df.write.mode("overwrite").text(d)
        ...
        ...     # Read the text file as a DataFrame.
        ...     spark.read.schema(df.schema).format("text").load(d).sort("alphabets").show()
        +---------+
        |alphabets|
        +---------+
        |        a|
        |        b|
        |        c|
        +---------+
        """
        self._set_opts(compression=compression, lineSep=lineSep)
        self._jwrite.text(path)

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
        r"""Saves the content of the :class:`DataFrame` in CSV format at the specified path.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a CSV file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="csv") as d:
        ...     # Write a DataFrame into a CSV file
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.csv(d, mode="overwrite")
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     spark.read.schema(df.schema).format("csv").option(
        ...         "nullValue", "Hyukjin Kwon").load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
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
        self._jwrite.csv(path)

    def xml(
        self,
        path: str,
        rowTag: Optional[str] = None,
        mode: Optional[str] = None,
        attributePrefix: Optional[str] = None,
        valueTag: Optional[str] = None,
        rootTag: Optional[str] = None,
        declaration: Optional[bool] = None,
        arrayElementName: Optional[str] = None,
        nullValue: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        compression: Optional[str] = None,
        encoding: Optional[str] = None,
        validateName: Optional[bool] = None,
    ) -> None:
        r"""Saves the content of the :class:`DataFrame` in XML format at the specified path.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a XML file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="xml") as d:
        ...     # Write a DataFrame into a XML file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").option("rowTag", "person").xml(d)
        ...
        ...     # Read the XML file as a DataFrame.
        ...     spark.read.option("rowTag", "person").format("xml").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode)
        self._set_opts(
            rowTag=rowTag,
            attributePrefix=attributePrefix,
            valueTag=valueTag,
            rootTag=rootTag,
            declaration=declaration,
            arrayElementName=arrayElementName,
            nullValue=nullValue,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            compression=compression,
            encoding=encoding,
            validateName=validateName,
        )
        self._jwrite.xml(path)

    def orc(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in ORC format at the specified path.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        partitionBy : str or list, optional
            names of partitioning columns

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a ORC file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="orc") as d:
        ...     # Write a DataFrame into a ORC file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.orc(d, mode="overwrite")
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.format("orc").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self._jwrite.orc(path)

    def jdbc(
        self,
        url: str,
        table: str,
        mode: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` to an external database table via JDBC.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        table : str
            Name of the table in the external database.
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        properties : dict
            a dictionary of JDBC database connection arguments. Normally at
            least properties "user" and "password" with their corresponding values.
            For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Notes
        -----
        Don't create too many partitions in parallel on a large cluster;
        otherwise Spark might crash your external database systems.
        """
        from py4j.java_gateway import JavaClass

        if properties is None:
            properties = dict()

        assert self._spark._sc._gateway is not None
        jprop = JavaClass(
            "java.util.Properties",
            self._spark._sc._gateway._gateway_client,
        )()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self.mode(mode)._jwrite.jdbc(url, table, jprop)


class DataFrameWriterV2:
    """
    Interface used to write a class:`pyspark.sql.dataframe.DataFrame`
    to external storage using the v2 API.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, df: "DataFrame", table: str):
        self._df = df
        self._spark = df.sparkSession
        self._jwriter = df._jdf.writeTo(table)

    def using(self, provider: str) -> "DataFrameWriterV2":
        """
        Specifies a provider for the underlying output data source.
        Spark's default catalog supports "parquet", "json", etc.

        .. versionadded: 3.1.0
        """
        self._jwriter.using(provider)
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameWriterV2":
        """
        Add a write option.

        .. versionadded: 3.1.0
        """
        self._jwriter.option(key, to_str(value))
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameWriterV2":
        """
        Add write options.

        .. versionadded: 3.1.0
        """
        options = {k: to_str(v) for k, v in options.items()}
        self._jwriter.options(options)
        return self

    def tableProperty(self, property: str, value: str) -> "DataFrameWriterV2":
        """
        Add table property.

        .. versionadded: 3.1.0
        """
        self._jwriter.tableProperty(property, value)
        return self

    def partitionedBy(self, col: "ColumnOrName", *cols: "ColumnOrName") -> "DataFrameWriterV2":
        """
        Partition the output table created by `create`, `createOrReplace`, or `replace` using
        the given columns or transforms.

        When specified, the table data will be stored by these values for efficient reads.

        For example, when a table is partitioned by day, it may be stored
        in a directory layout like:

        * `table/day=2019-06-01/`
        * `table/day=2019-06-02/`

        Partitioning is one of the most widely used techniques to optimize physical data layout.
        It provides a coarse-grained index for skipping unnecessary data reads when queries have
        predicates on the partitioned columns. In order for partitioning to work well, the number
        of distinct values in each column should typically be less than tens of thousands.

        `col` and `cols` support only the following functions:

        * :py:func:`pyspark.sql.functions.years`
        * :py:func:`pyspark.sql.functions.months`
        * :py:func:`pyspark.sql.functions.days`
        * :py:func:`pyspark.sql.functions.hours`
        * :py:func:`pyspark.sql.functions.bucket`

        .. versionadded: 3.1.0
        """
        from pyspark.sql.classic.column import _to_seq, _to_java_column

        col = _to_java_column(col)
        cols = _to_seq(self._spark._sc, [_to_java_column(c) for c in cols])
        self._jwriter.partitionedBy(col, cols)
        return self

    def clusterBy(self, col: str, *cols: str) -> "DataFrameWriterV2":
        """
        Clusters the data by the given columns to optimize query performance.
        """
        from pyspark.sql.classic.column import _to_seq

        self._jwriter.clusterBy(col, _to_seq(self._spark._sc, cols))
        return self

    def create(self) -> None:
        """
        Create a new table from the contents of the data frame.

        The new table's schema, partition layout, properties, and other configuration will be
        based on the configuration set on this writer.

        .. versionadded: 3.1.0
        """
        self._jwriter.create()

    def replace(self) -> None:
        """
        Replace an existing table with the contents of the data frame.

        The existing table's schema, partition layout, properties, and other configuration will be
        replaced with the contents of the data frame and the configuration set on this writer.

        .. versionadded: 3.1.0
        """
        self._jwriter.replace()

    def createOrReplace(self) -> None:
        """
        Create a new table or replace an existing table with the contents of the data frame.

        The output table's schema, partition layout, properties,
        and other configuration will be based on the contents of the data frame
        and the configuration set on this writer.
        If the table exists, its configuration and data will be replaced.

        .. versionadded: 3.1.0
        """
        self._jwriter.createOrReplace()

    def append(self) -> None:
        """
        Append the contents of the data frame to the output table.

        .. versionadded: 3.1.0
        """
        self._jwriter.append()

    def overwrite(self, condition: "ColumnOrName") -> None:
        """
        Overwrite rows matching the given filter condition with the contents of the data frame in
        the output table.

        .. versionadded: 3.1.0
        """
        from pyspark.sql.classic.column import _to_java_column

        condition = _to_java_column(condition)
        self._jwriter.overwrite(condition)

    def overwritePartitions(self) -> None:
        """
        Overwrite all partition for which the data frame contains at least one row with the contents
        of the data frame in the output table.

        This operation is equivalent to Hive's `INSERT OVERWRITE ... PARTITION`, which replaces
        partitions dynamically depending on the contents of the data frame.

        .. versionadded: 3.1.0
        """
        self._jwriter.overwritePartitions()


def _test() -> None:
    import doctest
    import os
    import py4j
    from pyspark.core.context import SparkContext
    from pyspark.sql import SparkSession
    import pyspark.sql.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.readwriter.__dict__.copy()
    sc = SparkContext("local[4]", "PythonTest")
    try:
        spark = SparkSession._getActiveSessionOrCreate()
    except py4j.protocol.Py4JError:
        spark = SparkSession(sc)

    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.readwriter,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
