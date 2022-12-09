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


from typing import Dict
from typing import Optional, Union, List, overload, Tuple, cast, Any
from typing import TYPE_CHECKING

from pyspark.sql.connect.plan import Read, DataSource, LogicalPlan, WriteOperation
from pyspark.sql.types import StructType
from pyspark.sql.utils import to_str

if TYPE_CHECKING:
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect._typing import OptionalPrimitiveType
    from pyspark.sql.connect.session import SparkSession


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
    TODO(SPARK-40539) Achieve parity with PySpark.
    """

    def __init__(self, client: "SparkSession"):
        self._client = client
        self._format = ""
        self._schema = ""
        self._options: Dict[str, str] = {}

    def format(self, source: str) -> "DataFrameReader":
        """
        Specifies the input data source format.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        source : str
        string, name of the data source, e.g. 'json', 'parquet'.

        """
        self._format = source
        return self

    # TODO(SPARK-40539): support StructType in python client and support schema as StructType.
    def schema(self, schema: str) -> "DataFrameReader":
        """
        Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        schema : str
        a DDL-formatted string
        (For example ``col0 INT, col1 DOUBLE``).

        """
        self._schema = schema
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameReader":
        """
        Adds an input option for the underlying data source.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        key : str
            The key for the option to set. key string is case-insensitive.
        value
            The value for the option to set.

        """
        self._options[key] = str(value)
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameReader":
        """
        Adds input options for the underlying data source.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        **options : dict
            The dictionary of string keys and prmitive-type values.
        """
        for k in options:
            self.option(k, to_str(options[k]))
        return self

    def load(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        schema: Optional[str] = None,
        **options: "OptionalPrimitiveType",
    ) -> "DataFrame":
        """
        Loads data from a data source and returns it as a :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        path : str or list, optional
            optional string or a list of string for file-system backed data sources.
        format : str, optional
            optional string for format of the data source.
        schema : str, optional
            optional DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        **options : dict
            all other string options
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if path is not None:
            self.option("path", path)

        plan = DataSource(format=self._format, schema=self._schema, options=self._options)
        return self._df(plan)

    def _df(self, plan: LogicalPlan) -> "DataFrame":
        # The import is needed here to avoid circular import issues.
        from pyspark.sql.connect.dataframe import DataFrame

        return DataFrame.withPlan(plan, self._client)

    def table(self, tableName: str) -> "DataFrame":
        return self._df(Read(tableName))

    def json(
        self,
        path: str,
        schema: Optional[str] = None,
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
        """
        Loads JSON files and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        path : str
            string represents path to the JSON dataset
        schema : str, optional
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
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.json(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
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
        return self.load(path=path, format="json", schema=schema)

    def parquet(self, path: str, **options: "OptionalPrimitiveType") -> "DataFrame":
        """
        Loads Parquet files, returning the result as a :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        path : str

        Other Parameters
        ----------------
        **options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.parquet(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
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

        return self.load(path=path, format="parquet")


class DataFrameWriter(OptionUtils):
    """
    Interface used to write a :class:`DataFrame` to external storage systems
    (e.g. file systems, key-value stores, etc). Use :attr:`DataFrame.write`
    to access this.

    .. versionadded:: 3.4.0
    """

    def __init__(self, plan: "LogicalPlan", session: "SparkSession"):
        self._df: "LogicalPlan" = plan
        self._spark: "SparkSession" = session
        self._write: "WriteOperation" = WriteOperation(self._df)

    def mode(self, saveMode: Optional[str]) -> "DataFrameWriter":
        """Specifies the behavior when data or table already exists.

        Options include:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        .. versionadded:: 3.4.0

        Examples
        --------
        Raise an error when writing to an existing path.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     spark.createDataFrame(
        ...         [{"age": 80, "name": "Xinrong Meng"}]
        ...     ).write.mode("error").format("parquet").save(d)
        Traceback (most recent call last):
            ...
        pyspark.sql.utils.AnalysisException: ...

        Write a Parquet file back with various options, and read it back.

        >>> with tempfile.TemporaryDirectory() as d:
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
            self._write.mode = saveMode
        return self

    def format(self, source: str) -> "DataFrameWriter":
        """Specifies the underlying output data source.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> spark.range(1).write.format('parquet')
        <pyspark.sql.readwriter.DataFrameWriter object ...>

        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
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
        self._write.source = source
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameWriter":
        """
        Adds an output option for the underlying data source.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        key : str
            The key for the option to set.
        value
            The value for the option to set.

        Examples
        --------
        >>> spark.range(1).write.option("key", "value")
        <pyspark.sql.readwriter.DataFrameWriter object ...>

        Specify the option 'nullValue' with writing a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
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
        self._write.options[key] = to_str(value)
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameWriter":
        """
        Adds output options for the underlying data source.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        **options : dict
            The dictionary of string keys and primitive-type values.

        Examples
        --------
        >>> spark.range(1).write.option("key", "value")
        <pyspark.sql.readwriter.DataFrameWriter object ...>

        Specify the option 'nullValue' and 'header' with writing a CSV file.
        >>> from pyspark.sql.types import StructType,StructField, StringType, IntegerType
        >>> schema = StructType([
        ...     StructField("age",IntegerType(),True),
        ...     StructField("name",StringType(),True),
        ...])
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     df = spark.createDataFrame([(100, None], schema=schema)
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
            self._write.options[k] = to_str(options[k])
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

        .. versionadded:: 3.4.0

        Parameters
        ----------
        cols : str or list
            name of columns

        Examples
        --------
        Write a DataFrame into a Parquet file in a partitioned manner, and read it back.

        >>> import tempfile
        >>> import os
        >>> with tempfile.TemporaryDirectory() as d:
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
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]

        self._write.partitioning_cols = cast(List[str], cols)
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
        if not isinstance(numBuckets, int):
            raise TypeError("numBuckets should be an int, got {0}.".format(type(numBuckets)))

        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError("col is a {0} but cols are not empty".format(type(col)))

            col, cols = col[0], col[1:]  # type: ignore[assignment]

        if not all(isinstance(c, str) for c in cols) or not (isinstance(col, str)):
            raise TypeError("all names should be `str`")

        self._write.num_buckets = numBuckets
        self._write.bucket_cols = cast(List[str], cols)
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

        .. versionadded:: 3.4.0

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
        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError("col is a {0} but cols are not empty".format(type(col)))

            col, cols = col[0], col[1:]  # type: ignore[assignment]

        if not all(isinstance(c, str) for c in cols) or not (isinstance(col, str)):
            raise TypeError("all names should be `str`")

        self._write.sort_cols = cast(List[str], cols)
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

        .. versionadded:: 3.4.0

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
        >>> with tempfile.TemporaryDirectory() as d:
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
        self._write.path = path
        self._spark.client.execute_command(self._write.command(self._spark.client))

    def insertInto(self, tableName: str, overwrite: Optional[bool] = None) -> None:
        """Inserts the content of the :class:`DataFrame` to the specified table.

        It requires that the schema of the :class:`DataFrame` is the same as the
        schema of the table.

        .. versionadded:: 3.4.0

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
        self.saveAsTable(tableName)

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

        .. versionadded:: 3.4.0

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
        self._write.table_name = name
        self._spark.client.execute_command(self._write.command(self._spark.client))

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

        .. versionadded:: 3.4.0

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
        >>> with tempfile.TemporaryDirectory() as d:
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
        self.format("json").save(path)

    def parquet(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        .. versionadded:: 3.4.0

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
        >>> with tempfile.TemporaryDirectory() as d:
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
        self.option("compression", compression)
        self.format("parquet").save(path)

    def text(
        self, path: str, compression: Optional[str] = None, lineSep: Optional[str] = None
    ) -> None:
        """Saves the content of the DataFrame in a text file at the specified path.
        The text files will be encoded as UTF-8.

        .. versionadded:: 3.4.0

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
        >>> with tempfile.TemporaryDirectory() as d:
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
        self.format("text").save(path)

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

        .. versionadded:: 3.4.0

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
        >>> with tempfile.TemporaryDirectory() as d:
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
        |100|null|
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
        self.format("csv").save(path)

    def orc(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in ORC format at the specified path.

        .. versionadded:: 3.4.0

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
        >>> with tempfile.TemporaryDirectory() as d:
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
        self.format("orc").save(path)

    def jdbc(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("jdbc() not supported for DataFrameWriter")
