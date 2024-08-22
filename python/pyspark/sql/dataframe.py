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

# mypy: disable-error-code="empty-body"

from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
    TYPE_CHECKING,
)

from pyspark import _NoValue
from pyspark._globals import _NoValueType
from pyspark.util import is_remote_only
from pyspark.storagelevel import StorageLevel
from pyspark.resource import ResourceProfile
from pyspark.sql.column import Column
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.merge import MergeIntoWriter
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import StructType, Row
from pyspark.sql.utils import dispatch_df_method

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    import pyarrow as pa
    from pyspark.core.context import SparkContext
    from pyspark.core.rdd import RDD
    from pyspark._typing import PrimitiveType
    from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
    from pyspark.sql._typing import (
        ColumnOrName,
        ColumnOrNameOrOrdinal,
        LiteralType,
        OptionalPrimitiveType,
    )
    from pyspark.sql.context import SQLContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql.group import GroupedData
    from pyspark.sql.observation import Observation
    from pyspark.sql.pandas._typing import (
        PandasMapIterFunction,
        ArrowMapIterFunction,
        DataFrameLike as PandasDataFrameLike,
    )
    from pyspark.sql.metrics import ExecutionInfo


__all__ = ["DataFrame", "DataFrameNaFunctions", "DataFrameStatFunctions"]


class DataFrame:
    """A distributed collection of data grouped into named columns.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Examples
    --------
    A :class:`DataFrame` is equivalent to a relational table in Spark SQL,
    and can be created using various functions in :class:`SparkSession`:

    >>> people = spark.createDataFrame([
    ...     {"deptId": 1, "age": 40, "name": "Hyukjin Kwon", "gender": "M", "salary": 50},
    ...     {"deptId": 1, "age": 50, "name": "Takuya Ueshin", "gender": "M", "salary": 100},
    ...     {"deptId": 2, "age": 60, "name": "Xinrong Meng", "gender": "F", "salary": 150},
    ...     {"deptId": 3, "age": 20, "name": "Haejoon Lee", "gender": "M", "salary": 200}
    ... ])

    Once created, it can be manipulated using the various domain-specific-language
    (DSL) functions defined in: :class:`DataFrame`, :class:`Column`.

    To select a column from the :class:`DataFrame`, use the apply method:

    >>> age_col = people.age

    A more concrete example:

    >>> # To create DataFrame using SparkSession
    ... department = spark.createDataFrame([
    ...     {"id": 1, "name": "PySpark"},
    ...     {"id": 2, "name": "ML"},
    ...     {"id": 3, "name": "Spark SQL"}
    ... ])

    >>> people.filter(people.age > 30).join(
    ...     department, people.deptId == department.id).groupBy(
    ...     department.name, "gender").agg(
    ...         {"salary": "avg", "age": "max"}).sort("max(age)").show()
    +-------+------+-----------+--------+
    |   name|gender|avg(salary)|max(age)|
    +-------+------+-----------+--------+
    |PySpark|     M|       75.0|      50|
    |     ML|     F|      150.0|      60|
    +-------+------+-----------+--------+

    Notes
    -----
    A DataFrame should only be created as described above. It should not be directly
    created via using the constructor.
    """

    # HACK ALERT!! this is to reduce the backward compatibility concern, and returns
    # Spark Classic DataFrame by default. This is NOT an API, and NOT supposed to
    # be directly invoked. DO NOT use this constructor.
    _sql_ctx: Optional["SQLContext"]
    _session: "SparkSession"
    _sc: "SparkContext"
    _jdf: "JavaObject"
    is_cached: bool
    _schema: Optional[StructType]
    _lazy_rdd: Optional["RDD[Row]"]
    _support_repr_html: bool

    def __new__(
        cls,
        jdf: "JavaObject",
        sql_ctx: Union["SQLContext", "SparkSession"],
    ) -> "DataFrame":
        from pyspark.sql.classic.dataframe import DataFrame

        return DataFrame.__new__(DataFrame, jdf, sql_ctx)

    @property
    def sparkSession(self) -> "SparkSession":
        """Returns Spark session that created this :class:`DataFrame`.

        .. versionadded:: 3.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`SparkSession`

        Examples
        --------
        >>> df = spark.range(1)
        >>> type(df.sparkSession)
        <class '...session.SparkSession'>
        """
        ...

    if not is_remote_only():

        @property
        def rdd(self) -> "RDD[Row]":
            """Returns the content as an :class:`pyspark.RDD` of :class:`Row`.

            .. versionadded:: 1.3.0

            Returns
            -------
            :class:`RDD`

            Examples
            --------
            >>> df = spark.range(1)
            >>> type(df.rdd)
            <class 'pyspark.core.rdd.RDD'>
            """
            ...

    @property
    def na(self) -> "DataFrameNaFunctions":
        """Returns a :class:`DataFrameNaFunctions` for handling missing values.

        .. versionadded:: 1.3.1

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`DataFrameNaFunctions`

        Examples
        --------
        >>> df = spark.sql("SELECT 1 AS c1, int(NULL) AS c2")
        >>> type(df.na)
        <class '...dataframe.DataFrameNaFunctions'>

        Replace the missing values as 2.

        >>> df.na.fill(2).show()
        +---+---+
        | c1| c2|
        +---+---+
        |  1|  2|
        +---+---+
        """
        ...

    @property
    def stat(self) -> "DataFrameStatFunctions":
        """Returns a :class:`DataFrameStatFunctions` for statistic functions.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`DataFrameStatFunctions`

        Examples
        --------
        >>> import pyspark.sql.functions as f
        >>> df = spark.range(3).withColumn("c", f.expr("id + 1"))
        >>> type(df.stat)
        <class '...dataframe.DataFrameStatFunctions'>
        >>> df.stat.corr("id", "c")
        1.0
        """
        ...

    if not is_remote_only():

        def toJSON(self, use_unicode: bool = True) -> "RDD[str]":
            """Converts a :class:`DataFrame` into a :class:`RDD` of string.

            Each row is turned into a JSON document as one element in the returned RDD.

            .. versionadded:: 1.3.0

            Parameters
            ----------
            use_unicode : bool, optional, default True
                Whether to convert to unicode or not.

            Returns
            -------
            :class:`RDD`

            Examples
            --------
            >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
            >>> df.toJSON().first()
            '{"age":2,"name":"Alice"}'
            """
            ...

    @dispatch_df_method
    def registerTempTable(self, name: str) -> None:
        """Registers this :class:`DataFrame` as a temporary table using the given name.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        .. deprecated:: 2.0.0
            Use :meth:`DataFrame.createOrReplaceTempView` instead.

        Parameters
        ----------
        name : str
            Name of the temporary table to register.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.registerTempTable("people")
        >>> df2 = spark.sql("SELECT * FROM people")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        >>> spark.catalog.dropTempView("people")
        True

        """
        ...

    @dispatch_df_method
    def createTempView(self, name: str) -> None:
        """Creates a local temporary view with this :class:`DataFrame`.

        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.
        throws :class:`TempTableAlreadyExistsException`, if the view name already exists in the
        catalog.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name : str
            Name of the view.

        Examples
        --------
        Example 1: Creating and querying a local temporary view

        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.createTempView("people")
        >>> spark.sql("SELECT * FROM people").show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+

        Example 2: Attempting to create a temporary view with an existing name

        >>> df.createTempView("people")  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        AnalysisException: "Temporary table 'people' already exists;"

        Example 3: Creating and dropping a local temporary view

        >>> spark.catalog.dropTempView("people")
        True
        >>> df.createTempView("people")

        Example 4: Creating temporary views with multiple DataFrames with
        :meth:`SparkSession.table`

        >>> df1 = spark.createDataFrame([(1, "John"), (2, "Jane")], schema=["id", "name"])
        >>> df2 = spark.createDataFrame([(3, "Jake"), (4, "Jill")], schema=["id", "name"])
        >>> df1.createTempView("table1")
        >>> df2.createTempView("table2")
        >>> result_df = spark.table("table1").union(spark.table("table2"))
        >>> result_df.show()
        +---+----+
        | id|name|
        +---+----+
        |  1|John|
        |  2|Jane|
        |  3|Jake|
        |  4|Jill|
        +---+----+
        """
        ...

    @dispatch_df_method
    def createOrReplaceTempView(self, name: str) -> None:
        """Creates or replaces a local temporary view with this :class:`DataFrame`.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name : str
            Name of the view.

        Notes
        -----
        The lifetime of this temporary table is tied to the :class:`SparkSession`
        that was used to create this :class:`DataFrame`.

        Examples
        --------
        Example 1: Creating a local temporary view named 'people'.

        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.createOrReplaceTempView("people")

        Example 2: Replacing the local temporary view.

        >>> df2 = df.filter(df.age > 3)
        >>> # Replace the local temporary view with the filtered DataFrame
        >>> df2.createOrReplaceTempView("people")
        >>> # Query the temporary view
        >>> df3 = spark.sql("SELECT * FROM people")
        >>> # Check if the DataFrames are equal
        ... assert sorted(df3.collect()) == sorted(df2.collect())

        Example 3: Dropping the temporary view.

        >>> # Drop the local temporary view
        ... spark.catalog.dropTempView("people")
        True
        """
        ...

    @dispatch_df_method
    def createGlobalTempView(self, name: str) -> None:
        """Creates a global temporary view with this :class:`DataFrame`.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name : str
            Name of the view.

        Notes
        -----
        The lifetime of this temporary view is tied to this Spark application.
        throws :class:`TempTableAlreadyExistsException`, if the view name already exists in the
        catalog.

        Examples
        --------
        Example 1: Creating and querying a global temporary view

        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.createGlobalTempView("people")
        >>> df2 = spark.sql("SELECT * FROM global_temp.people")
        >>> df2.show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+

        Example 2: Attempting to create a duplicate global temporary view

        >>> df.createGlobalTempView("people")  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        AnalysisException: "Temporary table 'people' already exists;"

        Example 3: Dropping a global temporary view

        >>> spark.catalog.dropGlobalTempView("people")
        True
        """
        ...

    @dispatch_df_method
    def createOrReplaceGlobalTempView(self, name: str) -> None:
        """Creates or replaces a global temporary view using the given name.

        The lifetime of this temporary view is tied to this Spark application.

        .. versionadded:: 2.2.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name : str
            Name of the view.

        Examples
        --------
        Example 1: Creating a global temporary view with a DataFrame

        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.createOrReplaceGlobalTempView("people")

        Example 2: Replacing a global temporary view with a filtered DataFrame

        >>> df2 = df.filter(df.age > 3)
        >>> df2.createOrReplaceGlobalTempView("people")
        >>> df3 = spark.table("global_temp.people")
        >>> sorted(df3.collect()) == sorted(df2.collect())
        True

        Example 3: Dropping a global temporary view
        >>> spark.catalog.dropGlobalTempView("people")
        True
        """
        ...

    @property
    def write(self) -> DataFrameWriter:
        """
        Interface for saving the content of the non-streaming :class:`DataFrame` out into external
        storage.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`DataFrameWriter`

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> type(df.write)
        <class '...readwriter.DataFrameWriter'>

        Write the DataFrame as a table.

        >>> _ = spark.sql("DROP TABLE IF EXISTS tab2")
        >>> df.write.saveAsTable("tab2")
        >>> _ = spark.sql("DROP TABLE tab2")
        """
        ...

    @property
    def writeStream(self) -> DataStreamWriter:
        """
        Interface for saving the content of the streaming :class:`DataFrame` out into external
        storage.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        This API is evolving.

        Returns
        -------
        :class:`DataStreamWriter`

        Examples
        --------
        >>> import time
        >>> import tempfile
        >>> df = spark.readStream.format("rate").load()
        >>> type(df.writeStream)
        <class '...streaming.readwriter.DataStreamWriter'>

        >>> with tempfile.TemporaryDirectory(prefix="writeStream") as d:
        ...     # Create a table with Rate source.
        ...     query = df.writeStream.toTable(
        ...         "my_table", checkpointLocation=d)
        ...     time.sleep(3)
        ...     query.stop()
        """
        ...

    @property
    def schema(self) -> StructType:
        """Returns the schema of this :class:`DataFrame` as a :class:`pyspark.sql.types.StructType`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`StructType`

        Examples
        --------
        Example 1: Retrieve the inferred schema of the current DataFrame.

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.schema
        StructType([StructField('age', LongType(), True),
                    StructField('name', StringType(), True)])

        Example 2: Retrieve the schema of the current DataFrame (DDL-formatted schema).

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")],
        ...     "age INT, name STRING")
        >>> df.schema
        StructType([StructField('age', IntegerType(), True),
                    StructField('name', StringType(), True)])

        Example 3: Retrieve the specified schema of the current DataFrame.

        >>> from pyspark.sql.types import StructType, StructField, StringType
        >>> df = spark.createDataFrame(
        ...     [("a",), ("b",), ("c",)],
        ...     StructType([StructField("value", StringType(), False)]))
        >>> df.schema
        StructType([StructField('value', StringType(), False)])

        """
        ...

    @dispatch_df_method
    def printSchema(self, level: Optional[int] = None) -> None:
        """Prints out the schema in the tree format.
        Optionally allows to specify how many levels to print if schema is nested.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        level : int, optional
            How many levels to print for nested schemas.

            .. versionadded:: 3.5.0

        Examples
        --------
        Example 1: Printing the schema of a DataFrame with basic columns

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.printSchema()
        root
         |-- age: long (nullable = true)
         |-- name: string (nullable = true)

        Example 2: Printing the schema with a specified level for nested columns

        >>> df = spark.createDataFrame([(1, (2, 2))], ["a", "b"])
        >>> df.printSchema(1)
        root
         |-- a: long (nullable = true)
         |-- b: struct (nullable = true)

        Example 3: Printing the schema with deeper nesting level

        >>> df.printSchema(2)
        root
         |-- a: long (nullable = true)
         |-- b: struct (nullable = true)
         |    |-- _1: long (nullable = true)
         |    |-- _2: long (nullable = true)

        Example 4: Printing the schema of a DataFrame with nullable and non-nullable columns

        >>> df = spark.range(1).selectExpr("id AS nonnullable", "NULL AS nullable")
        >>> df.printSchema()
        root
         |-- nonnullable: long (nullable = false)
         |-- nullable: void (nullable = true)
        """
        ...

    @dispatch_df_method
    def explain(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> None:
        """Prints the (logical and physical) plans to the console for debugging purposes.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        extended : bool, optional
            default ``False``. If ``False``, prints only the physical plan.
            When this is a string without specifying the ``mode``, it works as the mode is
            specified.
        mode : str, optional
            specifies the expected output format of plans.

            * ``simple``: Print only a physical plan.
            * ``extended``: Print both logical and physical plans.
            * ``codegen``: Print a physical plan and generated codes if they are available.
            * ``cost``: Print a logical plan and statistics if they are available.
            * ``formatted``: Split explain output into two sections: a physical plan outline \
                and node details.

            .. versionchanged:: 3.0.0
               Added optional argument `mode` to specify the expected output format of plans.

        Examples
        --------
        Example 1: Print out the physical plan only (default).

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.explain()  # doctest: +SKIP
        == Physical Plan ==
        *(1) Scan ExistingRDD[age...,name...]

        Example 2: Print out all parsed, analyzed, optimized, and physical plans.

        >>> df.explain(extended=True)
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...

        Example 3: Print out the plans with two sections: a physical plan outline and node details.

        >>> df.explain(mode="formatted")  # doctest: +SKIP
        == Physical Plan ==
        * Scan ExistingRDD (...)
        (1) Scan ExistingRDD [codegen id : ...]
        Output [2]: [age..., name...]
        ...

        Example 4: Print a logical plan and statistics if they are available.

        >>> df.explain(mode="cost")
        == Optimized Logical Plan ==
        ...Statistics...
        ...
        """
        ...

    @dispatch_df_method
    def exceptAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in this :class:`DataFrame` but
        not in another :class:`DataFrame` while preserving duplicates.

        This is equivalent to `EXCEPT ALL` in SQL.
        As standard in SQL, this function resolves columns by position (not by name).

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            The other :class:`DataFrame` to compare to.

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> df1 = spark.createDataFrame(
        ...         [("a", 1), ("a", 1), ("a", 1), ("a", 2), ("b",  3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("b", 3)], ["C1", "C2"])
        >>> df1.exceptAll(df2).show()
        +---+---+
        | C1| C2|
        +---+---+
        |  a|  1|
        |  a|  1|
        |  a|  2|
        |  c|  4|
        +---+---+

        """
        ...

    @dispatch_df_method
    def isLocal(self) -> bool:
        """Returns ``True`` if the :func:`collect` and :func:`take` methods can be run locally
        (without any Spark executors).

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        bool

        Examples
        --------
        >>> df = spark.sql("SHOW TABLES")
        >>> df.isLocal()
        True
        """
        ...

    @property
    def isStreaming(self) -> bool:
        """Returns ``True`` if this :class:`DataFrame` contains one or more sources that
        continuously return data as it arrives. A :class:`DataFrame` that reads data from a
        streaming source must be executed as a :class:`StreamingQuery` using the :func:`start`
        method in :class:`DataStreamWriter`.  Methods that return a single answer, (e.g.,
        :func:`count` or :func:`collect`) will throw an :class:`AnalysisException` when there
        is a streaming source present.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        This API is evolving.

        Returns
        -------
        bool
            Whether it's streaming DataFrame or not.

        Examples
        --------
        >>> df = spark.readStream.format("rate").load()
        >>> df.isStreaming
        True
        """
        ...

    @dispatch_df_method
    def isEmpty(self) -> bool:
        """
        Checks if the :class:`DataFrame` is empty and returns a boolean value.

        .. versionadded:: 3.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        bool
            Returns ``True`` if the DataFrame is empty, ``False`` otherwise.

        See Also
        --------
        DataFrame.count : Counts the number of rows in DataFrame.

        Notes
        -----
        - Unlike `count()`, this method does not trigger any computation.
        - An empty DataFrame has no rows. It may have columns, but no data.

        Examples
        --------
        Example 1: Checking if an empty DataFrame is empty

        >>> df_empty = spark.createDataFrame([], 'a STRING')
        >>> df_empty.isEmpty()
        True

        Example 2: Checking if a non-empty DataFrame is empty

        >>> df_non_empty = spark.createDataFrame(["a"], 'STRING')
        >>> df_non_empty.isEmpty()
        False

        Example 3: Checking if a DataFrame with null values is empty

        >>> df_nulls = spark.createDataFrame([(None, None)], 'a STRING, b INT')
        >>> df_nulls.isEmpty()
        False

        Example 4: Checking if a DataFrame with no rows but with columns is empty

        >>> df_no_rows = spark.createDataFrame([], 'id INT, value STRING')
        >>> df_no_rows.isEmpty()
        True
        """
        ...

    @dispatch_df_method
    def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:
        """
        Prints the first ``n`` rows of the DataFrame to the console.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        n : int, optional, default 20
            Number of rows to show.
        truncate : bool or int, optional, default True
            If set to ``True``, truncate strings longer than 20 chars.
            If set to a number greater than one, truncates long strings to length ``truncate``
            and align cells right.
        vertical : bool, optional
            If set to ``True``, print output rows vertically (one line per column value).

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (14, "Tom"), (23, "Alice"), (16, "Bob"), (19, "This is a super long name")],
        ...     ["age", "name"])

        Show :class:`DataFrame`

        >>> df.show()
        +---+--------------------+
        |age|                name|
        +---+--------------------+
        | 14|                 Tom|
        | 23|               Alice|
        | 16|                 Bob|
        | 19|This is a super l...|
        +---+--------------------+

        Show only top 2 rows.

        >>> df.show(2)
        +---+-----+
        |age| name|
        +---+-----+
        | 14|  Tom|
        | 23|Alice|
        +---+-----+
        only showing top 2 rows

        Show full column content without truncation.

        >>> df.show(truncate=False)
        +---+-------------------------+
        |age|name                     |
        +---+-------------------------+
        |14 |Tom                      |
        |23 |Alice                    |
        |16 |Bob                      |
        |19 |This is a super long name|
        +---+-------------------------+

        Show :class:`DataFrame` where the maximum number of characters is 3.

        >>> df.show(truncate=3)
        +---+----+
        |age|name|
        +---+----+
        | 14| Tom|
        | 23| Ali|
        | 16| Bob|
        | 19| Thi|
        +---+----+

        Show :class:`DataFrame` vertically.

        >>> df.show(vertical=True)
        -RECORD 0--------------------
        age  | 14
        name | Tom
        -RECORD 1--------------------
        age  | 23
        name | Alice
        -RECORD 2--------------------
        age  | 16
        name | Bob
        -RECORD 3--------------------
        age  | 19
        name | This is a super l...
        """
        ...

    @dispatch_df_method
    def __repr__(self) -> str:
        ...

    @dispatch_df_method
    def _repr_html_(self) -> Optional[str]:
        """Returns a :class:`DataFrame` with html code when you enabled eager evaluation
        by 'spark.sql.repl.eagerEval.enabled', this only called by REPL you are
        using support eager evaluation with HTML.
        """
        ...

    def checkpoint(self, eager: bool = True) -> "DataFrame":
        """Returns a checkpointed version of this :class:`DataFrame`. Checkpointing can be
        used to truncate the logical plan of this :class:`DataFrame`, which is especially
        useful in iterative algorithms where the plan may grow exponentially. It will be
        saved to files inside the checkpoint directory set with
        :meth:`SparkContext.setCheckpointDir`, or `spark.checkpoint.dir` configuration.

        .. versionadded:: 2.1.0

        .. versionchanged:: 4.0.0
            Supports Spark Connect.

        Parameters
        ----------
        eager : bool, optional, default True
            Whether to checkpoint this :class:`DataFrame` immediately.

        Returns
        -------
        :class:`DataFrame`
            Checkpointed DataFrame.

        Notes
        -----
        This API is experimental.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.checkpoint(False)  # doctest: +SKIP
        DataFrame[age: bigint, name: string]
        """
        ...

    def localCheckpoint(self, eager: bool = True) -> "DataFrame":
        """Returns a locally checkpointed version of this :class:`DataFrame`. Checkpointing can
        be used to truncate the logical plan of this :class:`DataFrame`, which is especially
        useful in iterative algorithms where the plan may grow exponentially. Local checkpoints
        are stored in the executors using the caching subsystem and therefore they are not
        reliable.

        .. versionadded:: 2.3.0

        .. versionchanged:: 4.0.0
            Supports Spark Connect.

        Parameters
        ----------
        eager : bool, optional, default True
            Whether to checkpoint this :class:`DataFrame` immediately.

        Returns
        -------
        :class:`DataFrame`
            Checkpointed DataFrame.

        Notes
        -----
        This API is experimental.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.localCheckpoint(False)
        DataFrame[age: bigint, name: string]
        """
        ...

    @dispatch_df_method
    def withWatermark(self, eventTime: str, delayThreshold: str) -> "DataFrame":
        """Defines an event time watermark for this :class:`DataFrame`. A watermark tracks a point
        in time before which we assume no more late data is going to arrive.

        Spark will use this watermark for several purposes:
          - To know when a given time window aggregation can be finalized and thus can be emitted
            when using output modes that do not allow updates.

          - To minimize the amount of state that we need to keep for on-going aggregations.

        The current watermark is computed by looking at the `MAX(eventTime)` seen across
        all of the partitions in the query minus a user specified `delayThreshold`.  Due to the cost
        of coordinating this value across partitions, the actual watermark used is only guaranteed
        to be at least `delayThreshold` behind the actual event time.  In some cases we may still
        process records that arrive more than `delayThreshold` late.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        eventTime : str
            the name of the column that contains the event time of the row.
        delayThreshold : str
            the minimum delay to wait to data to arrive late, relative to the
            latest record that has been processed in the form of an interval
            (e.g. "1 minute" or "5 hours").

        Returns
        -------
        :class:`DataFrame`
            Watermarked DataFrame

        Notes
        -----
        This is a feature only for Structured Streaming.

        This API is evolving.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> from pyspark.sql.functions import timestamp_seconds
        >>> df = spark.readStream.format("rate").load().selectExpr(
        ...     "value % 5 AS value", "timestamp")
        >>> df.select("value", df.timestamp.alias("time")).withWatermark("time", '10 minutes')
        DataFrame[value: bigint, time: timestamp]

        Group the data by window and value (0 - 4), and compute the count of each group.

        >>> import time
        >>> from pyspark.sql.functions import window
        >>> query = (df
        ...     .withWatermark("timestamp", "10 minutes")
        ...     .groupBy(
        ...         window(df.timestamp, "10 minutes", "5 minutes"),
        ...         df.value)
        ...     ).count().writeStream.outputMode("complete").format("console").start()
        >>> time.sleep(3)
        >>> query.stop()
        """
        ...

    @dispatch_df_method
    def hint(
        self, name: str, *parameters: Union["PrimitiveType", "Column", List["PrimitiveType"]]
    ) -> "DataFrame":
        """Specifies some hint on the current :class:`DataFrame`.

        .. versionadded:: 2.2.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name : str
            A name of the hint.
        parameters : str, list, float or int
            Optional parameters.

        Returns
        -------
        :class:`DataFrame`
            Hinted DataFrame

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df2 = spark.createDataFrame([Row(height=80, name="Tom"), Row(height=85, name="Bob")])
        >>> df.join(df2, "name").explain()  # doctest: +SKIP
        == Physical Plan ==
        ...
        ... +- SortMergeJoin ...
        ...

        Explicitly trigger the broadcast hashjoin by providing the hint in ``df2``.

        >>> df.join(df2.hint("broadcast"), "name").explain()
        == Physical Plan ==
        ...
        ... +- BroadcastHashJoin ...
        ...
        """
        ...

    @dispatch_df_method
    def count(self) -> int:
        """Returns the number of rows in this :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        int
            Number of rows.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

        Return the number of rows in the :class:`DataFrame`.

        >>> df.count()
        3
        """
        ...

    @dispatch_df_method
    def collect(self) -> List[Row]:
        """Returns all the records in the DataFrame as a list of :class:`Row`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        list
            A list of :class:`Row` objects, each representing a row in the DataFrame.

        See Also
        --------
        DataFrame.take : Returns the first `n` rows.
        DataFrame.head : Returns the first `n` rows.
        DataFrame.toPandas : Returns the data as a pandas DataFrame.
        DataFrame.toArrow : Returns the data as a PyArrow Table.

        Notes
        -----
        This method should only be used if the resulting list is expected to be small,
        as all the data is loaded into the driver's memory.

        Examples
        --------
        Example: Collecting all rows of a DataFrame

        >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.collect()
        [Row(age=14, name='Tom'), Row(age=23, name='Alice'), Row(age=16, name='Bob')]

        Example: Collecting all rows after filtering

        >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.filter(df.age > 15).collect()
        [Row(age=23, name='Alice'), Row(age=16, name='Bob')]

        Example: Collecting all rows after selecting specific columns

        >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.select("name").collect()
        [Row(name='Tom'), Row(name='Alice'), Row(name='Bob')]

        Example: Collecting all rows after applying a function to a column

        >>> from pyspark.sql.functions import upper
        >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.select(upper(df.name)).collect()
        [Row(upper(name)='TOM'), Row(upper(name)='ALICE'), Row(upper(name)='BOB')]

        Example: Collecting all rows from a DataFrame and converting a specific column to a list

        >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> rows = df.collect()
        >>> [row["name"] for row in rows]
        ['Tom', 'Alice', 'Bob']

        Example: Collecting all rows from a DataFrame and converting to a list of dictionaries

        >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> rows = df.collect()
        >>> [row.asDict() for row in rows]
        [{'age': 14, 'name': 'Tom'}, {'age': 23, 'name': 'Alice'}, {'age': 16, 'name': 'Bob'}]
        """
        ...

    @dispatch_df_method
    def toLocalIterator(self, prefetchPartitions: bool = False) -> Iterator[Row]:
        """
        Returns an iterator that contains all of the rows in this :class:`DataFrame`.
        The iterator will consume as much memory as the largest partition in this
        :class:`DataFrame`. With prefetch it may consume up to the memory of the 2 largest
        partitions.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        prefetchPartitions : bool, optional
            If Spark should pre-fetch the next partition before it is needed.

            .. versionchanged:: 3.4.0
                This argument does not take effect for Spark Connect.

        Returns
        -------
        Iterator
            Iterator of rows.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> list(df.toLocalIterator())
        [Row(age=14, name='Tom'), Row(age=23, name='Alice'), Row(age=16, name='Bob')]
        """
        ...

    @dispatch_df_method
    def limit(self, num: int) -> "DataFrame":
        """Limits the result count to the number specified.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        num : int
            Number of records to return. Will return this number of records
            or all records if the DataFrame contains less than this number of records.

        Returns
        -------
        :class:`DataFrame`
            Subset of the records

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.limit(1).show()
        +---+----+
        |age|name|
        +---+----+
        | 14| Tom|
        +---+----+
        >>> df.limit(0).show()
        +---+----+
        |age|name|
        +---+----+
        +---+----+
        """
        ...

    @dispatch_df_method
    def offset(self, num: int) -> "DataFrame":
        """Returns a new :class: `DataFrame` by skipping the first `n` rows.

        .. versionadded:: 3.4.0

        .. versionchanged:: 3.5.0
            Supports vanilla PySpark.

        Parameters
        ----------
        num : int
            Number of records to skip.

        Returns
        -------
        :class:`DataFrame`
            Subset of the records

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.offset(1).show()
        +---+-----+
        |age| name|
        +---+-----+
        | 23|Alice|
        | 16|  Bob|
        +---+-----+
        >>> df.offset(10).show()
        +---+----+
        |age|name|
        +---+----+
        +---+----+
        """
        ...

    @dispatch_df_method
    def take(self, num: int) -> List[Row]:
        """Returns the first ``num`` rows as a :class:`list` of :class:`Row`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        num : int
            Number of records to return. Will return this number of records
            or all records if the DataFrame contains less than this number of records..

        Returns
        -------
        list
            List of rows

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

        Return the first 2 rows of the :class:`DataFrame`.

        >>> df.take(2)
        [Row(age=14, name='Tom'), Row(age=23, name='Alice')]
        """
        ...

    @dispatch_df_method
    def tail(self, num: int) -> List[Row]:
        """
        Returns the last ``num`` rows as a :class:`list` of :class:`Row`.

        Running tail requires moving data into the application's driver process, and doing so with
        a very large ``num`` can crash the driver process with OutOfMemoryError.

        .. versionadded:: 3.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        num : int
            Number of records to return. Will return this number of records
            or all records if the DataFrame contains less than this number of records.

        Returns
        -------
        list
            List of rows

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

        >>> df.tail(2)
        [Row(age=23, name='Alice'), Row(age=16, name='Bob')]
        """
        ...

    @dispatch_df_method
    def foreach(self, f: Callable[[Row], None]) -> None:
        """Applies the ``f`` function to all :class:`Row` of this :class:`DataFrame`.

        This is a shorthand for ``df.rdd.foreach()``.

        .. versionadded:: 1.3.0

        .. versionchanged:: 4.0.0
            Supports Spark Connect.

        Parameters
        ----------
        f : function
            A function that accepts one parameter which will
            receive each row to process.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> def func(person):
        ...     print(person.name)
        ...
        >>> df.foreach(func)
        """
        ...

    @dispatch_df_method
    def foreachPartition(self, f: Callable[[Iterator[Row]], None]) -> None:
        """Applies the ``f`` function to each partition of this :class:`DataFrame`.

        This a shorthand for ``df.rdd.foreachPartition()``.

        .. versionadded:: 1.3.0

        .. versionchanged:: 4.0.0
            Supports Spark Connect.

        Parameters
        ----------
        f : function
            A function that accepts one parameter which will receive
            each partition to process.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> def func(itr):
        ...     for person in itr:
        ...         print(person.name)
        ...
        >>> df.foreachPartition(func)
        """
        ...

    @dispatch_df_method
    def cache(self) -> "DataFrame":
        """Persists the :class:`DataFrame` with the default storage level (`MEMORY_AND_DISK_DESER`).

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        The default storage level has changed to `MEMORY_AND_DISK_DESER` to match Scala in 3.0.

        Returns
        -------
        :class:`DataFrame`
            Cached DataFrame.

        Examples
        --------
        >>> df = spark.range(1)
        >>> df.cache()
        DataFrame[id: bigint]

        >>> df.explain()
        == Physical Plan ==
        InMemoryTableScan ...
        """
        ...

    @dispatch_df_method
    def persist(
        self,
        storageLevel: StorageLevel = (StorageLevel.MEMORY_AND_DISK_DESER),
    ) -> "DataFrame":
        """Sets the storage level to persist the contents of the :class:`DataFrame` across
        operations after the first time it is computed. This can only be used to assign
        a new storage level if the :class:`DataFrame` does not have a storage level set yet.
        If no storage level is specified defaults to (`MEMORY_AND_DISK_DESER`)

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        The default storage level has changed to `MEMORY_AND_DISK_DESER` to match Scala in 3.0.

        Parameters
        ----------
        storageLevel : :class:`StorageLevel`
            Storage level to set for persistence. Default is MEMORY_AND_DISK_DESER.

        Returns
        -------
        :class:`DataFrame`
            Persisted DataFrame.

        Examples
        --------
        >>> df = spark.range(1)
        >>> df.persist()
        DataFrame[id: bigint]

        >>> df.explain()
        == Physical Plan ==
        InMemoryTableScan ...

        Persists the data in the disk by specifying the storage level.

        >>> from pyspark.storagelevel import StorageLevel
        >>> df.persist(StorageLevel.DISK_ONLY)
        DataFrame[id: bigint]
        """
        ...

    @property
    def storageLevel(self) -> StorageLevel:
        """Get the :class:`DataFrame`'s current storage level.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`StorageLevel`
            Currently defined storage level.

        Examples
        --------
        >>> df1 = spark.range(10)
        >>> df1.storageLevel
        StorageLevel(False, False, False, False, 1)
        >>> df1.cache().storageLevel
        StorageLevel(True, True, False, True, 1)

        >>> df2 = spark.range(5)
        >>> df2.persist(StorageLevel.DISK_ONLY_2).storageLevel
        StorageLevel(True, False, False, False, 2)
        """
        ...

    @dispatch_df_method
    def unpersist(self, blocking: bool = False) -> "DataFrame":
        """Marks the :class:`DataFrame` as non-persistent, and remove all blocks for it from
        memory and disk.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        `blocking` default has changed to ``False`` to match Scala in 2.0.

        Parameters
        ----------
        blocking : bool
            Whether to block until all blocks are deleted.

        Returns
        -------
        :class:`DataFrame`
            Unpersisted DataFrame.

        Examples
        --------
        >>> df = spark.range(1)
        >>> df.persist()
        DataFrame[id: bigint]
        >>> df.unpersist()
        DataFrame[id: bigint]
        >>> df = spark.range(1)
        >>> df.unpersist(True)
        DataFrame[id: bigint]
        """
        self.is_cached = False
        self._jdf.unpersist(blocking)
        return self

    @dispatch_df_method
    def coalesce(self, numPartitions: int) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` that has exactly `numPartitions` partitions.

        Similar to coalesce defined on an :class:`RDD`, this operation results in a
        narrow dependency, e.g. if you go from 1000 partitions to 100 partitions,
        there will not be a shuffle, instead each of the 100 new partitions will
        claim 10 of the current partitions. If a larger number of partitions is requested,
        it will stay at the current number of partitions.

        However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
        this may result in your computation taking place on fewer nodes than
        you like (e.g. one node in the case of numPartitions = 1). To avoid this,
        you can call repartition(). This will add a shuffle step, but means the
        current upstream partitions will be executed in parallel (per whatever
        the current partitioning is).

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        numPartitions : int
            specify the target number of partitions

        Returns
        -------
        :class:`DataFrame`

        Examples
        --------
        >>> from pyspark.sql import functions as sf
        >>> spark.range(0, 10, 1, 3).select(
        ...     sf.spark_partition_id().alias("partition")
        ... ).distinct().sort("partition").show()
        +---------+
        |partition|
        +---------+
        |        0|
        |        1|
        |        2|
        +---------+

        >>> from pyspark.sql import functions as sf
        >>> spark.range(0, 10, 1, 3).coalesce(1).select(
        ...     sf.spark_partition_id().alias("partition")
        ... ).distinct().sort("partition").show()
        +---------+
        |partition|
        +---------+
        |        0|
        +---------+
        """
        return DataFrame(self._jdf.coalesce(numPartitions), self.sparkSession)

    @overload
    def repartition(self, numPartitions: int, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def repartition(self, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def repartition(
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` partitioned by the given partitioning expressions. The
        resulting :class:`DataFrame` is hash partitioned.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        numPartitions : int
            can be an int to specify the target number of partitions or a Column.
            If it is a Column, it will be used as the first partitioning column. If not specified,
            the default number of partitions is used.
        cols : str or :class:`Column`
            partitioning columns.

            .. versionchanged:: 1.6.0
               Added optional arguments to specify the partitioning columns. Also made numPartitions
               optional if partitioning columns are specified.

        Returns
        -------
        :class:`DataFrame`
            Repartitioned DataFrame.

        Examples
        --------
        >>> from pyspark.sql import functions as sf
        >>> df = spark.range(0, 64, 1, 9).withColumn(
        ...     "name", sf.concat(sf.lit("name_"), sf.col("id").cast("string"))
        ... ).withColumn(
        ...     "age", sf.col("id") - 32
        ... )
        >>> df.select(
        ...     sf.spark_partition_id().alias("partition")
        ... ).distinct().sort("partition").show()
        +---------+
        |partition|
        +---------+
        |        0|
        |        1|
        |        2|
        |        3|
        |        4|
        |        5|
        |        6|
        |        7|
        |        8|
        +---------+

        Repartition the data into 10 partitions.

        >>> df.repartition(10).select(
        ...     sf.spark_partition_id().alias("partition")
        ... ).distinct().sort("partition").show()
        +---------+
        |partition|
        +---------+
        |        0|
        |        1|
        |        2|
        |        3|
        |        4|
        |        5|
        |        6|
        |        7|
        |        8|
        |        9|
        +---------+

        Repartition the data into 7 partitions by 'age' column.

        >>> df.repartition(7, "age").select(
        ...     sf.spark_partition_id().alias("partition")
        ... ).distinct().sort("partition").show()
        +---------+
        |partition|
        +---------+
        |        0|
        |        1|
        |        2|
        |        3|
        |        4|
        |        5|
        |        6|
        +---------+

        Repartition the data into 3 partitions by 'age' and 'name' columns.

        >>> df.repartition(3, "name", "age").select(
        ...     sf.spark_partition_id().alias("partition")
        ... ).distinct().sort("partition").show()
        +---------+
        |partition|
        +---------+
        |        0|
        |        1|
        |        2|
        +---------+
        """
        ...

    @overload
    def repartitionByRange(self, numPartitions: int, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def repartitionByRange(self, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def repartitionByRange(
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` partitioned by the given partitioning expressions. The
        resulting :class:`DataFrame` is range partitioned.

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        numPartitions : int
            can be an int to specify the target number of partitions or a Column.
            If it is a Column, it will be used as the first partitioning column. If not specified,
            the default number of partitions is used.
        cols : str or :class:`Column`
            partitioning columns.

        Returns
        -------
        :class:`DataFrame`
            Repartitioned DataFrame.

        Notes
        -----
        At least one partition-by expression must be specified.
        When no explicit sort order is specified, "ascending nulls first" is assumed.

        Due to performance reasons this method uses sampling to estimate the ranges.
        Hence, the output may not be consistent, since sampling can return different values.
        The sample size can be controlled by the config
        `spark.sql.execution.rangeExchange.sampleSizePerPartition`.

        Examples
        --------
        Repartition the data into 2 partitions by range in 'age' column.
        For example, the first partition can have ``(14, "Tom")`` and ``(16, "Bob")``,
        and the second partition would have ``(23, "Alice")``.

        >>> from pyspark.sql import functions as sf
        >>> spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"]
        ... ).repartitionByRange(2, "age").select(
        ...     "age", "name", sf.spark_partition_id()
        ... ).show()
        +---+-----+--------------------+
        |age| name|SPARK_PARTITION_ID()|
        +---+-----+--------------------+
        | 14|  Tom|                   0|
        | 16|  Bob|                   0|
        | 23|Alice|                   1|
        +---+-----+--------------------+
        """
        ...

    @dispatch_df_method
    def distinct(self) -> "DataFrame":
        """Returns a new :class:`DataFrame` containing the distinct rows in this :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with distinct records.

        See Also
        --------
        DataFrame.dropDuplicates : Remove duplicate rows from this DataFrame.

        Examples
        --------
        Remove duplicate rows from a DataFrame

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (23, "Alice")], ["age", "name"])
        >>> df.distinct().show()
        +---+-----+
        |age| name|
        +---+-----+
        | 14|  Tom|
        | 23|Alice|
        +---+-----+

        Count the number of distinct rows in a DataFrame

        >>> df.distinct().count()
        2

        Get distinct rows from a DataFrame with multiple columns

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom", "M"), (23, "Alice", "F"), (23, "Alice", "F"), (14, "Tom", "M")],
        ...     ["age", "name", "gender"])
        >>> df.distinct().show()
        +---+-----+------+
        |age| name|gender|
        +---+-----+------+
        | 14|  Tom|     M|
        | 23|Alice|     F|
        +---+-----+------+

        Get distinct values from a specific column in a DataFrame

        >>> df.select("name").distinct().show()
        +-----+
        | name|
        +-----+
        |  Tom|
        |Alice|
        +-----+

        Count the number of distinct values in a specific column

        >>> df.select("name").distinct().count()
        2

        Get distinct values from multiple columns in DataFrame

        >>> df.select("name", "gender").distinct().show()
        +-----+------+
        | name|gender|
        +-----+------+
        |  Tom|     M|
        |Alice|     F|
        +-----+------+

        Get distinct rows from a DataFrame with null values

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom", "M"), (23, "Alice", "F"), (23, "Alice", "F"), (14, "Tom", None)],
        ...     ["age", "name", "gender"])
        >>> df.distinct().show()
        +---+-----+------+
        |age| name|gender|
        +---+-----+------+
        | 14|  Tom|     M|
        | 23|Alice|     F|
        | 14|  Tom|  NULL|
        +---+-----+------+

        Get distinct non-null values from a DataFrame

        >>> df.distinct().filter(df.gender.isNotNull()).show()
        +---+-----+------+
        |age| name|gender|
        +---+-----+------+
        | 14|  Tom|     M|
        | 23|Alice|     F|
        +---+-----+------+
        """
        ...

    @overload
    def sample(self, fraction: float, seed: Optional[int] = ...) -> "DataFrame":
        ...

    @overload
    def sample(
        self,
        withReplacement: Optional[bool],
        fraction: float,
        seed: Optional[int] = ...,
    ) -> "DataFrame":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def sample(
        self,
        withReplacement: Optional[Union[float, bool]] = None,
        fraction: Optional[Union[int, float]] = None,
        seed: Optional[int] = None,
    ) -> "DataFrame":
        """Returns a sampled subset of this :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        withReplacement : bool, optional
            Sample with replacement or not (default ``False``).
        fraction : float, optional
            Fraction of rows to generate, range [0.0, 1.0].
        seed : int, optional
            Seed for sampling (default a random seed).

        Returns
        -------
        :class:`DataFrame`
            Sampled rows from given DataFrame.

        Notes
        -----
        This is not guaranteed to provide exactly the fraction specified of the total
        count of the given :class:`DataFrame`.

        `fraction` is required and, `withReplacement` and `seed` are optional.

        Examples
        --------
        >>> df = spark.range(10)
        >>> df.sample(0.5, 3).count() # doctest: +SKIP
        7
        >>> df.sample(fraction=0.5, seed=3).count() # doctest: +SKIP
        7
        >>> df.sample(withReplacement=True, fraction=0.5, seed=3).count() # doctest: +SKIP
        1
        >>> df.sample(1.0).count()
        10
        >>> df.sample(fraction=1.0).count()
        10
        >>> df.sample(False, fraction=1.0).count()
        10
        """
        ...

    @dispatch_df_method
    def sampleBy(
        self, col: "ColumnOrName", fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> "DataFrame":
        """
        Returns a stratified sample without replacement based on the
        fraction given on each stratum.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col : :class:`Column` or str
            column that defines strata

            .. versionchanged:: 3.0.0
               Added sampling by a column of :class:`Column`
        fractions : dict
            sampling fraction for each stratum. If a stratum is not
            specified, we treat its fraction as zero.
        seed : int, optional
            random seed

        Returns
        -------
        a new :class:`DataFrame` that represents the stratified sample

        Examples
        --------
        >>> from pyspark.sql.functions import col
        >>> dataset = spark.range(0, 100).select((col("id") % 3).alias("key"))
        >>> sampled = dataset.sampleBy("key", fractions={0: 0.1, 1: 0.2}, seed=0)
        >>> sampled.groupBy("key").count().orderBy("key").show()
        +---+-----+
        |key|count|
        +---+-----+
        |  0|    3|
        |  1|    6|
        +---+-----+
        >>> dataset.sampleBy(col("key"), fractions={2: 1.0}, seed=0).count()
        33
        """
        ...

    @dispatch_df_method
    def randomSplit(self, weights: List[float], seed: Optional[int] = None) -> List["DataFrame"]:
        """Randomly splits this :class:`DataFrame` with the provided weights.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        weights : list
            list of doubles as weights with which to split the :class:`DataFrame`.
            Weights will be normalized if they don't sum up to 1.0.
        seed : int, optional
            The seed for sampling.

        Returns
        -------
        list
            List of DataFrames.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([
        ...     Row(age=10, height=80, name="Alice"),
        ...     Row(age=5, height=None, name="Bob"),
        ...     Row(age=None, height=None, name="Tom"),
        ...     Row(age=None, height=None, name=None),
        ... ])

        >>> splits = df.randomSplit([1.0, 2.0], 24)
        >>> splits[0].count()
        2
        >>> splits[1].count()
        2
        """
        ...

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        """Returns all column names and their data types as a list.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        list
            List of columns as tuple pairs.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.dtypes
        [('age', 'bigint'), ('name', 'string')]
        """
        ...

    @property
    def columns(self) -> List[str]:
        """
        Retrieves the names of all columns in the :class:`DataFrame` as a list.

        The order of the column names in the list reflects their order in the DataFrame.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        list
            List of column names in the DataFrame.

        Examples
        --------
        Example 1: Retrieve column names of a DataFrame

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom", "CA"), (23, "Alice", "NY"), (16, "Bob", "TX")],
        ...     ["age", "name", "state"]
        ... )
        >>> df.columns
        ['age', 'name', 'state']

        Example 2: Using column names to project specific columns

        >>> selected_cols = [col for col in df.columns if col != "age"]
        >>> df.select(selected_cols).show()
        +-----+-----+
        | name|state|
        +-----+-----+
        |  Tom|   CA|
        |Alice|   NY|
        |  Bob|   TX|
        +-----+-----+

        Example 3: Checking if a specific column exists in a DataFrame

        >>> "state" in df.columns
        True
        >>> "salary" in df.columns
        False

        Example 4: Iterating over columns to apply a transformation

        >>> import pyspark.sql.functions as f
        >>> for col_name in df.columns:
        ...     df = df.withColumn(col_name, f.upper(f.col(col_name)))
        >>> df.show()
        +---+-----+-----+
        |age| name|state|
        +---+-----+-----+
        | 14|  TOM|   CA|
        | 23|ALICE|   NY|
        | 16|  BOB|   TX|
        +---+-----+-----+

        Example 5: Renaming columns and checking the updated column names

        >>> df = df.withColumnRenamed("name", "first_name")
        >>> df.columns
        ['age', 'first_name', 'state']

        Example 6: Using the `columns` property to ensure two DataFrames have the
        same columns before a union

        >>> df2 = spark.createDataFrame(
        ...     [(30, "Eve", "FL"), (40, "Sam", "WA")], ["age", "name", "location"])
        >>> df.columns == df2.columns
        False
        """
        ...

    @dispatch_df_method
    def colRegex(self, colName: str) -> Column:
        """
        Selects column based on the column name specified as a regex and returns it
        as :class:`Column`.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        colName : str
            string, column name specified as a regex.

        Returns
        -------
        :class:`Column`

        Examples
        --------
        >>> df = spark.createDataFrame([("a", 1), ("b", 2), ("c",  3)], ["Col1", "Col2"])
        >>> df.select(df.colRegex("`(Col1)?+.+`")).show()
        +----+
        |Col2|
        +----+
        |   1|
        |   2|
        |   3|
        +----+
        """
        ...

    @dispatch_df_method
    def to(self, schema: StructType) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` where each row is reconciled to match the specified
        schema.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        schema : :class:`StructType`
            Specified schema.

        Returns
        -------
        :class:`DataFrame`
            Reconciled DataFrame.

        Notes
        -----
        * Reorder columns and/or inner fields by name to match the specified schema.

        * Project away columns and/or inner fields that are not needed by the specified schema.
            Missing columns and/or inner fields (present in the specified schema but not input
            DataFrame) lead to failures.

        * Cast the columns and/or inner fields to match the data types in the specified schema,
            if the types are compatible, e.g., numeric to numeric (error if overflows), but
            not string to int.

        * Carry over the metadata from the specified schema, while the columns and/or inner fields
            still keep their own metadata if not overwritten by the specified schema.

        * Fail if the nullability is not compatible. For example, the column and/or inner field
            is nullable but the specified schema requires them to be not nullable.

        Supports Spark Connect.

        Examples
        --------
        >>> from pyspark.sql.types import StructField, StringType
        >>> df = spark.createDataFrame([("a", 1)], ["i", "j"])
        >>> df.schema
        StructType([StructField('i', StringType(), True), StructField('j', LongType(), True)])

        >>> schema = StructType([StructField("j", StringType()), StructField("i", StringType())])
        >>> df2 = df.to(schema)
        >>> df2.schema
        StructType([StructField('j', StringType(), True), StructField('i', StringType(), True)])
        >>> df2.show()
        +---+---+
        |  j|  i|
        +---+---+
        |  1|  a|
        +---+---+
        """
        ...

    @dispatch_df_method
    def alias(self, alias: str) -> "DataFrame":
        """Returns a new :class:`DataFrame` with an alias set.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        alias : str
            an alias name to be set for the :class:`DataFrame`.

        Returns
        -------
        :class:`DataFrame`
            Aliased DataFrame.

        Examples
        --------
        >>> from pyspark.sql.functions import col, desc
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df_as1 = df.alias("df_as1")
        >>> df_as2 = df.alias("df_as2")
        >>> joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
        >>> joined_df.select(
        ...     "df_as1.name", "df_as2.name", "df_as2.age").sort(desc("df_as1.name")).show()
        +-----+-----+---+
        | name| name|age|
        +-----+-----+---+
        |  Tom|  Tom| 14|
        |  Bob|  Bob| 16|
        |Alice|Alice| 23|
        +-----+-----+---+
        """
        ...

    @dispatch_df_method
    def crossJoin(self, other: "DataFrame") -> "DataFrame":
        """Returns the cartesian product with another :class:`DataFrame`.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the cartesian product.

        Returns
        -------
        :class:`DataFrame`
            Joined DataFrame.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df2 = spark.createDataFrame(
        ...     [Row(height=80, name="Tom"), Row(height=85, name="Bob")])
        >>> df.crossJoin(df2.select("height")).select("age", "name", "height").show()
        +---+-----+------+
        |age| name|height|
        +---+-----+------+
        | 14|  Tom|    80|
        | 14|  Tom|    85|
        | 23|Alice|    80|
        | 23|Alice|    85|
        | 16|  Bob|    80|
        | 16|  Bob|    85|
        +---+-----+------+
        """
        ...

    @dispatch_df_method
    def join(
        self,
        other: "DataFrame",
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
    ) -> "DataFrame":
        """
        Joins with another :class:`DataFrame`, using the given join expression.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the join
        on : str, list or :class:`Column`, optional
            a string for the join column name, a list of column names,
            a join expression (Column), or a list of Columns.
            If `on` is a string or a list of strings indicating the name of the join column(s),
            the column(s) must exist on both sides, and this performs an equi-join.
        how : str, optional
            default ``inner``. Must be one of: ``inner``, ``cross``, ``outer``,
            ``full``, ``fullouter``, ``full_outer``, ``left``, ``leftouter``, ``left_outer``,
            ``right``, ``rightouter``, ``right_outer``, ``semi``, ``leftsemi``, ``left_semi``,
            ``anti``, ``leftanti`` and ``left_anti``.

        Returns
        -------
        :class:`DataFrame`
            Joined DataFrame.

        Examples
        --------
        The following examples demonstrate various join types among ``df1``, ``df2``, and ``df3``.

        >>> import pyspark.sql.functions as sf
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([Row(name="Alice", age=2), Row(name="Bob", age=5)])
        >>> df2 = spark.createDataFrame([Row(name="Tom", height=80), Row(name="Bob", height=85)])
        >>> df3 = spark.createDataFrame([
        ...     Row(name="Alice", age=10, height=80),
        ...     Row(name="Bob", age=5, height=None),
        ...     Row(name="Tom", age=None, height=None),
        ...     Row(name=None, age=None, height=None),
        ... ])

        Inner join on columns (default)

        >>> df.join(df2, "name").show()
        +----+---+------+
        |name|age|height|
        +----+---+------+
        | Bob|  5|    85|
        +----+---+------+

        >>> df.join(df3, ["name", "age"]).show()
        +----+---+------+
        |name|age|height|
        +----+---+------+
        | Bob|  5|  NULL|
        +----+---+------+

        Outer join on a single column with an explicit join condition.

        When the join condition is explicited stated: `df.name == df2.name`, this will
        produce all records where the names match, as well as those that don't (since
        it's an outer join). If there are names in `df2` that are not present in `df`,
        they will appear with `NULL` in the `name` column of `df`, and vice versa for `df2`.

        >>> joined = df.join(df2, df.name == df2.name, "outer").sort(sf.desc(df.name))
        >>> joined.show() # doctest: +SKIP
        +-----+----+----+------+
        | name| age|name|height|
        +-----+----+----+------+
        |  Bob|   5| Bob|    85|
        |Alice|   2|NULL|  NULL|
        | NULL|NULL| Tom|    80|
        +-----+----+----+------+

        To unambiguously select output columns, specify the dataframe along with the column name:

        >>> joined.select(df.name, df2.height).show() # doctest: +SKIP
        +-----+------+
        | name|height|
        +-----+------+
        |  Bob|    85|
        |Alice|  NULL|
        | NULL|    80|
        +-----+------+

        However, in self-joins, direct column references can cause ambiguity:

        >>> df.join(df, df.name == df.name, "outer").select(df.name).show() # doctest: +SKIP
        Traceback (most recent call last):
        ...
        pyspark.errors.exceptions.captured.AnalysisException: Column name#0 are ambiguous...

        A better approach is to assign aliases to the dataframes, and then reference
        the ouptut columns from the join operation using these aliases:

        >>> df.alias("a").join(
        ...     df.alias("b"), sf.col("a.name") == sf.col("b.name"), "outer"
        ... ).sort(sf.desc("a.name")).select("a.name", "b.age").show()
        +-----+---+
        | name|age|
        +-----+---+
        |  Bob|  5|
        |Alice|  2|
        +-----+---+

        Outer join on a single column with implicit join condition using column name

        When you provide the column name directly as the join condition, Spark will treat
        both name columns as one, and will not produce separate columns for `df.name` and
        `df2.name`. This avoids having duplicate columns in the output.

        >>> df.join(df2, "name", "outer").sort(sf.desc("name")).show()
        +-----+----+------+
        | name| age|height|
        +-----+----+------+
        |  Tom|NULL|    80|
        |  Bob|   5|    85|
        |Alice|   2|  NULL|
        +-----+----+------+

        Outer join on multiple columns

        >>> df.join(df3, ["name", "age"], "outer").sort("name", "age").show()
        +-----+----+------+
        | name| age|height|
        +-----+----+------+
        | NULL|NULL|  NULL|
        |Alice|   2|  NULL|
        |Alice|  10|    80|
        |  Bob|   5|  NULL|
        |  Tom|NULL|  NULL|
        +-----+----+------+

        Left outer join on columns

        >>> df.join(df2, "name", "left_outer").show()
        +-----+---+------+
        | name|age|height|
        +-----+---+------+
        |Alice|  2|  NULL|
        |  Bob|  5|    85|
        +-----+---+------+

        Right outer join on columns

        >>> df.join(df2, "name", "right_outer").show()
        +----+----+------+
        |name| age|height|
        +----+----+------+
        | Tom|NULL|    80|
        | Bob|   5|    85|
        +----+----+------+

        Left semi join on columns

        >>> df.join(df2, "name", "left_semi").show()
        +----+---+
        |name|age|
        +----+---+
        | Bob|  5|
        +----+---+

        Left anti join on columns

        >>> df.join(df2, "name", "left_anti").show()
        +-----+---+
        | name|age|
        +-----+---+
        |Alice|  2|
        +-----+---+
        """
        ...

    # TODO(SPARK-22947): Fix the DataFrame API.
    @dispatch_df_method
    def _joinAsOf(
        self,
        other: "DataFrame",
        leftAsOfColumn: Union[str, Column],
        rightAsOfColumn: Union[str, Column],
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
        *,
        tolerance: Optional[Column] = None,
        allowExactMatches: bool = True,
        direction: str = "backward",
    ) -> "DataFrame":
        """
        Perform an as-of join.

        This is similar to a left-join except that we match on the nearest
        key rather than equal keys.

        .. versionchanged:: 4.0.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the join
        leftAsOfColumn : str or :class:`Column`
            a string for the as-of join column name, or a Column
        rightAsOfColumn : str or :class:`Column`
            a string for the as-of join column name, or a Column
        on : str, list or :class:`Column`, optional
            a string for the join column name, a list of column names,
            a join expression (Column), or a list of Columns.
            If `on` is a string or a list of strings indicating the name of the join column(s),
            the column(s) must exist on both sides, and this performs an equi-join.
        how : str, optional
            default ``inner``. Must be one of: ``inner`` and ``left``.
        tolerance : :class:`Column`, optional
            an asof tolerance within this range; must be compatible
            with the merge index.
        allowExactMatches : bool, optional
            default ``True``.
        direction : str, optional
            default ``backward``. Must be one of: ``backward``, ``forward``, and ``nearest``.

        Examples
        --------
        The following performs an as-of join between ``left`` and ``right``.

        >>> left = spark.createDataFrame([(1, "a"), (5, "b"), (10,  "c")], ["a", "left_val"])
        >>> right = spark.createDataFrame([(1, 1), (2, 2), (3, 3), (6, 6), (7, 7)],
        ...                               ["a", "right_val"])
        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a"
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=3),
         Row(a=10, left_val='c', right_val=7)]

        >>> from pyspark.sql import functions as sf
        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", tolerance=sf.lit(1)
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", how="left", tolerance=sf.lit(1)
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=None),
         Row(a=10, left_val='c', right_val=None)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", allowExactMatches=False
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=5, left_val='b', right_val=3),
         Row(a=10, left_val='c', right_val=7)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", direction="forward"
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=6)]
        """
        ...

    @dispatch_df_method
    def sortWithinPartitions(
        self,
        *cols: Union[int, str, Column, List[Union[int, str, Column]]],
        **kwargs: Any,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` with each partition sorted by the specified column(s).

        .. versionadded:: 1.6.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : int, str, list or :class:`Column`, optional
            list of :class:`Column` or column names or column ordinals to sort by.

            .. versionchanged:: 4.0.0
               Supports column ordinal.

        Other Parameters
        ----------------
        ascending : bool or list, optional, default True
            boolean or list of boolean.
            Sort ascending vs. descending. Specify list for multiple sort orders.
            If a list is specified, the length of the list must equal the length of the `cols`.

        Returns
        -------
        :class:`DataFrame`
            DataFrame sorted by partitions.

        Notes
        -----
        A column ordinal starts from 1, which is different from the
        0-based :meth:`__getitem__`.
        If a column ordinal is negative, it means sort descending.

        Examples
        --------
        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.sortWithinPartitions("age", ascending=False)
        DataFrame[age: bigint, name: string]

        >>> df.coalesce(1).sortWithinPartitions(1).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+

        >>> df.coalesce(1).sortWithinPartitions(-1).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+
        """
        ...

    @dispatch_df_method
    def sort(
        self,
        *cols: Union[int, str, Column, List[Union[int, str, Column]]],
        **kwargs: Any,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` sorted by the specified column(s).

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : int, str, list, or :class:`Column`, optional
             list of :class:`Column` or column names or column ordinals to sort by.

            .. versionchanged:: 4.0.0
               Supports column ordinal.

        Other Parameters
        ----------------
        ascending : bool or list, optional, default True
            boolean or list of boolean.
            Sort ascending vs. descending. Specify list for multiple sort orders.
            If a list is specified, the length of the list must equal the length of the `cols`.

        Returns
        -------
        :class:`DataFrame`
            Sorted DataFrame.

        Notes
        -----
        A column ordinal starts from 1, which is different from the
        0-based :meth:`__getitem__`.
        If a column ordinal is negative, it means sort descending.

        Examples
        --------
        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([
        ...     (2, "Alice"), (5, "Bob")], schema=["age", "name"])

        Sort the DataFrame in ascending order.

        >>> df.sort(sf.asc("age")).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+

        >>> df.sort(1).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+

        Sort the DataFrame in descending order.

        >>> df.sort(df.age.desc()).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+

        >>> df.orderBy(df.age.desc()).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+

        >>> df.sort("age", ascending=False).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+

        >>> df.sort(-1).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        +---+-----+

        Specify multiple columns

        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([
        ...     (2, "Alice"), (2, "Bob"), (5, "Bob")], schema=["age", "name"])
        >>> df.orderBy(sf.desc("age"), "name").show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        |  2|  Bob|
        +---+-----+

        >>> df.orderBy(-1, "name").show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        |  2|  Bob|
        +---+-----+

        >>> df.orderBy(-1, 2).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|Alice|
        |  2|  Bob|
        +---+-----+

        Specify multiple columns for sorting order at `ascending`.

        >>> df.orderBy(["age", "name"], ascending=[False, False]).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|  Bob|
        |  2|Alice|
        +---+-----+

        >>> df.orderBy([1, "name"], ascending=[False, False]).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|  Bob|
        |  2|Alice|
        +---+-----+

        >>> df.orderBy([1, 2], ascending=[False, False]).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  5|  Bob|
        |  2|  Bob|
        |  2|Alice|
        +---+-----+
        """
        ...

    orderBy = sort

    @dispatch_df_method
    def describe(self, *cols: Union[str, List[str]]) -> "DataFrame":
        """Computes basic statistics for numeric and string columns.

        .. versionadded:: 1.3.1

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        This includes count, mean, stddev, min, and max. If no columns are
        given, this function computes statistics for all numerical or string columns.

        Notes
        -----
        This function is meant for exploratory data analysis, as we make no
        guarantee about the backward compatibility of the schema of the resulting
        :class:`DataFrame`.

        Use summary for expanded statistics and control over which statistics to compute.

        Parameters
        ----------
        cols : str, list, optional
             Column name or list of column names to describe by (default All columns).

        Returns
        -------
        :class:`DataFrame`
            A new DataFrame that describes (provides statistics) given DataFrame.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [("Bob", 13, 40.3, 150.5), ("Alice", 12, 37.8, 142.3), ("Tom", 11, 44.1, 142.2)],
        ...     ["name", "age", "weight", "height"],
        ... )
        >>> df.describe(['age']).show()
        +-------+----+
        |summary| age|
        +-------+----+
        |  count|   3|
        |   mean|12.0|
        | stddev| 1.0|
        |    min|  11|
        |    max|  13|
        +-------+----+

        >>> df.describe(['age', 'weight', 'height']).show()
        +-------+----+------------------+-----------------+
        |summary| age|            weight|           height|
        +-------+----+------------------+-----------------+
        |  count|   3|                 3|                3|
        |   mean|12.0| 40.73333333333333|            145.0|
        | stddev| 1.0|3.1722757341273704|4.763402145525822|
        |    min|  11|              37.8|            142.2|
        |    max|  13|              44.1|            150.5|
        +-------+----+------------------+-----------------+

        See Also
        --------
        DataFrame.summary : Computes summary statistics for numeric and string columns.
        """
        ...

    @dispatch_df_method
    def summary(self, *statistics: str) -> "DataFrame":
        """Computes specified statistics for numeric and string columns. Available statistics are:
        - count
        - mean
        - stddev
        - min
        - max
        - arbitrary approximate percentiles specified as a percentage (e.g., 75%)

        If no statistics are given, this function computes count, mean, stddev, min,
        approximate quartiles (percentiles at 25%, 50%, and 75%), and max.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        statistics : str, optional
             Column names to calculate statistics by (default All columns).

        Returns
        -------
        :class:`DataFrame`
            A new DataFrame that provides statistics for the given DataFrame.

        Notes
        -----
        This function is meant for exploratory data analysis, as we make no
        guarantee about the backward compatibility of the schema of the resulting
        :class:`DataFrame`.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [("Bob", 13, 40.3, 150.5), ("Alice", 12, 37.8, 142.3), ("Tom", 11, 44.1, 142.2)],
        ...     ["name", "age", "weight", "height"],
        ... )
        >>> df.select("age", "weight", "height").summary().show()
        +-------+----+------------------+-----------------+
        |summary| age|            weight|           height|
        +-------+----+------------------+-----------------+
        |  count|   3|                 3|                3|
        |   mean|12.0| 40.73333333333333|            145.0|
        | stddev| 1.0|3.1722757341273704|4.763402145525822|
        |    min|  11|              37.8|            142.2|
        |    25%|  11|              37.8|            142.2|
        |    50%|  12|              40.3|            142.3|
        |    75%|  13|              44.1|            150.5|
        |    max|  13|              44.1|            150.5|
        +-------+----+------------------+-----------------+

        >>> df.select("age", "weight", "height").summary("count", "min", "25%", "75%", "max").show()
        +-------+---+------+------+
        |summary|age|weight|height|
        +-------+---+------+------+
        |  count|  3|     3|     3|
        |    min| 11|  37.8| 142.2|
        |    25%| 11|  37.8| 142.2|
        |    75%| 13|  44.1| 150.5|
        |    max| 13|  44.1| 150.5|
        +-------+---+------+------+

        See Also
        --------
        DataFrame.describe : Computes basic statistics for numeric and string columns.
        """
        ...

    @overload
    def head(self) -> Optional[Row]:
        ...

    @overload
    def head(self, n: int) -> List[Row]:
        ...

    @dispatch_df_method
    def head(self, n: Optional[int] = None) -> Union[Optional[Row], List[Row]]:
        """Returns the first ``n`` rows.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        This method should only be used if the resulting array is expected
        to be small, as all the data is loaded into the driver's memory.

        Parameters
        ----------
        n : int, optional
            default 1. Number of rows to return.

        Returns
        -------
        If n is supplied, return a list of :class:`Row` of length n
        or less if the DataFrame has fewer elements.
        If n is missing, return a single Row.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.head()
        Row(age=2, name='Alice')
        >>> df.head(1)
        [Row(age=2, name='Alice')]
        >>> df.head(0)
        []
        """
        ...

    @dispatch_df_method
    def first(self) -> Optional[Row]:
        """Returns the first row as a :class:`Row`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`Row`
            First row if :class:`DataFrame` is not empty, otherwise ``None``.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.first()
        Row(age=2, name='Alice')
        """
        ...

    @overload
    def __getitem__(self, item: Union[int, str]) -> Column:
        ...

    @overload
    def __getitem__(self, item: Union[Column, List, Tuple]) -> "DataFrame":
        ...

    @dispatch_df_method
    def __getitem__(self, item: Union[int, str, Column, List, Tuple]) -> Union[Column, "DataFrame"]:
        """Returns the column as a :class:`Column`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        item : int, str, :class:`Column`, list or tuple
            column index, column name, column, or a list or tuple of columns

        Returns
        -------
        :class:`Column` or :class:`DataFrame`
            a specified column, or a filtered or projected dataframe.

            * If the input `item` is an int or str, the output is a :class:`Column`.

            * If the input `item` is a :class:`Column`, the output is a :class:`DataFrame`
                filtered by this given :class:`Column`.

            * If the input `item` is a list or tuple, the output is a :class:`DataFrame`
                projected by this given list or tuple.


        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice"), (5, "Bob")], schema=["age", "name"])

        Retrieve a column instance.

        >>> df.select(df['age']).show()
        +---+
        |age|
        +---+
        |  2|
        |  5|
        +---+

        >>> df.select(df[1]).show()
        +-----+
        | name|
        +-----+
        |Alice|
        |  Bob|
        +-----+

        Select multiple string columns as index.

        >>> df[["name", "age"]].show()
        +-----+---+
        | name|age|
        +-----+---+
        |Alice|  2|
        |  Bob|  5|
        +-----+---+
        >>> df[df.age > 3].show()
        +---+----+
        |age|name|
        +---+----+
        |  5| Bob|
        +---+----+
        >>> df[df[0] > 3].show()
        +---+----+
        |age|name|
        +---+----+
        |  5| Bob|
        +---+----+
        """
        ...

    @dispatch_df_method
    def __getattr__(self, name: str) -> Column:
        """Returns the :class:`Column` denoted by ``name``.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        name : str
            Column name to return as :class:`Column`.

        Returns
        -------
        :class:`Column`
            Requested column.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice"), (5, "Bob")], schema=["age", "name"])

        Retrieve a column instance.

        >>> df.select(df.age).show()
        +---+
        |age|
        +---+
        |  2|
        |  5|
        +---+
        """
        ...

    @dispatch_df_method
    def __dir__(self) -> List[str]:
        """
        Examples
        --------
        >>> from pyspark.sql.functions import lit

        Create a dataframe with a column named 'id'.

        >>> df = spark.range(3)
        >>> [attr for attr in dir(df) if attr[0] == 'i'][:7] # Includes column id
        ['id', 'inputFiles', 'intersect', 'intersectAll', 'isEmpty', 'isLocal', 'isStreaming']

        Add a column named 'i_like_pancakes'.

        >>> df = df.withColumn('i_like_pancakes', lit(1))
        >>> [attr for attr in dir(df) if attr[0] == 'i'][:7] # Includes columns i_like_pancakes, id
        ['i_like_pancakes', 'id', 'inputFiles', 'intersect', 'intersectAll', 'isEmpty', 'isLocal']

        Try to add an existed column 'inputFiles'.

        >>> df = df.withColumn('inputFiles', lit(2))
        >>> [attr for attr in dir(df) if attr[0] == 'i'][:7] # Doesn't duplicate inputFiles
        ['i_like_pancakes', 'id', 'inputFiles', 'intersect', 'intersectAll', 'isEmpty', 'isLocal']

        Try to add a column named 'id2'.

        >>> df = df.withColumn('id2', lit(3))
        >>> [attr for attr in dir(df) if attr[0] == 'i'][:7] # result includes id2 and sorted
        ['i_like_pancakes', 'id', 'id2', 'inputFiles', 'intersect', 'intersectAll', 'isEmpty']

        Don't include columns that are not valid python identifiers.

        >>> df = df.withColumn('1', lit(4))
        >>> df = df.withColumn('name 1', lit(5))
        >>> [attr for attr in dir(df) if attr[0] == 'i'][:7] # Doesn't include 1 or name 1
        ['i_like_pancakes', 'id', 'id2', 'inputFiles', 'intersect', 'intersectAll', 'isEmpty']
        """
        ...

    @overload
    def select(self, *cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def select(self, __cols: Union[List[Column], List[str]]) -> "DataFrame":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def select(self, *cols: "ColumnOrName") -> "DataFrame":
        """Projects a set of expressions and returns a new :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : str, :class:`Column`, or list
            column names (string) or expressions (:class:`Column`).
            If one of the column names is '*', that column is expanded to include all columns
            in the current :class:`DataFrame`.

        Returns
        -------
        :class:`DataFrame`
            A DataFrame with subset (or all) of columns.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice"), (5, "Bob")], schema=["age", "name"])

        Select all columns in the DataFrame.

        >>> df.select('*').show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+

        Select a column with other expressions in the DataFrame.

        >>> df.select(df.name, (df.age + 10).alias('age')).show()
        +-----+---+
        | name|age|
        +-----+---+
        |Alice| 12|
        |  Bob| 15|
        +-----+---+
        """
        ...

    @overload
    def selectExpr(self, *expr: str) -> "DataFrame":
        ...

    @overload
    def selectExpr(self, *expr: List[str]) -> "DataFrame":
        ...

    @dispatch_df_method
    def selectExpr(self, *expr: Union[str, List[str]]) -> "DataFrame":
        """Projects a set of SQL expressions and returns a new :class:`DataFrame`.

        This is a variant of :func:`select` that accepts SQL expressions.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        :class:`DataFrame`
            A DataFrame with new/old columns transformed by expressions.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.selectExpr("age * 2", "abs(age)").show()
        +---------+--------+
        |(age * 2)|abs(age)|
        +---------+--------+
        |        4|       2|
        |       10|       5|
        +---------+--------+
        """
        ...

    @dispatch_df_method
    def filter(self, condition: "ColumnOrName") -> "DataFrame":
        """Filters rows using the given condition.

        :func:`where` is an alias for :func:`filter`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        condition : :class:`Column` or str
            A :class:`Column` of :class:`types.BooleanType`
            or a string of SQL expressions.

        Returns
        -------
        :class:`DataFrame`
            A new DataFrame with rows that satisfy the condition.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (2, "Alice", "Math"), (5, "Bob", "Physics"), (7, "Charlie", "Chemistry")],
        ...     schema=["age", "name", "subject"])

        Filter by :class:`Column` instances.

        >>> df.filter(df.age > 3).show()
        +---+-------+---------+
        |age|   name|  subject|
        +---+-------+---------+
        |  5|    Bob|  Physics|
        |  7|Charlie|Chemistry|
        +---+-------+---------+
        >>> df.where(df.age == 2).show()
        +---+-----+-------+
        |age| name|subject|
        +---+-----+-------+
        |  2|Alice|   Math|
        +---+-----+-------+

        Filter by SQL expression in a string.

        >>> df.filter("age > 3").show()
        +---+-------+---------+
        |age|   name|  subject|
        +---+-------+---------+
        |  5|    Bob|  Physics|
        |  7|Charlie|Chemistry|
        +---+-------+---------+
        >>> df.where("age = 2").show()
        +---+-----+-------+
        |age| name|subject|
        +---+-----+-------+
        |  2|Alice|   Math|
        +---+-----+-------+

        Filter by multiple conditions.

        >>> df.filter((df.age > 3) & (df.subject == "Physics")).show()
        +---+----+-------+
        |age|name|subject|
        +---+----+-------+
        |  5| Bob|Physics|
        +---+----+-------+
        >>> df.filter((df.age == 2) | (df.subject == "Chemistry")).show()
        +---+-------+---------+
        |age|   name|  subject|
        +---+-------+---------+
        |  2|  Alice|     Math|
        |  7|Charlie|Chemistry|
        +---+-------+---------+

        Filter by multiple conditions using SQL expression.

        >>> df.filter("age > 3 AND name = 'Bob'").show()
        +---+----+-------+
        |age|name|subject|
        +---+----+-------+
        |  5| Bob|Physics|
        +---+----+-------+

        Filter using the :func:`Column.isin` function.

        >>> df.filter(df.name.isin("Alice", "Bob")).show()
        +---+-----+-------+
        |age| name|subject|
        +---+-----+-------+
        |  2|Alice|   Math|
        |  5|  Bob|Physics|
        +---+-----+-------+

        Filter by a list of values using the :func:`Column.isin` function.

        >>> df.filter(df.subject.isin(["Math", "Physics"])).show()
        +---+-----+-------+
        |age| name|subject|
        +---+-----+-------+
        |  2|Alice|   Math|
        |  5|  Bob|Physics|
        +---+-----+-------+

        Filter using the `~` operator to exclude certain values.

        >>> df.filter(~df.name.isin(["Alice", "Charlie"])).show()
        +---+----+-------+
        |age|name|subject|
        +---+----+-------+
        |  5| Bob|Physics|
        +---+----+-------+

        Filter using the :func:`Column.isNotNull` function.

        >>> df.filter(df.name.isNotNull()).show()
        +---+-------+---------+
        |age|   name|  subject|
        +---+-------+---------+
        |  2|  Alice|     Math|
        |  5|    Bob|  Physics|
        |  7|Charlie|Chemistry|
        +---+-------+---------+

        Filter using the :func:`Column.like` function.

        >>> df.filter(df.name.like("Al%")).show()
        +---+-----+-------+
        |age| name|subject|
        +---+-----+-------+
        |  2|Alice|   Math|
        +---+-----+-------+

        Filter using the :func:`Column.contains` function.

        >>> df.filter(df.name.contains("i")).show()
        +---+-------+---------+
        |age|   name|  subject|
        +---+-------+---------+
        |  2|  Alice|     Math|
        |  7|Charlie|Chemistry|
        +---+-------+---------+

        Filter using the :func:`Column.between` function.

        >>> df.filter(df.age.between(2, 5)).show()
        +---+-----+-------+
        |age| name|subject|
        +---+-----+-------+
        |  2|Alice|   Math|
        |  5|  Bob|Physics|
        +---+-----+-------+
        """
        ...

    @overload
    def groupBy(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":
        ...

    @overload
    def groupBy(self, __cols: Union[List[Column], List[str], List[int]]) -> "GroupedData":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def groupBy(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":
        """
        Groups the :class:`DataFrame` by the specified columns so that aggregation
        can be performed on them.
        See :class:`GroupedData` for all the available aggregate functions.

        :func:`groupby` is an alias for :func:`groupBy`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : list, str, int or :class:`Column`
            The columns to group by.
            Each element can be a column name (string) or an expression (:class:`Column`)
            or a column ordinal (int, 1-based) or list of them.

            .. versionchanged:: 4.0.0
               Supports column ordinal.

        Returns
        -------
        :class:`GroupedData`
            A :class:`GroupedData` object representing the grouped data by the specified columns.

        Notes
        -----
        A column ordinal starts from 1, which is different from the
        0-based :meth:`__getitem__`.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     ("Alice", 2), ("Bob", 2), ("Bob", 2), ("Bob", 5)], schema=["name", "age"])

        Example 1: Empty grouping columns triggers a global aggregation.

        >>> df.groupBy().avg().show()
        +--------+
        |avg(age)|
        +--------+
        |    2.75|
        +--------+

        Example 2: Group-by 'name', and specify a dictionary to calculate the summation of 'age'.

        >>> df.groupBy("name").agg({"age": "sum"}).sort("name").show()
        +-----+--------+
        | name|sum(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       9|
        +-----+--------+

        Example 3: Group-by 'name', and calculate maximum values.

        >>> df.groupBy(df.name).max().sort("name").show()
        +-----+--------+
        | name|max(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       5|
        +-----+--------+

        Example 4: Also group-by 'name', but using the column ordinal.

        >>> df.groupBy(1).max().sort("name").show()
        +-----+--------+
        | name|max(age)|
        +-----+--------+
        |Alice|       2|
        |  Bob|       5|
        +-----+--------+

        Example 5: Group-by 'name' and 'age', and calculate the number of rows in each group.

        >>> df.groupBy(["name", df.age]).count().sort("name", "age").show()
        +-----+---+-----+
        | name|age|count|
        +-----+---+-----+
        |Alice|  2|    1|
        |  Bob|  2|    2|
        |  Bob|  5|    1|
        +-----+---+-----+

        Example 6: Also Group-by 'name' and 'age', but using the column ordinal.

        >>> df.groupBy([df.name, 2]).count().sort("name", "age").show()
        +-----+---+-----+
        | name|age|count|
        +-----+---+-----+
        |Alice|  2|    1|
        |  Bob|  2|    2|
        |  Bob|  5|    1|
        +-----+---+-----+
        """
        ...

    @overload
    def rollup(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def rollup(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def rollup(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":
        """
        Create a multi-dimensional rollup for the current :class:`DataFrame` using
        the specified columns, allowing for aggregation on them.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : list, str, int or :class:`Column`
            The columns to roll-up by.
            Each element should be a column name (string) or an expression (:class:`Column`)
            or a column ordinal (int, 1-based) or list of them.

            .. versionchanged:: 4.0.0
               Supports column ordinal.

        Returns
        -------
        :class:`GroupedData`
            Rolled-up data based on the specified columns.

        Notes
        -----
        A column ordinal starts from 1, which is different from the
        0-based :meth:`__getitem__`.

        Examples
        --------
        >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], schema=["name", "age"])

        Example 1: Rollup-by 'name', and calculate the number of rows in each dimensional.

        >>> df.rollup("name").count().orderBy("name").show()
        +-----+-----+
        | name|count|
        +-----+-----+
        | NULL|    2|
        |Alice|    1|
        |  Bob|    1|
        +-----+-----+

        Example 2: Rollup-by 'name' and 'age',
        and calculate the number of rows in each dimensional.

        >>> df.rollup("name", df.age).count().orderBy("name", "age").show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | NULL|NULL|    2|
        |Alice|NULL|    1|
        |Alice|   2|    1|
        |  Bob|NULL|    1|
        |  Bob|   5|    1|
        +-----+----+-----+

        Example 3: Also Rollup-by 'name' and 'age', but using the column ordinal.

        >>> df.rollup(1, 2).count().orderBy(1, 2).show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | NULL|NULL|    2|
        |Alice|NULL|    1|
        |Alice|   2|    1|
        |  Bob|NULL|    1|
        |  Bob|   5|    1|
        +-----+----+-----+
        """
        ...

    @overload
    def cube(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def cube(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def cube(self, *cols: "ColumnOrName") -> "GroupedData":
        """
        Create a multi-dimensional cube for the current :class:`DataFrame` using
        the specified columns, allowing aggregations to be performed on them.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : list, str, int or :class:`Column`
            The columns to cube by.
            Each element should be a column name (string) or an expression (:class:`Column`)
            or a column ordinal (int, 1-based) or list of them.

            .. versionchanged:: 4.0.0
               Supports column ordinal.

        Returns
        -------
        :class:`GroupedData`
            Cube of the data based on the specified columns.

        Notes
        -----
        A column ordinal starts from 1, which is different from the
        0-based :meth:`__getitem__`.

        Examples
        --------
        >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], schema=["name", "age"])

        Example 1: Creating a cube on 'name',
        and calculate the number of rows in each dimensional.

        >>> df.cube("name").count().orderBy("name").show()
        +-----+-----+
        | name|count|
        +-----+-----+
        | NULL|    2|
        |Alice|    1|
        |  Bob|    1|
        +-----+-----+

        Example 2: Creating a cube on 'name' and 'age',
        and calculate the number of rows in each dimensional.

        >>> df.cube("name", df.age).count().orderBy("name", "age").show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | NULL|NULL|    2|
        | NULL|   2|    1|
        | NULL|   5|    1|
        |Alice|NULL|    1|
        |Alice|   2|    1|
        |  Bob|NULL|    1|
        |  Bob|   5|    1|
        +-----+----+-----+

        Example 3: Also creating a cube on 'name' and 'age', but using the column ordinal.

        >>> df.cube(1, 2).count().orderBy(1, 2).show()
        +-----+----+-----+
        | name| age|count|
        +-----+----+-----+
        | NULL|NULL|    2|
        | NULL|   2|    1|
        | NULL|   5|    1|
        |Alice|NULL|    1|
        |Alice|   2|    1|
        |  Bob|NULL|    1|
        |  Bob|   5|    1|
        +-----+----+-----+
        """
        ...

    @dispatch_df_method
    def groupingSets(
        self, groupingSets: Sequence[Sequence["ColumnOrName"]], *cols: "ColumnOrName"
    ) -> "GroupedData":
        """
        Create multi-dimensional aggregation for the current :class:`DataFrame` using the specified
        grouping sets, so we can run aggregation on them.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        groupingSets : sequence of sequence of columns or str
            Individual set of columns to group on.
        cols : :class:`Column` or str
            Addional grouping columns specified by users.
            Those columns are shown as the output columns after aggregation.

        Returns
        -------
        :class:`GroupedData`
            Grouping sets of the data based on the specified columns.

        Examples
        --------
        Example 1: Group by city and car_model, city, and all, and calculate the sum of quantity.

        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([
        ...     (100, 'Fremont', 'Honda Civic', 10),
        ...     (100, 'Fremont', 'Honda Accord', 15),
        ...     (100, 'Fremont', 'Honda CRV', 7),
        ...     (200, 'Dublin', 'Honda Civic', 20),
        ...     (200, 'Dublin', 'Honda Accord', 10),
        ...     (200, 'Dublin', 'Honda CRV', 3),
        ...     (300, 'San Jose', 'Honda Civic', 5),
        ...     (300, 'San Jose', 'Honda Accord', 8)
        ... ], schema="id INT, city STRING, car_model STRING, quantity INT")

        >>> df.groupingSets(
        ...     [("city", "car_model"), ("city",), ()],
        ...     "city", "car_model"
        ... ).agg(sf.sum(sf.col("quantity")).alias("sum")).sort("city", "car_model").show()
        +--------+------------+---+
        |    city|   car_model|sum|
        +--------+------------+---+
        |    NULL|        NULL| 78|
        |  Dublin|        NULL| 33|
        |  Dublin|Honda Accord| 10|
        |  Dublin|   Honda CRV|  3|
        |  Dublin| Honda Civic| 20|
        | Fremont|        NULL| 32|
        | Fremont|Honda Accord| 15|
        | Fremont|   Honda CRV|  7|
        | Fremont| Honda Civic| 10|
        |San Jose|        NULL| 13|
        |San Jose|Honda Accord|  8|
        |San Jose| Honda Civic|  5|
        +--------+------------+---+

        Example 2: Group by multiple columns and calculate both average and sum.

        >>> df.groupingSets(
        ...     [("city", "car_model"), ("city",), ()],
        ...     "city", "car_model"
        ... ).agg(
        ...     sf.avg(sf.col("quantity")).alias("avg_quantity"),
        ...     sf.sum(sf.col("quantity")).alias("sum_quantity")
        ... ).sort("city", "car_model").show()
        +--------+------------+------------------+------------+
        |    city|   car_model|      avg_quantity|sum_quantity|
        +--------+------------+------------------+------------+
        |    NULL|        NULL|              9.75|          78|
        |  Dublin|        NULL|              11.0|          33|
        |  Dublin|Honda Accord|              10.0|          10|
        |  Dublin|   Honda CRV|               3.0|           3|
        |  Dublin| Honda Civic|              20.0|          20|
        | Fremont|        NULL|10.666666666666666|          32|
        | Fremont|Honda Accord|              15.0|          15|
        | Fremont|   Honda CRV|               7.0|           7|
        | Fremont| Honda Civic|              10.0|          10|
        |San Jose|        NULL|               6.5|          13|
        |San Jose|Honda Accord|               8.0|           8|
        |San Jose| Honda Civic|               5.0|           5|
        +--------+------------+------------------+------------+

        See Also
        --------
        DataFrame.rollup : Compute hierarchical summaries at multiple levels.
        """
        ...

    @dispatch_df_method
    def unpivot(
        self,
        ids: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]],
        values: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]],
        variableColumnName: str,
        valueColumnName: str,
    ) -> "DataFrame":
        """
        Unpivot a DataFrame from wide format to long format, optionally leaving
        identifier columns set. This is the reverse to `groupBy(...).pivot(...).agg(...)`,
        except for the aggregation, which cannot be reversed.

        This function is useful to massage a DataFrame into a format where some
        columns are identifier columns ("ids"), while all other columns ("values")
        are "unpivoted" to the rows, leaving just two non-id columns, named as given
        by `variableColumnName` and `valueColumnName`.

        When no "id" columns are given, the unpivoted DataFrame consists of only the
        "variable" and "value" columns.

        The `values` columns must not be empty so at least one value must be given to be unpivoted.
        When `values` is `None`, all non-id columns will be unpivoted.

        All "value" columns must share a least common data type. Unless they are the same data type,
        all "value" columns are cast to the nearest common data type. For instance, types
        `IntegerType` and `LongType` are cast to `LongType`, while `IntegerType` and `StringType`
        do not have a common data type and `unpivot` fails.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        ids : str, Column, tuple, list
            Column(s) to use as identifiers. Can be a single column or column name,
            or a list or tuple for multiple columns.
        values : str, Column, tuple, list, optional
            Column(s) to unpivot. Can be a single column or column name, or a list or tuple
            for multiple columns. If specified, must not be empty. If not specified, uses all
            columns that are not set as `ids`.
        variableColumnName : str
            Name of the variable column.
        valueColumnName : str
            Name of the value column.

        Returns
        -------
        :class:`DataFrame`
            Unpivoted DataFrame.

        Notes
        -----
        Supports Spark Connect.

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(1, 11, 1.1), (2, 12, 1.2)],
        ...     ["id", "int", "double"],
        ... )
        >>> df.show()
        +---+---+------+
        | id|int|double|
        +---+---+------+
        |  1| 11|   1.1|
        |  2| 12|   1.2|
        +---+---+------+

        >>> df.unpivot("id", ["int", "double"], "var", "val").show()
        +---+------+----+
        | id|   var| val|
        +---+------+----+
        |  1|   int|11.0|
        |  1|double| 1.1|
        |  2|   int|12.0|
        |  2|double| 1.2|
        +---+------+----+

        See Also
        --------
        DataFrame.melt
        """
        ...

    @dispatch_df_method
    def melt(
        self,
        ids: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]],
        values: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]],
        variableColumnName: str,
        valueColumnName: str,
    ) -> "DataFrame":
        """
        Unpivot a DataFrame from wide format to long format, optionally leaving
        identifier columns set. This is the reverse to `groupBy(...).pivot(...).agg(...)`,
        except for the aggregation, which cannot be reversed.

        :func:`melt` is an alias for :func:`unpivot`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        ids : str, Column, tuple, list, optional
            Column(s) to use as identifiers. Can be a single column or column name,
            or a list or tuple for multiple columns.
        values : str, Column, tuple, list, optional
            Column(s) to unpivot. Can be a single column or column name, or a list or tuple
            for multiple columns. If not specified or empty, use all columns that
            are not set as `ids`.
        variableColumnName : str
            Name of the variable column.
        valueColumnName : str
            Name of the value column.

        Returns
        -------
        :class:`DataFrame`
            Unpivoted DataFrame.

        See Also
        --------
        DataFrame.unpivot

        Notes
        -----
        Supports Spark Connect.
        """
        ...

    @dispatch_df_method
    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> "DataFrame":
        """Aggregate on the entire :class:`DataFrame` without groups
        (shorthand for ``df.groupBy().agg()``).

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        exprs : :class:`Column` or dict of key and value strings
            Columns or expressions to aggregate DataFrame by.

        Returns
        -------
        :class:`DataFrame`
            Aggregated DataFrame.

        Examples
        --------
        >>> from pyspark.sql import functions as sf
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.agg({"age": "max"}).show()
        +--------+
        |max(age)|
        +--------+
        |       5|
        +--------+
        >>> df.agg(sf.min(df.age)).show()
        +--------+
        |min(age)|
        +--------+
        |       2|
        +--------+
        """
        ...

    @dispatch_df_method
    def observe(
        self,
        observation: Union["Observation", str],
        *exprs: Column,
    ) -> "DataFrame":
        """Define (named) metrics to observe on the DataFrame. This method returns an 'observed'
        DataFrame that returns the same result as the input, with the following guarantees:

        * It will compute the defined aggregates (metrics) on all the data that is flowing through
            the Dataset at that point.

        * It will report the value of the defined aggregate columns as soon as we reach a completion
            point. A completion point is either the end of a query (batch mode) or the end of a
            streaming epoch. The value of the aggregates only reflects the data processed since
            the previous completion point.

        The metrics columns must either contain a literal (e.g. lit(42)), or should contain one or
        more aggregate functions (e.g. sum(a) or sum(a + b) + avg(c) - lit(1)). Expressions that
        contain references to the input Dataset's columns must always be wrapped in an aggregate
        function.

        A user can observe these metrics by adding
        Python's :class:`~pyspark.sql.streaming.StreamingQueryListener`,
        Scala/Java's ``org.apache.spark.sql.streaming.StreamingQueryListener`` or Scala/Java's
        ``org.apache.spark.sql.util.QueryExecutionListener`` to the spark session.

        .. versionadded:: 3.3.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        observation : :class:`Observation` or str
            `str` to specify the name, or an :class:`Observation` instance to obtain the metric.

            .. versionchanged:: 3.4.0
               Added support for `str` in this parameter.
        exprs : :class:`Column`
            column expressions (:class:`Column`).

        Returns
        -------
        :class:`DataFrame`
            the observed :class:`DataFrame`.

        Notes
        -----
        When ``observation`` is :class:`Observation`, this method only supports batch queries.
        When ``observation`` is a string, this method works for both batch and streaming queries.
        Continuous execution is currently not supported yet.

        Examples
        --------
        When ``observation`` is :class:`Observation`, only batch queries work as below.

        >>> from pyspark.sql.functions import col, count, lit, max
        >>> from pyspark.sql import Observation
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> observation = Observation("my metrics")
        >>> observed_df = df.observe(observation, count(lit(1)).alias("count"), max(col("age")))
        >>> observed_df.count()
        2
        >>> observation.get
        {'count': 2, 'max(age)': 5}

        When ``observation`` is a string, streaming queries also work as below.

        >>> from pyspark.sql.streaming import StreamingQueryListener
        >>> import time
        >>> class MyErrorListener(StreamingQueryListener):
        ...    def onQueryStarted(self, event):
        ...        pass
        ...
        ...    def onQueryProgress(self, event):
        ...        row = event.progress.observedMetrics.get("my_event")
        ...        # Trigger if the number of errors exceeds 5 percent
        ...        num_rows = row.rc
        ...        num_error_rows = row.erc
        ...        ratio = num_error_rows / num_rows
        ...        if ratio > 0.05:
        ...            # Trigger alert
        ...            pass
        ...
        ...    def onQueryIdle(self, event):
        ...        pass
        ...
        ...    def onQueryTerminated(self, event):
        ...        pass
        ...
        >>> error_listener = MyErrorListener()
        >>> spark.streams.addListener(error_listener)
        >>> sdf = spark.readStream.format("rate").load().withColumn(
        ...     "error", col("value")
        ... )
        >>> # Observe row count (rc) and error row count (erc) in the streaming Dataset
        ... observed_ds = sdf.observe(
        ...     "my_event",
        ...     count(lit(1)).alias("rc"),
        ...     count(col("error")).alias("erc"))
        >>> try:
        ...     q = observed_ds.writeStream.format("console").start()
        ...     time.sleep(5)
        ...
        ... finally:
        ...     q.stop()
        ...     spark.streams.removeListener(error_listener)
        ...
        """
        ...

    @dispatch_df_method
    def union(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing the union of rows in this and another
        :class:`DataFrame`.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be unioned.

        Returns
        -------
        :class:`DataFrame`
            A new :class:`DataFrame` containing the combined rows with corresponding columns.

        See Also
        --------
        DataFrame.unionAll

        Notes
        -----
        This method performs a SQL-style set union of the rows from both `DataFrame` objects,
        with no automatic deduplication of elements.

        Use the `distinct()` method to perform deduplication of rows.

        The method resolves columns by position (not by name), following the standard behavior
        in SQL.

        Examples
        --------
        Example 1: Combining two DataFrames with the same schema

        >>> df1 = spark.createDataFrame([(1, 'A'), (2, 'B')], ['id', 'value'])
        >>> df2 = spark.createDataFrame([(3, 'C'), (4, 'D')], ['id', 'value'])
        >>> df3 = df1.union(df2)
        >>> df3.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  1|    A|
        |  2|    B|
        |  3|    C|
        |  4|    D|
        +---+-----+

        Example 2: Combining two DataFrames with different schemas

        >>> from pyspark.sql.functions import lit
        >>> df1 = spark.createDataFrame([(100001, 1), (100002, 2)], schema="id LONG, money INT")
        >>> df2 = spark.createDataFrame([(3, 100003), (4, 100003)], schema="money INT, id LONG")
        >>> df1 = df1.withColumn("age", lit(30))
        >>> df2 = df2.withColumn("age", lit(40))
        >>> df3 = df1.union(df2)
        >>> df3.show()
        +------+------+---+
        |    id| money|age|
        +------+------+---+
        |100001|     1| 30|
        |100002|     2| 30|
        |     3|100003| 40|
        |     4|100003| 40|
        +------+------+---+

        Example 3: Combining two DataFrames with mismatched columns

        >>> df1 = spark.createDataFrame([(1, 2)], ["A", "B"])
        >>> df2 = spark.createDataFrame([(3, 4)], ["C", "D"])
        >>> df3 = df1.union(df2)
        >>> df3.show()
        +---+---+
        |  A|  B|
        +---+---+
        |  1|  2|
        |  3|  4|
        +---+---+

        Example 4: Combining duplicate rows from two different DataFrames

        >>> df1 = spark.createDataFrame([(1, 'A'), (2, 'B'), (3, 'C')], ['id', 'value'])
        >>> df2 = spark.createDataFrame([(3, 'C'), (4, 'D')], ['id', 'value'])
        >>> df3 = df1.union(df2).distinct().sort("id")
        >>> df3.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  1|    A|
        |  2|    B|
        |  3|    C|
        |  4|    D|
        +---+-----+
        """
        ...

    @dispatch_df_method
    def unionAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing the union of rows in this and another
        :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be combined

        Returns
        -------
        :class:`DataFrame`
            A new :class:`DataFrame` containing combined rows from both dataframes.

        Notes
        -----
        This method combines all rows from both `DataFrame` objects with no automatic
        deduplication of elements.

        Use the `distinct()` method to perform deduplication of rows.

        :func:`unionAll` is an alias to :func:`union`

        See Also
        --------
        DataFrame.union
        """
        ...

    @dispatch_df_method
    def unionByName(self, other: "DataFrame", allowMissingColumns: bool = False) -> "DataFrame":
        """Returns a new :class:`DataFrame` containing union of rows in this and another
        :class:`DataFrame`.

        This method performs a union operation on both input DataFrames, resolving columns by
        name (rather than position). When `allowMissingColumns` is True, missing columns will
        be filled with null.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be combined.
        allowMissingColumns : bool, optional, default False
           Specify whether to allow missing columns.

           .. versionadded:: 3.1.0

        Returns
        -------
        :class:`DataFrame`
            A new :class:`DataFrame` containing the combined rows with corresponding
            columns of the two given DataFrames.

        Examples
        --------
        Example 1: Union of two DataFrames with same columns in different order.

        >>> df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
        >>> df2 = spark.createDataFrame([[4, 5, 6]], ["col1", "col2", "col0"])
        >>> df1.unionByName(df2).show()
        +----+----+----+
        |col0|col1|col2|
        +----+----+----+
        |   1|   2|   3|
        |   6|   4|   5|
        +----+----+----+

        Example 2: Union with missing columns and setting `allowMissingColumns=True`.

        >>> df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
        >>> df2 = spark.createDataFrame([[4, 5, 6]], ["col1", "col2", "col3"])
        >>> df1.unionByName(df2, allowMissingColumns=True).show()
        +----+----+----+----+
        |col0|col1|col2|col3|
        +----+----+----+----+
        |   1|   2|   3|NULL|
        |NULL|   4|   5|   6|
        +----+----+----+----+

        Example 3: Union of two DataFrames with few common columns.

        >>> df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
        >>> df2 = spark.createDataFrame([[4, 5, 6, 7]], ["col1", "col2", "col3", "col4"])
        >>> df1.unionByName(df2, allowMissingColumns=True).show()
        +----+----+----+----+----+
        |col0|col1|col2|col3|col4|
        +----+----+----+----+----+
        |   1|   2|   3|NULL|NULL|
        |NULL|   4|   5|   6|   7|
        +----+----+----+----+----+

        Example 4: Union of two DataFrames with completely different columns.

        >>> df1 = spark.createDataFrame([[0, 1, 2]], ["col0", "col1", "col2"])
        >>> df2 = spark.createDataFrame([[3, 4, 5]], ["col3", "col4", "col5"])
        >>> df1.unionByName(df2, allowMissingColumns=True).show()
        +----+----+----+----+----+----+
        |col0|col1|col2|col3|col4|col5|
        +----+----+----+----+----+----+
        |   0|   1|   2|NULL|NULL|NULL|
        |NULL|NULL|NULL|   3|   4|   5|
        +----+----+----+----+----+----+
        """
        ...

    @dispatch_df_method
    def intersect(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows only in
        both this :class:`DataFrame` and another :class:`DataFrame`.
        Note that any duplicates are removed. To preserve duplicates
        use :func:`intersectAll`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be combined.

        Returns
        -------
        :class:`DataFrame`
            Combined DataFrame.

        Notes
        -----
        This is equivalent to `INTERSECT` in SQL.

        Examples
        --------
        Example 1: Intersecting two DataFrames with the same schema

        >>> df1 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3)], ["C1", "C2"])
        >>> result_df = df1.intersect(df2).sort("C1", "C2")
        >>> result_df.show()
        +---+---+
        | C1| C2|
        +---+---+
        |  a|  1|
        |  b|  3|
        +---+---+

        Example 2: Intersecting two DataFrames with different schemas

        >>> df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
        >>> df2 = spark.createDataFrame([(2, "B"), (3, "C")], ["id", "value"])
        >>> result_df = df1.intersect(df2).sort("id", "value")
        >>> result_df.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  2|    B|
        +---+-----+

        Example 3: Intersecting all rows from two DataFrames with mismatched columns

        >>> df1 = spark.createDataFrame([(1, 2), (1, 2), (3, 4)], ["A", "B"])
        >>> df2 = spark.createDataFrame([(1, 2), (1, 2)], ["C", "D"])
        >>> result_df = df1.intersect(df2).sort("A", "B")
        >>> result_df.show()
        +---+---+
        |  A|  B|
        +---+---+
        |  1|  2|
        +---+---+
        """
        ...

    @dispatch_df_method
    def intersectAll(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in both this :class:`DataFrame`
        and another :class:`DataFrame` while preserving duplicates.

        This is equivalent to `INTERSECT ALL` in SQL. As standard in SQL, this function
        resolves columns by position (not by name).

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be combined.

        Returns
        -------
        :class:`DataFrame`
            Combined DataFrame.

        Examples
        --------
        Example 1: Intersecting two DataFrames with the same schema

        >>> df1 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3)], ["C1", "C2"])
        >>> result_df = df1.intersectAll(df2).sort("C1", "C2")
        >>> result_df.show()
        +---+---+
        | C1| C2|
        +---+---+
        |  a|  1|
        |  a|  1|
        |  b|  3|
        +---+---+

        Example 2: Intersecting two DataFrames with different schemas

        >>> df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
        >>> df2 = spark.createDataFrame([(2, "B"), (3, "C")], ["id", "value"])
        >>> result_df = df1.intersectAll(df2).sort("id", "value")
        >>> result_df.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  2|    B|
        +---+-----+

        Example 3: Intersecting all rows from two DataFrames with mismatched columns

        >>> df1 = spark.createDataFrame([(1, 2), (1, 2), (3, 4)], ["A", "B"])
        >>> df2 = spark.createDataFrame([(1, 2), (1, 2)], ["C", "D"])
        >>> result_df = df1.intersectAll(df2).sort("A", "B")
        >>> result_df.show()
        +---+---+
        |  A|  B|
        +---+---+
        |  1|  2|
        |  1|  2|
        +---+---+
        """
        ...

    @dispatch_df_method
    def subtract(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing rows in this :class:`DataFrame`
        but not in another :class:`DataFrame`.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Another :class:`DataFrame` that needs to be subtracted.

        Returns
        -------
        :class:`DataFrame`
            Subtracted DataFrame.

        Notes
        -----
        This is equivalent to `EXCEPT DISTINCT` in SQL.

        Examples
        --------
        Example 1: Subtracting two DataFrames with the same schema

        >>> df1 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3), ("c", 4)], ["C1", "C2"])
        >>> df2 = spark.createDataFrame([("a", 1), ("a", 1), ("b", 3)], ["C1", "C2"])
        >>> result_df = df1.subtract(df2)
        >>> result_df.show()
        +---+---+
        | C1| C2|
        +---+---+
        |  c|  4|
        +---+---+

        Example 2: Subtracting two DataFrames with different schemas

        >>> df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
        >>> df2 = spark.createDataFrame([(2, "B"), (3, "C")], ["id", "value"])
        >>> result_df = df1.subtract(df2)
        >>> result_df.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  1|    A|
        +---+-----+

        Example 3: Subtracting two DataFrames with mismatched columns

        >>> df1 = spark.createDataFrame([(1, 2)], ["A", "B"])
        >>> df2 = spark.createDataFrame([(1, 2)], ["C", "D"])
        >>> result_df = df1.subtract(df2)
        >>> result_df.show()
        +---+---+
        |  A|  B|
        +---+---+
        +---+---+
        """
        ...

    @dispatch_df_method
    def dropDuplicates(
        self, subset: Optional[Union[str, List[str], _NoValueType]] = _NoValue, *subset_varargs: str
    ) -> "DataFrame":
        """Return a new :class:`DataFrame` with duplicate rows removed,
        optionally only considering certain columns.

        For a static batch :class:`DataFrame`, it just drops duplicate rows. For a streaming
        :class:`DataFrame`, it will keep all data across triggers as intermediate state to drop
        duplicates rows. You can use :func:`withWatermark` to limit how late the duplicate data can
        be and the system will accordingly limit the state. In addition, data older than
        watermark will be dropped to avoid any possibility of duplicates.

        :func:`drop_duplicates` is an alias for :func:`dropDuplicates`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        .. versionchanged:: 4.0.0
            Supports variable-length argument

        Parameters
        ----------
        subset : list of column names, optional
            List of columns to use for duplicate comparison (default All columns).

        subset_varargs : optional arguments used for supporting variable-length argument.

        Returns
        -------
        :class:`DataFrame`
            DataFrame without duplicates.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([
        ...     Row(name='Alice', age=5, height=80),
        ...     Row(name='Alice', age=5, height=80),
        ...     Row(name='Alice', age=10, height=80)
        ... ])

        Deduplicate the same rows.

        >>> df.dropDuplicates().show()
        +-----+---+------+
        | name|age|height|
        +-----+---+------+
        |Alice|  5|    80|
        |Alice| 10|    80|
        +-----+---+------+

        Deduplicate values on 'name' and 'height' columns.

        >>> df.dropDuplicates('name', 'height').show()
        +-----+---+------+
        | name|age|height|
        +-----+---+------+
        |Alice|  5|    80|
        +-----+---+------+

        Deduplicate values on 'name' and 'height' columns.

        >>> df.dropDuplicates(subset=['name', 'height']).show()
        +-----+---+------+
        | name|age|height|
        +-----+---+------+
        |Alice|  5|    80|
        +-----+---+------+
        """
        ...

    @dispatch_df_method
    def dropDuplicatesWithinWatermark(
        self, subset: Optional[Union[str, List[str], _NoValueType]] = _NoValue, *subset_varargs: str
    ) -> "DataFrame":
        """Return a new :class:`DataFrame` with duplicate rows removed,
         optionally only considering certain columns, within watermark.

         This only works with streaming :class:`DataFrame`, and watermark for the input
         :class:`DataFrame` must be set via :func:`withWatermark`.

        For a streaming :class:`DataFrame`, this will keep all data across triggers as intermediate
        state to drop duplicated rows. The state will be kept to guarantee the semantic, "Events
        are deduplicated as long as the time distance of earliest and latest events are smaller
        than the delay threshold of watermark." Users are encouraged to set the delay threshold of
        watermark longer than max timestamp differences among duplicated events.

        Note: too late data older than watermark will be dropped.

         .. versionadded:: 3.5.0

         .. versionchanged:: 4.0.0
            Supports variable-length argument

         Parameters
         ----------
         subset : List of column names, optional
             List of columns to use for duplicate comparison (default All columns).

         subset_varargs : optional arguments used for supporting variable-length argument.

         Returns
         -------
         :class:`DataFrame`
             DataFrame without duplicates.

         Notes
         -----
         Supports Spark Connect.

         Examples
         --------
         >>> from pyspark.sql import Row
         >>> from pyspark.sql.functions import timestamp_seconds
         >>> df = spark.readStream.format("rate").load().selectExpr(
         ...     "value % 5 AS value", "timestamp")
         >>> df.select("value", df.timestamp.alias("time")).withWatermark("time", '10 minutes')
         DataFrame[value: bigint, time: timestamp]

         Deduplicate the same rows.

         >>> df.dropDuplicatesWithinWatermark() # doctest: +SKIP

         Deduplicate values on 'value' columns.

         >>> df.dropDuplicatesWithinWatermark('value')  # doctest: +SKIP

         Deduplicate values on 'value' columns.

         >>> df.dropDuplicatesWithinWatermark(subset=['value'])  # doctest: +SKIP
        """
        ...

    @dispatch_df_method
    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` omitting rows with null or NaN values.
        :func:`DataFrame.dropna` and :func:`DataFrameNaFunctions.drop` are
        aliases of each other.

        .. versionadded:: 1.3.1

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        how : str, optional, the values that can be 'any' or 'all', default 'any'.
            If 'any', drop a row if it contains any nulls.
            If 'all', drop a row only if all its values are null.
        thresh: int, optional, default None.
            If specified, drop rows that have less than `thresh` non-null values.
            This overwrites the `how` parameter.
        subset : str, tuple or list, optional
            optional list of column names to consider.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with null only rows excluded.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([
        ...     Row(age=10, height=80.0, name="Alice"),
        ...     Row(age=5, height=float("nan"), name="Bob"),
        ...     Row(age=None, height=None, name="Tom"),
        ...     Row(age=None, height=float("nan"), name=None),
        ... ])

        Example 1: Drop the row if it contains any null or NaN.

        >>> df.na.drop().show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|  80.0|Alice|
        +---+------+-----+

        Example 2: Drop the row only if all its values are null or NaN.

        >>> df.na.drop(how='all').show()
        +----+------+-----+
        | age|height| name|
        +----+------+-----+
        |  10|  80.0|Alice|
        |   5|   NaN|  Bob|
        |NULL|  NULL|  Tom|
        +----+------+-----+

        Example 3: Drop rows that have less than `thresh` non-null and non-NaN values.

        >>> df.na.drop(thresh=2).show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|  80.0|Alice|
        |  5|   NaN|  Bob|
        +---+------+-----+

        Example 4: Drop rows with null and NaN values in the specified columns.

        >>> df.na.drop(subset=['age', 'name']).show()
        +---+------+-----+
        |age|height| name|
        +---+------+-----+
        | 10|  80.0|Alice|
        |  5|   NaN|  Bob|
        +---+------+-----+
        """
        ...

    @overload
    def fillna(
        self,
        value: "LiteralType",
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = ...,
    ) -> "DataFrame":
        ...

    @overload
    def fillna(self, value: Dict[str, "LiteralType"]) -> "DataFrame":
        ...

    @dispatch_df_method
    def fillna(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` which null values are filled with new value.
        :func:`DataFrame.fillna` and :func:`DataFrameNaFunctions.fill` are
        aliases of each other.

        .. versionadded:: 1.3.1

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        value : int, float, string, bool or dict, the value to replace null values with.
            If the value is a dict, then `subset` is ignored and `value` must be a mapping
            from column name (string) to replacement value. The replacement value must be
            an int, float, boolean, or string.
        subset : str, tuple or list, optional
            optional list of column names to consider.
            Columns specified in subset that do not have matching data types are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with replaced null values.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (10, 80.5, "Alice", None),
        ...     (5, None, "Bob", None),
        ...     (None, None, "Tom", None),
        ...     (None, None, None, True)],
        ...     schema=["age", "height", "name", "bool"])

        Example 1: Fill all null values with 50 for numeric columns.

        >>> df.na.fill(50).show()
        +---+------+-----+----+
        |age|height| name|bool|
        +---+------+-----+----+
        | 10|  80.5|Alice|NULL|
        |  5|  50.0|  Bob|NULL|
        | 50|  50.0|  Tom|NULL|
        | 50|  50.0| NULL|true|
        +---+------+-----+----+

        Example 2: Fill all null values with ``False`` for boolean columns.

        >>> df.na.fill(False).show()
        +----+------+-----+-----+
        | age|height| name| bool|
        +----+------+-----+-----+
        |  10|  80.5|Alice|false|
        |   5|  NULL|  Bob|false|
        |NULL|  NULL|  Tom|false|
        |NULL|  NULL| NULL| true|
        +----+------+-----+-----+

        Example 3: Fill all null values with to 50 and "unknown" for
            'age' and 'name' column respectively.

        >>> df.na.fill({'age': 50, 'name': 'unknown'}).show()
        +---+------+-------+----+
        |age|height|   name|bool|
        +---+------+-------+----+
        | 10|  80.5|  Alice|NULL|
        |  5|  NULL|    Bob|NULL|
        | 50|  NULL|    Tom|NULL|
        | 50|  NULL|unknown|true|
        +---+------+-------+----+

        Example 4: Fill all null values with "Spark" for 'name' column.

        >>> df.na.fill(value = 'Spark', subset = 'name').show()
        +----+------+-----+----+
        | age|height| name|bool|
        +----+------+-----+----+
        |  10|  80.5|Alice|NULL|
        |   5|  NULL|  Bob|NULL|
        |NULL|  NULL|  Tom|NULL|
        |NULL|  NULL|Spark|true|
        +----+------+-----+----+
        """
        ...

    @overload
    def replace(
        self,
        to_replace: "LiteralType",
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> "DataFrame":
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: List["OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> "DataFrame":
        ...

    @overload
    def replace(
        self,
        to_replace: Dict["LiteralType", "OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> "DataFrame":
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> "DataFrame":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def replace(
        self,
        to_replace: Union[
            "LiteralType", List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]
        ],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame` replacing a value with another value.
        :func:`DataFrame.replace` and :func:`DataFrameNaFunctions.replace` are
        aliases of each other.
        Values to_replace and value must have the same type and can only be numerics, booleans,
        or strings. Value can have None. When replacing, the new value will be cast
        to the type of the existing column.
        For numeric replacements all values to be replaced should have unique
        floating point representation. In case of conflicts (for example with `{42: -1, 42.0: 1}`)
        and arbitrary replacement will be used.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        to_replace : bool, int, float, string, list or dict, the value to be replaced.
            If the value is a dict, then `value` is ignored or can be omitted, and `to_replace`
            must be a mapping between a value and a replacement.
        value : bool, int, float, string or None, optional
            The replacement value must be a bool, int, float, string or None. If `value` is a
            list, `value` should be of the same length and type as `to_replace`.
            If `value` is a scalar and `to_replace` is a sequence, then `value` is
            used as a replacement for each item in `to_replace`.
        subset : list, optional
            optional list of column names to consider.
            Columns specified in subset that do not have matching data types are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with replaced values.

        Examples
        --------
        >>> df = spark.createDataFrame([
        ...     (10, 80, "Alice"),
        ...     (5, None, "Bob"),
        ...     (None, 10, "Tom"),
        ...     (None, None, None)],
        ...     schema=["age", "height", "name"])

        Example 1: Replace 10 to 20 in all columns.

        >>> df.na.replace(10, 20).show()
        +----+------+-----+
        | age|height| name|
        +----+------+-----+
        |  20|    80|Alice|
        |   5|  NULL|  Bob|
        |NULL|    20|  Tom|
        |NULL|  NULL| NULL|
        +----+------+-----+

        Example 2: Replace 'Alice' to null in all columns.

        >>> df.na.replace('Alice', None).show()
        +----+------+----+
        | age|height|name|
        +----+------+----+
        |  10|    80|NULL|
        |   5|  NULL| Bob|
        |NULL|    10| Tom|
        |NULL|  NULL|NULL|
        +----+------+----+

        Example 3: Replace 'Alice' to 'A', and 'Bob' to 'B' in the 'name' column.

        >>> df.na.replace(['Alice', 'Bob'], ['A', 'B'], 'name').show()
        +----+------+----+
        | age|height|name|
        +----+------+----+
        |  10|    80|   A|
        |   5|  NULL|   B|
        |NULL|    10| Tom|
        |NULL|  NULL|NULL|
        +----+------+----+

        Example 4: Replace 10 to 20 in the 'name' column.

        >>> df.na.replace(10, 18, 'age').show()
        +----+------+-----+
        | age|height| name|
        +----+------+-----+
        |  18|    80|Alice|
        |   5|  NULL|  Bob|
        |NULL|    10|  Tom|
        |NULL|  NULL| NULL|
        +----+------+-----+
        """
        ...

    @overload
    def approxQuantile(
        self,
        col: str,
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[float]:
        ...

    @overload
    def approxQuantile(
        self,
        col: Union[List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[List[float]]:
        ...

    @dispatch_df_method
    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        """
        Calculates the approximate quantiles of numerical columns of a
        :class:`DataFrame`.

        The result of this algorithm has the following deterministic bound:
        If the :class:`DataFrame` has N elements and if we request the quantile at
        probability `p` up to error `err`, then the algorithm will return
        a sample `x` from the :class:`DataFrame` so that the *exact* rank of `x` is
        close to (p * N). More precisely,

          floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).

        This method implements a variation of the Greenwald-Khanna
        algorithm (with some speed optimizations). The algorithm was first
        present in [[https://doi.org/10.1145/375663.375670
        Space-efficient Online Computation of Quantile Summaries]]
        by Greenwald and Khanna.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col: str, tuple or list
            Can be a single column name, or a list of names for multiple columns.

            .. versionchanged:: 2.2.0
               Added support for multiple columns.
        probabilities : list or tuple of floats
            a list of quantile probabilities
            Each number must be a float in the range [0, 1].
            For example 0.0 is the minimum, 0.5 is the median, 1.0 is the maximum.
        relativeError : float
            The relative target precision to achieve
            (>= 0). If set to zero, the exact quantiles are computed, which
            could be very expensive. Note that values greater than 1 are
            accepted but gives the same result as 1.

        Returns
        -------
        list
            the approximate quantiles at the given probabilities.

            * If the input `col` is a string, the output is a list of floats.

            * If the input `col` is a list or tuple of strings, the output is also a
                list, but each element in it is a list of floats, i.e., the output
                is a list of list of floats.

        Notes
        -----
        Null values will be ignored in numerical columns before calculation.
        For columns only containing null values, an empty list is returned.

        Examples
        --------
        Example 1: Calculating quantiles for a single column

        >>> data = [(1,), (2,), (3,), (4,), (5,)]
        >>> df = spark.createDataFrame(data, ["values"])
        >>> quantiles = df.approxQuantile("values", [0.0, 0.5, 1.0], 0.05)
        >>> quantiles
        [1.0, 3.0, 5.0]

        Example 2: Calculating quantiles for multiple columns

        >>> data = [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
        >>> df = spark.createDataFrame(data, ["col1", "col2"])
        >>> quantiles = df.approxQuantile(["col1", "col2"], [0.0, 0.5, 1.0], 0.05)
        >>> quantiles
        [[1.0, 3.0, 5.0], [10.0, 30.0, 50.0]]

        Example 3: Handling null values

        >>> data = [(1,), (None,), (3,), (4,), (None,)]
        >>> df = spark.createDataFrame(data, ["values"])
        >>> quantiles = df.approxQuantile("values", [0.0, 0.5, 1.0], 0.05)
        >>> quantiles
        [1.0, 3.0, 4.0]

        Example 4: Calculating quantiles with low precision

        >>> data = [(1,), (2,), (3,), (4,), (5,)]
        >>> df = spark.createDataFrame(data, ["values"])
        >>> quantiles = df.approxQuantile("values", [0.0, 0.2, 1.0], 0.1)
        >>> quantiles
        [1.0, 1.0, 5.0]
        """
        ...

    @dispatch_df_method
    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        """
        Calculates the correlation of two columns of a :class:`DataFrame` as a double value.
        Currently only supports the Pearson Correlation Coefficient.
        :func:`DataFrame.corr` and :func:`DataFrameStatFunctions.corr` are aliases of each other.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col1 : str
            The name of the first column
        col2 : str
            The name of the second column
        method : str, optional
            The correlation method. Currently only supports "pearson"

        Returns
        -------
        float
            Pearson Correlation Coefficient of two columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(1, 12), (10, 1), (19, 8)], ["c1", "c2"])
        >>> df.corr("c1", "c2")
        -0.3592106040535498
        >>> df = spark.createDataFrame([(11, 12), (10, 11), (9, 10)], ["small", "bigger"])
        >>> df.corr("small", "bigger")
        1.0

        """
        ...

    @dispatch_df_method
    def cov(self, col1: str, col2: str) -> float:
        """
        Calculate the sample covariance for the given columns, specified by their names, as a
        double value. :func:`DataFrame.cov` and :func:`DataFrameStatFunctions.cov` are aliases.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col1 : str
            The name of the first column
        col2 : str
            The name of the second column

        Returns
        -------
        float
            Covariance of two columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(1, 12), (10, 1), (19, 8)], ["c1", "c2"])
        >>> df.cov("c1", "c2")
        -18.0
        >>> df = spark.createDataFrame([(11, 12), (10, 11), (9, 10)], ["small", "bigger"])
        >>> df.cov("small", "bigger")
        1.0

        """
        ...

    @dispatch_df_method
    def crosstab(self, col1: str, col2: str) -> "DataFrame":
        """
        Computes a pair-wise frequency table of the given columns. Also known as a contingency
        table.
        The first column of each row will be the distinct values of `col1` and the column names
        will be the distinct values of `col2`. The name of the first column will be `$col1_$col2`.
        Pairs that have no occurrences will have zero as their counts.
        :func:`DataFrame.crosstab` and :func:`DataFrameStatFunctions.crosstab` are aliases.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col1 : str
            The name of the first column. Distinct items will make the first item of
            each row.
        col2 : str
            The name of the second column. Distinct items will make the column names
            of the :class:`DataFrame`.

        Returns
        -------
        :class:`DataFrame`
            Frequency matrix of two columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(1, 11), (1, 11), (3, 10), (4, 8), (4, 8)], ["c1", "c2"])
        >>> df.crosstab("c1", "c2").sort("c1_c2").show()
        +-----+---+---+---+
        |c1_c2| 10| 11|  8|
        +-----+---+---+---+
        |    1|  0|  2|  0|
        |    3|  1|  0|  0|
        |    4|  0|  0|  2|
        +-----+---+---+---+

        """
        ...

    @dispatch_df_method
    def freqItems(
        self, cols: Union[List[str], Tuple[str]], support: Optional[float] = None
    ) -> "DataFrame":
        """
        Finding frequent items for columns, possibly with false positives. Using the
        frequent element count algorithm described in
        "https://doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou".
        :func:`DataFrame.freqItems` and :func:`DataFrameStatFunctions.freqItems` are aliases.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : list or tuple
            Names of the columns to calculate frequent items for as a list or tuple of
            strings.
        support : float, optional
            The frequency with which to consider an item 'frequent'. Default is 1%.
            The support must be greater than 1e-4.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with frequent items.

        Notes
        -----
        This function is meant for exploratory data analysis, as we make no
        guarantee about the backward compatibility of the schema of the resulting
        :class:`DataFrame`.

        Examples
        --------
        >>> df = spark.createDataFrame([(1, 11), (1, 11), (3, 10), (4, 8), (4, 8)], ["c1", "c2"])
        >>> df.freqItems(["c1", "c2"]).show()  # doctest: +SKIP
        +------------+------------+
        |c1_freqItems|c2_freqItems|
        +------------+------------+
        |   [4, 1, 3]| [8, 11, 10]|
        +------------+------------+
        """
        ...

    @dispatch_df_method
    def _ipython_key_completions_(self) -> List[str]:
        """Returns the names of columns in this :class:`DataFrame`.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df._ipython_key_completions_()
        ['age', 'name']

        Would return illegal identifiers.
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age 1", "name?1"])
        >>> df._ipython_key_completions_()
        ['age 1', 'name?1']
        """
        ...

    @dispatch_df_method
    def withColumns(self, *colsMap: Dict[str, Column]) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` by adding multiple columns or replacing the
        existing columns that have the same names.

        The colsMap is a map of column name and column, the column must only refer to attributes
        supplied by this Dataset. It is an error to add columns that refer to some other Dataset.

        .. versionadded:: 3.3.0
           Added support for multiple columns adding

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        colsMap : dict
            a dict of column name and :class:`Column`. Currently, only a single map is supported.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with new or replaced columns.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.withColumns({'age2': df.age + 2, 'age3': df.age + 3}).show()
        +---+-----+----+----+
        |age| name|age2|age3|
        +---+-----+----+----+
        |  2|Alice|   4|   5|
        |  5|  Bob|   7|   8|
        +---+-----+----+----+
        """
        ...

    @dispatch_df_method
    def withColumn(self, colName: str, col: Column) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` by adding a column or replacing the
        existing column that has the same name.

        The column expression must be an expression over this :class:`DataFrame`; attempting to add
        a column from some other :class:`DataFrame` will raise an error.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        colName : str
            string, name of the new column.
        col : :class:`Column`
            a :class:`Column` expression for the new column.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with new or replaced column.

        Notes
        -----
        This method introduces a projection internally. Therefore, calling it multiple
        times, for instance, via loops in order to add multiple columns can generate big
        plans which can cause performance issues and even `StackOverflowException`.
        To avoid this, use :func:`select` with multiple columns at once.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df.withColumn('age2', df.age + 2).show()
        +---+-----+----+
        |age| name|age2|
        +---+-----+----+
        |  2|Alice|   4|
        |  5|  Bob|   7|
        +---+-----+----+
        """
        ...

    @dispatch_df_method
    def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` by renaming an existing column.
        This is a no-op if the schema doesn't contain the given column name.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        existing : str
            The name of the existing column to be renamed.
        new : str
            The new name to be assigned to the column.

        Returns
        -------
        :class:`DataFrame`
            A new DataFrame with renamed column.

        See Also
        --------
        DataFrame.withColumnsRenamed

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])

        Example 1: Rename a single column

        >>> df.withColumnRenamed("age", "age2").show()
        +----+-----+
        |age2| name|
        +----+-----+
        |   2|Alice|
        |   5|  Bob|
        +----+-----+

        Example 2: Rename a column that does not exist (no-op)

        >>> df.withColumnRenamed("non_existing", "new_name").show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+

        Example 3: Rename multiple columns

        >>> df.withColumnRenamed("age", "age2").withColumnRenamed("name", "name2").show()
        +----+-----+
        |age2|name2|
        +----+-----+
        |   2|Alice|
        |   5|  Bob|
        +----+-----+
        """
        ...

    @dispatch_df_method
    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "DataFrame":
        """
        Returns a new :class:`DataFrame` by renaming multiple columns.
        This is a no-op if the schema doesn't contain the given column names.

        .. versionadded:: 3.4.0
           Added support for multiple columns renaming

        Parameters
        ----------
        colsMap : dict
            A dict of existing column names and corresponding desired column names.
            Currently, only a single map is supported.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with renamed columns.

        See Also
        --------
        DataFrame.withColumnRenamed

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])

        Example 1: Rename a single column

        >>> df.withColumnsRenamed({"age": "age2"}).show()
        +----+-----+
        |age2| name|
        +----+-----+
        |   2|Alice|
        |   5|  Bob|
        +----+-----+

        Example 2: Rename multiple columns

        >>> df.withColumnsRenamed({"age": "age2", "name": "name2"}).show()
        +----+-----+
        |age2|name2|
        +----+-----+
        |   2|Alice|
        |   5|  Bob|
        +----+-----+

        Example 3: Rename non-existing column (no-op)

        >>> df.withColumnsRenamed({"non_existing": "new_name"}).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+

        Example 4: Rename with an empty dictionary (no-op)

        >>> df.withColumnsRenamed({}).show()
        +---+-----+
        |age| name|
        +---+-----+
        |  2|Alice|
        |  5|  Bob|
        +---+-----+
        """
        ...

    @dispatch_df_method
    def withMetadata(self, columnName: str, metadata: Dict[str, Any]) -> "DataFrame":
        """Returns a new :class:`DataFrame` by updating an existing column with metadata.

        .. versionadded:: 3.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        columnName : str
            string, name of the existing column to update the metadata.
        metadata : dict
            dict, new metadata to be assigned to df.schema[columnName].metadata

        Returns
        -------
        :class:`DataFrame`
            DataFrame with updated metadata column.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        >>> df_meta = df.withMetadata('age', {'foo': 'bar'})
        >>> df_meta.schema['age'].metadata
        {'foo': 'bar'}
        """
        ...

    @overload
    def drop(self, cols: "ColumnOrName") -> "DataFrame":
        ...

    @overload
    def drop(self, *cols: str) -> "DataFrame":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def drop(self, *cols: "ColumnOrName") -> "DataFrame":
        """
        Returns a new :class:`DataFrame` without specified columns.
        This is a no-op if the schema doesn't contain the given column name(s).

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols: str or :class:`Column`
            A name of the column, or the :class:`Column` to be dropped.

        Returns
        -------
        :class:`DataFrame`
            A new :class:`DataFrame` without the specified columns.

        Notes
        -----
        - When an input is a column name, it is treated literally without further interpretation.
          Otherwise, it will try to match the equivalent expression.
          So dropping a column by its name `drop(colName)` has a different semantic
          with directly dropping the column `drop(col(colName))`.

        Examples
        --------
        Example 1: Drop a column by name.

        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.drop('age').show()
        +-----+
        | name|
        +-----+
        |  Tom|
        |Alice|
        |  Bob|
        +-----+

        Example 2: Drop a column by :class:`Column` object.

        >>> df.drop(df.age).show()
        +-----+
        | name|
        +-----+
        |  Tom|
        |Alice|
        |  Bob|
        +-----+

        Example 3: Drop the column that joined both DataFrames on.

        >>> df2 = spark.createDataFrame([(80, "Tom"), (85, "Bob")], ["height", "name"])
        >>> df.join(df2, df.name == df2.name).drop('name').sort('age').show()
        +---+------+
        |age|height|
        +---+------+
        | 14|    80|
        | 16|    85|
        +---+------+

        >>> df3 = df.join(df2)
        >>> df3.show()
        +---+-----+------+----+
        |age| name|height|name|
        +---+-----+------+----+
        | 14|  Tom|    80| Tom|
        | 14|  Tom|    85| Bob|
        | 23|Alice|    80| Tom|
        | 23|Alice|    85| Bob|
        | 16|  Bob|    80| Tom|
        | 16|  Bob|    85| Bob|
        +---+-----+------+----+

        Example 4: Drop two column by the same name.

        >>> df3.drop("name").show()
        +---+------+
        |age|height|
        +---+------+
        | 14|    80|
        | 14|    85|
        | 23|    80|
        | 23|    85|
        | 16|    80|
        | 16|    85|
        +---+------+

        Example 5: Can not drop col('name') due to ambiguous reference.

        >>> from pyspark.sql import functions as sf
        >>> df3.drop(sf.col("name")).show()
        Traceback (most recent call last):
        ...
        pyspark.errors.exceptions.captured.AnalysisException: [AMBIGUOUS_REFERENCE] Reference...

        Example 6: Can not find a column matching the expression "a.b.c".

        >>> from pyspark.sql import functions as sf
        >>> df4 = df.withColumn("a.b.c", sf.lit(1))
        >>> df4.show()
        +---+-----+-----+
        |age| name|a.b.c|
        +---+-----+-----+
        | 14|  Tom|    1|
        | 23|Alice|    1|
        | 16|  Bob|    1|
        +---+-----+-----+

        >>> df4.drop("a.b.c").show()
        +---+-----+
        |age| name|
        +---+-----+
        | 14|  Tom|
        | 23|Alice|
        | 16|  Bob|
        +---+-----+

        >>> df4.drop(sf.col("a.b.c")).show()
        +---+-----+-----+
        |age| name|a.b.c|
        +---+-----+-----+
        | 14|  Tom|    1|
        | 23|Alice|    1|
        | 16|  Bob|    1|
        +---+-----+-----+
        """
        ...

    @dispatch_df_method
    def toDF(self, *cols: str) -> "DataFrame":
        """Returns a new :class:`DataFrame` that with new specified column names

        .. versionadded:: 1.6.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        *cols : tuple
            a tuple of string new column name. The length of the
            list needs to be the same as the number of columns in the initial
            :class:`DataFrame`

        Returns
        -------
        :class:`DataFrame`
            DataFrame with new column names.

        Examples
        --------
        >>> df = spark.createDataFrame([(14, "Tom"), (23, "Alice"),
        ...     (16, "Bob")], ["age", "name"])
        >>> df.toDF('f1', 'f2').show()
        +---+-----+
        | f1|   f2|
        +---+-----+
        | 14|  Tom|
        | 23|Alice|
        | 16|  Bob|
        +---+-----+
        """
        ...

    @dispatch_df_method
    def transform(self, func: Callable[..., "DataFrame"], *args: Any, **kwargs: Any) -> "DataFrame":
        """Returns a new :class:`DataFrame`. Concise syntax for chaining custom transformations.

        .. versionadded:: 3.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        func : function
            a function that takes and returns a :class:`DataFrame`.
        *args
            Positional arguments to pass to func.

            .. versionadded:: 3.3.0
        **kwargs
            Keyword arguments to pass to func.

            .. versionadded:: 3.3.0

        Returns
        -------
        :class:`DataFrame`
            Transformed DataFrame.

        Examples
        --------
        >>> from pyspark.sql.functions import col
        >>> df = spark.createDataFrame([(1, 1.0), (2, 2.0)], ["int", "float"])
        >>> def cast_all_to_int(input_df):
        ...     return input_df.select([col(col_name).cast("int") for col_name in input_df.columns])
        ...
        >>> def sort_columns_asc(input_df):
        ...     return input_df.select(*sorted(input_df.columns))
        ...
        >>> df.transform(cast_all_to_int).transform(sort_columns_asc).show()
        +-----+---+
        |float|int|
        +-----+---+
        |    1|  1|
        |    2|  2|
        +-----+---+

        >>> def add_n(input_df, n):
        ...     return input_df.select([(col(col_name) + n).alias(col_name)
        ...                             for col_name in input_df.columns])
        >>> df.transform(add_n, 1).transform(add_n, n=10).show()
        +---+-----+
        |int|float|
        +---+-----+
        | 12| 12.0|
        | 13| 13.0|
        +---+-----+
        """
        ...

    @dispatch_df_method
    def sameSemantics(self, other: "DataFrame") -> bool:
        """
        Returns `True` when the logical query plans inside both :class:`DataFrame`\\s are equal and
        therefore return the same results.

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        The equality comparison here is simplified by tolerating the cosmetic differences
        such as attribute names.

        This API can compare both :class:`DataFrame`\\s very fast but can still return
        `False` on the :class:`DataFrame` that return the same results, for instance, from
        different plans. Such false negative semantic can be useful when caching as an example.

        This API is a developer API.

        Parameters
        ----------
        other : :class:`DataFrame`
            The other DataFrame to compare against.

        Returns
        -------
        bool
            Whether these two DataFrames are similar.

        Examples
        --------
        >>> df1 = spark.range(10)
        >>> df2 = spark.range(10)
        >>> df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col1", df2.id * 2))
        True
        >>> df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col1", df2.id + 2))
        False
        >>> df1.withColumn("col1", df1.id * 2).sameSemantics(df2.withColumn("col0", df2.id * 2))
        True
        """
        ...

    @dispatch_df_method
    def semanticHash(self) -> int:
        """
        Returns a hash code of the logical query plan against this :class:`DataFrame`.

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        Unlike the standard hash code, the hash is calculated against the query plan
        simplified by tolerating the cosmetic differences such as attribute names.

        This API is a developer API.

        Returns
        -------
        int
            Hash value.

        Examples
        --------
        >>> spark.range(10).selectExpr("id as col0").semanticHash()  # doctest: +SKIP
        1855039936
        >>> spark.range(10).selectExpr("id as col1").semanticHash()  # doctest: +SKIP
        1855039936
        """
        ...

    @dispatch_df_method
    def inputFiles(self) -> List[str]:
        """
        Returns a best-effort snapshot of the files that compose this :class:`DataFrame`.
        This method simply asks each constituent BaseRelation for its respective files and
        takes the union of all results. Depending on the source relations, this may not find
        all input files. Duplicates are removed.

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Returns
        -------
        list
            List of file paths.

        Examples
        --------
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory(prefix="inputFiles") as d:
        ...     # Write a single-row DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).repartition(1).write.json(d, mode="overwrite")
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     df = spark.read.format("json").load(d)
        ...
        ...     # Returns the number of input files.
        ...     len(df.inputFiles())
        1
        """
        ...

    @dispatch_df_method
    def where(self, condition: "ColumnOrName") -> "DataFrame":
        """
        :func:`where` is an alias for :func:`filter`.

        .. versionadded:: 1.3.0
        """
        ...

    # Two aliases below were added for pandas compatibility many years ago.
    # There are too many differences compared to pandas and we cannot just
    # make it "compatible" by adding aliases. Therefore, we stop adding such
    # aliases as of Spark 3.0. Two methods below remain just
    # for legacy users currently.
    @overload
    def groupby(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":
        ...

    @overload
    def groupby(self, __cols: Union[List[Column], List[str], List[int]]) -> "GroupedData":
        ...

    @dispatch_df_method  # type: ignore[misc]
    def groupby(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":
        """
        :func:`groupby` is an alias for :func:`groupBy`.

        .. versionadded:: 1.4.0
        """
        ...

    @dispatch_df_method
    def drop_duplicates(
        self, subset: Optional[Union[str, List[str], _NoValueType]] = _NoValue, *subset_varargs: str
    ) -> "DataFrame":
        """
        :func:`drop_duplicates` is an alias for :func:`dropDuplicates`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect

        .. versionchanged:: 4.0.0
            Supports variable-length argument
        """
        ...

    @dispatch_df_method
    def writeTo(self, table: str) -> DataFrameWriterV2:
        """
        Create a write configuration builder for v2 sources.

        This builder is used to configure and execute write operations.

        For example, to append or create or replace existing tables.

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        table : str
            Target table name to write to.

        Returns
        -------
        :class:`DataFrameWriterV2`
            DataFrameWriterV2 to use further to specify how to save the data

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
        >>> df.writeTo("catalog.db.table").append()  # doctest: +SKIP
        >>> df.writeTo(                              # doctest: +SKIP
        ...     "catalog.db.table"
        ... ).partitionedBy("col").createOrReplace()
        """
        ...

    @dispatch_df_method
    def mergeInto(self, table: str, condition: Column) -> MergeIntoWriter:
        """
        Merges a set of updates, insertions, and deletions based on a source table into
        a target table.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        table : str
            Target table name to merge into.
        condition : :class:`Column`
            The condition that determines whether a row in the target table matches one in the
            source DataFrame.

        Returns
        -------
        :class:`MergeIntoWriter`
            MergeIntoWriter to use further to specify how to merge the source DataFrame
            into the target table.

        Examples
        --------
        >>> from pyspark.sql.functions import expr
        >>> source = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["id", "name"])
        >>> (source.mergeInto("target", "id")  # doctest: +SKIP
        ...     .whenMatched().update({ "name": source.name })
        ...     .whenNotMatched().insertAll()
        ...     .whenNotMatchedBySource().delete()
        ...     .merge())

        Notes
        -----
        This method does not support streaming queries.
        """
        ...

    @dispatch_df_method
    def pandas_api(
        self, index_col: Optional[Union[str, List[str]]] = None
    ) -> "PandasOnSparkDataFrame":
        """
        Converts the existing DataFrame into a pandas-on-Spark DataFrame.

        .. versionadded:: 3.2.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        If a pandas-on-Spark DataFrame is converted to a Spark DataFrame and then back
        to pandas-on-Spark, it will lose the index information and the original index
        will be turned into a normal column.

        This is only available if Pandas is installed and available.

        Parameters
        ----------
        index_col: str or list of str, optional
            Index column of table in Spark.

        Returns
        -------
        :class:`PandasOnSparkDataFrame`

        See Also
        --------
        pyspark.pandas.frame.DataFrame.to_spark

        Examples
        --------
        >>> df = spark.createDataFrame(
        ...     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

        >>> df.pandas_api()  # doctest: +SKIP
           age   name
        0   14    Tom
        1   23  Alice
        2   16    Bob

        We can specify the index columns.

        >>> df.pandas_api(index_col="age")  # doctest: +SKIP
              name
        age
        14     Tom
        23   Alice
        16     Bob
        """
        ...

    @dispatch_df_method
    def mapInPandas(
        self,
        func: "PandasMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> "DataFrame":
        """
        Maps an iterator of batches in the current :class:`DataFrame` using a Python native
        function that is performed on pandas DataFrames both as input and output,
        and returns the result as a :class:`DataFrame`.

        This method applies the specified Python function to an iterator of
        `pandas.DataFrame`\\s, each representing a batch of rows from the original DataFrame.
        The returned iterator of `pandas.DataFrame`\\s are combined as a :class:`DataFrame`.
        The size of the function's input and output can be different. Each `pandas.DataFrame`
        size can be controlled by `spark.sql.execution.arrow.maxRecordsPerBatch`.

        .. versionadded:: 3.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        func : function
            a Python native function that takes an iterator of `pandas.DataFrame`\\s, and
            outputs an iterator of `pandas.DataFrame`\\s.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        barrier : bool, optional, default False
            Use barrier mode execution, ensuring that all Python workers in the stage will be
            launched concurrently.

            .. versionadded: 3.5.0

        profile : :class:`pyspark.resource.ResourceProfile`. The optional ResourceProfile
            to be used for mapInPandas.

            .. versionadded: 4.0.0


        Examples
        --------
        >>> df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

        Filter rows with id equal to 1:

        >>> def filter_func(iterator):
        ...     for pdf in iterator:
        ...         yield pdf[pdf.id == 1]
        ...
        >>> df.mapInPandas(filter_func, df.schema).show()  # doctest: +SKIP
        +---+---+
        | id|age|
        +---+---+
        |  1| 21|
        +---+---+

        Compute the mean age for each id:

        >>> def mean_age(iterator):
        ...     for pdf in iterator:
        ...         yield pdf.groupby("id").mean().reset_index()
        ...
        >>> df.mapInPandas(mean_age, "id: bigint, age: double").show()  # doctest: +SKIP
        +---+----+
        | id| age|
        +---+----+
        |  1|21.0|
        |  2|30.0|
        +---+----+

        Add a new column with the double of the age:

        >>> def double_age(iterator):
        ...     for pdf in iterator:
        ...         pdf["double_age"] = pdf["age"] * 2
        ...         yield pdf
        ...
        >>> df.mapInPandas(
        ...     double_age, "id: bigint, age: bigint, double_age: bigint").show()  # doctest: +SKIP
        +---+---+----------+
        | id|age|double_age|
        +---+---+----------+
        |  1| 21|        42|
        |  2| 30|        60|
        +---+---+----------+

        Set ``barrier`` to ``True`` to force the ``mapInPandas`` stage running in the
        barrier mode, it ensures all Python workers in the stage will be
        launched concurrently.

        >>> df.mapInPandas(filter_func, df.schema, barrier=True).show()  # doctest: +SKIP
        +---+---+
        | id|age|
        +---+---+
        |  1| 21|
        +---+---+

        Notes
        -----
        This API is experimental

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        DataFrame.mapInArrow
        """
        ...

    @dispatch_df_method
    def mapInArrow(
        self,
        func: "ArrowMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> "DataFrame":
        """
        Maps an iterator of batches in the current :class:`DataFrame` using a Python native
        function that is performed on `pyarrow.RecordBatch`\\s both as input and output,
        and returns the result as a :class:`DataFrame`.

        This method applies the specified Python function to an iterator of
        `pyarrow.RecordBatch`\\s, each representing a batch of rows from the original DataFrame.
        The returned iterator of `pyarrow.RecordBatch`\\s are combined as a :class:`DataFrame`.
        The size of the function's input and output can be different. Each `pyarrow.RecordBatch`
        size can be controlled by `spark.sql.execution.arrow.maxRecordsPerBatch`.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        func : function
            a Python native function that takes an iterator of `pyarrow.RecordBatch`\\s, and
            outputs an iterator of `pyarrow.RecordBatch`\\s.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        barrier : bool, optional, default False
            Use barrier mode execution, ensuring that all Python workers in the stage will be
            launched concurrently.

            .. versionadded: 3.5.0

        profile : :class:`pyspark.resource.ResourceProfile`. The optional ResourceProfile
            to be used for mapInArrow.

            .. versionadded: 4.0.0

        Examples
        --------
        >>> import pyarrow  # doctest: +SKIP
        >>> df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))
        >>> def filter_func(iterator):
        ...     for batch in iterator:
        ...         pdf = batch.to_pandas()
        ...         yield pyarrow.RecordBatch.from_pandas(pdf[pdf.id == 1])
        >>> df.mapInArrow(filter_func, df.schema).show()  # doctest: +SKIP
        +---+---+
        | id|age|
        +---+---+
        |  1| 21|
        +---+---+

        Set ``barrier`` to ``True`` to force the ``mapInArrow`` stage running in the
        barrier mode, it ensures all Python workers in the stage will be
        launched concurrently.

        >>> df.mapInArrow(filter_func, df.schema, barrier=True).show()  # doctest: +SKIP
        +---+---+
        | id|age|
        +---+---+
        |  1| 21|
        +---+---+

        Notes
        -----
        This API is unstable, and for developers.

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        DataFrame.mapInPandas
        """
        ...

    @dispatch_df_method
    def toArrow(self) -> "pa.Table":
        """
        Returns the contents of this :class:`DataFrame` as PyArrow ``pyarrow.Table``.

        This is only available if PyArrow is installed and available.

        .. versionadded:: 4.0.0

        Notes
        -----
        This method should only be used if the resulting PyArrow ``pyarrow.Table`` is
        expected to be small, as all the data is loaded into the driver's memory.

        This API is a developer API.

        Examples
        --------
        >>> df.toArrow()  # doctest: +SKIP
        pyarrow.Table
        age: int64
        name: string
        ----
        age: [[2,5]]
        name: [["Alice","Bob"]]
        """
        ...

    def toPandas(self) -> "PandasDataFrameLike":
        """
        Returns the contents of this :class:`DataFrame` as Pandas ``pandas.DataFrame``.

        This is only available if Pandas is installed and available.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        This method should only be used if the resulting Pandas ``pandas.DataFrame`` is
        expected to be small, as all the data is loaded into the driver's memory.

        Usage with ``spark.sql.execution.arrow.pyspark.enabled=True`` is experimental.

        Examples
        --------
        >>> df.toPandas()  # doctest: +SKIP
           age   name
        0    2  Alice
        1    5    Bob
        """
        ...

    @property
    def executionInfo(self) -> Optional["ExecutionInfo"]:
        """
        Returns a QueryExecution object after the query was executed.

        The queryExecution method allows to introspect information about the actual
        query execution after the successful execution. Accessing this member before
        the query execution will return None.

        If the same DataFrame is executed multiple times, the execution info will be
        overwritten by the latest operation.

        .. versionadded:: 4.0.0

        Returns
        -------
        An instance of QueryExecution or None when the value is not set yet.

        Notes
        -----
        This is an API dedicated to Spark Connect client only. With regular Spark Session, it throws
        an exception.
        """
        ...


class DataFrameNaFunctions:
    """Functionality for working with missing data in :class:`DataFrame`.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, df: DataFrame):
        self.df = df

    @dispatch_df_method
    def drop(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> DataFrame:
        ...

    drop.__doc__ = DataFrame.dropna.__doc__

    @overload
    def fill(self, value: "LiteralType", subset: Optional[List[str]] = ...) -> DataFrame:
        ...

    @overload
    def fill(self, value: Dict[str, "LiteralType"]) -> DataFrame:
        ...

    @dispatch_df_method
    def fill(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        ...

    fill.__doc__ = DataFrame.fillna.__doc__

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: List["OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> DataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: Dict["LiteralType", "OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> DataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> DataFrame:
        ...

    @dispatch_df_method  # type: ignore[misc]
    def replace(
        self,
        to_replace: Union[List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> DataFrame:
        ...

    replace.__doc__ = DataFrame.replace.__doc__


class DataFrameStatFunctions:
    """Functionality for statistic functions with :class:`DataFrame`.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, df: DataFrame):
        self.df = df

    @overload
    def approxQuantile(
        self,
        col: str,
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[float]:
        ...

    @overload
    def approxQuantile(
        self,
        col: Union[List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[List[float]]:
        ...

    @dispatch_df_method
    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        ...

    approxQuantile.__doc__ = DataFrame.approxQuantile.__doc__

    @dispatch_df_method
    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        ...

    corr.__doc__ = DataFrame.corr.__doc__

    @dispatch_df_method
    def cov(self, col1: str, col2: str) -> float:
        ...

    cov.__doc__ = DataFrame.cov.__doc__

    @dispatch_df_method
    def crosstab(self, col1: str, col2: str) -> DataFrame:
        ...

    crosstab.__doc__ = DataFrame.crosstab.__doc__

    @dispatch_df_method
    def freqItems(self, cols: List[str], support: Optional[float] = None) -> DataFrame:
        ...

    freqItems.__doc__ = DataFrame.freqItems.__doc__

    @dispatch_df_method
    def sampleBy(
        self, col: str, fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> DataFrame:
        ...

    sampleBy.__doc__ = DataFrame.sampleBy.__doc__
