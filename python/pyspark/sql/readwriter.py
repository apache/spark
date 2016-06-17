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

if sys.version >= '3':
    basestring = unicode = str

from py4j.java_gateway import JavaClass

from pyspark import RDD, since, keyword_only
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.column import _to_seq
from pyspark.sql.types import *
from pyspark.sql import utils

__all__ = ["DataFrameReader", "DataFrameWriter", "DataStreamReader", "DataStreamWriter"]


def to_str(value):
    """
    A wrapper over str(), but converts bool values to lower case strings.
    If None is given, just returns None, instead of converting it to string "None".
    """
    if isinstance(value, bool):
        return str(value).lower()
    elif value is None:
        return value
    else:
        return str(value)


class ReaderUtils(object):

    def _set_json_opts(self, schema, primitivesAsString, prefersDecimal,
                       allowComments, allowUnquotedFieldNames, allowSingleQuotes,
                       allowNumericLeadingZero, allowBackslashEscapingAnyCharacter,
                       mode, columnNameOfCorruptRecord):
        """
        Set options based on the Json optional parameters
        """
        if schema is not None:
            self.schema(schema)
        if primitivesAsString is not None:
            self.option("primitivesAsString", primitivesAsString)
        if prefersDecimal is not None:
            self.option("prefersDecimal", prefersDecimal)
        if allowComments is not None:
            self.option("allowComments", allowComments)
        if allowUnquotedFieldNames is not None:
            self.option("allowUnquotedFieldNames", allowUnquotedFieldNames)
        if allowSingleQuotes is not None:
            self.option("allowSingleQuotes", allowSingleQuotes)
        if allowNumericLeadingZero is not None:
            self.option("allowNumericLeadingZero", allowNumericLeadingZero)
        if allowBackslashEscapingAnyCharacter is not None:
            self.option("allowBackslashEscapingAnyCharacter", allowBackslashEscapingAnyCharacter)
        if mode is not None:
            self.option("mode", mode)
        if columnNameOfCorruptRecord is not None:
            self.option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)

    def _set_csv_opts(self, schema, sep, encoding, quote, escape,
                      comment, header, inferSchema, ignoreLeadingWhiteSpace,
                      ignoreTrailingWhiteSpace, nullValue, nanValue, positiveInf, negativeInf,
                      dateFormat, maxColumns, maxCharsPerColumn, mode):
        """
        Set options based on the CSV optional parameters
        """
        if schema is not None:
            self.schema(schema)
        if sep is not None:
            self.option("sep", sep)
        if encoding is not None:
            self.option("encoding", encoding)
        if quote is not None:
            self.option("quote", quote)
        if escape is not None:
            self.option("escape", escape)
        if comment is not None:
            self.option("comment", comment)
        if header is not None:
            self.option("header", header)
        if inferSchema is not None:
            self.option("inferSchema", inferSchema)
        if ignoreLeadingWhiteSpace is not None:
            self.option("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace)
        if ignoreTrailingWhiteSpace is not None:
            self.option("ignoreTrailingWhiteSpace", ignoreTrailingWhiteSpace)
        if nullValue is not None:
            self.option("nullValue", nullValue)
        if nanValue is not None:
            self.option("nanValue", nanValue)
        if positiveInf is not None:
            self.option("positiveInf", positiveInf)
        if negativeInf is not None:
            self.option("negativeInf", negativeInf)
        if dateFormat is not None:
            self.option("dateFormat", dateFormat)
        if maxColumns is not None:
            self.option("maxColumns", maxColumns)
        if maxCharsPerColumn is not None:
            self.option("maxCharsPerColumn", maxCharsPerColumn)
        if mode is not None:
            self.option("mode", mode)


class DataFrameReader(ReaderUtils):
    """
    Interface used to load a :class:`DataFrame` from external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`spark.read`
    to access this.

    .. versionadded:: 1.4
    """

    def __init__(self, spark):
        self._jreader = spark._ssql_ctx.read()
        self._spark = spark

    def _df(self, jdf):
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(jdf, self._spark)

    @since(1.4)
    def format(self, source):
        """Specifies the input data source format.

        :param source: string, name of the data source, e.g. 'json', 'parquet'.

        >>> df = spark.read.format('json').load('python/test_support/sql/people.json')
        >>> df.dtypes
        [('age', 'bigint'), ('name', 'string')]

        """
        self._jreader = self._jreader.format(source)
        return self

    @since(1.4)
    def schema(self, schema):
        """Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        :param schema: a StructType object
        """
        if not isinstance(schema, StructType):
            raise TypeError("schema should be StructType")
        jschema = self._spark._ssql_ctx.parseDataType(schema.json())
        self._jreader = self._jreader.schema(jschema)
        return self

    @since(1.5)
    def option(self, key, value):
        """Adds an input option for the underlying data source.
        """
        self._jreader = self._jreader.option(key, to_str(value))
        return self

    @since(1.4)
    def options(self, **options):
        """Adds input options for the underlying data source.
        """
        for k in options:
            self._jreader = self._jreader.option(k, to_str(options[k]))
        return self

    @since(1.4)
    def load(self, path=None, format=None, schema=None, **options):
        """Loads data from a data source and returns it as a :class`DataFrame`.

        :param path: optional string or a list of string for file-system backed data sources.
        :param format: optional string for format of the data source. Default to 'parquet'.
        :param schema: optional :class:`StructType` for the input schema.
        :param options: all other string options

        >>> df = spark.read.load('python/test_support/sql/parquet_partitioned', opt1=True,
        ...     opt2=1, opt3='str')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]

        >>> df = spark.read.format('json').load(['python/test_support/sql/people.json',
        ...     'python/test_support/sql/people1.json'])
        >>> df.dtypes
        [('age', 'bigint'), ('aka', 'string'), ('name', 'string')]
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if path is not None:
            if type(path) != list:
                path = [path]
            return self._df(self._jreader.load(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        else:
            return self._df(self._jreader.load())

    @since(1.4)
    def json(self, path, schema=None, primitivesAsString=None, prefersDecimal=None,
             allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None,
             allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None,
             mode=None, columnNameOfCorruptRecord=None):
        """
        Loads a JSON file (one object per line) or an RDD of Strings storing JSON objects
        (one object per record) and returns the result as a :class`DataFrame`.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        :param path: string represents path to the JSON dataset,
                     or RDD of Strings storing JSON objects.
        :param schema: an optional :class:`StructType` for the input schema.
        :param primitivesAsString: infers all primitive values as a string type. If None is set,
                                   it uses the default value, ``false``.
        :param prefersDecimal: infers all floating-point values as a decimal type. If the values
                               do not fit in decimal, then it infers them as doubles. If None is
                               set, it uses the default value, ``false``.
        :param allowComments: ignores Java/C++ style comment in JSON records. If None is set,
                              it uses the default value, ``false``.
        :param allowUnquotedFieldNames: allows unquoted JSON field names. If None is set,
                                        it uses the default value, ``false``.
        :param allowSingleQuotes: allows single quotes in addition to double quotes. If None is
                                        set, it uses the default value, ``true``.
        :param allowNumericLeadingZero: allows leading zeros in numbers (e.g. 00012). If None is
                                        set, it uses the default value, ``false``.
        :param allowBackslashEscapingAnyCharacter: allows accepting quoting of all character
                                                   using backslash quoting mechanism. If None is
                                                   set, it uses the default value, ``false``.
        :param mode: allows a mode for dealing with corrupt records during parsing. If None is
                     set, it uses the default value, ``PERMISSIVE``.

                *  ``PERMISSIVE`` : sets other fields to ``null`` when it meets a corrupted \
                  record and puts the malformed string into a new field configured by \
                 ``columnNameOfCorruptRecord``. When a schema is set by user, it sets \
                 ``null`` for extra fields.
                *  ``DROPMALFORMED`` : ignores the whole corrupted records.
                *  ``FAILFAST`` : throws an exception when it meets corrupted records.

        :param columnNameOfCorruptRecord: allows renaming the new field having malformed string
                                          created by ``PERMISSIVE`` mode. This overrides
                                          ``spark.sql.columnNameOfCorruptRecord``. If None is set,
                                          it uses the value specified in
                                          ``spark.sql.columnNameOfCorruptRecord``.

        >>> df1 = spark.read.json('python/test_support/sql/people.json')
        >>> df1.dtypes
        [('age', 'bigint'), ('name', 'string')]
        >>> rdd = sc.textFile('python/test_support/sql/people.json')
        >>> df2 = spark.read.json(rdd)
        >>> df2.dtypes
        [('age', 'bigint'), ('name', 'string')]

        """
        self._set_json_opts(schema, primitivesAsString, prefersDecimal,
                            allowComments, allowUnquotedFieldNames, allowSingleQuotes,
                            allowNumericLeadingZero, allowBackslashEscapingAnyCharacter,
                            mode, columnNameOfCorruptRecord)
        if isinstance(path, basestring):
            path = [path]
        if type(path) == list:
            return self._df(self._jreader.json(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        elif isinstance(path, RDD):
            def func(iterator):
                for x in iterator:
                    if not isinstance(x, basestring):
                        x = unicode(x)
                    if isinstance(x, unicode):
                        x = x.encode("utf-8")
                    yield x
            keyed = path.mapPartitions(func)
            keyed._bypass_serializer = True
            jrdd = keyed._jrdd.map(self._spark._jvm.BytesToString())
            return self._df(self._jreader.json(jrdd))
        else:
            raise TypeError("path can be only string or RDD")

    @since(1.4)
    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.

        :param tableName: string, name of the table.

        >>> df = spark.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.createOrReplaceTempView('tmpTable')
        >>> spark.read.table('tmpTable').dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        return self._df(self._jreader.table(tableName))

    @since(1.4)
    def parquet(self, *paths):
        """Loads a Parquet file, returning the result as a :class:`DataFrame`.

        You can set the following Parquet-specific option(s) for reading Parquet files:
            * ``mergeSchema``: sets whether we should merge schemas collected from all \
                Parquet part-files. This will override ``spark.sql.parquet.mergeSchema``. \
                The default value is specified in ``spark.sql.parquet.mergeSchema``.

        >>> df = spark.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        return self._df(self._jreader.parquet(_to_seq(self._spark._sc, paths)))

    @ignore_unicode_prefix
    @since(1.6)
    def text(self, paths):
        """
        Loads text files and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.

        Each line in the text file is a new row in the resulting DataFrame.

        :param paths: string, or list of strings, for input path(s).

        >>> df = spark.read.text('python/test_support/sql/text-test.txt')
        >>> df.collect()
        [Row(value=u'hello'), Row(value=u'this')]
        """
        if isinstance(paths, basestring):
            path = [paths]
        return self._df(self._jreader.text(self._spark._sc._jvm.PythonUtils.toSeq(path)))

    @since(2.0)
    def csv(self, path, schema=None, sep=None, encoding=None, quote=None, escape=None,
            comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None,
            ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None,
            negativeInf=None, dateFormat=None, maxColumns=None, maxCharsPerColumn=None, mode=None):
        """Loads a CSV file and returns the result as a  :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        :param path: string, or list of strings, for input path(s).
        :param schema: an optional :class:`StructType` for the input schema.
        :param sep: sets the single character as a separator for each field and value.
                    If None is set, it uses the default value, ``,``.
        :param encoding: decodes the CSV files by the given encoding type. If None is set,
                         it uses the default value, ``UTF-8``.
        :param quote: sets the single character used for escaping quoted values where the
                      separator can be part of the value. If None is set, it uses the default
                      value, ``"``. If you would like to turn off quotations, you need to set an
                      empty string.
        :param escape: sets the single character used for escaping quotes inside an already
                       quoted value. If None is set, it uses the default value, ``\``.
        :param comment: sets the single character used for skipping lines beginning with this
                        character. By default (None), it is disabled.
        :param header: uses the first line as names of columns. If None is set, it uses the
                       default value, ``false``.
        :param inferSchema: infers the input schema automatically from data. It requires one extra
                       pass over the data. If None is set, it uses the default value, ``false``.
        :param ignoreLeadingWhiteSpace: defines whether or not leading whitespaces from values
                                        being read should be skipped. If None is set, it uses
                                        the default value, ``false``.
        :param ignoreTrailingWhiteSpace: defines whether or not trailing whitespaces from values
                                         being read should be skipped. If None is set, it uses
                                         the default value, ``false``.
        :param nullValue: sets the string representation of a null value. If None is set, it uses
                          the default value, empty string.
        :param nanValue: sets the string representation of a non-number value. If None is set, it
                         uses the default value, ``NaN``.
        :param positiveInf: sets the string representation of a positive infinity value. If None
                            is set, it uses the default value, ``Inf``.
        :param negativeInf: sets the string representation of a negative infinity value. If None
                            is set, it uses the default value, ``Inf``.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to both date type and timestamp type. By default, it is None
                           which means trying to parse times and date by
                           ``java.sql.Timestamp.valueOf()`` and ``java.sql.Date.valueOf()``.
        :param maxColumns: defines a hard limit of how many columns a record can have. If None is
                           set, it uses the default value, ``20480``.
        :param maxCharsPerColumn: defines the maximum number of characters allowed for any given
                                  value being read. If None is set, it uses the default value,
                                  ``1000000``.
        :param mode: allows a mode for dealing with corrupt records during parsing. If None is
                     set, it uses the default value, ``PERMISSIVE``.

                * ``PERMISSIVE`` : sets other fields to ``null`` when it meets a corrupted record.
                    When a schema is set by user, it sets ``null`` for extra fields.
                * ``DROPMALFORMED`` : ignores the whole corrupted records.
                * ``FAILFAST`` : throws an exception when it meets corrupted records.

        >>> df = spark.read.csv('python/test_support/sql/ages.csv')
        >>> df.dtypes
        [('_c0', 'string'), ('_c1', 'string')]
        """

        self._set_csv_opts(schema, sep, encoding, quote, escape,
                           comment, header, inferSchema, ignoreLeadingWhiteSpace,
                           ignoreTrailingWhiteSpace, nullValue, nanValue, positiveInf, negativeInf,
                           dateFormat, maxColumns, maxCharsPerColumn, mode)
        if isinstance(path, basestring):
            path = [path]
        return self._df(self._jreader.csv(self._spark._sc._jvm.PythonUtils.toSeq(path)))

    @since(1.5)
    def orc(self, path):
        """Loads an ORC file, returning the result as a :class:`DataFrame`.

        .. note:: Currently ORC support is only available together with Hive support.

        >>> df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> df.dtypes
        [('a', 'bigint'), ('b', 'int'), ('c', 'int')]
        """
        return self._df(self._jreader.orc(path))

    @since(1.4)
    def jdbc(self, url, table, column=None, lowerBound=None, upperBound=None, numPartitions=None,
             predicates=None, properties=None):
        """
        Construct a :class:`DataFrame` representing the database table named ``table``
        accessible via JDBC URL ``url`` and connection ``properties``.

        Partitions of the table will be retrieved in parallel if either ``column`` or
        ``predicates`` is specified.

        If both ``column`` and ``predicates`` are specified, ``column`` will be used.

        .. note:: Don't create too many partitions in parallel on a large cluster; \
        otherwise Spark might crash your external database systems.

        :param url: a JDBC URL of the form ``jdbc:subprotocol:subname``
        :param table: the name of the table
        :param column: the name of an integer column that will be used for partitioning;
                       if this parameter is specified, then ``numPartitions``, ``lowerBound``
                       (inclusive), and ``upperBound`` (exclusive) will form partition strides
                       for generated WHERE clause expressions used to split the column
                       ``column`` evenly
        :param lowerBound: the minimum value of ``column`` used to decide partition stride
        :param upperBound: the maximum value of ``column`` used to decide partition stride
        :param numPartitions: the number of partitions
        :param predicates: a list of expressions suitable for inclusion in WHERE clauses;
                           each one defines one partition of the :class:`DataFrame`
        :param properties: a dictionary of JDBC database connection arguments; normally,
                           at least a "user" and "password" property should be included
        :return: a DataFrame
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._spark._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        if column is not None:
            if numPartitions is None:
                numPartitions = self._spark._sc.defaultParallelism
            return self._df(self._jreader.jdbc(url, table, column, int(lowerBound), int(upperBound),
                                               int(numPartitions), jprop))
        if predicates is not None:
            gateway = self._spark._sc._gateway
            jpredicates = utils.toJArray(gateway, gateway.jvm.java.lang.String, predicates)
            return self._df(self._jreader.jdbc(url, table, jpredicates, jprop))
        return self._df(self._jreader.jdbc(url, table, jprop))


class DataFrameWriter(object):
    """
    Interface used to write a :class:`DataFrame` to external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`DataFrame.write`
    to access this.

    .. versionadded:: 1.4
    """
    def __init__(self, df):
        self._df = df
        self._spark = df.sql_ctx
        self._jwrite = df._jdf.write()

    def _sq(self, jsq):
        from pyspark.sql.streaming import StreamingQuery
        return StreamingQuery(jsq)

    @since(1.4)
    def mode(self, saveMode):
        """Specifies the behavior when data or table already exists.

        Options include:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        >>> df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        # At the JVM side, the default value of mode is already set to "error".
        # So, if the given saveMode is None, we will not call JVM-side's mode method.
        if saveMode is not None:
            self._jwrite = self._jwrite.mode(saveMode)
        return self

    @since(1.4)
    def format(self, source):
        """Specifies the underlying output data source.

        :param source: string, name of the data source, e.g. 'json', 'parquet'.

        >>> df.write.format('json').save(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self._jwrite = self._jwrite.format(source)
        return self

    @since(1.5)
    def option(self, key, value):
        """Adds an output option for the underlying data source.
        """
        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    @since(1.4)
    def options(self, **options):
        """Adds output options for the underlying data source.
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, to_str(options[k]))
        return self

    @since(1.4)
    def partitionBy(self, *cols):
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        :param cols: name of columns

        >>> df.write.partitionBy('year', 'month').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._jwrite = self._jwrite.partitionBy(_to_seq(self._spark._sc, cols))
        return self

    @since(1.4)
    def save(self, path=None, format=None, mode=None, partitionBy=None, **options):
        """Saves the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        :param path: the path in a Hadoop supported file system
        :param format: the format used to save
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns
        :param options: all other string options

        >>> df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
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

    @since(1.4)
    def insertInto(self, tableName, overwrite=False):
        """Inserts the content of the :class:`DataFrame` to the specified table.

        It requires that the schema of the class:`DataFrame` is the same as the
        schema of the table.

        Optionally overwriting any existing data.
        """
        self._jwrite.mode("overwrite" if overwrite else "append").insertInto(tableName)

    @since(1.4)
    def saveAsTable(self, name, format=None, mode=None, partitionBy=None, **options):
        """Saves the content of the :class:`DataFrame` as the specified table.

        In the case the table already exists, behavior of this function depends on the
        save mode, specified by the `mode` function (default to throwing an exception).
        When `mode` is `Overwrite`, the schema of the :class:`DataFrame` does not need to be
        the same as that of the existing table.

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param name: the table name
        :param format: the format used to save
        :param mode: one of `append`, `overwrite`, `error`, `ignore` (default: error)
        :param partitionBy: names of partitioning columns
        :param options: all other string options
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        self._jwrite.saveAsTable(name)

    @since(1.4)
    def json(self, path, mode=None, compression=None):
        """Saves the content of the :class:`DataFrame` in JSON format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).

        >>> df.write.json(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if compression is not None:
            self.option("compression", compression)
        self._jwrite.json(path)

    @since(1.4)
    def parquet(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, snappy, gzip, and lzo).
                            This will override ``spark.sql.parquet.compression.codec``. If None
                            is set, it uses the value specified in
                            ``spark.sql.parquet.compression.codec``.

        >>> df.write.parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if compression is not None:
            self.option("compression", compression)
        self._jwrite.parquet(path)

    @since(1.6)
    def text(self, path, compression=None):
        """Saves the content of the DataFrame in a text file at the specified path.

        :param path: the path in any Hadoop supported file system
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).

        The DataFrame must have only one column that is of string type.
        Each row becomes a new line in the output file.
        """
        if compression is not None:
            self.option("compression", compression)
        self._jwrite.text(path)

    @since(2.0)
    def csv(self, path, mode=None, compression=None, sep=None, quote=None, escape=None,
            header=None, nullValue=None, escapeQuotes=None):
        """Saves the content of the :class:`DataFrame` in CSV format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.

        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).
        :param sep: sets the single character as a separator for each field and value. If None is
                    set, it uses the default value, ``,``.
        :param quote: sets the single character used for escaping quoted values where the
                      separator can be part of the value. If None is set, it uses the default
                      value, ``"``. If you would like to turn off quotations, you need to set an
                      empty string.
        :param escape: sets the single character used for escaping quotes inside an already
                       quoted value. If None is set, it uses the default value, ``\``
        :param escapeQuotes: A flag indicating whether values containing quotes should always
                             be enclosed in quotes. If None is set, it uses the default value
                             ``true``, escaping all values containing a quote character.
        :param header: writes the names of columns as the first line. If None is set, it uses
                       the default value, ``false``.
        :param nullValue: sets the string representation of a null value. If None is set, it uses
                          the default value, empty string.

        >>> df.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if compression is not None:
            self.option("compression", compression)
        if sep is not None:
            self.option("sep", sep)
        if quote is not None:
            self.option("quote", quote)
        if escape is not None:
            self.option("escape", escape)
        if header is not None:
            self.option("header", header)
        if nullValue is not None:
            self.option("nullValue", nullValue)
        if escapeQuotes is not None:
            self.option("escapeQuotes", nullValue)
        self._jwrite.csv(path)

    @since(1.5)
    def orc(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in ORC format at the specified path.

        .. note:: Currently ORC support is only available together with Hive support.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, snappy, zlib, and lzo).
                            This will override ``orc.compress``. If None is set, it uses the
                            default value, ``snappy``.

        >>> orc_df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> orc_df.write.orc(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if compression is not None:
            self.option("compression", compression)
        self._jwrite.orc(path)

    @since(1.4)
    def jdbc(self, url, table, mode=None, properties=None):
        """Saves the content of the :class:`DataFrame` to an external database table via JDBC.

        .. note:: Don't create too many partitions in parallel on a large cluster; \
        otherwise Spark might crash your external database systems.

        :param url: a JDBC URL of the form ``jdbc:subprotocol:subname``
        :param table: Name of the table in the external database.
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param properties: JDBC database connection arguments, a list of
                           arbitrary string tag/value. Normally at least a
                           "user" and "password" property should be included.
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._spark._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self._jwrite.mode(mode).jdbc(url, table, jprop)


class DataStreamReader(ReaderUtils):
    """
    Interface used to load a streaming :class:`DataFrame` from external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`spark.readStream`
    to access this.

    .. note:: Experimental.

    .. versionadded:: 2.0
    """

    def __init__(self, spark):
        self._jreader = spark._ssql_ctx.readStream()
        self._spark = spark

    def _df(self, jdf):
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(jdf, self._spark)

    @since(2.0)
    def format(self, source):
        """Specifies the input data source format.

        .. note:: Experimental.

        :param source: string, name of the data source, e.g. 'json', 'parquet'.

        >>> s = spark.readStream.format("text")
        """
        self._jreader = self._jreader.format(source)
        return self

    @since(2.0)
    def schema(self, schema):
        """Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        .. note:: Experimental.

        :param schema: a StructType object

        >>> s = spark.readStream.schema(sdf_schema)
        """
        if not isinstance(schema, StructType):
            raise TypeError("schema should be StructType")
        jschema = self._spark._ssql_ctx.parseDataType(schema.json())
        self._jreader = self._jreader.schema(jschema)
        return self

    @since(2.0)
    def option(self, key, value):
        """Adds an input option for the underlying data source.

        .. note:: Experimental.

        >>> s = spark.readStream.option("x", 1)
        """
        self._jreader = self._jreader.option(key, to_str(value))
        return self

    @since(2.0)
    def options(self, **options):
        """Adds input options for the underlying data source.

        .. note:: Experimental.

        >>> s = spark.readStream.options(x="1", y=2)
        """
        for k in options:
            self._jreader = self._jreader.option(k, to_str(options[k]))
        return self

    @since(2.0)
    def load(self, path=None, format=None, schema=None, **options):
        """Loads a data stream from a data source and returns it as a :class`DataFrame`.

        .. note:: Experimental.

        :param path: optional string for file-system backed data sources.
        :param format: optional string for format of the data source. Default to 'parquet'.
        :param schema: optional :class:`StructType` for the input schema.
        :param options: all other string options

        >>> json_sdf = spark.readStream.format("json")\
                                       .schema(sdf_schema)\
                                       .load(os.path.join(tempfile.mkdtemp(),'data'))
        >>> json_sdf.isStreaming
        True
        >>> json_sdf.schema == sdf_schema
        True
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if path is not None:
            if type(path) != str or len(path.strip()) == 0:
                raise ValueError("If the path is provided for stream, it needs to be a " +
                                 "non-empty string. List of paths are not supported.")
            return self._df(self._jreader.load(path))
        else:
            return self._df(self._jreader.load())

    @since(2.0)
    def json(self, path, schema=None, primitivesAsString=None, prefersDecimal=None,
             allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None,
             allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None,
             mode=None, columnNameOfCorruptRecord=None):
        """
        Loads a JSON file stream (one object per line) and returns a :class`DataFrame`.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. note:: Experimental.

        :param path: string represents path to the JSON dataset,
                     or RDD of Strings storing JSON objects.
        :param schema: an optional :class:`StructType` for the input schema.
        :param primitivesAsString: infers all primitive values as a string type. If None is set,
                                   it uses the default value, ``false``.
        :param prefersDecimal: infers all floating-point values as a decimal type. If the values
                               do not fit in decimal, then it infers them as doubles. If None is
                               set, it uses the default value, ``false``.
        :param allowComments: ignores Java/C++ style comment in JSON records. If None is set,
                              it uses the default value, ``false``.
        :param allowUnquotedFieldNames: allows unquoted JSON field names. If None is set,
                                        it uses the default value, ``false``.
        :param allowSingleQuotes: allows single quotes in addition to double quotes. If None is
                                        set, it uses the default value, ``true``.
        :param allowNumericLeadingZero: allows leading zeros in numbers (e.g. 00012). If None is
                                        set, it uses the default value, ``false``.
        :param allowBackslashEscapingAnyCharacter: allows accepting quoting of all character
                                                   using backslash quoting mechanism. If None is
                                                   set, it uses the default value, ``false``.
        :param mode: allows a mode for dealing with corrupt records during parsing. If None is
                     set, it uses the default value, ``PERMISSIVE``.

                *  ``PERMISSIVE`` : sets other fields to ``null`` when it meets a corrupted \
                  record and puts the malformed string into a new field configured by \
                 ``columnNameOfCorruptRecord``. When a schema is set by user, it sets \
                 ``null`` for extra fields.
                *  ``DROPMALFORMED`` : ignores the whole corrupted records.
                *  ``FAILFAST`` : throws an exception when it meets corrupted records.

        :param columnNameOfCorruptRecord: allows renaming the new field having malformed string
                                          created by ``PERMISSIVE`` mode. This overrides
                                          ``spark.sql.columnNameOfCorruptRecord``. If None is set,
                                          it uses the value specified in
                                          ``spark.sql.columnNameOfCorruptRecord``.

        >>> json_sdf = spark.readStream.json(os.path.join(tempfile.mkdtemp(), 'data'), \
                schema = sdf_schema)
        >>> json_sdf.isStreaming
        True
        >>> json_sdf.schema == sdf_schema
        True
        """
        self._set_json_opts(schema, primitivesAsString, prefersDecimal,
                            allowComments, allowUnquotedFieldNames, allowSingleQuotes,
                            allowNumericLeadingZero, allowBackslashEscapingAnyCharacter,
                            mode, columnNameOfCorruptRecord)
        if isinstance(path, basestring):
            return self._df(self._jreader.json(path))
        else:
            raise TypeError("path can be only a single string")

    @since(2.0)
    def parquet(self, path):
        """Loads a Parquet file stream, returning the result as a :class:`DataFrame`.

        You can set the following Parquet-specific option(s) for reading Parquet files:
            * ``mergeSchema``: sets whether we should merge schemas collected from all \
                Parquet part-files. This will override ``spark.sql.parquet.mergeSchema``. \
                The default value is specified in ``spark.sql.parquet.mergeSchema``.

        .. note:: Experimental.

        >>> parquet_sdf = spark.readStream.schema(sdf_schema)\
                .parquet(os.path.join(tempfile.mkdtemp()))
        >>> parquet_sdf.isStreaming
        True
        >>> parquet_sdf.schema == sdf_schema
        True
        """
        if isinstance(path, basestring):
            return self._df(self._jreader.parquet(path))
        else:
            raise TypeError("path can be only a single string")

    @ignore_unicode_prefix
    @since(2.0)
    def text(self, path):
        """
        Loads a text file stream and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.

        Each line in the text file is a new row in the resulting DataFrame.

        .. note:: Experimental.

        :param paths: string, or list of strings, for input path(s).

        >>> text_sdf = spark.readStream.text(os.path.join(tempfile.mkdtemp(), 'data'))
        >>> text_sdf.isStreaming
        True
        >>> "value" in str(text_sdf.schema)
        True
        """
        if isinstance(path, basestring):
            return self._df(self._jreader.text(path))
        else:
            raise TypeError("path can be only a single string")

    @since(2.0)
    def csv(self, path, schema=None, sep=None, encoding=None, quote=None, escape=None,
            comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None,
            ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None,
            negativeInf=None, dateFormat=None, maxColumns=None, maxCharsPerColumn=None, mode=None):
        """Loads a CSV file stream and returns the result as a  :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        .. note:: Experimental.

        :param path: string, or list of strings, for input path(s).
        :param schema: an optional :class:`StructType` for the input schema.
        :param sep: sets the single character as a separator for each field and value.
                    If None is set, it uses the default value, ``,``.
        :param encoding: decodes the CSV files by the given encoding type. If None is set,
                         it uses the default value, ``UTF-8``.
        :param quote: sets the single character used for escaping quoted values where the
                      separator can be part of the value. If None is set, it uses the default
                      value, ``"``. If you would like to turn off quotations, you need to set an
                      empty string.
        :param escape: sets the single character used for escaping quotes inside an already
                       quoted value. If None is set, it uses the default value, ``\``.
        :param comment: sets the single character used for skipping lines beginning with this
                        character. By default (None), it is disabled.
        :param header: uses the first line as names of columns. If None is set, it uses the
                       default value, ``false``.
        :param inferSchema: infers the input schema automatically from data. It requires one extra
                       pass over the data. If None is set, it uses the default value, ``false``.
        :param ignoreLeadingWhiteSpace: defines whether or not leading whitespaces from values
                                        being read should be skipped. If None is set, it uses
                                        the default value, ``false``.
        :param ignoreTrailingWhiteSpace: defines whether or not trailing whitespaces from values
                                         being read should be skipped. If None is set, it uses
                                         the default value, ``false``.
        :param nullValue: sets the string representation of a null value. If None is set, it uses
                          the default value, empty string.
        :param nanValue: sets the string representation of a non-number value. If None is set, it
                         uses the default value, ``NaN``.
        :param positiveInf: sets the string representation of a positive infinity value. If None
                            is set, it uses the default value, ``Inf``.
        :param negativeInf: sets the string representation of a negative infinity value. If None
                            is set, it uses the default value, ``Inf``.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to both date type and timestamp type. By default, it is None
                           which means trying to parse times and date by
                           ``java.sql.Timestamp.valueOf()`` and ``java.sql.Date.valueOf()``.
        :param maxColumns: defines a hard limit of how many columns a record can have. If None is
                           set, it uses the default value, ``20480``.
        :param maxCharsPerColumn: defines the maximum number of characters allowed for any given
                                  value being read. If None is set, it uses the default value,
                                  ``1000000``.
        :param mode: allows a mode for dealing with corrupt records during parsing. If None is
                     set, it uses the default value, ``PERMISSIVE``.

                * ``PERMISSIVE`` : sets other fields to ``null`` when it meets a corrupted record.
                    When a schema is set by user, it sets ``null`` for extra fields.
                * ``DROPMALFORMED`` : ignores the whole corrupted records.
                * ``FAILFAST`` : throws an exception when it meets corrupted records.

        >>> csv_sdf = spark.readStream.csv(os.path.join(tempfile.mkdtemp(), 'data'), \
                schema = sdf_schema)
        >>> csv_sdf.isStreaming
        True
        >>> csv_sdf.schema == sdf_schema
        True
        """

        self._set_csv_opts(schema, sep, encoding, quote, escape,
                           comment, header, inferSchema, ignoreLeadingWhiteSpace,
                           ignoreTrailingWhiteSpace, nullValue, nanValue, positiveInf, negativeInf,
                           dateFormat, maxColumns, maxCharsPerColumn, mode)
        if isinstance(path, basestring):
            return self._df(self._jreader.csv(path))
        else:
            raise TypeError("path can be only a single string")


class DataStreamWriter(object):
    """
    Interface used to write a streaming :class:`DataFrame` to external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`DataFrame.writeStream`
    to access this.

    .. note:: Experimental.

    .. versionadded:: 2.0
    """

    def __init__(self, df):
        self._df = df
        self._spark = df.sql_ctx
        self._jwrite = df._jdf.writeStream()

    def _sq(self, jsq):
        from pyspark.sql.streaming import StreamingQuery
        return StreamingQuery(jsq)

    @since(2.0)
    def outputMode(self, outputMode):
        """Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.

        Options include:

        * `append`:Only the new rows in the streaming DataFrame/Dataset will be written to
           the sink
        * `complete`:All the rows in the streaming DataFrame/Dataset will be written to the sink
           every time these is some updates

       .. note:: Experimental.

        >>> writer = sdf.writeStream.outputMode('append')
        """
        if not outputMode or type(outputMode) != str or len(outputMode.strip()) == 0:
            raise ValueError('The output mode must be a non-empty string. Got: %s' % outputMode)
        self._jwrite = self._jwrite.outputMode(outputMode)
        return self

    @since(2.0)
    def format(self, source):
        """Specifies the underlying output data source.

        .. note:: Experimental.

        :param source: string, name of the data source, e.g. 'json', 'parquet'.

        >>> writer = sdf.writeStream.format('json')
        """
        self._jwrite = self._jwrite.format(source)
        return self

    @since(2.0)
    def option(self, key, value):
        """Adds an output option for the underlying data source.

        .. note:: Experimental.
        """
        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    @since(2.0)
    def options(self, **options):
        """Adds output options for the underlying data source.

       .. note:: Experimental.
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, to_str(options[k]))
        return self

    @since(2.0)
    def partitionBy(self, *cols):
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        .. note:: Experimental.

        :param cols: name of columns

        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._jwrite = self._jwrite.partitionBy(_to_seq(self._spark._sc, cols))
        return self

    @since(2.0)
    def queryName(self, queryName):
        """Specifies the name of the :class:`StreamingQuery` that can be started with
        :func:`start`. This name must be unique among all the currently active queries
        in the associated SparkSession.

        .. note:: Experimental.

        :param queryName: unique name for the query

        >>> writer = sdf.writeStream.queryName('streaming_query')
        """
        if not queryName or type(queryName) != str or len(queryName.strip()) == 0:
            raise ValueError('The queryName must be a non-empty string. Got: %s' % queryName)
        self._jwrite = self._jwrite.queryName(queryName)
        return self

    @keyword_only
    @since(2.0)
    def trigger(self, processingTime=None):
        """Set the trigger for the stream query. If this is not set it will run the query as fast
        as possible, which is equivalent to setting the trigger to ``processingTime='0 seconds'``.

        .. note:: Experimental.

        :param processingTime: a processing time interval as a string, e.g. '5 seconds', '1 minute'.

        >>> # trigger the query for execution every 5 seconds
        >>> writer = sdf.writeStream.trigger(processingTime='5 seconds')
        """
        from pyspark.sql.streaming import ProcessingTime
        trigger = None
        if processingTime is not None:
            if type(processingTime) != str or len(processingTime.strip()) == 0:
                raise ValueError('The processing time must be a non empty string. Got: %s' %
                                 processingTime)
            trigger = ProcessingTime(processingTime)
        if trigger is None:
            raise ValueError('A trigger was not provided. Supported triggers: processingTime.')
        self._jwrite = self._jwrite.trigger(trigger._to_java_trigger(self._spark))
        return self

    @ignore_unicode_prefix
    @since(2.0)
    def start(self, path=None, format=None, partitionBy=None, queryName=None, **options):
        """Streams the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        .. note:: Experimental.

        :param path: the path in a Hadoop supported file system
        :param format: the format used to save

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns
        :param queryName: unique name for the query
        :param options: All other string options. You may want to provide a `checkpointLocation`
            for most streams, however it is not required for a `memory` stream.

        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sq.isActive
        True
        >>> sq.name
        u'this_query'
        >>> sq.stop()
        >>> sq.isActive
        False
        >>> sq = sdf.writeStream.trigger(processingTime='5 seconds').start(
        ...     queryName='that_query', format='memory')
        >>> sq.name
        u'that_query'
        >>> sq.isActive
        True
        >>> sq.stop()
        """
        self.options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if queryName is not None:
            self.queryName(queryName)
        if path is None:
            return self._sq(self._jwrite.start())
        else:
            return self._sq(self._jwrite.start(path))


def _test():
    import doctest
    import os
    import tempfile
    import py4j
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession, Row
    import pyspark.sql.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.readwriter.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    try:
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    except py4j.protocol.Py4JError:
        spark = SparkSession(sc)

    globs['tempfile'] = tempfile
    globs['os'] = os
    globs['sc'] = sc
    globs['spark'] = spark
    globs['df'] = spark.read.parquet('python/test_support/sql/parquet_partitioned')
    globs['sdf'] = \
        spark.readStream.format('text').load('python/test_support/sql/streaming')
    globs['sdf_schema'] = StructType([StructField("data", StringType(), False)])
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.readwriter, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    sc.stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
