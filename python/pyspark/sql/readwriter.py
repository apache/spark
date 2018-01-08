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

__all__ = ["DataFrameReader", "DataFrameWriter"]


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


class OptionUtils(object):

    def _set_opts(self, schema=None, **options):
        """
        Set named options (filter out those the value is None)
        """
        if schema is not None:
            self.schema(schema)
        for k, v in options.items():
            if v is not None:
                self.option(k, v)


class DataFrameReader(OptionUtils):
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

        :param schema: a :class:`pyspark.sql.types.StructType` object or a DDL-formatted string
                       (For example ``col0 INT, col1 DOUBLE``).

        >>> s = spark.read.schema("col0 INT, col1 DOUBLE")
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        if isinstance(schema, StructType):
            jschema = spark._jsparkSession.parseDataType(schema.json())
            self._jreader = self._jreader.schema(jschema)
        elif isinstance(schema, basestring):
            self._jreader = self._jreader.schema(schema)
        else:
            raise TypeError("schema should be StructType or string")
        return self

    @since(1.5)
    def option(self, key, value):
        """Adds an input option for the underlying data source.

        You can set the following option(s) for reading files:
            * ``timeZone``: sets the string that indicates a timezone to be used to parse timestamps
                in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
        """
        self._jreader = self._jreader.option(key, to_str(value))
        return self

    @since(1.4)
    def options(self, **options):
        """Adds input options for the underlying data source.

        You can set the following option(s) for reading files:
            * ``timeZone``: sets the string that indicates a timezone to be used to parse timestamps
                in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
        """
        for k in options:
            self._jreader = self._jreader.option(k, to_str(options[k]))
        return self

    @since(1.4)
    def load(self, path=None, format=None, schema=None, **options):
        """Loads data from a data source and returns it as a :class`DataFrame`.

        :param path: optional string or a list of string for file-system backed data sources.
        :param format: optional string for format of the data source. Default to 'parquet'.
        :param schema: optional :class:`pyspark.sql.types.StructType` for the input schema
                       or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
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
        if isinstance(path, basestring):
            return self._df(self._jreader.load(path))
        elif path is not None:
            if type(path) != list:
                path = [path]
            return self._df(self._jreader.load(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        else:
            return self._df(self._jreader.load())

    @since(1.4)
    def json(self, path, schema=None, primitivesAsString=None, prefersDecimal=None,
             allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None,
             allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None,
             mode=None, columnNameOfCorruptRecord=None, dateFormat=None, timestampFormat=None,
             multiLine=None, allowUnquotedControlChars=None):
        """
        Loads JSON files and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        :param path: string represents path to the JSON dataset, or a list of paths,
                     or RDD of Strings storing JSON objects.
        :param schema: an optional :class:`pyspark.sql.types.StructType` for the input schema or
                       a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
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

                * ``PERMISSIVE`` : sets other fields to ``null`` when it meets a corrupted \
                 record, and puts the malformed string into a field configured by \
                 ``columnNameOfCorruptRecord``. To keep corrupt records, an user can set \
                 a string type field named ``columnNameOfCorruptRecord`` in an user-defined \
                 schema. If a schema does not have the field, it drops corrupt records during \
                 parsing. When inferring a schema, it implicitly adds a \
                 ``columnNameOfCorruptRecord`` field in an output schema.
                *  ``DROPMALFORMED`` : ignores the whole corrupted records.
                *  ``FAILFAST`` : throws an exception when it meets corrupted records.

        :param columnNameOfCorruptRecord: allows renaming the new field having malformed string
                                          created by ``PERMISSIVE`` mode. This overrides
                                          ``spark.sql.columnNameOfCorruptRecord``. If None is set,
                                          it uses the value specified in
                                          ``spark.sql.columnNameOfCorruptRecord``.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format. Custom date
                                formats follow the formats at ``java.text.SimpleDateFormat``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
        :param multiLine: parse one record, which may span multiple lines, per file. If None is
                          set, it uses the default value, ``false``.
        :param allowUnquotedControlChars: allows JSON Strings to contain unquoted control
                                          characters (ASCII characters with value less than 32,
                                          including tab and line feed characters) or not.

        >>> df1 = spark.read.json('python/test_support/sql/people.json')
        >>> df1.dtypes
        [('age', 'bigint'), ('name', 'string')]
        >>> rdd = sc.textFile('python/test_support/sql/people.json')
        >>> df2 = spark.read.json(rdd)
        >>> df2.dtypes
        [('age', 'bigint'), ('name', 'string')]

        """
        self._set_opts(
            schema=schema, primitivesAsString=primitivesAsString, prefersDecimal=prefersDecimal,
            allowComments=allowComments, allowUnquotedFieldNames=allowUnquotedFieldNames,
            allowSingleQuotes=allowSingleQuotes, allowNumericLeadingZero=allowNumericLeadingZero,
            allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode, columnNameOfCorruptRecord=columnNameOfCorruptRecord, dateFormat=dateFormat,
            timestampFormat=timestampFormat, multiLine=multiLine,
            allowUnquotedControlChars=allowUnquotedControlChars)
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
            raise TypeError("path can be only string, list or RDD")

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
        """Loads Parquet files, returning the result as a :class:`DataFrame`.

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
    def text(self, paths, wholetext=False):
        """
        Loads text files and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.

        Each line in the text file is a new row in the resulting DataFrame.

        :param paths: string, or list of strings, for input path(s).
        :param wholetext: if true, read each file from input path(s) as a single row.

        >>> df = spark.read.text('python/test_support/sql/text-test.txt')
        >>> df.collect()
        [Row(value=u'hello'), Row(value=u'this')]
        >>> df = spark.read.text('python/test_support/sql/text-test.txt', wholetext=True)
        >>> df.collect()
        [Row(value=u'hello\\nthis')]
        """
        self._set_opts(wholetext=wholetext)
        if isinstance(paths, basestring):
            paths = [paths]
        return self._df(self._jreader.text(self._spark._sc._jvm.PythonUtils.toSeq(paths)))

    @since(2.0)
    def csv(self, path, schema=None, sep=None, encoding=None, quote=None, escape=None,
            comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None,
            ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None,
            negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None,
            maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None,
            columnNameOfCorruptRecord=None, multiLine=None, charToEscapeQuoteEscaping=None):
        """Loads a CSV file and returns the result as a  :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        :param path: string, or list of strings, for input path(s),
                     or RDD of Strings storing CSV rows.
        :param schema: an optional :class:`pyspark.sql.types.StructType` for the input schema
                       or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        :param sep: sets a single character as a separator for each field and value.
                    If None is set, it uses the default value, ``,``.
        :param encoding: decodes the CSV files by the given encoding type. If None is set,
                         it uses the default value, ``UTF-8``.
        :param quote: sets a single character used for escaping quoted values where the
                      separator can be part of the value. If None is set, it uses the default
                      value, ``"``. If you would like to turn off quotations, you need to set an
                      empty string.
        :param escape: sets a single character used for escaping quotes inside an already
                       quoted value. If None is set, it uses the default value, ``\``.
        :param comment: sets a single character used for skipping lines beginning with this
                        character. By default (None), it is disabled.
        :param header: uses the first line as names of columns. If None is set, it uses the
                       default value, ``false``.
        :param inferSchema: infers the input schema automatically from data. It requires one extra
                       pass over the data. If None is set, it uses the default value, ``false``.
        :param ignoreLeadingWhiteSpace: A flag indicating whether or not leading whitespaces from
                                        values being read should be skipped. If None is set, it
                                        uses the default value, ``false``.
        :param ignoreTrailingWhiteSpace: A flag indicating whether or not trailing whitespaces from
                                         values being read should be skipped. If None is set, it
                                         uses the default value, ``false``.
        :param nullValue: sets the string representation of a null value. If None is set, it uses
                          the default value, empty string. Since 2.0.1, this ``nullValue`` param
                          applies to all supported types including the string type.
        :param nanValue: sets the string representation of a non-number value. If None is set, it
                         uses the default value, ``NaN``.
        :param positiveInf: sets the string representation of a positive infinity value. If None
                            is set, it uses the default value, ``Inf``.
        :param negativeInf: sets the string representation of a negative infinity value. If None
                            is set, it uses the default value, ``Inf``.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format. Custom date
                                formats follow the formats at ``java.text.SimpleDateFormat``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
        :param maxColumns: defines a hard limit of how many columns a record can have. If None is
                           set, it uses the default value, ``20480``.
        :param maxCharsPerColumn: defines the maximum number of characters allowed for any given
                                  value being read. If None is set, it uses the default value,
                                  ``-1`` meaning unlimited length.
        :param maxMalformedLogPerPartition: this parameter is no longer used since Spark 2.2.0.
                                            If specified, it is ignored.
        :param mode: allows a mode for dealing with corrupt records during parsing. If None is
                     set, it uses the default value, ``PERMISSIVE``.

                * ``PERMISSIVE`` : sets other fields to ``null`` when it meets a corrupted \
                  record, and puts the malformed string into a field configured by \
                  ``columnNameOfCorruptRecord``. To keep corrupt records, an user can set \
                  a string type field named ``columnNameOfCorruptRecord`` in an \
                  user-defined schema. If a schema does not have the field, it drops corrupt \
                  records during parsing. When a length of parsed CSV tokens is shorter than \
                  an expected length of a schema, it sets `null` for extra fields.
                * ``DROPMALFORMED`` : ignores the whole corrupted records.
                * ``FAILFAST`` : throws an exception when it meets corrupted records.

        :param columnNameOfCorruptRecord: allows renaming the new field having malformed string
                                          created by ``PERMISSIVE`` mode. This overrides
                                          ``spark.sql.columnNameOfCorruptRecord``. If None is set,
                                          it uses the value specified in
                                          ``spark.sql.columnNameOfCorruptRecord``.
        :param multiLine: parse records, which may span multiple lines. If None is
                          set, it uses the default value, ``false``.
        :param charToEscapeQuoteEscaping: sets a single character used for escaping the escape for
                                          the quote character. If None is set, the default value is
                                          escape character when escape and quote characters are
                                          different, ``\0`` otherwise.

        >>> df = spark.read.csv('python/test_support/sql/ages.csv')
        >>> df.dtypes
        [('_c0', 'string'), ('_c1', 'string')]
        >>> rdd = sc.textFile('python/test_support/sql/ages.csv')
        >>> df2 = spark.read.csv(rdd)
        >>> df2.dtypes
        [('_c0', 'string'), ('_c1', 'string')]
        """
        self._set_opts(
            schema=schema, sep=sep, encoding=encoding, quote=quote, escape=escape, comment=comment,
            header=header, inferSchema=inferSchema, ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace, nullValue=nullValue,
            nanValue=nanValue, positiveInf=positiveInf, negativeInf=negativeInf,
            dateFormat=dateFormat, timestampFormat=timestampFormat, maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition, mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord, multiLine=multiLine,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping)
        if isinstance(path, basestring):
            path = [path]
        if type(path) == list:
            return self._df(self._jreader.csv(self._spark._sc._jvm.PythonUtils.toSeq(path)))
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
            # see SPARK-22112
            # There aren't any jvm api for creating a dataframe from rdd storing csv.
            # We can do it through creating a jvm dataset firstly and using the jvm api
            # for creating a dataframe from dataset storing csv.
            jdataset = self._spark._ssql_ctx.createDataset(
                jrdd.rdd(),
                self._spark._jvm.Encoders.STRING())
            return self._df(self._jreader.csv(jdataset))
        else:
            raise TypeError("path can be only string, list or RDD")

    @since(1.5)
    def orc(self, path):
        """Loads ORC files, returning the result as a :class:`DataFrame`.

        .. note:: Currently ORC support is only available together with Hive support.

        >>> df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> df.dtypes
        [('a', 'bigint'), ('b', 'int'), ('c', 'int')]
        """
        if isinstance(path, basestring):
            path = [path]
        return self._df(self._jreader.orc(_to_seq(self._spark._sc, path)))

    @since(1.4)
    def jdbc(self, url, table, column=None, lowerBound=None, upperBound=None, numPartitions=None,
             predicates=None, properties=None):
        """
        Construct a :class:`DataFrame` representing the database table named ``table``
        accessible via JDBC URL ``url`` and connection ``properties``.

        Partitions of the table will be retrieved in parallel if either ``column`` or
        ``predicates`` is specified. ``lowerBound`, ``upperBound`` and ``numPartitions``
        is needed when ``column`` is specified.

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
        :param properties: a dictionary of JDBC database connection arguments. Normally at
                           least properties "user" and "password" with their corresponding values.
                           For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }
        :return: a DataFrame
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._spark._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        if column is not None:
            assert lowerBound is not None, "lowerBound can not be None when ``column`` is specified"
            assert upperBound is not None, "upperBound can not be None when ``column`` is specified"
            assert numPartitions is not None, \
                "numPartitions can not be None when ``column`` is specified"
            return self._df(self._jreader.jdbc(url, table, column, int(lowerBound), int(upperBound),
                                               int(numPartitions), jprop))
        if predicates is not None:
            gateway = self._spark._sc._gateway
            jpredicates = utils.toJArray(gateway, gateway.jvm.java.lang.String, predicates)
            return self._df(self._jreader.jdbc(url, table, jpredicates, jprop))
        return self._df(self._jreader.jdbc(url, table, jprop))


class DataFrameWriter(OptionUtils):
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
        * `error` or `errorifexists`: Throw an exception if data already exists.
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

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a timezone to be used to format
                timestamps in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
        """
        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    @since(1.4)
    def options(self, **options):
        """Adds output options for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a timezone to be used to format
                timestamps in the JSON/CSV datasources or partition values.
                If it isn't set, it uses the default value, session local timezone.
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

    @since(2.3)
    def bucketBy(self, numBuckets, col, *cols):
        """Buckets the output by the given columns.If specified,
        the output is laid out on the file system similar to Hive's bucketing scheme.

        :param numBuckets: the number of buckets to save
        :param col: a name of a column, or a list of names.
        :param cols: additional names (optional). If `col` is a list it should be empty.

        .. note:: Applicable for file-based data sources in combination with
                  :py:meth:`DataFrameWriter.saveAsTable`.

        >>> (df.write.format('parquet')  # doctest: +SKIP
        ...     .bucketBy(100, 'year', 'month')
        ...     .mode("overwrite")
        ...     .saveAsTable('bucketed_table'))
        """
        if not isinstance(numBuckets, int):
            raise TypeError("numBuckets should be an int, got {0}.".format(type(numBuckets)))

        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError("col is a {0} but cols are not empty".format(type(col)))

            col, cols = col[0], col[1:]

        if not all(isinstance(c, basestring) for c in cols) or not(isinstance(col, basestring)):
            raise TypeError("all names should be `str`")

        self._jwrite = self._jwrite.bucketBy(numBuckets, col, _to_seq(self._spark._sc, cols))
        return self

    @since(2.3)
    def sortBy(self, col, *cols):
        """Sorts the output in each bucket by the given columns on the file system.

        :param col: a name of a column, or a list of names.
        :param cols: additional names (optional). If `col` is a list it should be empty.

        >>> (df.write.format('parquet')  # doctest: +SKIP
        ...     .bucketBy(100, 'year', 'month')
        ...     .sortBy('day')
        ...     .mode("overwrite")
        ...     .saveAsTable('sorted_bucketed_table'))
        """
        if isinstance(col, (list, tuple)):
            if cols:
                raise ValueError("col is a {0} but cols are not empty".format(type(col)))

            col, cols = col[0], col[1:]

        if not all(isinstance(c, basestring) for c in cols) or not(isinstance(col, basestring)):
            raise TypeError("all names should be `str`")

        self._jwrite = self._jwrite.sortBy(col, _to_seq(self._spark._sc, cols))
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
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
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
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param name: the table name
        :param format: the format used to save
        :param mode: one of `append`, `overwrite`, `error`, `errorifexists`, `ignore` \
                     (default: error)
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
    def json(self, path, mode=None, compression=None, dateFormat=None, timestampFormat=None):
        """Saves the content of the :class:`DataFrame` in JSON format
        (`JSON Lines text format or newline-delimited JSON <http://jsonlines.org/>`_) at the
        specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format. Custom date
                                formats follow the formats at ``java.text.SimpleDateFormat``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.

        >>> df.write.json(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        self._set_opts(
            compression=compression, dateFormat=dateFormat, timestampFormat=timestampFormat)
        self._jwrite.json(path)

    @since(1.4)
    def parquet(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
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
        self._set_opts(compression=compression)
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
        self._set_opts(compression=compression)
        self._jwrite.text(path)

    @since(2.0)
    def csv(self, path, mode=None, compression=None, sep=None, quote=None, escape=None,
            header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None,
            timestampFormat=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None,
            charToEscapeQuoteEscaping=None):
        """Saves the content of the :class:`DataFrame` in CSV format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, bzip2, gzip, lz4,
                            snappy and deflate).
        :param sep: sets a single character as a separator for each field and value. If None is
                    set, it uses the default value, ``,``.
        :param quote: sets a single character used for escaping quoted values where the
                      separator can be part of the value. If None is set, it uses the default
                      value, ``"``. If an empty string is set, it uses ``u0000`` (null character).
        :param escape: sets a single character used for escaping quotes inside an already
                       quoted value. If None is set, it uses the default value, ``\``
        :param escapeQuotes: a flag indicating whether values containing quotes should always
                             be enclosed in quotes. If None is set, it uses the default value
                             ``true``, escaping all values containing a quote character.
        :param quoteAll: a flag indicating whether all values should always be enclosed in
                          quotes. If None is set, it uses the default value ``false``,
                          only escaping values containing a quote character.
        :param header: writes the names of columns as the first line. If None is set, it uses
                       the default value, ``false``.
        :param nullValue: sets the string representation of a null value. If None is set, it uses
                          the default value, empty string.
        :param dateFormat: sets the string that indicates a date format. Custom date formats
                           follow the formats at ``java.text.SimpleDateFormat``. This
                           applies to date type. If None is set, it uses the
                           default value, ``yyyy-MM-dd``.
        :param timestampFormat: sets the string that indicates a timestamp format. Custom date
                                formats follow the formats at ``java.text.SimpleDateFormat``.
                                This applies to timestamp type. If None is set, it uses the
                                default value, ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX``.
        :param ignoreLeadingWhiteSpace: a flag indicating whether or not leading whitespaces from
                                        values being written should be skipped. If None is set, it
                                        uses the default value, ``true``.
        :param ignoreTrailingWhiteSpace: a flag indicating whether or not trailing whitespaces from
                                         values being written should be skipped. If None is set, it
                                         uses the default value, ``true``.
        :param charToEscapeQuoteEscaping: sets a single character used for escaping the escape for
                                          the quote character. If None is set, the default value is
                                          escape character when escape and quote characters are
                                          different, ``\0`` otherwise..

        >>> df.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        self._set_opts(compression=compression, sep=sep, quote=quote, escape=escape, header=header,
                       nullValue=nullValue, escapeQuotes=escapeQuotes, quoteAll=quoteAll,
                       dateFormat=dateFormat, timestampFormat=timestampFormat,
                       ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
                       ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
                       charToEscapeQuoteEscaping=charToEscapeQuoteEscaping)
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
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        :param partitionBy: names of partitioning columns
        :param compression: compression codec to use when saving to file. This can be one of the
                            known case-insensitive shorten names (none, snappy, zlib, and lzo).
                            This will override ``orc.compress`` and
                            ``spark.sql.orc.compression.codec``. If None is set, it uses the value
                            specified in ``spark.sql.orc.compression.codec``.

        >>> orc_df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> orc_df.write.orc(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
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
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        :param properties: a dictionary of JDBC database connection arguments. Normally at
                           least properties "user" and "password" with their corresponding values.
                           For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._spark._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self.mode(mode)._jwrite.jdbc(url, table, jprop)


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
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.readwriter, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    sc.stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
