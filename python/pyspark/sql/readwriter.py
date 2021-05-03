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

from py4j.java_gateway import JavaClass

from pyspark import RDD, since
from pyspark.sql.column import _to_seq, _to_java_column
from pyspark.sql.types import StructType
from pyspark.sql import utils
from pyspark.sql.utils import to_str

__all__ = ["DataFrameReader", "DataFrameWriter"]


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
    (e.g. file systems, key-value stores, etc). Use :attr:`SparkSession.read`
    to access this.

    .. versionadded:: 1.4
    """

    def __init__(self, spark):
        self._jreader = spark._ssql_ctx.read()
        self._spark = spark

    def _df(self, jdf):
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(jdf, self._spark)

    def format(self, source):
        """Specifies the input data source format.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> df = spark.read.format('json').load('python/test_support/sql/people.json')
        >>> df.dtypes
        [('age', 'bigint'), ('name', 'string')]

        """
        self._jreader = self._jreader.format(source)
        return self

    def schema(self, schema):
        """Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        schema : :class:`pyspark.sql.types.StructType` or str
            a :class:`pyspark.sql.types.StructType` object or a DDL-formatted string
            (For example ``col0 INT, col1 DOUBLE``).

        >>> s = spark.read.schema("col0 INT, col1 DOUBLE")
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        if isinstance(schema, StructType):
            jschema = spark._jsparkSession.parseDataType(schema.json())
            self._jreader = self._jreader.schema(jschema)
        elif isinstance(schema, str):
            self._jreader = self._jreader.schema(schema)
        else:
            raise TypeError("schema should be StructType or string")
        return self

    @since(1.5)
    def option(self, key, value):
        """Adds an input option for the underlying data source.

        You can set the following option(s) for reading files:
            * ``timeZone``: sets the string that indicates a time zone ID to be used to parse
                timestamps in the JSON/CSV datasources or partition values. The following
                formats of `timeZone` are supported:

                * Region-based zone ID: It should have the form 'area/city', such as \
                  'America/Los_Angeles'.
                * Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or \
                 '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.

                Other short names like 'CST' are not recommended to use because they can be
                ambiguous. If it isn't set, the current value of the SQL config
                ``spark.sql.session.timeZone`` is used by default.
            * ``pathGlobFilter``: an optional glob pattern to only include files with paths matching
                the pattern. The syntax follows org.apache.hadoop.fs.GlobFilter.
                It does not change the behavior of partition discovery.
            * ``modifiedBefore``: an optional timestamp to only include files with
                modification times occurring before the specified time. The provided timestamp
                must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
            * ``modifiedAfter``: an optional timestamp to only include files with
                modification times occurring after the specified time. The provided timestamp
                must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        """
        self._jreader = self._jreader.option(key, to_str(value))
        return self

    @since(1.4)
    def options(self, **options):
        """Adds input options for the underlying data source.

        You can set the following option(s) for reading files:
            * ``timeZone``: sets the string that indicates a time zone ID to be used to parse
                timestamps in the JSON/CSV datasources or partition values. The following
                formats of `timeZone` are supported:

                * Region-based zone ID: It should have the form 'area/city', such as \
                  'America/Los_Angeles'.
                * Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or \
                 '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.

                Other short names like 'CST' are not recommended to use because they can be
                ambiguous. If it isn't set, the current value of the SQL config
                ``spark.sql.session.timeZone`` is used by default.
            * ``pathGlobFilter``: an optional glob pattern to only include files with paths matching
                the pattern. The syntax follows org.apache.hadoop.fs.GlobFilter.
                It does not change the behavior of partition discovery.
            * ``modifiedBefore``: an optional timestamp to only include files with
                modification times occurring before the specified time. The provided timestamp
                must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
            * ``modifiedAfter``: an optional timestamp to only include files with
                modification times occurring after the specified time. The provided timestamp
                must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        """
        for k in options:
            self._jreader = self._jreader.option(k, to_str(options[k]))
        return self

    def load(self, path=None, format=None, schema=None, **options):
        """Loads data from a data source and returns it as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

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
        >>> df = spark.read.format("parquet").load('python/test_support/sql/parquet_partitioned',
        ...     opt1=True, opt2=1, opt3='str')
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
        if isinstance(path, str):
            return self._df(self._jreader.load(path))
        elif path is not None:
            if type(path) != list:
                path = [path]
            return self._df(self._jreader.load(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        else:
            return self._df(self._jreader.load())

    def json(self, path, schema=None, primitivesAsString=None, prefersDecimal=None,
             allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None,
             allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None,
             mode=None, columnNameOfCorruptRecord=None, dateFormat=None, timestampFormat=None,
             multiLine=None, allowUnquotedControlChars=None, lineSep=None, samplingRatio=None,
             dropFieldIfAllNull=None, encoding=None, locale=None, pathGlobFilter=None,
             recursiveFileLookup=None, allowNonNumericNumbers=None,
             modifiedBefore=None, modifiedAfter=None):
        """
        Loads JSON files and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        path : str, list or :class:`RDD`
            string represents path to the JSON dataset, or a list of paths,
            or RDD of Strings storing JSON objects.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema or
            a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        primitivesAsString : str or bool, optional
            infers all primitive values as a string type. If None is set,
            it uses the default value, ``false``.
        prefersDecimal : str or bool, optional
            infers all floating-point values as a decimal type. If the values
            do not fit in decimal, then it infers them as doubles. If None is
            set, it uses the default value, ``false``.
        allowComments : str or bool, optional
            ignores Java/C++ style comment in JSON records. If None is set,
            it uses the default value, ``false``.
        allowUnquotedFieldNames : str or bool, optional
            allows unquoted JSON field names. If None is set,
            it uses the default value, ``false``.
        allowSingleQuotes : str or bool, optional
            allows single quotes in addition to double quotes. If None is
            set, it uses the default value, ``true``.
        allowNumericLeadingZero : str or bool, optional
            allows leading zeros in numbers (e.g. 00012). If None is
            set, it uses the default value, ``false``.
        allowBackslashEscapingAnyCharacter : str or bool, optional
            allows accepting quoting of all character
            using backslash quoting mechanism. If None is
            set, it uses the default value, ``false``.
        mode : str, optional
            allows a mode for dealing with corrupt records during parsing. If None is
                     set, it uses the default value, ``PERMISSIVE``.

            * ``PERMISSIVE``: when it meets a corrupted record, puts the malformed string \
              into a field configured by ``columnNameOfCorruptRecord``, and sets malformed \
              fields to ``null``. To keep corrupt records, an user can set a string type \
              field named ``columnNameOfCorruptRecord`` in an user-defined schema. If a \
              schema does not have the field, it drops corrupt records during parsing. \
              When inferring a schema, it implicitly adds a ``columnNameOfCorruptRecord`` \
              field in an output schema.
            *  ``DROPMALFORMED``: ignores the whole corrupted records.
            *  ``FAILFAST``: throws an exception when it meets corrupted records.

        columnNameOfCorruptRecord: str, optional
            allows renaming the new field having malformed string
            created by ``PERMISSIVE`` mode. This overrides
            ``spark.sql.columnNameOfCorruptRecord``. If None is set,
            it uses the value specified in
            ``spark.sql.columnNameOfCorruptRecord``.
        dateFormat : str, optional
            sets the string that indicates a date format. Custom date formats
            follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to date type. If None is set, it uses the
            default value, ``yyyy-MM-dd``.
        timestampFormat : str, optional
            sets the string that indicates a timestamp format.
            Custom date formats follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to timestamp type. If None is set, it uses the
            default value, ``yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]``.
        multiLine : str or bool, optional
            parse one record, which may span multiple lines, per file. If None is
            set, it uses the default value, ``false``.
        allowUnquotedControlChars : str or bool, optional
            allows JSON Strings to contain unquoted control
            characters (ASCII characters with value less than 32,
            including tab and line feed characters) or not.
        encoding : str or bool, optional
            allows to forcibly set one of standard basic or extended encoding for
            the JSON files. For example UTF-16BE, UTF-32LE. If None is set,
            the encoding of input JSON will be detected automatically
            when the multiLine option is set to ``true``.
        lineSep : str, optional
            defines the line separator that should be used for parsing. If None is
            set, it covers all ``\\r``, ``\\r\\n`` and ``\\n``.
        samplingRatio : str or float, optional
            defines fraction of input JSON objects used for schema inferring.
            If None is set, it uses the default value, ``1.0``.
        dropFieldIfAllNull : str or bool, optional
            whether to ignore column of all null values or empty
            array/struct during schema inference. If None is set, it
            uses the default value, ``false``.
        locale : str, optional
            sets a locale as language tag in IETF BCP 47 format. If None is set,
            it uses the default value, ``en-US``. For instance, ``locale`` is used while
            parsing dates and timestamps.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option
            disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        allowNonNumericNumbers : str or bool
            allows JSON parser to recognize set of "Not-a-Number" (NaN)
            tokens as legal floating number values. If None is set,
            it uses the default value, ``true``.

                * ``+INF``: for positive infinity, as well as alias of
                            ``+Infinity`` and ``Infinity``.
                *  ``-INF``: for negative infinity, alias ``-Infinity``.
                *  ``NaN``: for other not-a-numbers, like result of division by zero.
        modifiedBefore : an optional timestamp to only include files with
            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedAfter : an optional timestamp to only include files with
            modification times occurring after the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)


        Examples
        --------
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
            allowUnquotedControlChars=allowUnquotedControlChars, lineSep=lineSep,
            samplingRatio=samplingRatio, dropFieldIfAllNull=dropFieldIfAllNull, encoding=encoding,
            locale=locale, pathGlobFilter=pathGlobFilter, recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore, modifiedAfter=modifiedAfter,
            allowNonNumericNumbers=allowNonNumericNumbers)
        if isinstance(path, str):
            path = [path]
        if type(path) == list:
            return self._df(self._jreader.json(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        elif isinstance(path, RDD):
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
            return self._df(self._jreader.json(jrdd))
        else:
            raise TypeError("path can be only string, list or RDD")

    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        tableName : str
            string, name of the table.

        Examples
        --------
        >>> df = spark.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.createOrReplaceTempView('tmpTable')
        >>> spark.read.table('tmpTable').dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        return self._df(self._jreader.table(tableName))

    def parquet(self, *paths, **options):
        """
        Loads Parquet files, returning the result as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        paths : str

        Other Parameters
        ----------------
        mergeSchema : str or bool, optional
            sets whether we should merge schemas collected from all
            Parquet part-files. This will override
            ``spark.sql.parquet.mergeSchema``. The default value is specified in
            ``spark.sql.parquet.mergeSchema``.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option
            disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa

            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedBefore (batch only) : an optional timestamp to only include files with
            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedAfter (batch only) : an optional timestamp to only include files with
            modification times occurring after the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)

        Examples
        --------
        >>> df = spark.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        mergeSchema = options.get('mergeSchema', None)
        pathGlobFilter = options.get('pathGlobFilter', None)
        modifiedBefore = options.get('modifiedBefore', None)
        modifiedAfter = options.get('modifiedAfter', None)
        recursiveFileLookup = options.get('recursiveFileLookup', None)
        self._set_opts(mergeSchema=mergeSchema, pathGlobFilter=pathGlobFilter,
                       recursiveFileLookup=recursiveFileLookup, modifiedBefore=modifiedBefore,
                       modifiedAfter=modifiedAfter)

        return self._df(self._jreader.parquet(_to_seq(self._spark._sc, paths)))

    def text(self, paths, wholetext=False, lineSep=None, pathGlobFilter=None,
             recursiveFileLookup=None, modifiedBefore=None,
             modifiedAfter=None):
        """
        Loads text files and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.
        The text files must be encoded as UTF-8.

        By default, each line in the text file is a new row in the resulting DataFrame.

        .. versionadded:: 1.6.0

        Parameters
        ----------
        paths : str or list
            string, or list of strings, for input path(s).
        wholetext : str or bool, optional
            if true, read each file from input path(s) as a single row.
        lineSep : str, optional
            defines the line separator that should be used for parsing. If None is
            set, it covers all ``\\r``, ``\\r\\n`` and ``\\n``.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa

            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedBefore (batch only) : an optional timestamp to only include files with
            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedAfter (batch only) : an optional timestamp to only include files with
            modification times occurring after the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)

        Examples
        --------
        >>> df = spark.read.text('python/test_support/sql/text-test.txt')
        >>> df.collect()
        [Row(value='hello'), Row(value='this')]
        >>> df = spark.read.text('python/test_support/sql/text-test.txt', wholetext=True)
        >>> df.collect()
        [Row(value='hello\\nthis')]
        """
        self._set_opts(
            wholetext=wholetext, lineSep=lineSep, pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup, modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter)

        if isinstance(paths, str):
            paths = [paths]
        return self._df(self._jreader.text(self._spark._sc._jvm.PythonUtils.toSeq(paths)))

    def csv(self, path, schema=None, sep=None, encoding=None, quote=None, escape=None,
            comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None,
            ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None,
            negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None,
            maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None,
            columnNameOfCorruptRecord=None, multiLine=None, charToEscapeQuoteEscaping=None,
            samplingRatio=None, enforceSchema=None, emptyValue=None, locale=None, lineSep=None,
            pathGlobFilter=None, recursiveFileLookup=None, modifiedBefore=None, modifiedAfter=None,
            unescapedQuoteHandling=None):
        r"""Loads a CSV file and returns the result as a  :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        path : str or list
            string, or list of strings, for input path(s),
            or RDD of Strings storing CSV rows.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        sep : str, optional
            sets a separator (one or more characters) for each field and value. If None is
            set, it uses the default value, ``,``.
        encoding : str, optional
            decodes the CSV files by the given encoding type. If None is set,
            it uses the default value, ``UTF-8``.
        quote : str, optional
            sets a single character used for escaping quoted values where the
            separator can be part of the value. If None is set, it uses the default
            value, ``"``. If you would like to turn off quotations, you need to set an
            empty string.
        escape : str, optional
            sets a single character used for escaping quotes inside an already
            quoted value. If None is set, it uses the default value, ``\``.
        comment : str, optional
            sets a single character used for skipping lines beginning with this
            character. By default (None), it is disabled.
        header : str or bool, optional
            uses the first line as names of columns. If None is set, it uses the
            default value, ``false``.

            .. note:: if the given path is a RDD of Strings, this header
                option will remove all lines same with the header if exists.

        inferSchema : str or bool, optional
            infers the input schema automatically from data. It requires one extra
            pass over the data. If None is set, it uses the default value, ``false``.
        enforceSchema : str or bool, optional
            If it is set to ``true``, the specified or inferred schema will be
            forcibly applied to datasource files, and headers in CSV files will be
            ignored. If the option is set to ``false``, the schema will be
            validated against all headers in CSV files or the first header in RDD
            if the ``header`` option is set to ``true``. Field names in the schema
            and column names in CSV headers are checked by their positions
            taking into account ``spark.sql.caseSensitive``. If None is set,
            ``true`` is used by default. Though the default value is ``true``,
            it is recommended to disable the ``enforceSchema`` option
            to avoid incorrect results.
        ignoreLeadingWhiteSpace : str or bool, optional
            A flag indicating whether or not leading whitespaces from
            values being read should be skipped. If None is set, it
            uses the default value, ``false``.
        ignoreTrailingWhiteSpace : str or bool, optional
            A flag indicating whether or not trailing whitespaces from
            values being read should be skipped. If None is set, it
            uses the default value, ``false``.
        nullValue : str, optional
            sets the string representation of a null value. If None is set, it uses
            the default value, empty string. Since 2.0.1, this ``nullValue`` param
            applies to all supported types including the string type.
        nanValue : str, optional
            sets the string representation of a non-number value. If None is set, it
            uses the default value, ``NaN``.
        positiveInf : str, optional
            sets the string representation of a positive infinity value. If None
            is set, it uses the default value, ``Inf``.
        negativeInf : str, optional
            sets the string representation of a negative infinity value. If None
            is set, it uses the default value, ``Inf``.
        dateFormat : str, optional
            sets the string that indicates a date format. Custom date formats
            follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to date type. If None is set, it uses the
            default value, ``yyyy-MM-dd``.
        timestampFormat : str, optional
            sets the string that indicates a timestamp format.
            Custom date formats follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to timestamp type. If None is set, it uses the
            default value, ``yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]``.
        maxColumns : str or int, optional
            defines a hard limit of how many columns a record can have. If None is
            set, it uses the default value, ``20480``.
        maxCharsPerColumn : str or int, optional
            defines the maximum number of characters allowed for any given
            value being read. If None is set, it uses the default value,
            ``-1`` meaning unlimited length.
        maxMalformedLogPerPartition : str or int, optional
            this parameter is no longer used since Spark 2.2.0.
            If specified, it is ignored.
        mode : str, optional
            allows a mode for dealing with corrupt records during parsing. If None is
            set, it uses the default value, ``PERMISSIVE``. Note that Spark tries to
            parse only required columns in CSV under column pruning. Therefore, corrupt
            records can be different based on required set of fields. This behavior can
            be controlled by ``spark.sql.csv.parser.columnPruning.enabled``
            (enabled by default).

            * ``PERMISSIVE``: when it meets a corrupted record, puts the malformed string \
              into a field configured by ``columnNameOfCorruptRecord``, and sets malformed \
              fields to ``null``. To keep corrupt records, an user can set a string type \
              field named ``columnNameOfCorruptRecord`` in an user-defined schema. If a \
              schema does not have the field, it drops corrupt records during parsing. \
              A record with less/more tokens than schema is not a corrupted record to CSV. \
              When it meets a record having fewer tokens than the length of the schema, \
              sets ``null`` to extra fields. When the record has more tokens than the \
              length of the schema, it drops extra tokens.
            * ``DROPMALFORMED``: ignores the whole corrupted records.
            * ``FAILFAST``: throws an exception when it meets corrupted records.

        columnNameOfCorruptRecord : str, optional
            allows renaming the new field having malformed string
            created by ``PERMISSIVE`` mode. This overrides
            ``spark.sql.columnNameOfCorruptRecord``. If None is set,
            it uses the value specified in
            ``spark.sql.columnNameOfCorruptRecord``.
        multiLine : str or bool, optional
            parse records, which may span multiple lines. If None is
            set, it uses the default value, ``false``.
        charToEscapeQuoteEscaping : str, optional
            sets a single character used for escaping the escape for
            the quote character. If None is set, the default value is
            escape character when escape and quote characters are
            different, ``\0`` otherwise.
        samplingRatio : str or float, optional
            defines fraction of rows used for schema inferring.
            If None is set, it uses the default value, ``1.0``.
        emptyValue : str, optional
            sets the string representation of an empty value. If None is set, it uses
            the default value, empty string.
        locale : str, optional
            sets a locale as language tag in IETF BCP 47 format. If None is set,
            it uses the default value, ``en-US``. For instance, ``locale`` is used while
            parsing dates and timestamps.
        lineSep : str, optional
            defines the line separator that should be used for parsing. If None is
            set, it covers all ``\\r``, ``\\r\\n`` and ``\\n``.
            Maximum length is 1 character.
        pathGlobFilter : str or bool, optional
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        recursiveFileLookup : str or bool, optional
            recursively scan a directory for files. Using this option disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa

            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedBefore (batch only) : an optional timestamp to only include files with
            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedAfter (batch only) : an optional timestamp to only include files with
            modification times occurring after the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        unescapedQuoteHandling : str, optional
            defines how the CsvParser will handle values with unescaped quotes. If None is
            set, it uses the default value, ``STOP_AT_DELIMITER``.

            * ``STOP_AT_CLOSING_QUOTE``: If unescaped quotes are found in the input, accumulate
              the quote character and proceed parsing the value as a quoted value, until a closing
              quote is found.
            * ``BACK_TO_DELIMITER``: If unescaped quotes are found in the input, consider the value
              as an unquoted value. This will make the parser accumulate all characters of the current
              parsed value until the delimiter is found. If no delimiter is found in the value, the
              parser will continue accumulating characters from the input until a delimiter or line
              ending is found.
            * ``STOP_AT_DELIMITER``: If unescaped quotes are found in the input, consider the value
              as an unquoted value. This will make the parser accumulate all characters until the
              delimiter or a line ending is found in the input.
            * ``SKIP_VALUE``: If unescaped quotes are found in the input, the content parsed
              for the given value will be skipped and the value set in nullValue will be produced
              instead.
            * ``RAISE_ERROR``: If unescaped quotes are found in the input, a TextParsingException
              will be thrown.

        Examples
        --------
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
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping, samplingRatio=samplingRatio,
            enforceSchema=enforceSchema, emptyValue=emptyValue, locale=locale, lineSep=lineSep,
            pathGlobFilter=pathGlobFilter, recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore, modifiedAfter=modifiedAfter,
            unescapedQuoteHandling=unescapedQuoteHandling)
        if isinstance(path, str):
            path = [path]
        if type(path) == list:
            return self._df(self._jreader.csv(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        elif isinstance(path, RDD):
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
            jdataset = self._spark._ssql_ctx.createDataset(
                jrdd.rdd(),
                self._spark._jvm.Encoders.STRING())
            return self._df(self._jreader.csv(jdataset))
        else:
            raise TypeError("path can be only string, list or RDD")

    def orc(self, path, mergeSchema=None, pathGlobFilter=None, recursiveFileLookup=None,
            modifiedBefore=None, modifiedAfter=None):
        """Loads ORC files, returning the result as a :class:`DataFrame`.

        .. versionadded:: 1.5.0

        Parameters
        ----------
        path : str or list
        mergeSchema : str or bool, optional
            sets whether we should merge schemas collected from all
            ORC part-files. This will override ``spark.sql.orc.mergeSchema``.
            The default value is specified in ``spark.sql.orc.mergeSchema``.
        pathGlobFilter : str or bool
            an optional glob pattern to only include files with paths matching
            the pattern. The syntax follows `org.apache.hadoop.fs.GlobFilter`.
            It does not change the behavior of
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa
        recursiveFileLookup : str or bool
            recursively scan a directory for files. Using this option
            disables
            `partition discovery <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_.  # noqa

            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedBefore : an optional timestamp to only include files with
            modification times occurring before the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
        modifiedAfter : an optional timestamp to only include files with
            modification times occurring after the specified time. The provided timestamp
            must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)

        Examples
        --------
        >>> df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> df.dtypes
        [('a', 'bigint'), ('b', 'int'), ('c', 'int')]
        """
        self._set_opts(mergeSchema=mergeSchema, pathGlobFilter=pathGlobFilter,
                       modifiedBefore=modifiedBefore, modifiedAfter=modifiedAfter,
                       recursiveFileLookup=recursiveFileLookup)
        if isinstance(path, str):
            path = [path]
        return self._df(self._jreader.orc(_to_seq(self._spark._sc, path)))

    def jdbc(self, url, table, column=None, lowerBound=None, upperBound=None, numPartitions=None,
             predicates=None, properties=None):
        """
        Construct a :class:`DataFrame` representing the database table named ``table``
        accessible via JDBC URL ``url`` and connection ``properties``.

        Partitions of the table will be retrieved in parallel if either ``column`` or
        ``predicates`` is specified. ``lowerBound``, ``upperBound`` and ``numPartitions``
        is needed when ``column`` is specified.

        If both ``column`` and ``predicates`` are specified, ``column`` will be used.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        url : str
            a JDBC URL of the form ``jdbc:subprotocol:subname``
        table : str
            the name of the table
        column : str, optional
            the name of a column of numeric, date, or timestamp type
            that will be used for partitioning;
            if this parameter is specified, then ``numPartitions``, ``lowerBound``
            (inclusive), and ``upperBound`` (exclusive) will form partition strides
            for generated WHERE clause expressions used to split the column
            ``column`` evenly
        lowerBound : str or int, optional
            the minimum value of ``column`` used to decide partition stride
        upperBound : str or int, optional
            the maximum value of ``column`` used to decide partition stride
        numPartitions : int, optional
            the number of partitions
        predicates : list, optional
            a list of expressions suitable for inclusion in WHERE clauses;
            each one defines one partition of the :class:`DataFrame`
        properties : dict, optional
            a dictionary of JDBC database connection arguments. Normally at
            least properties "user" and "password" with their corresponding values.
            For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }

        Notes
        -----
        Don't create too many partitions in parallel on a large cluster;
        otherwise Spark might crash your external database systems.

        Returns
        -------
        :class:`DataFrame`
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
    (e.g. file systems, key-value stores, etc). Use :attr:`DataFrame.write`
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

    def mode(self, saveMode):
        """Specifies the behavior when data or table already exists.

        Options include:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        .. versionadded:: 1.4.0

        Examples
        --------
        >>> df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        # At the JVM side, the default value of mode is already set to "error".
        # So, if the given saveMode is None, we will not call JVM-side's mode method.
        if saveMode is not None:
            self._jwrite = self._jwrite.mode(saveMode)
        return self

    def format(self, source):
        """Specifies the underlying output data source.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> df.write.format('json').save(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self._jwrite = self._jwrite.format(source)
        return self

    @since(1.5)
    def option(self, key, value):
        """Adds an output option for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a time zone ID to be used to format
                timestamps in the JSON/CSV datasources or partition values. The following
                formats of `timeZone` are supported:

                * Region-based zone ID: It should have the form 'area/city', such as \
                  'America/Los_Angeles'.
                * Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or \
                 '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.

                Other short names like 'CST' are not recommended to use because they can be
                ambiguous. If it isn't set, the current value of the SQL config
                ``spark.sql.session.timeZone`` is used by default.
        """
        self._jwrite = self._jwrite.option(key, to_str(value))
        return self

    @since(1.4)
    def options(self, **options):
        """Adds output options for the underlying data source.

        You can set the following option(s) for writing files:
            * ``timeZone``: sets the string that indicates a time zone ID to be used to format
                timestamps in the JSON/CSV datasources or partition values. The following
                formats of `timeZone` are supported:

                * Region-based zone ID: It should have the form 'area/city', such as \
                  'America/Los_Angeles'.
                * Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or \
                 '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.

                Other short names like 'CST' are not recommended to use because they can be
                ambiguous. If it isn't set, the current value of the SQL config
                ``spark.sql.session.timeZone`` is used by default.
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, to_str(options[k]))
        return self

    def partitionBy(self, *cols):
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        cols : str or list
            name of columns

        Examples
        --------
        >>> df.write.partitionBy('year', 'month').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]
        self._jwrite = self._jwrite.partitionBy(_to_seq(self._spark._sc, cols))
        return self

    def bucketBy(self, numBuckets, col, *cols):
        """Buckets the output by the given columns.If specified,
        the output is laid out on the file system similar to Hive's bucketing scheme.

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

        if not all(isinstance(c, str) for c in cols) or not(isinstance(col, str)):
            raise TypeError("all names should be `str`")

        self._jwrite = self._jwrite.bucketBy(numBuckets, col, _to_seq(self._spark._sc, cols))
        return self

    def sortBy(self, col, *cols):
        """Sorts the output in each bucket by the given columns on the file system.

        .. versionadded:: 2.3.0

        Parameters
        ----------
        col : str, tuple or list
            a name of a column, or a list of names.
        cols : str
            additional names (optional). If `col` is a list it should be empty.

        Examples
        --------
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

        if not all(isinstance(c, str) for c in cols) or not(isinstance(col, str)):
            raise TypeError("all names should be `str`")

        self._jwrite = self._jwrite.sortBy(col, _to_seq(self._spark._sc, cols))
        return self

    def save(self, path=None, format=None, mode=None, partitionBy=None, **options):
        """Saves the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        .. versionadded:: 1.4.0

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
        >>> df.write.mode("append").save(os.path.join(tempfile.mkdtemp(), 'data'))
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
    def insertInto(self, tableName, overwrite=None):
        """Inserts the content of the :class:`DataFrame` to the specified table.

        It requires that the schema of the :class:`DataFrame` is the same as the
        schema of the table.

        Optionally overwriting any existing data.
        """
        if overwrite is not None:
            self.mode("overwrite" if overwrite else "append")
        self._jwrite.insertInto(tableName)

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

        .. versionadded:: 1.4.0

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
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        self._jwrite.saveAsTable(name)

    def json(self, path, mode=None, compression=None, dateFormat=None, timestampFormat=None,
             lineSep=None, encoding=None, ignoreNullFields=None):
        """Saves the content of the :class:`DataFrame` in JSON format
        (`JSON Lines text format or newline-delimited JSON <http://jsonlines.org/>`_) at the
        specified path.

        .. versionadded:: 1.4.0

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
        compression : str, optional
            compression codec to use when saving to file. This can be one of the
            known case-insensitive shorten names (none, bzip2, gzip, lz4,
            snappy and deflate).
        dateFormat : str, optional
            sets the string that indicates a date format. Custom date formats
            follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to date type. If None is set, it uses the
            default value, ``yyyy-MM-dd``.
        timestampFormat : str, optional
            sets the string that indicates a timestamp format.
            Custom date formats follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to timestamp type. If None is set, it uses the
            default value, ``yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]``.
        encoding : str, optional
            specifies encoding (charset) of saved json files. If None is set,
            the default UTF-8 charset will be used.
        lineSep : str, optional
            defines the line separator that should be used for writing. If None is
            set, it uses the default value, ``\\n``.
        ignoreNullFields : str or bool, optional
            Whether to ignore null fields when generating JSON objects.
            If None is set, it uses the default value, ``true``.

        Examples
        --------
        >>> df.write.json(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        self._set_opts(
            compression=compression, dateFormat=dateFormat, timestampFormat=timestampFormat,
            lineSep=lineSep, encoding=encoding, ignoreNullFields=ignoreNullFields)
        self._jwrite.json(path)

    def parquet(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        .. versionadded:: 1.4.0

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
        compression : str, optional
            compression codec to use when saving to file. This can be one of the
            known case-insensitive shorten names (none, uncompressed, snappy, gzip,
            lzo, brotli, lz4, and zstd). This will override
            ``spark.sql.parquet.compression.codec``. If None is set, it uses the
            value specified in ``spark.sql.parquet.compression.codec``.

        Examples
        --------
        >>> df.write.parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self._jwrite.parquet(path)

    def text(self, path, compression=None, lineSep=None):
        """Saves the content of the DataFrame in a text file at the specified path.
        The text files will be encoded as UTF-8.

        .. versionadded:: 1.6.0

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        compression : str, optional
            compression codec to use when saving to file. This can be one of the
            known case-insensitive shorten names (none, bzip2, gzip, lz4,
            snappy and deflate).
        lineSep : str, optional
            defines the line separator that should be used for writing. If None is
            set, it uses the default value, ``\\n``.

        The DataFrame must have only one column that is of string type.
        Each row becomes a new line in the output file.
        """
        self._set_opts(compression=compression, lineSep=lineSep)
        self._jwrite.text(path)

    def csv(self, path, mode=None, compression=None, sep=None, quote=None, escape=None,
            header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None,
            timestampFormat=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None,
            charToEscapeQuoteEscaping=None, encoding=None, emptyValue=None, lineSep=None):
        r"""Saves the content of the :class:`DataFrame` in CSV format at the specified path.

        .. versionadded:: 2.0.0

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

        compression : str, optional
            compression codec to use when saving to file. This can be one of the
            known case-insensitive shorten names (none, bzip2, gzip, lz4,
            snappy and deflate).
        sep : str, optional
            sets a separator (one or more characters) for each field and value. If None is
            set, it uses the default value, ``,``.
        quote : str, optional
            sets a single character used for escaping quoted values where the
            separator can be part of the value. If None is set, it uses the default
            value, ``"``. If an empty string is set, it uses ``u0000`` (null character).
        escape : str, optional
            sets a single character used for escaping quotes inside an already
            quoted value. If None is set, it uses the default value, ``\``
        escapeQuotes : str or bool, optional
            a flag indicating whether values containing quotes should always
            be enclosed in quotes. If None is set, it uses the default value
            ``true``, escaping all values containing a quote character.
        quoteAll : str or bool, optional
            a flag indicating whether all values should always be enclosed in
            quotes. If None is set, it uses the default value ``false``,
            only escaping values containing a quote character.
        header : str or bool, optional
            writes the names of columns as the first line. If None is set, it uses
            the default value, ``false``.
        nullValue : str, optional
            sets the string representation of a null value. If None is set, it uses
            the default value, empty string.
        dateFormat : str, optional
            sets the string that indicates a date format. Custom date formats follow
            the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to date type. If None is set, it uses the
            default value, ``yyyy-MM-dd``.
        timestampFormat : str, optional
            sets the string that indicates a timestamp format.
            Custom date formats follow the formats at
            `datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.  # noqa
            This applies to timestamp type. If None is set, it uses the
            default value, ``yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]``.
        ignoreLeadingWhiteSpace : str or bool, optional
            a flag indicating whether or not leading whitespaces from
            values being written should be skipped. If None is set, it
            uses the default value, ``true``.
        ignoreTrailingWhiteSpace : str or bool, optional
            a flag indicating whether or not trailing whitespaces from
            values being written should be skipped. If None is set, it
            uses the default value, ``true``.
        charToEscapeQuoteEscaping : str, optional
            sets a single character used for escaping the escape for
            the quote character. If None is set, the default value is
            escape character when escape and quote characters are
            different, ``\0`` otherwise..
        encoding : str, optional
            sets the encoding (charset) of saved csv files. If None is set,
            the default UTF-8 charset will be used.
        emptyValue : str, optional
            sets the string representation of an empty value. If None is set, it uses
            the default value, ``""``.
        lineSep : str, optional
            defines the line separator that should be used for writing. If None is
            set, it uses the default value, ``\\n``. Maximum length is 1 character.

        Examples
        --------
        >>> df.write.csv(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        self._set_opts(compression=compression, sep=sep, quote=quote, escape=escape, header=header,
                       nullValue=nullValue, escapeQuotes=escapeQuotes, quoteAll=quoteAll,
                       dateFormat=dateFormat, timestampFormat=timestampFormat,
                       ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
                       ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
                       charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
                       encoding=encoding, emptyValue=emptyValue, lineSep=lineSep)
        self._jwrite.csv(path)

    def orc(self, path, mode=None, partitionBy=None, compression=None):
        """Saves the content of the :class:`DataFrame` in ORC format at the specified path.

        .. versionadded:: 1.5.0

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
        compression : str, optional
            compression codec to use when saving to file. This can be one of the
            known case-insensitive shorten names (none, snappy, zlib, and lzo).
            This will override ``orc.compress`` and
            ``spark.sql.orc.compression.codec``. If None is set, it uses the value
            specified in ``spark.sql.orc.compression.codec``.

        Examples
        --------
        >>> orc_df = spark.read.orc('python/test_support/sql/orc_partitioned')
        >>> orc_df.write.orc(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self._jwrite.orc(path)

    def jdbc(self, url, table, mode=None, properties=None):
        """Saves the content of the :class:`DataFrame` to an external database table via JDBC.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        url : str
            a JDBC URL of the form ``jdbc:subprotocol:subname``
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

        Notes
        -----
        Don't create too many partitions in parallel on a large cluster;
        otherwise Spark might crash your external database systems.
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._spark._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self.mode(mode)._jwrite.jdbc(url, table, jprop)


class DataFrameWriterV2(object):
    """
    Interface used to write a class:`pyspark.sql.dataframe.DataFrame`
    to external storage using the v2 API.

    .. versionadded:: 3.1.0
    """

    def __init__(self, df, table):
        self._df = df
        self._spark = df.sql_ctx
        self._jwriter = df._jdf.writeTo(table)

    @since(3.1)
    def using(self, provider):
        """
        Specifies a provider for the underlying output data source.
        Spark's default catalog supports "parquet", "json", etc.
        """
        self._jwriter.using(provider)
        return self

    @since(3.1)
    def option(self, key, value):
        """
        Add a write option.
        """
        self._jwriter.option(key, to_str(value))
        return self

    @since(3.1)
    def options(self, **options):
        """
        Add write options.
        """
        options = {k: to_str(v) for k, v in options.items()}
        self._jwriter.options(options)
        return self

    @since(3.1)
    def tableProperty(self, property, value):
        """
        Add table property.
        """
        self._jwriter.tableProperty(property, value)
        return self

    @since(3.1)
    def partitionedBy(self, col, *cols):
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

        """
        col = _to_java_column(col)
        cols = _to_seq(self._spark._sc, [_to_java_column(c) for c in cols])
        return self

    @since(3.1)
    def create(self):
        """
        Create a new table from the contents of the data frame.

        The new table's schema, partition layout, properties, and other configuration will be
        based on the configuration set on this writer.
        """
        self._jwriter.create()

    @since(3.1)
    def replace(self):
        """
        Replace an existing table with the contents of the data frame.

        The existing table's schema, partition layout, properties, and other configuration will be
        replaced with the contents of the data frame and the configuration set on this writer.
        """
        self._jwriter.replace()

    @since(3.1)
    def createOrReplace(self):
        """
        Create a new table or replace an existing table with the contents of the data frame.

        The output table's schema, partition layout, properties,
        and other configuration will be based on the contents of the data frame
        and the configuration set on this writer.
        If the table exists, its configuration and data will be replaced.
        """
        self._jwriter.createOrReplace()

    @since(3.1)
    def append(self):
        """
        Append the contents of the data frame to the output table.
        """
        self._jwriter.append()

    @since(3.1)
    def overwrite(self, condition):
        """
        Overwrite rows matching the given filter condition with the contents of the data frame in
        the output table.
        """
        self._jwriter.overwrite(condition)

    @since(3.1)
    def overwritePartitions(self):
        """
        Overwrite all partition for which the data frame contains at least one row with the contents
        of the data frame in the output table.

        This operation is equivalent to Hive's `INSERT OVERWRITE ... PARTITION`, which replaces
        partitions dynamically depending on the contents of the data frame.
        """
        self._jwriter.overwritePartitions()


def _test():
    import doctest
    import os
    import tempfile
    import py4j
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession
    import pyspark.sql.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.readwriter.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    try:
        spark = SparkSession.builder.getOrCreate()
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
        sys.exit(-1)


if __name__ == "__main__":
    _test()
