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

from pyspark import RDD, since
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.column import _to_seq
from pyspark.sql.types import *
from pyspark.sql import utils

__all__ = ["DataFrameReader", "DataFrameWriter"]


def to_str(value):
    """
    A wrapper over str(), but convert bool values to lower case string
    """
    if isinstance(value, bool):
        return str(value).lower()
    else:
        return str(value)


class DataFrameReader(object):
    """
    Interface used to load a :class:`DataFrame` from external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`SQLContext.read`
    to access this.

    ::Note: Experimental

    .. versionadded:: 1.4
    """

    def __init__(self, sqlContext):
        self._jreader = sqlContext._ssql_ctx.read()
        self._sqlContext = sqlContext

    def _df(self, jdf):
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(jdf, self._sqlContext)

    @since(1.4)
    def format(self, source):
        """Specifies the input data source format.

        :param source: string, name of the data source, e.g. 'json', 'parquet'.

        >>> df = sqlContext.read.format('json').load('python/test_support/sql/people.json')
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
        jschema = self._sqlContext._ssql_ctx.parseDataType(schema.json())
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

        >>> df = sqlContext.read.load('python/test_support/sql/parquet_partitioned', opt1=True,
        ...     opt2=1, opt3='str')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]

        >>> df = sqlContext.read.format('json').load(['python/test_support/sql/people.json',
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
            if type(path) == list:
                return self._df(
                    self._jreader.load(self._sqlContext._sc._jvm.PythonUtils.toSeq(path)))
            else:
                return self._df(self._jreader.load(path))
        else:
            return self._df(self._jreader.load())

    @since(1.4)
    def json(self, path, schema=None):
        """
        Loads a JSON file (one object per line) or an RDD of Strings storing JSON objects
        (one object per record) and returns the result as a :class`DataFrame`.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        :param path: string represents path to the JSON dataset,
                     or RDD of Strings storing JSON objects.
        :param schema: an optional :class:`StructType` for the input schema.

        You can set the following JSON-specific options to deal with non-standard JSON files:
            * ``primitivesAsString`` (default ``false``): infers all primitive values as a string \
                type
            * ``allowComments`` (default ``false``): ignores Java/C++ style comment in JSON records
            * ``allowUnquotedFieldNames`` (default ``false``): allows unquoted JSON field names
            * ``allowSingleQuotes`` (default ``true``): allows single quotes in addition to double \
                quotes
            * ``allowNumericLeadingZeros`` (default ``false``): allows leading zeros in numbers \
                (e.g. 00012)

        >>> df1 = sqlContext.read.json('python/test_support/sql/people.json')
        >>> df1.dtypes
        [('age', 'bigint'), ('name', 'string')]
        >>> rdd = sc.textFile('python/test_support/sql/people.json')
        >>> df2 = sqlContext.read.json(rdd)
        >>> df2.dtypes
        [('age', 'bigint'), ('name', 'string')]

        """
        if schema is not None:
            self.schema(schema)
        if isinstance(path, basestring):
            return self._df(self._jreader.json(path))
        elif type(path) == list:
            return self._df(self._jreader.json(self._sqlContext._sc._jvm.PythonUtils.toSeq(path)))
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
            jrdd = keyed._jrdd.map(self._sqlContext._jvm.BytesToString())
            return self._df(self._jreader.json(jrdd))
        else:
            raise TypeError("path can be only string or RDD")

    @since(1.4)
    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.

        :param tableName: string, name of the table.

        >>> df = sqlContext.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.registerTempTable('tmpTable')
        >>> sqlContext.read.table('tmpTable').dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        return self._df(self._jreader.table(tableName))

    @since(1.4)
    def parquet(self, *paths):
        """Loads a Parquet file, returning the result as a :class:`DataFrame`.

        >>> df = sqlContext.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        return self._df(self._jreader.parquet(_to_seq(self._sqlContext._sc, paths)))

    @ignore_unicode_prefix
    @since(1.6)
    def text(self, paths):
        """Loads a text file and returns a [[DataFrame]] with a single string column named "value".

        Each line in the text file is a new row in the resulting DataFrame.

        :param paths: string, or list of strings, for input path(s).

        >>> df = sqlContext.read.text('python/test_support/sql/text-test.txt')
        >>> df.collect()
        [Row(value=u'hello'), Row(value=u'this')]
        """
        if isinstance(paths, basestring):
            paths = [paths]
        return self._df(self._jreader.text(self._sqlContext._sc._jvm.PythonUtils.toSeq(paths)))

    @since(1.5)
    def orc(self, path):
        """Loads an ORC file, returning the result as a :class:`DataFrame`.

        ::Note: Currently ORC support is only available together with
        :class:`HiveContext`.

        >>> df = hiveContext.read.orc('python/test_support/sql/orc_partitioned')
        >>> df.dtypes
        [('a', 'bigint'), ('b', 'int'), ('c', 'int')]
        """
        return self._df(self._jreader.orc(path))

    @since(1.4)
    def jdbc(self, url, table, column=None, lowerBound=None, upperBound=None, numPartitions=None,
             predicates=None, properties=None):
        """
        Construct a :class:`DataFrame` representing the database table accessible
        via JDBC URL `url` named `table` and connection `properties`.

        The `column` parameter could be used to partition the table, then it will
        be retrieved in parallel based on the parameters passed to this function.

        The `predicates` parameter gives a list expressions suitable for inclusion
        in WHERE clauses; each one defines one partition of the :class:`DataFrame`.

        ::Note: Don't create too many partitions in parallel on a large cluster;
        otherwise Spark might crash your external database systems.

        :param url: a JDBC URL
        :param table: name of table
        :param column: the column used to partition
        :param lowerBound: the lower bound of partition column
        :param upperBound: the upper bound of the partition column
        :param numPartitions: the number of partitions
        :param predicates: a list of expressions
        :param properties: JDBC database connection arguments, a list of arbitrary string
                           tag/value. Normally at least a "user" and "password" property
                           should be included.
        :return: a DataFrame
        """
        if properties is None:
            properties = dict()
        jprop = JavaClass("java.util.Properties", self._sqlContext._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        if column is not None:
            if numPartitions is None:
                numPartitions = self._sqlContext._sc.defaultParallelism
            return self._df(self._jreader.jdbc(url, table, column, int(lowerBound), int(upperBound),
                                               int(numPartitions), jprop))
        if predicates is not None:
            gateway = self._sqlContext._sc._gateway
            jpredicates = utils.toJArray(gateway, gateway.jvm.java.lang.String, predicates)
            return self._df(self._jreader.jdbc(url, table, jpredicates, jprop))
        return self._df(self._jreader.jdbc(url, table, jprop))


class DataFrameWriter(object):
    """
    Interface used to write a [[DataFrame]] to external storage systems
    (e.g. file systems, key-value stores, etc). Use :func:`DataFrame.write`
    to access this.

    ::Note: Experimental

    .. versionadded:: 1.4
    """
    def __init__(self, df):
        self._df = df
        self._sqlContext = df.sql_ctx
        self._jwrite = df._jdf.write()

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
        self._jwrite = self._jwrite.option(key, value)
        return self

    @since(1.4)
    def options(self, **options):
        """Adds output options for the underlying data source.
        """
        for k in options:
            self._jwrite = self._jwrite.option(k, options[k])
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
        self._jwrite = self._jwrite.partitionBy(_to_seq(self._sqlContext._sc, cols))
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
        When `mode` is `Overwrite`, the schema of the [[DataFrame]] does not need to be
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
    def json(self, path, mode=None):
        """Saves the content of the :class:`DataFrame` in JSON format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.

        >>> df.write.json(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)._jwrite.json(path)

    @since(1.4)
    def parquet(self, path, mode=None, partitionBy=None):
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns

        >>> df.write.parquet(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._jwrite.parquet(path)

    @since(1.6)
    def text(self, path):
        """Saves the content of the DataFrame in a text file at the specified path.

        The DataFrame must have only one column that is of string type.
        Each row becomes a new line in the output file.
        """
        self._jwrite.text(path)

    @since(1.5)
    def orc(self, path, mode=None, partitionBy=None):
        """Saves the content of the :class:`DataFrame` in ORC format at the specified path.

        ::Note: Currently ORC support is only available together with
        :class:`HiveContext`.

        :param path: the path in any Hadoop supported file system
        :param mode: specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` (default case): Throw an exception if data already exists.
        :param partitionBy: names of partitioning columns

        >>> orc_df = hiveContext.read.orc('python/test_support/sql/orc_partitioned')
        >>> orc_df.write.orc(os.path.join(tempfile.mkdtemp(), 'data'))
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._jwrite.orc(path)

    @since(1.4)
    def jdbc(self, url, table, mode=None, properties=None):
        """Saves the content of the :class:`DataFrame` to a external database table via JDBC.

        .. note:: Don't create too many partitions in parallel on a large cluster;\
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
        jprop = JavaClass("java.util.Properties", self._sqlContext._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self._jwrite.mode(mode).jdbc(url, table, jprop)


def _test():
    import doctest
    import os
    import tempfile
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext, HiveContext
    import pyspark.sql.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.readwriter.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')

    globs['tempfile'] = tempfile
    globs['os'] = os
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['hiveContext'] = HiveContext(sc)
    globs['df'] = globs['sqlContext'].read.parquet('python/test_support/sql/parquet_partitioned')

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.readwriter, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
