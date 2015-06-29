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

from py4j.java_gateway import JavaClass

from pyspark.sql import since
from pyspark.sql.column import _to_seq
from pyspark.sql.types import *

__all__ = ["DataFrameReader", "DataFrameWriter"]


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
        self._jreader = self._jreader.option(key, value)
        return self

    @since(1.4)
    def options(self, **options):
        """Adds input options for the underlying data source.
        """
        for k in options:
            self._jreader = self._jreader.option(k, options[k])
        return self

    @since(1.4)
    def load(self, path=None, format=None, schema=None, **options):
        """Loads data from a data source and returns it as a :class`DataFrame`.

        :param path: optional string for file-system backed data sources.
        :param format: optional string for format of the data source. Default to 'parquet'.
        :param schema: optional :class:`StructType` for the input schema.
        :param options: all other string options

        >>> df = sqlContext.read.load('python/test_support/sql/parquet_partitioned')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if path is not None:
            return self._df(self._jreader.load(path))
        else:
            return self._df(self._jreader.load())

    @since(1.4)
    def json(self, path, schema=None):
        """
        Loads a JSON file (one object per line) and returns the result as
        a :class`DataFrame`.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        :param path: string, path to the JSON dataset.
        :param schema: an optional :class:`StructType` for the input schema.

        >>> df = sqlContext.read.json('python/test_support/sql/people.json')
        >>> df.dtypes
        [('age', 'bigint'), ('name', 'string')]

        """
        if schema is not None:
            self.schema(schema)
        return self._df(self._jreader.json(path))

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
    def parquet(self, *path):
        """Loads a Parquet file, returning the result as a :class:`DataFrame`.

        >>> df = sqlContext.read.parquet('python/test_support/sql/parquet_partitioned')
        >>> df.dtypes
        [('name', 'string'), ('year', 'int'), ('month', 'int'), ('day', 'int')]
        """
        return self._df(self._jreader.parquet(_to_seq(self._sqlContext._sc, path)))

    @since(1.4)
    def jdbc(self, url, table, column=None, lowerBound=None, upperBound=None, numPartitions=None,
             predicates=None, properties={}):
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
        jprop = JavaClass("java.util.Properties", self._sqlContext._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        if column is not None:
            if numPartitions is None:
                numPartitions = self._sqlContext._sc.defaultParallelism
            return self._df(self._jreader.jdbc(url, table, column, int(lowerBound), int(upperBound),
                                               int(numPartitions), jprop))
        if predicates is not None:
            arr = self._sqlContext._sc._jvm.PythonUtils.toArray(predicates)
            return self._df(self._jreader.jdbc(url, table, arr, jprop))
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
        if len(cols) > 0:
            self._jwrite = self._jwrite.partitionBy(_to_seq(self._sqlContext._sc, cols))
        return self

    @since(1.4)
    def save(self, path=None, format=None, mode=None, partitionBy=(), **options):
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
        self.partitionBy(partitionBy).mode(mode).options(**options)
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
    def saveAsTable(self, name, format=None, mode=None, partitionBy=(), **options):
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
        self.partitionBy(partitionBy).mode(mode).options(**options)
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
    def parquet(self, path, mode=None, partitionBy=()):
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
        self.partitionBy(partitionBy).mode(mode)
        self._jwrite.parquet(path)

    @since(1.4)
    def jdbc(self, url, table, mode=None, properties={}):
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
        jprop = JavaClass("java.util.Properties", self._sqlContext._sc._gateway._gateway_client)()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self._jwrite.mode(mode).jdbc(url, table, jprop)


def _test():
    import doctest
    import os
    import tempfile
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.readwriter.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')

    globs['tempfile'] = tempfile
    globs['os'] = os
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['df'] = globs['sqlContext'].read.parquet('python/test_support/sql/parquet_partitioned')

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.readwriter, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
