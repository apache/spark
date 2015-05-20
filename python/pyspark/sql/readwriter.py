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
    def load(self, path=None, format=None, schema=None, **options):
        """Loads data from a data source and returns it as a :class`DataFrame`.

        :param path: optional string for file-system backed data sources.
        :param format: optional string for format of the data source. Default to 'parquet'.
        :param schema: optional :class:`StructType` for the input schema.
        :param options: all other string options
        """
        jreader = self._jreader
        if format is not None:
            jreader = jreader.format(format)
        if schema is not None:
            if not isinstance(schema, StructType):
                raise TypeError("schema should be StructType")
            jschema = self._sqlContext._ssql_ctx.parseDataType(schema.json())
            jreader = jreader.schema(jschema)
        for k in options:
            jreader = jreader.option(k, options[k])
        if path is not None:
            return self._df(jreader.load(path))
        else:
            return self._df(jreader.load())

    @since(1.4)
    def json(self, path, schema=None):
        """
        Loads a JSON file (one object per line) and returns the result as
        a :class`DataFrame`.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        :param path: string, path to the JSON dataset.
        :param schema: an optional :class:`StructType` for the input schema.

        >>> import tempfile, shutil
        >>> jsonFile = tempfile.mkdtemp()
        >>> shutil.rmtree(jsonFile)
        >>> with open(jsonFile, 'w') as f:
        ...     f.writelines(jsonStrings)
        >>> df1 = sqlContext.read.json(jsonFile)
        >>> df1.printSchema()
        root
         |-- field1: long (nullable = true)
         |-- field2: string (nullable = true)
         |-- field3: struct (nullable = true)
         |    |-- field4: long (nullable = true)

        >>> from pyspark.sql.types import *
        >>> schema = StructType([
        ...     StructField("field2", StringType()),
        ...     StructField("field3",
        ...         StructType([StructField("field5", ArrayType(IntegerType()))]))])
        >>> df2 = sqlContext.read.json(jsonFile, schema)
        >>> df2.printSchema()
        root
         |-- field2: string (nullable = true)
         |-- field3: struct (nullable = true)
         |    |-- field5: array (nullable = true)
         |    |    |-- element: integer (containsNull = true)
        """
        if schema is None:
            jdf = self._jreader.json(path)
        else:
            jschema = self._sqlContext._ssql_ctx.parseDataType(schema.json())
            jdf = self._jreader.schema(jschema).json(path)
        return self._df(jdf)

    @since(1.4)
    def table(self, tableName):
        """Returns the specified table as a :class:`DataFrame`.

        >>> sqlContext.registerDataFrameAsTable(df, "table1")
        >>> df2 = sqlContext.read.table("table1")
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
        """
        return self._df(self._jreader.table(tableName))

    @since(1.4)
    def parquet(self, *path):
        """Loads a Parquet file, returning the result as a :class:`DataFrame`.

        >>> import tempfile, shutil
        >>> parquetFile = tempfile.mkdtemp()
        >>> shutil.rmtree(parquetFile)
        >>> df.saveAsParquetFile(parquetFile)
        >>> df2 = sqlContext.read.parquet(parquetFile)
        >>> sorted(df.collect()) == sorted(df2.collect())
        True
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
    def save(self, path=None, format=None, mode="error", **options):
        """
        Saves the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Additionally, mode is used to specify the behavior of the save operation when
        data already exists in the data source. There are four modes:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param path: the path in a Hadoop supported file system
        :param format: the format used to save
        :param mode: one of `append`, `overwrite`, `error`, `ignore` (default: error)
        :param options: all other string options
        """
        jwrite = self._jwrite.mode(mode)
        if format is not None:
            jwrite = jwrite.format(format)
        for k in options:
            jwrite = jwrite.option(k, options[k])
        if path is None:
            jwrite.save()
        else:
            jwrite.save(path)

    @since(1.4)
    def saveAsTable(self, name, format=None, mode="error", **options):
        """
        Saves the contents of this :class:`DataFrame` to a data source as a table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Additionally, mode is used to specify the behavior of the saveAsTable operation when
        table already exists in the data source. There are four modes:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param name: the table name
        :param format: the format used to save
        :param mode: one of `append`, `overwrite`, `error`, `ignore` (default: error)
        :param options: all other string options
        """
        jwrite = self._jwrite.mode(mode)
        if format is not None:
            jwrite = jwrite.format(format)
        for k in options:
            jwrite = jwrite.option(k, options[k])
        return jwrite.saveAsTable(name)

    @since(1.4)
    def json(self, path, mode="error"):
        """
        Saves the content of the :class:`DataFrame` in JSON format at the
        specified path.

        Additionally, mode is used to specify the behavior of the save operation when
        data already exists in the data source. There are four modes:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param path: the path in any Hadoop supported file system
        :param mode: one of `append`, `overwrite`, `error`, `ignore` (default: error)
        """
        return self._jwrite.mode(mode).json(path)

    @since(1.4)
    def parquet(self, path, mode="error"):
        """
        Saves the content of the :class:`DataFrame` in Parquet format at the
        specified path.

        Additionally, mode is used to specify the behavior of the save operation when
        data already exists in the data source. There are four modes:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param path: the path in any Hadoop supported file system
        :param mode: one of `append`, `overwrite`, `error`, `ignore` (default: error)
        """
        return self._jwrite.mode(mode).parquet(path)

    @since(1.4)
    def jdbc(self, url, table, mode="error", properties={}):
        """
        Saves the content of the :class:`DataFrame` to a external database table
        via JDBC.

        In the case the table already exists in the external database,
        behavior of this function depends on the save mode, specified by the `mode`
        function (default to throwing an exception). There are four modes:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        :param url: a JDBC URL of the form `jdbc:subprotocol:subname`
        :param table: Name of the table in the external database.
        :param mode: one of `append`, `overwrite`, `error`, `ignore` (default: error)
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
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.readwriter
    globs = pyspark.sql.readwriter.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['df'] = sc.parallelize([(2, 'Alice'), (5, 'Bob')]) \
        .toDF(StructType([StructField('age', IntegerType()),
                          StructField('name', StringType())]))
    jsonStrings = [
        '{"field1": 1, "field2": "row1", "field3":{"field4":11}}',
        '{"field1" : 2, "field3":{"field4":22, "field5": [10, 11]},'
        '"field6":[{"field7": "row2"}]}',
        '{"field1" : null, "field2": "row3", '
        '"field3":{"field4":33, "field5": []}}'
    ]
    globs['jsonStrings'] = jsonStrings
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.readwriter, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
