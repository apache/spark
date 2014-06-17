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

from pyspark.rdd import RDD
from pyspark.serializers import BatchedSerializer, PickleSerializer

from py4j.protocol import Py4JError

__all__ = ["SQLContext", "HiveContext", "LocalHiveContext", "TestHiveContext", "SchemaRDD", "Row"]


class SQLContext:
    """Main entry point for SparkSQL functionality.

    A SQLContext can be used create L{SchemaRDD}s, register L{SchemaRDD}s as
    tables, execute SQL over tables, cache tables, and read parquet files.
    """

    def __init__(self, sparkContext, sqlContext = None):
        """Create a new SQLContext.

        @param sparkContext: The SparkContext to wrap.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.inferSchema(srdd) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        ValueError:...

        >>> bad_rdd = sc.parallelize([1,2,3])
        >>> sqlCtx.inferSchema(bad_rdd) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        ValueError:...

        >>> allTypes = sc.parallelize([{"int" : 1, "string" : "string", "double" : 1.0, "long": 1L,
        ... "boolean" : True}])
        >>> srdd = sqlCtx.inferSchema(allTypes).map(lambda x: (x.int, x.string, x.double, x.long,
        ... x.boolean))
        >>> srdd.collect()[0]
        (1, u'string', 1.0, 1, True)
        """
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        self._pythonToJavaMap = self._jvm.PythonRDD.pythonToJavaMap

        if sqlContext:
            self._scala_SQLContext = sqlContext

    @property
    def _ssql_ctx(self):
        """Accessor for the JVM SparkSQL context.

        Subclasses can override this property to provide their own
        JVM Contexts.
        """
        if not hasattr(self, '_scala_SQLContext'):
            self._scala_SQLContext = self._jvm.SQLContext(self._jsc.sc())
        return self._scala_SQLContext

    def inferSchema(self, rdd):
        """Infer and apply a schema to an RDD of L{dict}s.

        We peek at the first row of the RDD to determine the fields names
        and types, and then use that to extract all the dictionaries. Nested
        collections are supported, which include array, dict, list, set, and
        tuple.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.collect() == [{"field1" : 1, "field2" : "row1"}, {"field1" : 2, "field2": "row2"},
        ...                    {"field1" : 3, "field2": "row3"}]
        True

        >>> from array import array
        >>> srdd = sqlCtx.inferSchema(nestedRdd1)
        >>> srdd.collect() == [{"f1" : array('i', [1, 2]), "f2" : {"row1" : 1.0}},
        ...                    {"f1" : array('i', [2, 3]), "f2" : {"row2" : 2.0}}]
        True

        >>> srdd = sqlCtx.inferSchema(nestedRdd2)
        >>> srdd.collect() == [{"f1" : [[1, 2], [2, 3]], "f2" : set([1, 2]), "f3" : (1, 2)},
        ...                    {"f1" : [[2, 3], [3, 4]], "f2" : set([2, 3]), "f3" : (2, 3)}]
        True
        """
        if (rdd.__class__ is SchemaRDD):
            raise ValueError("Cannot apply schema to %s" % SchemaRDD.__name__)
        elif not isinstance(rdd.first(), dict):
            raise ValueError("Only RDDs with dictionaries can be converted to %s: %s" %
                             (SchemaRDD.__name__, rdd.first()))

        jrdd = self._pythonToJavaMap(rdd._jrdd)
        srdd = self._ssql_ctx.inferSchema(jrdd.rdd())
        return SchemaRDD(srdd, self)

    def registerRDDAsTable(self, rdd, tableName):
        """Registers the given RDD as a temporary table in the catalog.

        Temporary tables exist only during the lifetime of this instance of
        SQLContext.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        """
        if (rdd.__class__ is SchemaRDD):
            jschema_rdd = rdd._jschema_rdd
            self._ssql_ctx.registerRDDAsTable(jschema_rdd, tableName)
        else:
            raise ValueError("Can only register SchemaRDD as table")

    def parquetFile(self, path):
        """Loads a Parquet file, returning the result as a L{SchemaRDD}.

        >>> import tempfile, shutil
        >>> parquetFile = tempfile.mkdtemp()
        >>> shutil.rmtree(parquetFile)
        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.saveAsParquetFile(parquetFile)
        >>> srdd2 = sqlCtx.parquetFile(parquetFile)
        >>> sorted(srdd.collect()) == sorted(srdd2.collect())
        True
        """
        jschema_rdd = self._ssql_ctx.parquetFile(path)
        return SchemaRDD(jschema_rdd, self)

    def sql(self, sqlQuery):
        """Return a L{SchemaRDD} representing the result of the given query.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> srdd2.collect() == [{"f1" : 1, "f2" : "row1"}, {"f1" : 2, "f2": "row2"},
        ...                     {"f1" : 3, "f2": "row3"}]
        True
        """
        return SchemaRDD(self._ssql_ctx.sql(sqlQuery), self)

    def table(self, tableName):
        """Returns the specified table as a L{SchemaRDD}.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.table("table1")
        >>> sorted(srdd.collect()) == sorted(srdd2.collect())
        True
        """
        return SchemaRDD(self._ssql_ctx.table(tableName), self)

    def cacheTable(self, tableName):
        """Caches the specified table in-memory."""
        self._ssql_ctx.cacheTable(tableName)

    def uncacheTable(self, tableName):
        """Removes the specified table from the in-memory cache."""
        self._ssql_ctx.uncacheTable(tableName)


class HiveContext(SQLContext):
    """A variant of Spark SQL that integrates with data stored in Hive.

    Configuration for Hive is read from hive-site.xml on the classpath.
    It supports running both SQL and HiveQL commands.
    """

    @property
    def _ssql_ctx(self):
        try:
            if not hasattr(self, '_scala_HiveContext'):
                self._scala_HiveContext = self._get_hive_ctx()
            return self._scala_HiveContext
        except Py4JError as e:
            raise Exception("You must build Spark with Hive. Export 'SPARK_HIVE=true' and run " \
                            "sbt/sbt assembly" , e)

    def _get_hive_ctx(self):
        return self._jvm.HiveContext(self._jsc.sc())

    def hiveql(self, hqlQuery):
        """
        Runs a query expressed in HiveQL, returning the result as a L{SchemaRDD}.
        """
        return SchemaRDD(self._ssql_ctx.hiveql(hqlQuery), self)

    def hql(self, hqlQuery):
        """
        Runs a query expressed in HiveQL, returning the result as a L{SchemaRDD}.
        """
        return self.hiveql(hqlQuery)


class LocalHiveContext(HiveContext):
    """Starts up an instance of hive where metadata is stored locally.

    An in-process metadata data is created with data stored in ./metadata.
    Warehouse data is stored in in ./warehouse.

    >>> import os
    >>> hiveCtx = LocalHiveContext(sc)
    >>> try:
    ...     supress = hiveCtx.hql("DROP TABLE src")
    ... except Exception:
    ...     pass
    >>> kv1 = os.path.join(os.environ["SPARK_HOME"], 'examples/src/main/resources/kv1.txt')
    >>> supress = hiveCtx.hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    >>> supress = hiveCtx.hql("LOAD DATA LOCAL INPATH '%s' INTO TABLE src" % kv1)
    >>> results = hiveCtx.hql("FROM src SELECT value").map(lambda r: int(r.value.split('_')[1]))
    >>> num = results.count()
    >>> reduce_sum = results.reduce(lambda x, y: x + y)
    >>> num
    500
    >>> reduce_sum
    130091
    """

    def _get_hive_ctx(self):
        return self._jvm.LocalHiveContext(self._jsc.sc())


class TestHiveContext(HiveContext):

    def _get_hive_ctx(self):
        return self._jvm.TestHiveContext(self._jsc.sc())


# TODO: Investigate if it is more efficient to use a namedtuple. One problem is that named tuples
# are custom classes that must be generated per Schema.
class Row(dict):
    """A row in L{SchemaRDD}.

    An extended L{dict} that takes a L{dict} in its constructor, and
    exposes those items as fields.

    >>> r = Row({"hello" : "world", "foo" : "bar"})
    >>> r.hello
    'world'
    >>> r.foo
    'bar'
    """

    def __init__(self, d):
        d.update(self.__dict__)
        self.__dict__ = d
        dict.__init__(self, d)


class SchemaRDD(RDD):
    """An RDD of L{Row} objects that has an associated schema.

    The underlying JVM object is a SchemaRDD, not a PythonRDD, so we can
    utilize the relational query api exposed by SparkSQL.

    For normal L{pyspark.rdd.RDD} operations (map, count, etc.) the
    L{SchemaRDD} is not operated on directly, as it's underlying
    implementation is a RDD composed of Java objects. Instead it is
    converted to a PythonRDD in the JVM, on which Python operations can
    be done.
    """

    def __init__(self, jschema_rdd, sql_ctx):
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx._sc
        self._jschema_rdd = jschema_rdd

        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = self.sql_ctx._sc
        self._jrdd_deserializer = self.ctx.serializer

    @property
    def _jrdd(self):
        """Lazy evaluation of PythonRDD object.

        Only done when a user calls methods defined by the
        L{pyspark.rdd.RDD} super class (map, filter, etc.).
        """
        if not hasattr(self, '_lazy_jrdd'):
            self._lazy_jrdd = self._toPython()._jrdd
        return self._lazy_jrdd

    @property
    def _id(self):
        return self._jrdd.id()

    def saveAsParquetFile(self, path):
        """Save the contents as a Parquet file, preserving the schema.

        Files that are written out using this method can be read back in as
        a SchemaRDD using the L{SQLContext.parquetFile} method.

        >>> import tempfile, shutil
        >>> parquetFile = tempfile.mkdtemp()
        >>> shutil.rmtree(parquetFile)
        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.saveAsParquetFile(parquetFile)
        >>> srdd2 = sqlCtx.parquetFile(parquetFile)
        >>> sorted(srdd2.collect()) == sorted(srdd.collect())
        True
        """
        self._jschema_rdd.saveAsParquetFile(path)

    def registerAsTable(self, name):
        """Registers this RDD as a temporary table using the given name.

        The lifetime of this temporary table is tied to the L{SQLContext}
        that was used to create this SchemaRDD.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.registerAsTable("test")
        >>> srdd2 = sqlCtx.sql("select * from test")
        >>> sorted(srdd.collect()) == sorted(srdd2.collect())
        True
        """
        self._jschema_rdd.registerAsTable(name)

    def insertInto(self, tableName, overwrite = False):
        """Inserts the contents of this SchemaRDD into the specified table.

        Optionally overwriting any existing data.
        """
        self._jschema_rdd.insertInto(tableName, overwrite)

    def saveAsTable(self, tableName):
        """Creates a new table with the contents of this SchemaRDD."""
        self._jschema_rdd.saveAsTable(tableName)

    def count(self):
        """Return the number of elements in this RDD.

        Unlike the base RDD implementation of count, this implementation
        leverages the query optimizer to compute the count on the SchemaRDD,
        which supports features such as filter pushdown.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.count()
        3L
        >>> srdd.count() == srdd.map(lambda x: x).count()
        True
        """
        return self._jschema_rdd.count()

    def _toPython(self):
        # We have to import the Row class explicitly, so that the reference Pickler has is
        # pyspark.sql.Row instead of __main__.Row
        from pyspark.sql import Row
        jrdd = self._jschema_rdd.javaToPython()
        # TODO: This is inefficient, we should construct the Python Row object
        # in Java land in the javaToPython function. May require a custom
        # pickle serializer in Pyrolite
        return RDD(jrdd, self._sc, BatchedSerializer(
                        PickleSerializer())).map(lambda d: Row(d))

    # We override the default cache/persist/checkpoint behavior as we want to cache the underlying
    # SchemaRDD object in the JVM, not the PythonRDD checkpointed by the super class
    def cache(self):
        self.is_cached = True
        self._jschema_rdd.cache()
        return self

    def persist(self, storageLevel):
        self.is_cached = True
        javaStorageLevel = self.ctx._getJavaStorageLevel(storageLevel)
        self._jschema_rdd.persist(javaStorageLevel)
        return self

    def unpersist(self):
        self.is_cached = False
        self._jschema_rdd.unpersist()
        return self

    def checkpoint(self):
        self.is_checkpointed = True
        self._jschema_rdd.checkpoint()

    def isCheckpointed(self):
        return self._jschema_rdd.isCheckpointed()

    def getCheckpointFile(self):
        checkpointFile = self._jschema_rdd.getCheckpointFile()
        if checkpointFile.isDefined():
            return checkpointFile.get()
        else:
            return None

    def coalesce(self, numPartitions, shuffle=False):
        rdd = self._jschema_rdd.coalesce(numPartitions, shuffle)
        return SchemaRDD(rdd, self.sql_ctx)

    def distinct(self):
        rdd = self._jschema_rdd.distinct()
        return SchemaRDD(rdd, self.sql_ctx)

    def intersection(self, other):
        if (other.__class__ is SchemaRDD):
            rdd = self._jschema_rdd.intersection(other._jschema_rdd)
            return SchemaRDD(rdd, self.sql_ctx)
        else:
            raise ValueError("Can only intersect with another SchemaRDD")

    def repartition(self, numPartitions):
        rdd = self._jschema_rdd.repartition(numPartitions)
        return SchemaRDD(rdd, self.sql_ctx)

    def subtract(self, other, numPartitions=None):
        if (other.__class__ is SchemaRDD):
            if numPartitions is None:
                rdd = self._jschema_rdd.subtract(other._jschema_rdd)
            else:
                rdd = self._jschema_rdd.subtract(other._jschema_rdd, numPartitions)
            return SchemaRDD(rdd, self.sql_ctx)
        else:
            raise ValueError("Can only subtract another SchemaRDD")

def _test():
    import doctest
    from array import array
    from pyspark.context import SparkContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext('local[4]', 'PythonTest', batchSize=2)
    globs['sc'] = sc
    globs['sqlCtx'] = SQLContext(sc)
    globs['rdd'] = sc.parallelize([{"field1" : 1, "field2" : "row1"},
        {"field1" : 2, "field2": "row2"}, {"field1" : 3, "field2": "row3"}])
    globs['nestedRdd1'] = sc.parallelize([
        {"f1" : array('i', [1, 2]), "f2" : {"row1" : 1.0}},
        {"f1" : array('i', [2, 3]), "f2" : {"row2" : 2.0}}])
    globs['nestedRdd2'] = sc.parallelize([
        {"f1" : [[1, 2], [2, 3]], "f2" : set([1, 2]), "f3" : (1, 2)},
        {"f1" : [[2, 3], [3, 4]], "f2" : set([2, 3]), "f3" : (2, 3)}])
    (failure_count, test_count) = doctest.testmod(globs=globs,optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()

