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
import types
import array
import itertools
from operator import itemgetter
from collections import namedtuple

from pyspark.rdd import RDD
from pyspark.serializers import BatchedSerializer, PickleSerializer

from py4j.protocol import Py4JError

__all__ = ["SQLContext", "HiveContext", "LocalHiveContext", "TestHiveContext", "SchemaRDD", "Row"]


# FIXME: these will be updated to use new API
def _extend_tree(lines):
    parts = []

    def subtree(depth):
        sub = parts
        while depth > 1:
            sub = sub[-1]
            depth -= 1
        return sub

    for line in lines:
        subtree(line.count('|')).append([line])

    return parts

def _parse_tree(tree):
    if isinstance(tree[0], basestring):
        name, _type = tree[0].split(":")
        name = name.split(" ")[-1]
        if len(tree) == 1:
            return (name, _type.strip())
        else:
            return (name, _parse_tree(tree[1:]))
    else:
        return tuple(_parse_tree(sub) for sub in tree)

def parse_tree_schema(tree):
    lines = tree.split("\n")[1:-1]
    parts = _extend_tree(lines)
    return _parse_tree(parts)


_cached_namedtuples = {}
def _restore_object(fields, obj):
    """ Restore namedtuple object during unpickling. """
    cls = _cached_namedtuples.get(fields)
    if cls is None:
        cls = namedtuple("Row", fields)
        _cached_namedtuples[fields] = cls
    return cls(*obj)

def _create_object(cls, v):
    """ Create an customized object with class `cls`. """
    return cls(v) if v is not None else v

def _create_getter(schema, i):
    """ Create a getter for item `i` with schema """
    # TODO: cache created class
    cls = _create_cls(schema)
    if cls:
        def getter(self):
            return _create_object(cls, self[i])
        return getter
    return itemgetter(i)

def _create_cls(schema):
    """
    Create an class by schama 

    The created class is similar to namedtuple, but can have nested schema.
    """
    # this can not be in global
    from operator import itemgetter

    # TODO: update to new DataType
    if isinstance(schema, list):
        if not schema:
            return
        cls = _create_cls(schema[0])
        if not cls:
            return
        class List(list):
            def __getitem__(self, i):
                return _create_object(cls, list.__getitem__(self, i))
            def __reduce__(self):
                return (list, (list(self),))
        return List

    elif isinstance(schema, dict):
        if not schema:
            return
        vcls = _create_cls(schema['value'])
        if not vcls:
            return
        class Dict(dict):
            def __getitem__(self, k):
                return _create_object(vcls, dict.__getitem__(self, k))
        return Dict

    # builtin types
    elif not isinstance(schema, tuple):
        return


    class Row(tuple):
        """ Row in SchemaRDD """
        _fields = tuple(n for n, _ in schema)

        for __i,__x in enumerate(schema):
            if isinstance(__x, tuple):
                __name, _type = __x
                if _type and isinstance(_type, (tuple,list)):
                    locals()[__name] = property(_create_getter(_type,__i))
                else:
                    locals()[__name] = property(itemgetter(__i))
                del __name, _type
            else:
                locals()[__x] = property(itemgetter(__i))
        del __i, __x

        def __equals__(self, x):
            if type(self) != type(x):
                return False
            for name in self._fields:
                if getattr(self, name) != getattr(x, name):
                    return False
            return True

        def __repr__(self):
            return ("Row(%s)" % ", ".join("%s=%r" % (n, getattr(self, n))
                    for n in self._fields))

        def __str__(self):
            return repr(self)

        def __reduce__(self):
            return (_restore_object, (self._fields, tuple(self)))

    return Row


class SQLContext:
    """Main entry point for SparkSQL functionality.

    A SQLContext can be used create L{SchemaRDD}s, register L{SchemaRDD}s as
    tables, execute SQL over tables, cache tables, and read parquet files.
    """

    def __init__(self, sparkContext, sqlContext=None):
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

        >>> from datetime import datetime
        >>> allTypes = sc.parallelize([{"int": 1, "string": "string", "double": 1.0, "long": 1L,
        ... "boolean": True, "time": datetime(2010, 1, 1, 1, 1, 1), "dict": {"a": 1},
        ... "list": [1, 2, 3]}])
        >>> srdd = sqlCtx.inferSchema(allTypes).map(lambda x: (x.int, x.string, x.double, x.long,
        ... x.boolean, x.time, x.dict["a"], x.list))
        >>> srdd.collect()[0]
        (1, u'string', 1.0, 1, True, datetime.datetime(2010, 1, 1, 1, 1, 1), 1, [1, 2, 3])
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
        >>> srdd.collect()[0]
        Row(field1=1, field2=u'row1')

        >>> from array import array
        >>> srdd = sqlCtx.inferSchema(nestedRdd1)
        >>> srdd.collect()
        [Row(f2={u'row1': 1.0}, f1=[1, 2]), Row(f2={u'row2': 2.0}, f1=[2, 3])]

        >>> srdd = sqlCtx.inferSchema(nestedRdd2)
        >>> srdd.collect()
        [Row(f2=[1, 2], f1=[[1, 2], [2, 3]]), Row(f2=[2, 3], f1=[[2, 3], [3, 4]])]
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

    def jsonFile(self, path):
        """Loads a text file storing one JSON object per line,
           returning the result as a L{SchemaRDD}.
           It goes through the entire dataset once to determine the schema.

        >>> import tempfile, shutil
        >>> jsonFile = tempfile.mkdtemp()
        >>> shutil.rmtree(jsonFile)
        >>> ofn = open(jsonFile, 'w')
        >>> for json in jsonStrings:
        ...   print>>ofn, json
        >>> ofn.close()
        >>> srdd = sqlCtx.jsonFile(jsonFile)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, field6 as f4 from table1")
        >>> srdd2.collect()
        [Row(f1=1, f2=u'row1', f3=Row(field4=11, field5=None), f4=None), \
Row(f1=2, f2=None, f3=Row(field4=22, field5=[10, 11]), f4=Row(field7=(u'row2',))), \
Row(f1=None, f2=u'row3', f3=Row(field4=33, field5=[]), f4=None)]
        """
        jschema_rdd = self._ssql_ctx.jsonFile(path)
        return SchemaRDD(jschema_rdd, self)

    def jsonRDD(self, rdd):
        """Loads an RDD storing one JSON object per string, returning the result as a L{SchemaRDD}.
           It goes through the entire dataset once to determine the schema.

        >>> srdd = sqlCtx.jsonRDD(json)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, field6 as f4 from table1")
        >>> srdd2.collect()
        [Row(f1=1, f2=u'row1', f3=Row(field4=11, field5=None), f4=None), \
Row(f1=2, f2=None, f3=Row(field4=22, field5=[10, 11]), f4=Row(field7=(u'row2',))), \
Row(f1=None, f2=u'row3', f3=Row(field4=33, field5=[]), f4=None)]
        """
        def func(iterator):
            for x in iterator:
                if not isinstance(x, basestring):
                    x = unicode(x)
                yield x.encode("utf-8")
        keyed = rdd.mapPartitions(func)
        keyed._bypass_serializer = True
        jrdd = keyed._jrdd.map(self._jvm.BytesToString())
        jschema_rdd = self._ssql_ctx.jsonRDD(jrdd.rdd())
        return SchemaRDD(jschema_rdd, self)

    def sql(self, sqlQuery):
        """Return a L{SchemaRDD} representing the result of the given query.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> srdd2.collect()
        [Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]
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
            raise Exception("You must build Spark with Hive. Export 'SPARK_HIVE=true' and run "
                            "sbt/sbt assembly", e)

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

    disable these tests tempory
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


# a stub type, the real type is dynamic generated.
class Row(tuple):
    """
    A row in L{SchemaRDD}. The fields in it can be accessed like attributes.
    """

class SchemaRDD(RDD):
    """An RDD of L{Row} objects that has an associated schema.

    The underlying JVM object is a SchemaRDD, not a PythonRDD, so we can
    utilize the relational query api exposed by SparkSQL.

    For normal L{pyspark.rdd.RDD} operations (map, count, etc.) the
    L{SchemaRDD} is not operated on directly, as it's underlying
    implementation is an RDD composed of Java objects. Instead it is
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
        # the _jrdd is created by javaToPython(), serialized by pickle
        self._jrdd_deserializer = BatchedSerializer(PickleSerializer())

    @property
    def _jrdd(self):
        """Lazy evaluation of PythonRDD object.

        Only done when a user calls methods defined by the
        L{pyspark.rdd.RDD} super class (map, filter, etc.).
        """
        if not hasattr(self, '_lazy_jrdd'):
            self._lazy_jrdd = self._jschema_rdd.javaToPython()
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

    def insertInto(self, tableName, overwrite=False):
        """Inserts the contents of this SchemaRDD into the specified table.

        Optionally overwriting any existing data.
        """
        self._jschema_rdd.insertInto(tableName, overwrite)

    def saveAsTable(self, tableName):
        """Creates a new table with the contents of this SchemaRDD."""
        self._jschema_rdd.saveAsTable(tableName)

    def schemaString(self):
        """Returns the output schema in the tree format."""
        return self._jschema_rdd.schemaString()

    def printSchema(self):
        """Prints out the schema in the tree format."""
        print self.schemaString()

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

    def collect(self):
        """
        Return a list that contains all of the rows in this RDD.

        Each object in the list is on Row, the fields can be accessed as
        attributes.
        """
        rows = RDD.collect(self)
        schema = parse_tree_schema(self._jschema_rdd.schemaString())
        cls = _create_cls(schema)
        return map(cls, rows)

    # convert Row in JavaSchemaRDD into namedtuple, let access fields easier
    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """
        Return a new RDD by applying a function to each partition of this RDD,
        while tracking the index of the original partition.

        >>> rdd = sc.parallelize([1, 2, 3, 4], 4)
        >>> def f(splitIndex, iterator): yield splitIndex
        >>> rdd.mapPartitionsWithIndex(f).sum()
        6
        """
        rdd = RDD(self._jrdd, self._sc, self._jrdd_deserializer)

        schema = parse_tree_schema(self._jschema_rdd.schemaString())
        def applySchema(_, it):
            cls = _create_cls(schema)
            return itertools.imap(cls, it)
        
        objrdd = rdd.mapPartitionsWithIndex(applySchema, preservesPartitioning)
        return objrdd.mapPartitionsWithIndex(f, preservesPartitioning)

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
    globs['rdd'] = sc.parallelize(
        [{"field1": 1, "field2": "row1"},
         {"field1": 2, "field2": "row2"},
         {"field1": 3, "field2": "row3"}]
    )
    jsonStrings = [
        '{"field1": 1, "field2": "row1", "field3":{"field4":11}}',
        '{"field1" : 2, "field3":{"field4":22, "field5": [10, 11]}, "field6":[{"field7": "row2"}]}',
        '{"field1" : null, "field2": "row3", "field3":{"field4":33, "field5": []}}'
    ]
    globs['jsonStrings'] = jsonStrings
    globs['json'] = sc.parallelize(jsonStrings)
    globs['nestedRdd1'] = sc.parallelize([
        {"f1": array('i', [1, 2]), "f2": {"row1": 1.0}},
        {"f1": array('i', [2, 3]), "f2": {"row2": 2.0}}])
    globs['nestedRdd2'] = sc.parallelize([
        {"f1": [[1, 2], [2, 3]], "f2": [1, 2]},
        {"f1": [[2, 3], [3, 4]], "f2": [2, 3]}])
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
