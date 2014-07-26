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


from operator import itemgetter
import time
import sys
import types
import array
import warnings

from pyspark.rdd import RDD
from pyspark.serializers import BatchedSerializer, PickleSerializer

from py4j.protocol import Py4JError

__all__ = ["SQLContext", "HiveContext", "LocalHiveContext", "TestHiveContext", "SchemaRDD", "Row"]


# TODO: all types supported by Spark SQL
class SQLType:
    Int = 1
    String = 2
    Float = 3

IS = isinstance

def infer_schema(r, top=False):
    """
    Infer field names and types from r

    >>> from collections import namedtuple
    >>> Person = namedtuple("Person", "name age")
    >>> infer_schema(Person("Alice", 11))
    (('name', 'str'), ('age', 'int'))

    >>> class Point:
    ...     def __init__(self, x, y):
    ...         self.x = x
    ...         self.y = y
    ...
    >>> infer_schema(Point(1,2))
    (('x', 'int'), ('y', 'int'))
    """
    if IS(r, tuple):
        if hasattr(r, "_fields"):  # is namedtuple
            return tuple(zip(r._fields, (infer_schema(x) for x in r)))
        if top:
            raise Exception("Can not infer schema for tuple")
        return ()
    elif IS(r, list):
        return [infer_schema(r.pop())] if r else []
    elif IS(r, dict):
        if not r:
            return {}
        if top:
            keys = sorted(r.keys())
            return tuple((k, infer_schema(r[k])) for k in keys)
        k,v = r.iteritems().next()
        return dict(key=infer_schema(k), value=infer_schema(v))
    elif hasattr(r, "__dict__"):
        keys = sorted(k for k in r.__dict__.keys() if not k.startswith('_'))
        return tuple((k, infer_schema(r.__dict__[k])) for k in keys)
    #elif IS(r, (int, long, float, complex, basestring, bool, types.NoneType)):
    else:
        return str(type(r))[7:-2]

def is_combined_type(obj):
    return IS(obj, (tuple, list, dict))

def simple_schema(s):
    """
    generate simple schema by remove the type in schema

    >>> s = (("A", "int"), ("B", (("X", "int"), ("Y", "int"))))
    >>> simple_schema(s)
    ('A', ('B', ('X', 'Y')))
    """
    if IS(s, tuple):
        if all(isinstance(x, tuple) and len(x) == 2 for x in s):
            r = ((name, simple_schema(_type))
                    if is_combined_type(_type) else name
                    for name, _type in s)
            return tuple(r)
        elif s and is_combined_type(s[0]):
            return (simple_schema(s[0]),)
        else:
            return ()

    elif IS(s, list):
        return ([simple_schema(s[0])]
                if s and is_combined_type(s[0]) else [])

    elif IS(s, dict):
        return dict(key=s["key"], value=simple_type(s["value"]))

def full_schema(s):
    """
    Convert the simple schema into full schema with types

    >>> s = ('A', ('B', ('X', 'Y')))
    >>> full_schema(s)
    (('A', None), ('B', (('X', None), ('Y', None))))
    """
    if isinstance(s, tuple):
        return tuple((n, None) if isinstance(n, basestring)
                               else (n[0], full_schema(n[1]))
                     for n in s)
    elif isinstance(s, list):
        return ([full_schema(s[0])] if s else [])
    elif IS(s, dict):
        return ({"key":s["key"], "value": full_schema(s["value"])}
                if s else {})
    else:
        return s

def drop_schema(r, s):
    """
    Drop the type of row `r` according to schema `s`

    Return an tuple having exact order of fields with
    schema.

    >>> r = {'a': 1, 'b': 2}
    >>> drop_schema(r, (('a', None), ('b', None)))
    (1, 2)
    """
    if isinstance(s, tuple):
        if not s:
            return r
        if not IS(r, dict):
            return tuple(drop_schema(getattr(r, name), ss)
                         for name, ss in s)
        else:
            return tuple(drop_schema(r.get(name), ss)
                         for name, ss in s)
    elif isinstance(s, list):
        if not s:
            return r
        return [drop_schema(x, s[0]) for x in r]
    elif isinstance(s, dict):
        if not s:
            return r
        return dict((k, drop_schema(v, s['value']))
                    for k,v in r.iteritems())
    else:
        return r

def add_schema(r, s):
    """
    Convert tuple into dict

    >>> r = (1, 2)
    >>> add_schema(r, (('a', None), ('b', None)))
    {'a': 1, 'b': 2}
    """
    if isinstance(s, tuple):
        if not s:
            return r
        return dict((name, add_schema(x, _type))
                     for x, (name, _type) in zip(r, s))
    elif isinstance(s, list):
        if not s:
            return r
        return [add_schema(x, s[0]) for x in r]
    elif isinstance(s, dict):
        if not s:
            return r
        return dict((k, add_schema(v, s['value']))
                    for k,v in r.iteritems())
    else:
        return r

def abstract(s):
    """
    get abstract from schema

    >>> abstract(('a', 'b', 'c'))
    '(a b c)'
    >>> abstract((('a', ('b', 'c')), ('b', []), ('c', [('a', 'b')])))
    '(a(b c) b[] c[a b])'
    """
    if isinstance(s, basestring):
        return s
    if isinstance(s, tuple):
        return '(%s)' % ' '.join(field[0] + abstract(field[1])
                if IS(field, tuple) else field
                for field in s)
    if isinstance(s, list):
        if not s:
            return '[]'
        sub = abstract(s[0])
        if sub.startswith('('):
            sub = sub[1:-1]
        return '[%s]' % sub
    if isinstance(s, dict):
        if not s:
            return '{}'
        k, v = s.items()[0]
        r = '{%s %s}' % (abstract(k), abstract(v))
        return r.replace('{ }', '{}')

def split(s):
    """
    split the schema abstract into fields

    >>> split("a b  c")
    ['a', 'b', 'c']
    >>> split("a(a b)")
    ['a(a b)']
    >>> split("a b[] c{a b}")
    ['a', 'b[]', 'c{a b}']
    >>> split(" ")
    []
    """
    pairs = {'(':')', '[':']', '{':'}'}
    r = []
    w = ''
    brackets = []
    for c in s:
        if c == ' ' and not brackets:
            if w:
                r.append(w)
            w = ''
        else:
            w += c
            if c in "([{":
                brackets.append(c)
            elif c in "}])":
                if not brackets or c != pairs[brackets.pop()]:
                    raise Exception("unexpected " + c)

    if brackets:
        raise Exception("brackets not closed: %s" % brackets)
    if w:
        r.append(w)
    return r

def parse_schema(s):
    """
    parse abstract into schema

    >>> parse_schema("a b  c")
    ('a', 'b', 'c')
    >>> parse_schema("a[b c] b[]")
    (('a', [('b', 'c')]), ('b', []))
    >>> parse_schema("c{} d{() (a b)}")
    (('c', {}), ('d', {'value': ('a', 'b'), 'key': None}))
    >>> parse_schema("a b(t)")
    ('a', ('b', ('t',)))
    """
    s = s.strip()
    if not s:
        return
    elif s.startswith('('):
        return parse_schema(s[1:-1])
    elif s.startswith('['):
        if len(s) == 2:
            return []
        return [parse_schema(s[1:-1])]
    elif s.startswith('{'):
        if len(s) == 2:
            return {}
        ks,vs = split(s[1:-1])
        return dict(key=parse_schema(ks), value=parse_schema(vs))
    parts = split(s)
    if len(parts) == 1:
        if set('({[') & set(s):
            idx = min((s.index(c) for c in '([{' if c in s))
            name = s[:idx]
            _type = parse_schema(s[idx:])
            if isinstance(_type, basestring):
                _type = (_type,)
            return (name, _type)
        else:
            return s
    else:
        return tuple(parse_schema(part) for part in parts)

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

def create(cls, v):
    return cls(v) if v is not None else v

def create_getter(schema, i):
    cls = create_cls(schema)
    def getter(self):
        return create(cls, self[i])
    return getter

def create_cls(schema):
    from operator import itemgetter

    if isinstance(schema, list):
        if not schema:
            return list
        cls = create_cls(schema[0])
        class List(list):
            def __getitem__(self, i):
                return create(cls, list.__getitem__(self, i))
            def __reduce__(self):
                return (list, (list(self),))
        return List

    elif isinstance(schema, dict):
        if not schema:
            return dict
        vcls = create_cls(schema['value'])
        class Dict(dict):
            def __getitem__(self, k):
                return create(vcls, dict.__getitem__(self, k))

    # builtin types
    elif not isinstance(schema, tuple):
        return schema


    class Row(tuple):

        _fields = tuple(n for n, _ in schema)

        for __i,__x in enumerate(schema):
            if isinstance(__x, tuple):
                __name, _type = __x
                if _type and isinstance(_type, (tuple,list)):
                    locals()[__name] = property(create_getter(_type,__i))
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
        >>> row = srdd.first()
        >>> row.field1, row.field2
        (1, u'row1')

        >>> from array import array
        >>> srdd = sqlCtx.inferSchema(nestedRdd1)
        >>> row = srdd.first()
        >>> row.f1, row.f2
        (array('i', [1, 2]), {u'row1': 1.0})

        >>> srdd = sqlCtx.inferSchema(nestedRdd2)
        >>> row = srdd.first()
        >>> row.f1, row.f2, row.f3
        ([[1, 2], [2, 3]], set([1, 2]), (1, 2))
        """
        if (rdd.__class__ is SchemaRDD):
            raise ValueError("Cannot apply schema to %s" % SchemaRDD.__name__)

        row = rdd.first()
        if not isinstance(row, (dict, tuple)):
            raise ValueError("Cannot apply schema to type %s" % type(row))
        if isinstance(row, dict):
            warnings.warn("using dict to infer schema is deprecated")

        schema = infer_schema(row, True)
        rdd = rdd.map(lambda r: drop_schema(r, schema))
        return self.applySchema(rdd, schema)

    def applySchema(self, rdd, schema):
        """
        Apply a schema to an RDD of L{tuple}s or L{list}s.

        The schema could be a string or tuple (with or without types), such as
        "name value" or ("name", "value") or (("name", str), ("value", int)).

        The schema can have nested struct (struct, list, map).

        In string schema, the fields are seperated by space. Each field can be
        followed by composit type, for example:

            "name address(city zipcode) items[] props{}"

        It's equal to (primary types are emitted):

            ("name", ("address, ("city", "zipcode")), ("item", []),
             ("props", {}))

        So it's also equal to:

            (("name", string),
             ("address", (("city", string), ("zipcode", int))),
             ("item", []),
             ("props", {}),

        The `schema` should match with the rows in `rdd`, first few rows will be
        checked.

        >>> rdd = sc.parallelize([("Alice", 11), ("Bob", 21)])
        >>> srdd = sqlCtx.applySchema(rdd, ("name", "age"))
        >>> srdd.first()
        Row(name=u'Alice', age=11)

        >>> srdd = sqlCtx.applySchema(rdd, "name age")
        >>> srdd.first()
        Row(name=u'Alice', age=11)
        """

        if isinstance(rdd, SchemaRDD):
            raise ValueError("Cannot apply schema to %s" % SchemaRDD.__name__)

        if isinstance(schema, basestring):
            schema = parse_schema(schema)
        schema = full_schema(schema)

        #TODO: verify schema
        row = rdd.first()

        #TODO: use new API
        rdd = rdd.map(lambda row: add_schema(row, schema))
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

    ## disable these tests tempory
    ## >>> import os
    ## >>> hiveCtx = LocalHiveContext(sc)
    ## >>> try:
    ## ...     supress = hiveCtx.hql("DROP TABLE src")
    ## ... except Exception:
    ## ...     pass
    ## >>> kv1 = os.path.join(os.environ["SPARK_HOME"], 'examples/src/main/resources/kv1.txt')
    ## >>> supress = hiveCtx.hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    ## >>> supress = hiveCtx.hql("LOAD DATA LOCAL INPATH '%s' INTO TABLE src" % kv1)
    ## >>> results = hiveCtx.hql("FROM src SELECT value").map(lambda r: int(r.value.split('_')[1]))
    ## >>> num = results.count()
    ## >>> reduce_sum = results.reduce(lambda x, y: x + y)
    ## >>> num
    ## 500
    ## >>> reduce_sum
    ## 130091
    """

    def _get_hive_ctx(self):
        return self._jvm.LocalHiveContext(self._jsc.sc())


class TestHiveContext(HiveContext):

    def _get_hive_ctx(self):
        return self._jvm.TestHiveContext(self._jsc.sc())


class Row(tuple):
    pass


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

    def __init__(self, jschema_rdd, sql_ctx, schema=None):
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx._sc
        self._jschema_rdd = jschema_rdd
        self.schema = schema or parse_tree_schema(jschema_rdd.schemaString())

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

    def take(self, n):
        """
        Take the first num elements of the RDD.

        It works by first scanning one partition, and use the results from
        that partition to estimate the number of additional partitions needed
        to satisfy the limit.

        Translated from the Scala implementation in RDD#take().

        >>> sc.parallelize([2, 3, 4, 5, 6]).cache().take(2)
        [2, 3]
        >>> sc.parallelize([2, 3, 4, 5, 6]).take(10)
        [2, 3, 4, 5, 6]
        >>> sc.parallelize(range(100), 100).filter(lambda x: x > 90).take(3)
        [91, 92, 93]
        """

        rdd = RDD(self._jrdd, self._sc, self._jrdd_deserializer)
        rows = rdd.take(n)
        cls = create_cls(self.schema)
        return [cls(r) for r in rows]

    def collect(self):
        rdd = RDD(self._jrdd, self._sc, self._jrdd_deserializer)
        rows = rdd.collect()
        cls = create_cls(self.schema)
        rows = [cls(r) for r in rows]
        return rows

    # convert Row in JavaSchemaRDD into namedtuple
    def mapPartitions(self, f, preservesPartitioning=False):
        rdd = RDD(self._jrdd, self._sc, self._jrdd_deserializer)

        schema = self.schema
        def applySchema(iter):
            cls = create_cls(schema)
            for i in iter:
                yield cls(i)

        objrdd = rdd.mapPartitions(applySchema, preservesPartitioning)
        return objrdd.mapPartitions(f, preservesPartitioning)

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
    from collections import namedtuple
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext('local[4]', 'PythonTest', batchSize=2)
    globs['sc'] = sc
    globs['sqlCtx'] = SQLContext(sc)
    Row = namedtuple("Row", "field1 field2")
    globs['rdd'] = sc.parallelize(
        [Row(1, "row1"),
         Row(2, "row2"),
         Row(3, "row3")]
    )
    jsonStrings = [
        '{"field1": 1, "field2": "row1", "field3":{"field4":11}}',
        '{"field1" : 2, "field3":{"field4":22, "field5": [10, 11]}, "field6":[{"field7": "row2"}]}',
        '{"field1" : null, "field2": "row3", "field3":{"field4":33, "field5": []}}'
    ]
    globs['jsonStrings'] = jsonStrings
    globs['json'] = sc.parallelize(jsonStrings)
    NestedRow1 = namedtuple("NestedRow1", "f1 f2")
    globs['nestedRdd1'] = sc.parallelize([
        NestedRow1(array('i', [1, 2]), {"row1": 1.0}),
        NestedRow1(array('i', [2, 3]), {"row2": 2.0})])
    NestedRow2 = namedtuple("NestedRow2", "f1 f2 f3")
    globs['nestedRdd2'] = sc.parallelize([
        NestedRow2([[1, 2], [2, 3]], set([1, 2]), (1, 2)),
        NestedRow2([[2, 3], [3, 4]], set([2, 3]), (2, 3))])
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
