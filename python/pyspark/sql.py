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

import warnings

from pyspark.rdd import RDD, PipelinedRDD
from pyspark.serializers import BatchedSerializer, PickleSerializer, CloudPickleSerializer

from itertools import chain, ifilter, imap

from py4j.protocol import Py4JError
from py4j.java_collections import ListConverter, MapConverter


__all__ = [
    "StringType", "BinaryType", "BooleanType", "TimestampType", "DecimalType",
    "DoubleType", "FloatType", "ByteType", "IntegerType", "LongType",
    "ShortType", "ArrayType", "MapType", "StructField", "StructType",
    "SQLContext", "HiveContext", "LocalHiveContext", "TestHiveContext", "SchemaRDD", "Row"]


class PrimitiveTypeSingleton(type):
    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super(PrimitiveTypeSingleton, cls).__call__()
        return cls._instances[cls]


class StringType(object):
    """Spark SQL StringType

    The data type representing string values.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "StringType"


class BinaryType(object):
    """Spark SQL BinaryType

    The data type representing bytearray values.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "BinaryType"


class BooleanType(object):
    """Spark SQL BooleanType

    The data type representing bool values.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "BooleanType"


class TimestampType(object):
    """Spark SQL TimestampType

    The data type representing datetime.datetime values.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "TimestampType"


class DecimalType(object):
    """Spark SQL DecimalType

    The data type representing decimal.Decimal values.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "DecimalType"


class DoubleType(object):
    """Spark SQL DoubleType

    The data type representing float values.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "DoubleType"


class FloatType(object):
    """Spark SQL FloatType

    The data type representing single precision floating-point values.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "FloatType"


class ByteType(object):
    """Spark SQL ByteType

    The data type representing int values with 1 singed byte.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "ByteType"


class IntegerType(object):
    """Spark SQL IntegerType

    The data type representing int values.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "IntegerType"


class LongType(object):
    """Spark SQL LongType

    The data type representing long values. If the any value is beyond the range of
    [-9223372036854775808, 9223372036854775807], please use DecimalType.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "LongType"


class ShortType(object):
    """Spark SQL ShortType

    The data type representing int values with 2 signed bytes.

    """
    __metaclass__ = PrimitiveTypeSingleton

    def __repr__(self):
        return "ShortType"


class ArrayType(object):
    """Spark SQL ArrayType

    The data type representing list values.
    An ArrayType object comprises two fields, elementType (a DataType) and containsNull (a bool).
    The field of elementType is used to specify the type of array elements.
    The field of containsNull is used to specify if the array has None values.

    """
    def __init__(self, elementType, containsNull=False):
        """Creates an ArrayType

        :param elementType: the data type of elements.
        :param containsNull: indicates whether the list contains None values.

        >>> ArrayType(StringType) == ArrayType(StringType, False)
        True
        >>> ArrayType(StringType, True) == ArrayType(StringType)
        False
        """
        self.elementType = elementType
        self.containsNull = containsNull

    def __repr__(self):
        return "ArrayType(" + self.elementType.__repr__() + "," + \
               str(self.containsNull).lower() + ")"

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.elementType == other.elementType and
                self.containsNull == other.containsNull)

    def __ne__(self, other):
        return not self.__eq__(other)


class MapType(object):
    """Spark SQL MapType

    The data type representing dict values.
    A MapType object comprises three fields,
    keyType (a DataType), valueType (a DataType) and valueContainsNull (a bool).
    The field of keyType is used to specify the type of keys in the map.
    The field of valueType is used to specify the type of values in the map.
    The field of valueContainsNull is used to specify if values of this map has None values.
    For values of a MapType column, keys are not allowed to have None values.

    """
    def __init__(self, keyType, valueType, valueContainsNull=True):
        """Creates a MapType
        :param keyType: the data type of keys.
        :param valueType: the data type of values.
        :param valueContainsNull: indicates whether values contains null values.

        >>> MapType(StringType, IntegerType) == MapType(StringType, IntegerType, True)
        True
        >>> MapType(StringType, IntegerType, False) == MapType(StringType, FloatType)
        False
        """
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def __repr__(self):
        return "MapType(" + self.keyType.__repr__() + "," + \
               self.valueType.__repr__() + "," + \
               str(self.valueContainsNull).lower() + ")"

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.keyType == other.keyType and
                self.valueType == other.valueType and
                self.valueContainsNull == other.valueContainsNull)

    def __ne__(self, other):
        return not self.__eq__(other)


class StructField(object):
    """Spark SQL StructField

    Represents a field in a StructType.
    A StructField object comprises three fields, name (a string), dataType (a DataType),
    and nullable (a bool). The field of name is the name of a StructField. The field of
    dataType specifies the data type of a StructField.
    The field of nullable specifies if values of a StructField can contain None values.

    """
    def __init__(self, name, dataType, nullable):
        """Creates a StructField
        :param name: the name of this field.
        :param dataType: the data type of this field.
        :param nullable: indicates whether values of this field can be null.

        >>> StructField("f1", StringType, True) == StructField("f1", StringType, True)
        True
        >>> StructField("f1", StringType, True) == StructField("f2", StringType, True)
        False
        """
        self.name = name
        self.dataType = dataType
        self.nullable = nullable

    def __repr__(self):
        return "StructField(" + self.name + "," + \
               self.dataType.__repr__() + "," + \
               str(self.nullable).lower() + ")"

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.name == other.name and
                self.dataType == other.dataType and
                self.nullable == other.nullable)

    def __ne__(self, other):
        return not self.__eq__(other)


class StructType(object):
    """Spark SQL StructType

    The data type representing namedtuple values.
    A StructType object comprises a list of L{StructField}s.

    """
    def __init__(self, fields):
        """Creates a StructType

        >>> struct1 = StructType([StructField("f1", StringType, True)])
        >>> struct2 = StructType([StructField("f1", StringType, True)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType([StructField("f1", StringType, True)])
        >>> struct2 = StructType([StructField("f1", StringType, True),
        ...   [StructField("f2", IntegerType, False)]])
        >>> struct1 == struct2
        False
        """
        self.fields = fields

    def __repr__(self):
        return "StructType(List(" + \
               ",".join([field.__repr__() for field in self.fields]) + "))"

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.fields == other.fields)

    def __ne__(self, other):
        return not self.__eq__(other)


def _parse_datatype_list(datatype_list_string):
    """Parses a list of comma separated data types."""
    index = 0
    datatype_list = []
    start = 0
    depth = 0
    while index < len(datatype_list_string):
        if depth == 0 and datatype_list_string[index] == ",":
            datatype_string = datatype_list_string[start:index].strip()
            datatype_list.append(_parse_datatype_string(datatype_string))
            start = index + 1
        elif datatype_list_string[index] == "(":
            depth += 1
        elif datatype_list_string[index] == ")":
            depth -= 1

        index += 1

    # Handle the last data type
    datatype_string = datatype_list_string[start:index].strip()
    datatype_list.append(_parse_datatype_string(datatype_string))
    return datatype_list


def _parse_datatype_string(datatype_string):
    """Parses the given data type string.

    >>> def check_datatype(datatype):
    ...     scala_datatype = sqlCtx._ssql_ctx.parseDataType(datatype.__repr__())
    ...     python_datatype = _parse_datatype_string(scala_datatype.toString())
    ...     return datatype == python_datatype
    >>> check_datatype(StringType())
    True
    >>> check_datatype(BinaryType())
    True
    >>> check_datatype(BooleanType())
    True
    >>> check_datatype(TimestampType())
    True
    >>> check_datatype(DecimalType())
    True
    >>> check_datatype(DoubleType())
    True
    >>> check_datatype(FloatType())
    True
    >>> check_datatype(ByteType())
    True
    >>> check_datatype(IntegerType())
    True
    >>> check_datatype(LongType())
    True
    >>> check_datatype(ShortType())
    True
    >>> # Simple ArrayType.
    >>> simple_arraytype = ArrayType(StringType(), True)
    >>> check_datatype(simple_arraytype)
    True
    >>> # Simple MapType.
    >>> simple_maptype = MapType(StringType(), LongType())
    >>> check_datatype(simple_maptype)
    True
    >>> # Simple StructType.
    >>> simple_structtype = StructType([
    ...     StructField("a", DecimalType(), False),
    ...     StructField("b", BooleanType(), True),
    ...     StructField("c", LongType(), True),
    ...     StructField("d", BinaryType(), False)])
    >>> check_datatype(simple_structtype)
    True
    >>> # Complex StructType.
    >>> complex_structtype = StructType([
    ...     StructField("simpleArray", simple_arraytype, True),
    ...     StructField("simpleMap", simple_maptype, True),
    ...     StructField("simpleStruct", simple_structtype, True),
    ...     StructField("boolean", BooleanType(), False)])
    >>> check_datatype(complex_structtype)
    True
    >>> # Complex ArrayType.
    >>> complex_arraytype = ArrayType(complex_structtype, True)
    >>> check_datatype(complex_arraytype)
    True
    >>> # Complex MapType.
    >>> complex_maptype = MapType(complex_structtype, complex_arraytype, False)
    >>> check_datatype(complex_maptype)
    True
    """
    left_bracket_index = datatype_string.find("(")
    if left_bracket_index == -1:
        # It is a primitive type.
        left_bracket_index = len(datatype_string)
    type_or_field = datatype_string[:left_bracket_index]
    rest_part = datatype_string[left_bracket_index+1:len(datatype_string)-1].strip()
    if type_or_field == "StringType":
        return StringType()
    elif type_or_field == "BinaryType":
        return BinaryType()
    elif type_or_field == "BooleanType":
        return BooleanType()
    elif type_or_field == "TimestampType":
        return TimestampType()
    elif type_or_field == "DecimalType":
        return DecimalType()
    elif type_or_field == "DoubleType":
        return DoubleType()
    elif type_or_field == "FloatType":
        return FloatType()
    elif type_or_field == "ByteType":
        return ByteType()
    elif type_or_field == "IntegerType":
        return IntegerType()
    elif type_or_field == "LongType":
        return LongType()
    elif type_or_field == "ShortType":
        return ShortType()
    elif type_or_field == "ArrayType":
        last_comma_index = rest_part.rfind(",")
        containsNull = True
        if rest_part[last_comma_index+1:].strip().lower() == "false":
            containsNull = False
        elementType = _parse_datatype_string(rest_part[:last_comma_index].strip())
        return ArrayType(elementType, containsNull)
    elif type_or_field == "MapType":
        last_comma_index = rest_part.rfind(",")
        valueContainsNull = True
        if rest_part[last_comma_index+1:].strip().lower() == "false":
            valueContainsNull = False
        keyType, valueType = _parse_datatype_list(rest_part[:last_comma_index].strip())
        return MapType(keyType, valueType, valueContainsNull)
    elif type_or_field == "StructField":
        first_comma_index = rest_part.find(",")
        name = rest_part[:first_comma_index].strip()
        last_comma_index = rest_part.rfind(",")
        nullable = True
        if rest_part[last_comma_index+1:].strip().lower() == "false":
            nullable = False
        dataType = _parse_datatype_string(
            rest_part[first_comma_index+1:last_comma_index].strip())
        return StructField(name, dataType, nullable)
    elif type_or_field == "StructType":
        # rest_part should be in the format like
        # List(StructField(field1,IntegerType,false)).
        field_list_string = rest_part[rest_part.find("(")+1:-1]
        fields = _parse_datatype_list(field_list_string)
        return StructType(fields)


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

    def registerFunction(self, name, f, returnType = "string"):
        def func(split, iterator): return imap(f, iterator)
        command = (func, self._sc.serializer, self._sc.serializer)
        env = MapConverter().convert(self._sc.environment,
                                     self._sc._gateway._gateway_client)
        includes = ListConverter().convert(self._sc._python_includes,
                                     self._sc._gateway._gateway_client)
        self._ssql_ctx.registerPython(name, bytearray(CloudPickleSerializer().dumps(command)), env, includes, self._sc.pythonExec, self._sc._javaAccumulator, returnType)

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
        >>> srdd.collect() == [{"f1" : [1, 2], "f2" : {"row1" : 1.0}},
        ...                    {"f1" : [2, 3], "f2" : {"row2" : 2.0}}]
        True

        >>> srdd = sqlCtx.inferSchema(nestedRdd2)
        >>> srdd.collect() == [{"f1" : [[1, 2], [2, 3]], "f2" : [1, 2]},
        ...                    {"f1" : [[2, 3], [3, 4]], "f2" : [2, 3]}]
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

    def applySchema(self, rdd, schema):
        """Applies the given schema to the given RDD of L{dict}s.

        >>> schema = StructType([StructField("field1", IntegerType(), False),
        ...     StructField("field2", StringType(), False)])
        >>> srdd = sqlCtx.applySchema(rdd, schema)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.sql("SELECT * from table1")
        >>> srdd2.collect() == [{"field1" : 1, "field2" : "row1"}, {"field1" : 2, "field2": "row2"},
        ...                    {"field1" : 3, "field2": "row3"}]
        True
        >>> from datetime import datetime
        >>> rdd = sc.parallelize([{"byte": 127, "short": -32768, "float": 1.0,
        ... "time": datetime(2010, 1, 1, 1, 1, 1), "map": {"a": 1}, "struct": {"b": 2},
        ... "list": [1, 2, 3]}])
        >>> schema = StructType([
        ...     StructField("byte", ByteType(), False),
        ...     StructField("short", ShortType(), False),
        ...     StructField("float", FloatType(), False),
        ...     StructField("time", TimestampType(), False),
        ...     StructField("map", MapType(StringType(), IntegerType(), False), False),
        ...     StructField("struct", StructType([StructField("b", ShortType(), False)]), False),
        ...     StructField("list", ArrayType(ByteType(), False), False),
        ...     StructField("null", DoubleType(), True)])
        >>> srdd = sqlCtx.applySchema(rdd, schema).map(
        ...     lambda x: (
        ...         x.byte, x.short, x.float, x.time, x.map["a"], x.struct["b"], x.list, x.null))
        >>> srdd.collect()[0]
        (127, -32768, 1.0, datetime.datetime(2010, 1, 1, 1, 1, 1), 1, 2, [1, 2, 3], None)
        """
        jrdd = self._pythonToJavaMap(rdd._jrdd)
        srdd = self._ssql_ctx.applySchemaToPythonRDD(jrdd.rdd(), schema.__repr__())
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

    def jsonFile(self, path, schema=None):
        """Loads a text file storing one JSON object per line as a L{SchemaRDD}.

        If the schema is provided, applies the given schema to this JSON dataset.
        Otherwise, it goes through the entire dataset once to determine the schema.

        >>> import tempfile, shutil
        >>> jsonFile = tempfile.mkdtemp()
        >>> shutil.rmtree(jsonFile)
        >>> ofn = open(jsonFile, 'w')
        >>> for json in jsonStrings:
        ...   print>>ofn, json
        >>> ofn.close()
        >>> srdd1 = sqlCtx.jsonFile(jsonFile)
        >>> sqlCtx.registerRDDAsTable(srdd1, "table1")
        >>> srdd2 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, field6 as f4 from table1")
        >>> srdd2.collect() == [
        ... {"f1":1, "f2":"row1", "f3":{"field4":11, "field5": None}, "f4":None},
        ... {"f1":2, "f2":None, "f3":{"field4":22,  "field5": [10, 11]}, "f4":[{"field7": "row2"}]},
        ... {"f1":None, "f2":"row3", "f3":{"field4":33, "field5": []}, "f4":None}]
        True
        >>> srdd3 = sqlCtx.jsonFile(jsonFile, srdd1.schema())
        >>> sqlCtx.registerRDDAsTable(srdd3, "table2")
        >>> srdd4 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, field6 as f4 from table2")
        >>> srdd4.collect() == [
        ... {"f1":1, "f2":"row1", "f3":{"field4":11, "field5": None}, "f4":None},
        ... {"f1":2, "f2":None, "f3":{"field4":22,  "field5": [10, 11]}, "f4":[{"field7": "row2"}]},
        ... {"f1":None, "f2":"row3", "f3":{"field4":33, "field5": []}, "f4":None}]
        True
        >>> schema = StructType([
        ...     StructField("field2", StringType(), True),
        ...     StructField("field3",
        ...         StructType([
        ...             StructField("field5", ArrayType(IntegerType(), False), True)]), False)])
        >>> srdd5 = sqlCtx.jsonFile(jsonFile, schema)
        >>> sqlCtx.registerRDDAsTable(srdd5, "table3")
        >>> srdd6 = sqlCtx.sql(
        ...   "SELECT field2 AS f1, field3.field5 as f2, field3.field5[0] as f3 from table3")
        >>> srdd6.collect() == [
        ... {"f1": "row1", "f2": None, "f3": None},
        ... {"f1": None, "f2": [10, 11], "f3": 10},
        ... {"f1": "row3", "f2": [], "f3": None}]
        True
        """
        if schema is None:
            jschema_rdd = self._ssql_ctx.jsonFile(path)
        else:
            scala_datatype = self._ssql_ctx.parseDataType(schema.__repr__())
            jschema_rdd = self._ssql_ctx.jsonFile(path, scala_datatype)
        return SchemaRDD(jschema_rdd, self)

    def jsonRDD(self, rdd, schema=None):
        """Loads an RDD storing one JSON object per string as a L{SchemaRDD}.

        If the schema is provided, applies the given schema to this JSON dataset.
        Otherwise, it goes through the entire dataset once to determine the schema.

        >>> srdd1 = sqlCtx.jsonRDD(json)
        >>> sqlCtx.registerRDDAsTable(srdd1, "table1")
        >>> srdd2 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, field6 as f4 from table1")
        >>> srdd2.collect() == [
        ... {"f1":1, "f2":"row1", "f3":{"field4":11, "field5": None}, "f4":None},
        ... {"f1":2, "f2":None, "f3":{"field4":22,  "field5": [10, 11]}, "f4":[{"field7": "row2"}]},
        ... {"f1":None, "f2":"row3", "f3":{"field4":33, "field5": []}, "f4":None}]
        True
        >>> srdd3 = sqlCtx.jsonRDD(json, srdd1.schema())
        >>> sqlCtx.registerRDDAsTable(srdd3, "table2")
        >>> srdd4 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, field6 as f4 from table2")
        >>> srdd4.collect() == [
        ... {"f1":1, "f2":"row1", "f3":{"field4":11, "field5": None}, "f4":None},
        ... {"f1":2, "f2":None, "f3":{"field4":22,  "field5": [10, 11]}, "f4":[{"field7": "row2"}]},
        ... {"f1":None, "f2":"row3", "f3":{"field4":33, "field5": []}, "f4":None}]
        True
        >>> schema = StructType([
        ...     StructField("field2", StringType(), True),
        ...     StructField("field3",
        ...         StructType([
        ...             StructField("field5", ArrayType(IntegerType(), False), True)]), False)])
        >>> srdd5 = sqlCtx.jsonRDD(json, schema)
        >>> sqlCtx.registerRDDAsTable(srdd5, "table3")
        >>> srdd6 = sqlCtx.sql(
        ...   "SELECT field2 AS f1, field3.field5 as f2, field3.field5[0] as f3 from table3")
        >>> srdd6.collect() == [
        ... {"f1": "row1", "f2": None, "f3": None},
        ... {"f1": None, "f2": [10, 11], "f3": 10},
        ... {"f1": "row3", "f2": [], "f3": None}]
        True
        """
        def func(split, iterator):
            for x in iterator:
                if not isinstance(x, basestring):
                    x = unicode(x)
                yield x.encode("utf-8")
        keyed = PipelinedRDD(rdd, func)
        keyed._bypass_serializer = True
        jrdd = keyed._jrdd.map(self._jvm.BytesToString())
        if schema is None:
            jschema_rdd = self._ssql_ctx.jsonRDD(jrdd.rdd())
        else:
            scala_datatype = self._ssql_ctx.parseDataType(schema.__repr__())
            jschema_rdd = self._ssql_ctx.jsonRDD(jrdd.rdd(), scala_datatype)
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

    def __init__(self, sparkContext, sqlContext=None):
      HiveContext.__init__(self, sparkContext, sqlContext)
      warnings.warn("LocalHiveContext is deprecated.  Use HiveContext instead.", DeprecationWarning)

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

    def insertInto(self, tableName, overwrite=False):
        """Inserts the contents of this SchemaRDD into the specified table.

        Optionally overwriting any existing data.
        """
        self._jschema_rdd.insertInto(tableName, overwrite)

    def saveAsTable(self, tableName):
        """Creates a new table with the contents of this SchemaRDD."""
        self._jschema_rdd.saveAsTable(tableName)

    def schema(self):
        """Returns the schema of this SchemaRDD (represented by a L{StructType})."""
        return _parse_datatype_string(self._jschema_rdd.schema().toString())

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
