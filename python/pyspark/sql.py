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

"""
public classes of Spark SQL:

    - L{SQLContext}
      Main entry point for SQL functionality.
    - L{SchemaRDD}
      A Resilient Distributed Dataset (RDD) with Schema information for the data contained. In
      addition to normal RDD operations, SchemaRDDs also support SQL.
    - L{Row}
      A Row of data returned by a Spark SQL query.
    - L{HiveContext}
      Main entry point for accessing data stored in Apache Hive..
"""

import itertools
import decimal
import datetime
import keyword
import warnings
import json
import re
from array import array
from operator import itemgetter
from itertools import imap

from py4j.protocol import Py4JError
from py4j.java_collections import ListConverter, MapConverter

from pyspark.rdd import RDD
from pyspark.serializers import BatchedSerializer, AutoBatchedSerializer, PickleSerializer, \
    CloudPickleSerializer, UTF8Deserializer
from pyspark.storagelevel import StorageLevel
from pyspark.traceback_utils import SCCallSiteSync


__all__ = [
    "StringType", "BinaryType", "BooleanType", "DateType", "TimestampType", "DecimalType",
    "DoubleType", "FloatType", "ByteType", "IntegerType", "LongType",
    "ShortType", "ArrayType", "MapType", "StructField", "StructType",
    "SQLContext", "HiveContext", "SchemaRDD", "Row"]


class DataType(object):

    """Spark SQL DataType"""

    def __repr__(self):
        return self.__class__.__name__

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def typeName(cls):
        return cls.__name__[:-4].lower()

    def jsonValue(self):
        return self.typeName()

    def json(self):
        return json.dumps(self.jsonValue(),
                          separators=(',', ':'),
                          sort_keys=True)


class PrimitiveTypeSingleton(type):

    """Metaclass for PrimitiveType"""

    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super(PrimitiveTypeSingleton, cls).__call__()
        return cls._instances[cls]


class PrimitiveType(DataType):

    """Spark SQL PrimitiveType"""

    __metaclass__ = PrimitiveTypeSingleton

    def __eq__(self, other):
        # because they should be the same object
        return self is other


class NullType(PrimitiveType):

    """Spark SQL NullType

    The data type representing None, used for the types which has not
    been inferred.
    """


class StringType(PrimitiveType):

    """Spark SQL StringType

    The data type representing string values.
    """


class BinaryType(PrimitiveType):

    """Spark SQL BinaryType

    The data type representing bytearray values.
    """


class BooleanType(PrimitiveType):

    """Spark SQL BooleanType

    The data type representing bool values.
    """


class DateType(PrimitiveType):

    """Spark SQL DateType

    The data type representing datetime.date values.
    """


class TimestampType(PrimitiveType):

    """Spark SQL TimestampType

    The data type representing datetime.datetime values.
    """


class DecimalType(DataType):

    """Spark SQL DecimalType

    The data type representing decimal.Decimal values.
    """

    def __init__(self, precision=None, scale=None):
        self.precision = precision
        self.scale = scale
        self.hasPrecisionInfo = precision is not None

    def jsonValue(self):
        if self.hasPrecisionInfo:
            return "decimal(%d,%d)" % (self.precision, self.scale)
        else:
            return "decimal"

    def __repr__(self):
        if self.hasPrecisionInfo:
            return "DecimalType(%d,%d)" % (self.precision, self.scale)
        else:
            return "DecimalType()"


class DoubleType(PrimitiveType):

    """Spark SQL DoubleType

    The data type representing float values.
    """


class FloatType(PrimitiveType):

    """Spark SQL FloatType

    The data type representing single precision floating-point values.
    """


class ByteType(PrimitiveType):

    """Spark SQL ByteType

    The data type representing int values with 1 singed byte.
    """


class IntegerType(PrimitiveType):

    """Spark SQL IntegerType

    The data type representing int values.
    """


class LongType(PrimitiveType):

    """Spark SQL LongType

    The data type representing long values. If the any value is
    beyond the range of [-9223372036854775808, 9223372036854775807],
    please use DecimalType.
    """


class ShortType(PrimitiveType):

    """Spark SQL ShortType

    The data type representing int values with 2 signed bytes.
    """


class ArrayType(DataType):

    """Spark SQL ArrayType

    The data type representing list values. An ArrayType object
    comprises two fields, elementType (a DataType) and containsNull (a bool).
    The field of elementType is used to specify the type of array elements.
    The field of containsNull is used to specify if the array has None values.

    """

    def __init__(self, elementType, containsNull=True):
        """Creates an ArrayType

        :param elementType: the data type of elements.
        :param containsNull: indicates whether the list contains None values.

        >>> ArrayType(StringType) == ArrayType(StringType, True)
        True
        >>> ArrayType(StringType, False) == ArrayType(StringType)
        False
        """
        self.elementType = elementType
        self.containsNull = containsNull

    def __repr__(self):
        return "ArrayType(%s,%s)" % (self.elementType,
                                     str(self.containsNull).lower())

    def jsonValue(self):
        return {"type": self.typeName(),
                "elementType": self.elementType.jsonValue(),
                "containsNull": self.containsNull}

    @classmethod
    def fromJson(cls, json):
        return ArrayType(_parse_datatype_json_value(json["elementType"]),
                         json["containsNull"])


class MapType(DataType):

    """Spark SQL MapType

    The data type representing dict values. A MapType object comprises
    three fields, keyType (a DataType), valueType (a DataType) and
    valueContainsNull (a bool).

    The field of keyType is used to specify the type of keys in the map.
    The field of valueType is used to specify the type of values in the map.
    The field of valueContainsNull is used to specify if values of this
    map has None values.

    For values of a MapType column, keys are not allowed to have None values.

    """

    def __init__(self, keyType, valueType, valueContainsNull=True):
        """Creates a MapType
        :param keyType: the data type of keys.
        :param valueType: the data type of values.
        :param valueContainsNull: indicates whether values contains
        null values.

        >>> (MapType(StringType, IntegerType)
        ...        == MapType(StringType, IntegerType, True))
        True
        >>> (MapType(StringType, IntegerType, False)
        ...        == MapType(StringType, FloatType))
        False
        """
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def __repr__(self):
        return "MapType(%s,%s,%s)" % (self.keyType, self.valueType,
                                      str(self.valueContainsNull).lower())

    def jsonValue(self):
        return {"type": self.typeName(),
                "keyType": self.keyType.jsonValue(),
                "valueType": self.valueType.jsonValue(),
                "valueContainsNull": self.valueContainsNull}

    @classmethod
    def fromJson(cls, json):
        return MapType(_parse_datatype_json_value(json["keyType"]),
                       _parse_datatype_json_value(json["valueType"]),
                       json["valueContainsNull"])


class StructField(DataType):

    """Spark SQL StructField

    Represents a field in a StructType.
    A StructField object comprises three fields, name (a string),
    dataType (a DataType) and nullable (a bool). The field of name
    is the name of a StructField. The field of dataType specifies
    the data type of a StructField.

    The field of nullable specifies if values of a StructField can
    contain None values.

    """

    def __init__(self, name, dataType, nullable=True, metadata=None):
        """Creates a StructField
        :param name: the name of this field.
        :param dataType: the data type of this field.
        :param nullable: indicates whether values of this field
                         can be null.
        :param metadata: metadata of this field, which is a map from string
                         to simple type that can be serialized to JSON
                         automatically

        >>> (StructField("f1", StringType, True)
        ...      == StructField("f1", StringType, True))
        True
        >>> (StructField("f1", StringType, True)
        ...      == StructField("f2", StringType, True))
        False
        """
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def __repr__(self):
        return "StructField(%s,%s,%s)" % (self.name, self.dataType,
                                          str(self.nullable).lower())

    def jsonValue(self):
        return {"name": self.name,
                "type": self.dataType.jsonValue(),
                "nullable": self.nullable,
                "metadata": self.metadata}

    @classmethod
    def fromJson(cls, json):
        return StructField(json["name"],
                           _parse_datatype_json_value(json["type"]),
                           json["nullable"],
                           json["metadata"])


class StructType(DataType):

    """Spark SQL StructType

    The data type representing rows.
    A StructType object comprises a list of L{StructField}.

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
        return ("StructType(List(%s))" %
                ",".join(str(field) for field in self.fields))

    def jsonValue(self):
        return {"type": self.typeName(),
                "fields": [f.jsonValue() for f in self.fields]}

    @classmethod
    def fromJson(cls, json):
        return StructType([StructField.fromJson(f) for f in json["fields"]])


class UserDefinedType(DataType):
    """
    .. note:: WARN: Spark Internal Use Only
    SQL User-Defined Type (UDT).
    """

    @classmethod
    def typeName(cls):
        return cls.__name__.lower()

    @classmethod
    def sqlType(cls):
        """
        Underlying SQL storage type for this UDT.
        """
        raise NotImplementedError("UDT must implement sqlType().")

    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        raise NotImplementedError("UDT must implement module().")

    @classmethod
    def scalaUDT(cls):
        """
        The class name of the paired Scala UDT.
        """
        raise NotImplementedError("UDT must have a paired Scala UDT.")

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        raise NotImplementedError("UDT must implement serialize().")

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        raise NotImplementedError("UDT must implement deserialize().")

    def json(self):
        return json.dumps(self.jsonValue(), separators=(',', ':'), sort_keys=True)

    def jsonValue(self):
        schema = {
            "type": "udt",
            "class": self.scalaUDT(),
            "pyClass": "%s.%s" % (self.module(), type(self).__name__),
            "sqlType": self.sqlType().jsonValue()
        }
        return schema

    @classmethod
    def fromJson(cls, json):
        pyUDT = json["pyClass"]
        split = pyUDT.rfind(".")
        pyModule = pyUDT[:split]
        pyClass = pyUDT[split+1:]
        m = __import__(pyModule, globals(), locals(), [pyClass], -1)
        UDT = getattr(m, pyClass)
        return UDT()

    def __eq__(self, other):
        return type(self) == type(other)


_all_primitive_types = dict((v.typeName(), v)
                            for v in globals().itervalues()
                            if type(v) is PrimitiveTypeSingleton and
                            v.__base__ == PrimitiveType)


_all_complex_types = dict((v.typeName(), v)
                          for v in [ArrayType, MapType, StructType])


def _parse_datatype_json_string(json_string):
    """Parses the given data type JSON string.
    >>> def check_datatype(datatype):
    ...     scala_datatype = sqlCtx._ssql_ctx.parseDataType(datatype.json())
    ...     python_datatype = _parse_datatype_json_string(scala_datatype.json())
    ...     return datatype == python_datatype
    >>> all(check_datatype(cls()) for cls in _all_primitive_types.values())
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
    ...     StructField("boolean", BooleanType(), False),
    ...     StructField("withMeta", DoubleType(), False, {"name": "age"})])
    >>> check_datatype(complex_structtype)
    True
    >>> # Complex ArrayType.
    >>> complex_arraytype = ArrayType(complex_structtype, True)
    >>> check_datatype(complex_arraytype)
    True
    >>> # Complex MapType.
    >>> complex_maptype = MapType(complex_structtype,
    ...                           complex_arraytype, False)
    >>> check_datatype(complex_maptype)
    True
    >>> check_datatype(ExamplePointUDT())
    True
    >>> structtype_with_udt = StructType([StructField("label", DoubleType(), False),
    ...                                   StructField("point", ExamplePointUDT(), False)])
    >>> check_datatype(structtype_with_udt)
    True
    """
    return _parse_datatype_json_value(json.loads(json_string))


_FIXED_DECIMAL = re.compile("decimal\\((\\d+),(\\d+)\\)")


def _parse_datatype_json_value(json_value):
    if type(json_value) is unicode:
        if json_value in _all_primitive_types.keys():
            return _all_primitive_types[json_value]()
        elif json_value == u'decimal':
            return DecimalType()
        elif _FIXED_DECIMAL.match(json_value):
            m = _FIXED_DECIMAL.match(json_value)
            return DecimalType(int(m.group(1)), int(m.group(2)))
        else:
            raise ValueError("Could not parse datatype: %s" % json_value)
    else:
        tpe = json_value["type"]
        if tpe in _all_complex_types:
            return _all_complex_types[tpe].fromJson(json_value)
        elif tpe == 'udt':
            return UserDefinedType.fromJson(json_value)
        else:
            raise ValueError("not supported type: %s" % tpe)


# Mapping Python types to Spark SQL DataType
_type_mappings = {
    type(None): NullType,
    bool: BooleanType,
    int: IntegerType,
    long: LongType,
    float: DoubleType,
    str: StringType,
    unicode: StringType,
    bytearray: BinaryType,
    decimal.Decimal: DecimalType,
    datetime.date: DateType,
    datetime.datetime: TimestampType,
    datetime.time: TimestampType,
}


def _infer_type(obj):
    """Infer the DataType from obj

    >>> p = ExamplePoint(1.0, 2.0)
    >>> _infer_type(p)
    ExamplePointUDT
    """
    if obj is None:
        raise ValueError("Can not infer type for None")

    if hasattr(obj, '__UDT__'):
        return obj.__UDT__

    dataType = _type_mappings.get(type(obj))
    if dataType is not None:
        return dataType()

    if isinstance(obj, dict):
        for key, value in obj.iteritems():
            if key is not None and value is not None:
                return MapType(_infer_type(key), _infer_type(value), True)
        else:
            return MapType(NullType(), NullType(), True)
    elif isinstance(obj, (list, array)):
        for v in obj:
            if v is not None:
                return ArrayType(_infer_type(obj[0]), True)
        else:
            return ArrayType(NullType(), True)
    else:
        try:
            return _infer_schema(obj)
        except ValueError:
            raise ValueError("not supported type: %s" % type(obj))


def _infer_schema(row):
    """Infer the schema from dict/namedtuple/object"""
    if isinstance(row, dict):
        items = sorted(row.items())

    elif isinstance(row, tuple):
        if hasattr(row, "_fields"):  # namedtuple
            items = zip(row._fields, tuple(row))
        elif hasattr(row, "__FIELDS__"):  # Row
            items = zip(row.__FIELDS__, tuple(row))
        elif all(isinstance(x, tuple) and len(x) == 2 for x in row):
            items = row
        else:
            raise ValueError("Can't infer schema from tuple")

    elif hasattr(row, "__dict__"):  # object
        items = sorted(row.__dict__.items())

    else:
        raise ValueError("Can not infer schema for type: %s" % type(row))

    fields = [StructField(k, _infer_type(v), True) for k, v in items]
    return StructType(fields)


def _need_python_to_sql_conversion(dataType):
    """
    Checks whether we need python to sql conversion for the given type.
    For now, only UDTs need this conversion.

    >>> _need_python_to_sql_conversion(DoubleType())
    False
    >>> schema0 = StructType([StructField("indices", ArrayType(IntegerType(), False), False),
    ...                       StructField("values", ArrayType(DoubleType(), False), False)])
    >>> _need_python_to_sql_conversion(schema0)
    False
    >>> _need_python_to_sql_conversion(ExamplePointUDT())
    True
    >>> schema1 = ArrayType(ExamplePointUDT(), False)
    >>> _need_python_to_sql_conversion(schema1)
    True
    >>> schema2 = StructType([StructField("label", DoubleType(), False),
    ...                       StructField("point", ExamplePointUDT(), False)])
    >>> _need_python_to_sql_conversion(schema2)
    True
    """
    if isinstance(dataType, StructType):
        return any([_need_python_to_sql_conversion(f.dataType) for f in dataType.fields])
    elif isinstance(dataType, ArrayType):
        return _need_python_to_sql_conversion(dataType.elementType)
    elif isinstance(dataType, MapType):
        return _need_python_to_sql_conversion(dataType.keyType) or \
            _need_python_to_sql_conversion(dataType.valueType)
    elif isinstance(dataType, UserDefinedType):
        return True
    else:
        return False


def _python_to_sql_converter(dataType):
    """
    Returns a converter that converts a Python object into a SQL datum for the given type.

    >>> conv = _python_to_sql_converter(DoubleType())
    >>> conv(1.0)
    1.0
    >>> conv = _python_to_sql_converter(ArrayType(DoubleType(), False))
    >>> conv([1.0, 2.0])
    [1.0, 2.0]
    >>> conv = _python_to_sql_converter(ExamplePointUDT())
    >>> conv(ExamplePoint(1.0, 2.0))
    [1.0, 2.0]
    >>> schema = StructType([StructField("label", DoubleType(), False),
    ...                      StructField("point", ExamplePointUDT(), False)])
    >>> conv = _python_to_sql_converter(schema)
    >>> conv((1.0, ExamplePoint(1.0, 2.0)))
    (1.0, [1.0, 2.0])
    """
    if not _need_python_to_sql_conversion(dataType):
        return lambda x: x

    if isinstance(dataType, StructType):
        names, types = zip(*[(f.name, f.dataType) for f in dataType.fields])
        converters = map(_python_to_sql_converter, types)

        def converter(obj):
            if isinstance(obj, dict):
                return tuple(c(obj.get(n)) for n, c in zip(names, converters))
            elif isinstance(obj, tuple):
                if hasattr(obj, "_fields") or hasattr(obj, "__FIELDS__"):
                    return tuple(c(v) for c, v in zip(converters, obj))
                elif all(isinstance(x, tuple) and len(x) == 2 for x in obj):  # k-v pairs
                    d = dict(obj)
                    return tuple(c(d.get(n)) for n, c in zip(names, converters))
                else:
                    return tuple(c(v) for c, v in zip(converters, obj))
            else:
                raise ValueError("Unexpected tuple %r with type %r" % (obj, dataType))
        return converter
    elif isinstance(dataType, ArrayType):
        element_converter = _python_to_sql_converter(dataType.elementType)
        return lambda a: [element_converter(v) for v in a]
    elif isinstance(dataType, MapType):
        key_converter = _python_to_sql_converter(dataType.keyType)
        value_converter = _python_to_sql_converter(dataType.valueType)
        return lambda m: dict([(key_converter(k), value_converter(v)) for k, v in m.items()])
    elif isinstance(dataType, UserDefinedType):
        return lambda obj: dataType.serialize(obj)
    else:
        raise ValueError("Unexpected type %r" % dataType)


def _has_nulltype(dt):
    """ Return whether there is NullType in `dt` or not """
    if isinstance(dt, StructType):
        return any(_has_nulltype(f.dataType) for f in dt.fields)
    elif isinstance(dt, ArrayType):
        return _has_nulltype((dt.elementType))
    elif isinstance(dt, MapType):
        return _has_nulltype(dt.keyType) or _has_nulltype(dt.valueType)
    else:
        return isinstance(dt, NullType)


def _merge_type(a, b):
    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif type(a) is not type(b):
        # TODO: type cast (such as int -> long)
        raise TypeError("Can not merge type %s and %s" % (a, b))

    # same type
    if isinstance(a, StructType):
        nfs = dict((f.name, f.dataType) for f in b.fields)
        fields = [StructField(f.name, _merge_type(f.dataType, nfs.get(f.name, NullType())))
                  for f in a.fields]
        names = set([f.name for f in fields])
        for n in nfs:
            if n not in names:
                fields.append(StructField(n, nfs[n]))
        return StructType(fields)

    elif isinstance(a, ArrayType):
        return ArrayType(_merge_type(a.elementType, b.elementType), True)

    elif isinstance(a, MapType):
        return MapType(_merge_type(a.keyType, b.keyType),
                       _merge_type(a.valueType, b.valueType),
                       True)
    else:
        return a


def _create_converter(dataType):
    """Create an converter to drop the names of fields in obj """
    if isinstance(dataType, ArrayType):
        conv = _create_converter(dataType.elementType)
        return lambda row: map(conv, row)

    elif isinstance(dataType, MapType):
        kconv = _create_converter(dataType.keyType)
        vconv = _create_converter(dataType.valueType)
        return lambda row: dict((kconv(k), vconv(v)) for k, v in row.iteritems())

    elif isinstance(dataType, NullType):
        return lambda x: None

    elif not isinstance(dataType, StructType):
        return lambda x: x

    # dataType must be StructType
    names = [f.name for f in dataType.fields]
    converters = [_create_converter(f.dataType) for f in dataType.fields]

    def convert_struct(obj):
        if obj is None:
            return

        if isinstance(obj, tuple):
            if hasattr(obj, "_fields"):
                d = dict(zip(obj._fields, obj))
            elif hasattr(obj, "__FIELDS__"):
                d = dict(zip(obj.__FIELDS__, obj))
            elif all(isinstance(x, tuple) and len(x) == 2 for x in obj):
                d = dict(obj)
            else:
                raise ValueError("unexpected tuple: %s" % str(obj))

        elif isinstance(obj, dict):
            d = obj
        elif hasattr(obj, "__dict__"):  # object
            d = obj.__dict__
        else:
            raise ValueError("Unexpected obj: %s" % obj)

        return tuple([conv(d.get(name)) for name, conv in zip(names, converters)])

    return convert_struct


_BRACKETS = {'(': ')', '[': ']', '{': '}'}


def _split_schema_abstract(s):
    """
    split the schema abstract into fields

    >>> _split_schema_abstract("a b  c")
    ['a', 'b', 'c']
    >>> _split_schema_abstract("a(a b)")
    ['a(a b)']
    >>> _split_schema_abstract("a b[] c{a b}")
    ['a', 'b[]', 'c{a b}']
    >>> _split_schema_abstract(" ")
    []
    """

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
            if c in _BRACKETS:
                brackets.append(c)
            elif c in _BRACKETS.values():
                if not brackets or c != _BRACKETS[brackets.pop()]:
                    raise ValueError("unexpected " + c)

    if brackets:
        raise ValueError("brackets not closed: %s" % brackets)
    if w:
        r.append(w)
    return r


def _parse_field_abstract(s):
    """
    Parse a field in schema abstract

    >>> _parse_field_abstract("a")
    StructField(a,None,true)
    >>> _parse_field_abstract("b(c d)")
    StructField(b,StructType(...c,None,true),StructField(d...
    >>> _parse_field_abstract("a[]")
    StructField(a,ArrayType(None,true),true)
    >>> _parse_field_abstract("a{[]}")
    StructField(a,MapType(None,ArrayType(None,true),true),true)
    """
    if set(_BRACKETS.keys()) & set(s):
        idx = min((s.index(c) for c in _BRACKETS if c in s))
        name = s[:idx]
        return StructField(name, _parse_schema_abstract(s[idx:]), True)
    else:
        return StructField(s, None, True)


def _parse_schema_abstract(s):
    """
    parse abstract into schema

    >>> _parse_schema_abstract("a b  c")
    StructType...a...b...c...
    >>> _parse_schema_abstract("a[b c] b{}")
    StructType...a,ArrayType...b...c...b,MapType...
    >>> _parse_schema_abstract("c{} d{a b}")
    StructType...c,MapType...d,MapType...a...b...
    >>> _parse_schema_abstract("a b(t)").fields[1]
    StructField(b,StructType(List(StructField(t,None,true))),true)
    """
    s = s.strip()
    if not s:
        return

    elif s.startswith('('):
        return _parse_schema_abstract(s[1:-1])

    elif s.startswith('['):
        return ArrayType(_parse_schema_abstract(s[1:-1]), True)

    elif s.startswith('{'):
        return MapType(None, _parse_schema_abstract(s[1:-1]))

    parts = _split_schema_abstract(s)
    fields = [_parse_field_abstract(p) for p in parts]
    return StructType(fields)


def _infer_schema_type(obj, dataType):
    """
    Fill the dataType with types infered from obj

    >>> schema = _parse_schema_abstract("a b c d")
    >>> row = (1, 1.0, "str", datetime.date(2014, 10, 10))
    >>> _infer_schema_type(row, schema)
    StructType...IntegerType...DoubleType...StringType...DateType...
    >>> row = [[1], {"key": (1, 2.0)}]
    >>> schema = _parse_schema_abstract("a[] b{c d}")
    >>> _infer_schema_type(row, schema)
    StructType...a,ArrayType...b,MapType(StringType,...c,IntegerType...
    """
    if dataType is None:
        return _infer_type(obj)

    if not obj:
        return NullType()

    if isinstance(dataType, ArrayType):
        eType = _infer_schema_type(obj[0], dataType.elementType)
        return ArrayType(eType, True)

    elif isinstance(dataType, MapType):
        k, v = obj.iteritems().next()
        return MapType(_infer_schema_type(k, dataType.keyType),
                       _infer_schema_type(v, dataType.valueType))

    elif isinstance(dataType, StructType):
        fs = dataType.fields
        assert len(fs) == len(obj), \
            "Obj(%s) have different length with fields(%s)" % (obj, fs)
        fields = [StructField(f.name, _infer_schema_type(o, f.dataType), True)
                  for o, f in zip(obj, fs)]
        return StructType(fields)

    else:
        raise ValueError("Unexpected dataType: %s" % dataType)


_acceptable_types = {
    BooleanType: (bool,),
    ByteType: (int, long),
    ShortType: (int, long),
    IntegerType: (int, long),
    LongType: (int, long),
    FloatType: (float,),
    DoubleType: (float,),
    DecimalType: (decimal.Decimal,),
    StringType: (str, unicode),
    BinaryType: (bytearray,),
    DateType: (datetime.date,),
    TimestampType: (datetime.datetime,),
    ArrayType: (list, tuple, array),
    MapType: (dict,),
    StructType: (tuple, list),
}


def _verify_type(obj, dataType):
    """
    Verify the type of obj against dataType, raise an exception if
    they do not match.

    >>> _verify_type(None, StructType([]))
    >>> _verify_type("", StringType())
    >>> _verify_type(0, IntegerType())
    >>> _verify_type(range(3), ArrayType(ShortType()))
    >>> _verify_type(set(), ArrayType(StringType())) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError:...
    >>> _verify_type({}, MapType(StringType(), IntegerType()))
    >>> _verify_type((), StructType([]))
    >>> _verify_type([], StructType([]))
    >>> _verify_type([1], StructType([])) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _verify_type(ExamplePoint(1.0, 2.0), ExamplePointUDT())
    >>> _verify_type([1.0, 2.0], ExamplePointUDT()) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    """
    # all objects are nullable
    if obj is None:
        return

    if isinstance(dataType, UserDefinedType):
        if not (hasattr(obj, '__UDT__') and obj.__UDT__ == dataType):
            raise ValueError("%r is not an instance of type %r" % (obj, dataType))
        _verify_type(dataType.serialize(obj), dataType.sqlType())
        return

    _type = type(dataType)
    assert _type in _acceptable_types, "unkown datatype: %s" % dataType

    # subclass of them can not be deserialized in JVM
    if type(obj) not in _acceptable_types[_type]:
        raise TypeError("%s can not accept object in type %s"
                        % (dataType, type(obj)))

    if isinstance(dataType, ArrayType):
        for i in obj:
            _verify_type(i, dataType.elementType)

    elif isinstance(dataType, MapType):
        for k, v in obj.iteritems():
            _verify_type(k, dataType.keyType)
            _verify_type(v, dataType.valueType)

    elif isinstance(dataType, StructType):
        if len(obj) != len(dataType.fields):
            raise ValueError("Length of object (%d) does not match with"
                             "length of fields (%d)" % (len(obj), len(dataType.fields)))
        for v, f in zip(obj, dataType.fields):
            _verify_type(v, f.dataType)


_cached_cls = {}


def _restore_object(dataType, obj):
    """ Restore object during unpickling. """
    # use id(dataType) as key to speed up lookup in dict
    # Because of batched pickling, dataType will be the
    # same object in most cases.
    k = id(dataType)
    cls = _cached_cls.get(k)
    if cls is None:
        # use dataType as key to avoid create multiple class
        cls = _cached_cls.get(dataType)
        if cls is None:
            cls = _create_cls(dataType)
            _cached_cls[dataType] = cls
        _cached_cls[k] = cls
    return cls(obj)


def _create_object(cls, v):
    """ Create an customized object with class `cls`. """
    # datetime.date would be deserialized as datetime.datetime
    # from java type, so we need to set it back.
    if cls is datetime.date and isinstance(v, datetime.datetime):
        return v.date()
    return cls(v) if v is not None else v


def _create_getter(dt, i):
    """ Create a getter for item `i` with schema """
    cls = _create_cls(dt)

    def getter(self):
        return _create_object(cls, self[i])

    return getter


def _has_struct_or_date(dt):
    """Return whether `dt` is or has StructType/DateType in it"""
    if isinstance(dt, StructType):
        return True
    elif isinstance(dt, ArrayType):
        return _has_struct_or_date(dt.elementType)
    elif isinstance(dt, MapType):
        return _has_struct_or_date(dt.keyType) or _has_struct_or_date(dt.valueType)
    elif isinstance(dt, DateType):
        return True
    elif isinstance(dt, UserDefinedType):
        return True
    return False


def _create_properties(fields):
    """Create properties according to fields"""
    ps = {}
    for i, f in enumerate(fields):
        name = f.name
        if (name.startswith("__") and name.endswith("__")
                or keyword.iskeyword(name)):
            warnings.warn("field name %s can not be accessed in Python,"
                          "use position to access it instead" % name)
        if _has_struct_or_date(f.dataType):
            # delay creating object until accessing it
            getter = _create_getter(f.dataType, i)
        else:
            getter = itemgetter(i)
        ps[name] = property(getter)
    return ps


def _create_cls(dataType):
    """
    Create an class by dataType

    The created class is similar to namedtuple, but can have nested schema.

    >>> schema = _parse_schema_abstract("a b c")
    >>> row = (1, 1.0, "str")
    >>> schema = _infer_schema_type(row, schema)
    >>> obj = _create_cls(schema)(row)
    >>> import pickle
    >>> pickle.loads(pickle.dumps(obj))
    Row(a=1, b=1.0, c='str')

    >>> row = [[1], {"key": (1, 2.0)}]
    >>> schema = _parse_schema_abstract("a[] b{c d}")
    >>> schema = _infer_schema_type(row, schema)
    >>> obj = _create_cls(schema)(row)
    >>> pickle.loads(pickle.dumps(obj))
    Row(a=[1], b={'key': Row(c=1, d=2.0)})
    >>> pickle.loads(pickle.dumps(obj.a))
    [1]
    >>> pickle.loads(pickle.dumps(obj.b))
    {'key': Row(c=1, d=2.0)}
    """

    if isinstance(dataType, ArrayType):
        cls = _create_cls(dataType.elementType)

        def List(l):
            if l is None:
                return
            return [_create_object(cls, v) for v in l]

        return List

    elif isinstance(dataType, MapType):
        kcls = _create_cls(dataType.keyType)
        vcls = _create_cls(dataType.valueType)

        def Dict(d):
            if d is None:
                return
            return dict((_create_object(kcls, k), _create_object(vcls, v)) for k, v in d.items())

        return Dict

    elif isinstance(dataType, DateType):
        return datetime.date

    elif isinstance(dataType, UserDefinedType):
        return lambda datum: dataType.deserialize(datum)

    elif not isinstance(dataType, StructType):
        # no wrapper for primitive types
        return lambda x: x

    class Row(tuple):

        """ Row in SchemaRDD """
        __DATATYPE__ = dataType
        __FIELDS__ = tuple(f.name for f in dataType.fields)
        __slots__ = ()

        # create property for fast access
        locals().update(_create_properties(dataType.fields))

        def asDict(self):
            """ Return as a dict """
            return dict((n, getattr(self, n)) for n in self.__FIELDS__)

        def __repr__(self):
            # call collect __repr__ for nested objects
            return ("Row(%s)" % ", ".join("%s=%r" % (n, getattr(self, n))
                                          for n in self.__FIELDS__))

        def __reduce__(self):
            return (_restore_object, (self.__DATATYPE__, tuple(self)))

    return Row


class SQLContext(object):

    """Main entry point for Spark SQL functionality.

    A SQLContext can be used create L{SchemaRDD}, register L{SchemaRDD} as
    tables, execute SQL over tables, cache tables, and read parquet files.
    """

    def __init__(self, sparkContext, sqlContext=None):
        """Create a new SQLContext.

        :param sparkContext: The SparkContext to wrap.
        :param sqlContext: An optional JVM Scala SQLContext. If set, we do not instatiate a new
        SQLContext in the JVM, instead we make all calls to this object.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.inferSchema(srdd) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        TypeError:...

        >>> bad_rdd = sc.parallelize([1,2,3])
        >>> sqlCtx.inferSchema(bad_rdd) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        ValueError:...

        >>> from datetime import datetime
        >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1L,
        ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
        ...     time=datetime(2014, 8, 1, 14, 1, 5))])
        >>> srdd = sqlCtx.inferSchema(allTypes)
        >>> srdd.registerTempTable("allTypes")
        >>> sqlCtx.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
        ...            'from allTypes where b and i > 0').collect()
        [Row(c0=2, c1=2.0, c2=False, c3=2, c4=0...8, 1, 14, 1, 5), a=1)]
        >>> srdd.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time,
        ...                     x.row.a, x.list)).collect()
        [(1, u'string', 1.0, 1, True, ...(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
        """
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        self._scala_SQLContext = sqlContext

    @property
    def _ssql_ctx(self):
        """Accessor for the JVM Spark SQL context.

        Subclasses can override this property to provide their own
        JVM Contexts.
        """
        if self._scala_SQLContext is None:
            self._scala_SQLContext = self._jvm.SQLContext(self._jsc.sc())
        return self._scala_SQLContext

    def registerFunction(self, name, f, returnType=StringType()):
        """Registers a lambda function as a UDF so it can be used in SQL statements.

        In addition to a name and the function itself, the return type can be optionally specified.
        When the return type is not given it default to a string and conversion will automatically
        be done.  For any other return type, the produced object must match the specified type.

        >>> sqlCtx.registerFunction("stringLengthString", lambda x: len(x))
        >>> sqlCtx.sql("SELECT stringLengthString('test')").collect()
        [Row(c0=u'4')]
        >>> sqlCtx.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
        >>> sqlCtx.sql("SELECT stringLengthInt('test')").collect()
        [Row(c0=4)]
        """
        func = lambda _, it: imap(lambda x: f(*x), it)
        command = (func, None,
                   AutoBatchedSerializer(PickleSerializer()),
                   AutoBatchedSerializer(PickleSerializer()))
        ser = CloudPickleSerializer()
        pickled_command = ser.dumps(command)
        if len(pickled_command) > (1 << 20):  # 1M
            broadcast = self._sc.broadcast(pickled_command)
            pickled_command = ser.dumps(broadcast)
        broadcast_vars = ListConverter().convert(
            [x._jbroadcast for x in self._sc._pickled_broadcast_vars],
            self._sc._gateway._gateway_client)
        self._sc._pickled_broadcast_vars.clear()
        env = MapConverter().convert(self._sc.environment,
                                     self._sc._gateway._gateway_client)
        includes = ListConverter().convert(self._sc._python_includes,
                                           self._sc._gateway._gateway_client)
        self._ssql_ctx.registerPython(name,
                                      bytearray(pickled_command),
                                      env,
                                      includes,
                                      self._sc.pythonExec,
                                      broadcast_vars,
                                      self._sc._javaAccumulator,
                                      returnType.json())

    def inferSchema(self, rdd, samplingRatio=None):
        """Infer and apply a schema to an RDD of L{Row}.

        When samplingRatio is specified, the schema is inferred by looking
        at the types of each row in the sampled dataset. Otherwise, the
        first 100 rows of the RDD are inspected. Nested collections are
        supported, which can include array, dict, list, Row, tuple,
        namedtuple, or object.

        Each row could be L{pyspark.sql.Row} object or namedtuple or objects.
        Using top level dicts is deprecated, as dict is used to represent Maps.

        If a single column has multiple distinct inferred types, it may cause
        runtime exceptions.

        >>> rdd = sc.parallelize(
        ...     [Row(field1=1, field2="row1"),
        ...      Row(field1=2, field2="row2"),
        ...      Row(field1=3, field2="row3")])
        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.collect()[0]
        Row(field1=1, field2=u'row1')

        >>> NestedRow = Row("f1", "f2")
        >>> nestedRdd1 = sc.parallelize([
        ...     NestedRow(array('i', [1, 2]), {"row1": 1.0}),
        ...     NestedRow(array('i', [2, 3]), {"row2": 2.0})])
        >>> srdd = sqlCtx.inferSchema(nestedRdd1)
        >>> srdd.collect()
        [Row(f1=[1, 2], f2={u'row1': 1.0}), ..., f2={u'row2': 2.0})]

        >>> nestedRdd2 = sc.parallelize([
        ...     NestedRow([[1, 2], [2, 3]], [1, 2]),
        ...     NestedRow([[2, 3], [3, 4]], [2, 3])])
        >>> srdd = sqlCtx.inferSchema(nestedRdd2)
        >>> srdd.collect()
        [Row(f1=[[1, 2], [2, 3]], f2=[1, 2]), ..., f2=[2, 3])]

        >>> from collections import namedtuple
        >>> CustomRow = namedtuple('CustomRow', 'field1 field2')
        >>> rdd = sc.parallelize(
        ...     [CustomRow(field1=1, field2="row1"),
        ...      CustomRow(field1=2, field2="row2"),
        ...      CustomRow(field1=3, field2="row3")])
        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.collect()[0]
        Row(field1=1, field2=u'row1')
        """

        if isinstance(rdd, SchemaRDD):
            raise TypeError("Cannot apply schema to SchemaRDD")

        first = rdd.first()
        if not first:
            raise ValueError("The first row in RDD is empty, "
                             "can not infer schema")
        if type(first) is dict:
            warnings.warn("Using RDD of dict to inferSchema is deprecated,"
                          "please use pyspark.sql.Row instead")

        if samplingRatio is None:
            schema = _infer_schema(first)
            if _has_nulltype(schema):
                for row in rdd.take(100)[1:]:
                    schema = _merge_type(schema, _infer_schema(row))
                    if not _has_nulltype(schema):
                        break
                else:
                    warnings.warn("Some of types cannot be determined by the "
                                  "first 100 rows, please try again with sampling")
        else:
            if samplingRatio > 0.99:
                rdd = rdd.sample(False, float(samplingRatio))
            schema = rdd.map(_infer_schema).reduce(_merge_type)

        converter = _create_converter(schema)
        rdd = rdd.map(converter)
        return self.applySchema(rdd, schema)

    def applySchema(self, rdd, schema):
        """
        Applies the given schema to the given RDD of L{tuple} or L{list}.

        These tuples or lists can contain complex nested structures like
        lists, maps or nested rows.

        The schema should be a StructType.

        It is important that the schema matches the types of the objects
        in each row or exceptions could be thrown at runtime.

        >>> rdd2 = sc.parallelize([(1, "row1"), (2, "row2"), (3, "row3")])
        >>> schema = StructType([StructField("field1", IntegerType(), False),
        ...     StructField("field2", StringType(), False)])
        >>> srdd = sqlCtx.applySchema(rdd2, schema)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.sql("SELECT * from table1")
        >>> srdd2.collect()
        [Row(field1=1, field2=u'row1'),..., Row(field1=3, field2=u'row3')]

        >>> from datetime import date, datetime
        >>> rdd = sc.parallelize([(127, -128L, -32768, 32767, 2147483647L, 1.0,
        ...     date(2010, 1, 1),
        ...     datetime(2010, 1, 1, 1, 1, 1),
        ...     {"a": 1}, (2,), [1, 2, 3], None)])
        >>> schema = StructType([
        ...     StructField("byte1", ByteType(), False),
        ...     StructField("byte2", ByteType(), False),
        ...     StructField("short1", ShortType(), False),
        ...     StructField("short2", ShortType(), False),
        ...     StructField("int", IntegerType(), False),
        ...     StructField("float", FloatType(), False),
        ...     StructField("date", DateType(), False),
        ...     StructField("time", TimestampType(), False),
        ...     StructField("map",
        ...         MapType(StringType(), IntegerType(), False), False),
        ...     StructField("struct",
        ...         StructType([StructField("b", ShortType(), False)]), False),
        ...     StructField("list", ArrayType(ByteType(), False), False),
        ...     StructField("null", DoubleType(), True)])
        >>> srdd = sqlCtx.applySchema(rdd, schema)
        >>> results = srdd.map(
        ...     lambda x: (x.byte1, x.byte2, x.short1, x.short2, x.int, x.float, x.date,
        ...         x.time, x.map["a"], x.struct.b, x.list, x.null))
        >>> results.collect()[0] # doctest: +NORMALIZE_WHITESPACE
        (127, -128, -32768, 32767, 2147483647, 1.0, datetime.date(2010, 1, 1),
             datetime.datetime(2010, 1, 1, 1, 1, 1), 1, 2, [1, 2, 3], None)

        >>> srdd.registerTempTable("table2")
        >>> sqlCtx.sql(
        ...   "SELECT byte1 - 1 AS byte1, byte2 + 1 AS byte2, " +
        ...     "short1 + 1 AS short1, short2 - 1 AS short2, int - 1 AS int, " +
        ...     "float + 1.5 as float FROM table2").collect()
        [Row(byte1=126, byte2=-127, short1=-32767, short2=32766, int=2147483646, float=2.5)]

        >>> rdd = sc.parallelize([(127, -32768, 1.0,
        ...     datetime(2010, 1, 1, 1, 1, 1),
        ...     {"a": 1}, (2,), [1, 2, 3])])
        >>> abstract = "byte short float time map{} struct(b) list[]"
        >>> schema = _parse_schema_abstract(abstract)
        >>> typedSchema = _infer_schema_type(rdd.first(), schema)
        >>> srdd = sqlCtx.applySchema(rdd, typedSchema)
        >>> srdd.collect()
        [Row(byte=127, short=-32768, float=1.0, time=..., list=[1, 2, 3])]
        """

        if isinstance(rdd, SchemaRDD):
            raise TypeError("Cannot apply schema to SchemaRDD")

        if not isinstance(schema, StructType):
            raise TypeError("schema should be StructType")

        # take the first few rows to verify schema
        rows = rdd.take(10)
        # Row() cannot been deserialized by Pyrolite
        if rows and isinstance(rows[0], tuple) and rows[0].__class__.__name__ == 'Row':
            rdd = rdd.map(tuple)
            rows = rdd.take(10)

        for row in rows:
            _verify_type(row, schema)

        # convert python objects to sql data
        converter = _python_to_sql_converter(schema)
        rdd = rdd.map(converter)

        jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
        srdd = self._ssql_ctx.applySchemaToPythonRDD(jrdd.rdd(), schema.json())
        return SchemaRDD(srdd.toJavaSchemaRDD(), self)

    def registerRDDAsTable(self, rdd, tableName):
        """Registers the given RDD as a temporary table in the catalog.

        Temporary tables exist only during the lifetime of this instance of
        SQLContext.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        """
        if (rdd.__class__ is SchemaRDD):
            srdd = rdd._jschema_rdd.baseSchemaRDD()
            self._ssql_ctx.registerRDDAsTable(srdd, tableName)
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
        jschema_rdd = self._ssql_ctx.parquetFile(path).toJavaSchemaRDD()
        return SchemaRDD(jschema_rdd, self)

    def jsonFile(self, path, schema=None, samplingRatio=1.0):
        """
        Loads a text file storing one JSON object per line as a
        L{SchemaRDD}.

        If the schema is provided, applies the given schema to this
        JSON dataset.

        Otherwise, it samples the dataset with ratio `samplingRatio` to
        determine the schema.

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
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, "
        ...   "field6 as f4 from table1")
        >>> for r in srdd2.collect():
        ...     print r
        Row(f1=1, f2=u'row1', f3=Row(field4=11, field5=None), f4=None)
        Row(f1=2, f2=None, f3=Row(field4=22,..., f4=[Row(field7=u'row2')])
        Row(f1=None, f2=u'row3', f3=Row(field4=33, field5=[]), f4=None)

        >>> srdd3 = sqlCtx.jsonFile(jsonFile, srdd1.schema())
        >>> sqlCtx.registerRDDAsTable(srdd3, "table2")
        >>> srdd4 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, "
        ...   "field6 as f4 from table2")
        >>> for r in srdd4.collect():
        ...    print r
        Row(f1=1, f2=u'row1', f3=Row(field4=11, field5=None), f4=None)
        Row(f1=2, f2=None, f3=Row(field4=22,..., f4=[Row(field7=u'row2')])
        Row(f1=None, f2=u'row3', f3=Row(field4=33, field5=[]), f4=None)

        >>> schema = StructType([
        ...     StructField("field2", StringType(), True),
        ...     StructField("field3",
        ...         StructType([
        ...             StructField("field5",
        ...                 ArrayType(IntegerType(), False), True)]), False)])
        >>> srdd5 = sqlCtx.jsonFile(jsonFile, schema)
        >>> sqlCtx.registerRDDAsTable(srdd5, "table3")
        >>> srdd6 = sqlCtx.sql(
        ...   "SELECT field2 AS f1, field3.field5 as f2, "
        ...   "field3.field5[0] as f3 from table3")
        >>> srdd6.collect()
        [Row(f1=u'row1', f2=None, f3=None)...Row(f1=u'row3', f2=[], f3=None)]
        """
        if schema is None:
            srdd = self._ssql_ctx.jsonFile(path, samplingRatio)
        else:
            scala_datatype = self._ssql_ctx.parseDataType(schema.json())
            srdd = self._ssql_ctx.jsonFile(path, scala_datatype)
        return SchemaRDD(srdd.toJavaSchemaRDD(), self)

    def jsonRDD(self, rdd, schema=None, samplingRatio=1.0):
        """Loads an RDD storing one JSON object per string as a L{SchemaRDD}.

        If the schema is provided, applies the given schema to this
        JSON dataset.

        Otherwise, it samples the dataset with ratio `samplingRatio` to
        determine the schema.

        >>> srdd1 = sqlCtx.jsonRDD(json)
        >>> sqlCtx.registerRDDAsTable(srdd1, "table1")
        >>> srdd2 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, "
        ...   "field6 as f4 from table1")
        >>> for r in srdd2.collect():
        ...     print r
        Row(f1=1, f2=u'row1', f3=Row(field4=11, field5=None), f4=None)
        Row(f1=2, f2=None, f3=Row(field4=22..., f4=[Row(field7=u'row2')])
        Row(f1=None, f2=u'row3', f3=Row(field4=33, field5=[]), f4=None)

        >>> srdd3 = sqlCtx.jsonRDD(json, srdd1.schema())
        >>> sqlCtx.registerRDDAsTable(srdd3, "table2")
        >>> srdd4 = sqlCtx.sql(
        ...   "SELECT field1 AS f1, field2 as f2, field3 as f3, "
        ...   "field6 as f4 from table2")
        >>> for r in srdd4.collect():
        ...     print r
        Row(f1=1, f2=u'row1', f3=Row(field4=11, field5=None), f4=None)
        Row(f1=2, f2=None, f3=Row(field4=22..., f4=[Row(field7=u'row2')])
        Row(f1=None, f2=u'row3', f3=Row(field4=33, field5=[]), f4=None)

        >>> schema = StructType([
        ...     StructField("field2", StringType(), True),
        ...     StructField("field3",
        ...         StructType([
        ...             StructField("field5",
        ...                 ArrayType(IntegerType(), False), True)]), False)])
        >>> srdd5 = sqlCtx.jsonRDD(json, schema)
        >>> sqlCtx.registerRDDAsTable(srdd5, "table3")
        >>> srdd6 = sqlCtx.sql(
        ...   "SELECT field2 AS f1, field3.field5 as f2, "
        ...   "field3.field5[0] as f3 from table3")
        >>> srdd6.collect()
        [Row(f1=u'row1', f2=None,...Row(f1=u'row3', f2=[], f3=None)]

        >>> sqlCtx.jsonRDD(sc.parallelize(['{}',
        ...         '{"key0": {"key1": "value1"}}'])).collect()
        [Row(key0=None), Row(key0=Row(key1=u'value1'))]
        >>> sqlCtx.jsonRDD(sc.parallelize(['{"key0": null}',
        ...         '{"key0": {"key1": "value1"}}'])).collect()
        [Row(key0=None), Row(key0=Row(key1=u'value1'))]
        """

        def func(iterator):
            for x in iterator:
                if not isinstance(x, basestring):
                    x = unicode(x)
                if isinstance(x, unicode):
                    x = x.encode("utf-8")
                yield x
        keyed = rdd.mapPartitions(func)
        keyed._bypass_serializer = True
        jrdd = keyed._jrdd.map(self._jvm.BytesToString())
        if schema is None:
            srdd = self._ssql_ctx.jsonRDD(jrdd.rdd(), samplingRatio)
        else:
            scala_datatype = self._ssql_ctx.parseDataType(schema.json())
            srdd = self._ssql_ctx.jsonRDD(jrdd.rdd(), scala_datatype)
        return SchemaRDD(srdd.toJavaSchemaRDD(), self)

    def sql(self, sqlQuery):
        """Return a L{SchemaRDD} representing the result of the given query.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.sql("SELECT field1 AS f1, field2 as f2 from table1")
        >>> srdd2.collect()
        [Row(f1=1, f2=u'row1'), Row(f1=2, f2=u'row2'), Row(f1=3, f2=u'row3')]
        """
        return SchemaRDD(self._ssql_ctx.sql(sqlQuery).toJavaSchemaRDD(), self)

    def table(self, tableName):
        """Returns the specified table as a L{SchemaRDD}.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> sqlCtx.registerRDDAsTable(srdd, "table1")
        >>> srdd2 = sqlCtx.table("table1")
        >>> sorted(srdd.collect()) == sorted(srdd2.collect())
        True
        """
        return SchemaRDD(self._ssql_ctx.table(tableName).toJavaSchemaRDD(), self)

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

    def __init__(self, sparkContext, hiveContext=None):
        """Create a new HiveContext.

        :param sparkContext: The SparkContext to wrap.
        :param hiveContext: An optional JVM Scala HiveContext. If set, we do not instatiate a new
        HiveContext in the JVM, instead we make all calls to this object.
        """
        SQLContext.__init__(self, sparkContext)

        if hiveContext:
            self._scala_HiveContext = hiveContext

    @property
    def _ssql_ctx(self):
        try:
            if not hasattr(self, '_scala_HiveContext'):
                self._scala_HiveContext = self._get_hive_ctx()
            return self._scala_HiveContext
        except Py4JError as e:
            raise Exception("You must build Spark with Hive. "
                            "Export 'SPARK_HIVE=true' and run "
                            "build/sbt assembly", e)

    def _get_hive_ctx(self):
        return self._jvm.HiveContext(self._jsc.sc())

    def hiveql(self, hqlQuery):
        """
        DEPRECATED: Use sql()
        """
        warnings.warn("hiveql() is deprecated as the sql function now parses using HiveQL by" +
                      "default. The SQL dialect for parsing can be set using 'spark.sql.dialect'",
                      DeprecationWarning)
        return SchemaRDD(self._ssql_ctx.hiveql(hqlQuery).toJavaSchemaRDD(), self)

    def hql(self, hqlQuery):
        """
        DEPRECATED: Use sql()
        """
        warnings.warn("hql() is deprecated as the sql function now parses using HiveQL by" +
                      "default. The SQL dialect for parsing can be set using 'spark.sql.dialect'",
                      DeprecationWarning)
        return self.hiveql(hqlQuery)


class LocalHiveContext(HiveContext):

    def __init__(self, sparkContext, sqlContext=None):
        HiveContext.__init__(self, sparkContext, sqlContext)
        warnings.warn("LocalHiveContext is deprecated. "
                      "Use HiveContext instead.", DeprecationWarning)

    def _get_hive_ctx(self):
        return self._jvm.LocalHiveContext(self._jsc.sc())


class TestHiveContext(HiveContext):

    def _get_hive_ctx(self):
        return self._jvm.TestHiveContext(self._jsc.sc())


def _create_row(fields, values):
    row = Row(*values)
    row.__FIELDS__ = fields
    return row


class Row(tuple):

    """
    A row in L{SchemaRDD}. The fields in it can be accessed like attributes.

    Row can be used to create a row object by using named arguments,
    the fields will be sorted by names.

    >>> row = Row(name="Alice", age=11)
    >>> row
    Row(age=11, name='Alice')
    >>> row.name, row.age
    ('Alice', 11)

    Row also can be used to create another Row like class, then it
    could be used to create Row objects, such as

    >>> Person = Row("name", "age")
    >>> Person
    <Row(name, age)>
    >>> Person("Alice", 11)
    Row(name='Alice', age=11)
    """

    def __new__(self, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if args:
            # create row class or objects
            return tuple.__new__(self, args)

        elif kwargs:
            # create row objects
            names = sorted(kwargs.keys())
            values = tuple(kwargs[n] for n in names)
            row = tuple.__new__(self, values)
            row.__FIELDS__ = names
            return row

        else:
            raise ValueError("No args or kwargs")

    def asDict(self):
        """
        Return as an dict
        """
        if not hasattr(self, "__FIELDS__"):
            raise TypeError("Cannot convert a Row class into dict")
        return dict(zip(self.__FIELDS__, self))

    # let obect acs like class
    def __call__(self, *args):
        """create new Row object"""
        return _create_row(self, args)

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__FIELDS__.index(item)
            return self[idx]
        except IndexError:
            raise AttributeError(item)

    def __reduce__(self):
        if hasattr(self, "__FIELDS__"):
            return (_create_row, (self.__FIELDS__, tuple(self)))
        else:
            return tuple.__reduce__(self)

    def __repr__(self):
        if hasattr(self, "__FIELDS__"):
            return "Row(%s)" % ", ".join("%s=%r" % (k, v)
                                         for k, v in zip(self.__FIELDS__, self))
        else:
            return "<Row(%s)>" % ", ".join(self)


def inherit_doc(cls):
    for name, func in vars(cls).items():
        # only inherit docstring for public functions
        if name.startswith("_"):
            continue
        if not func.__doc__:
            for parent in cls.__bases__:
                parent_func = getattr(parent, name, None)
                if parent_func and getattr(parent_func, "__doc__", None):
                    func.__doc__ = parent_func.__doc__
                    break
    return cls


@inherit_doc
class SchemaRDD(RDD):

    """An RDD of L{Row} objects that has an associated schema.

    The underlying JVM object is a SchemaRDD, not a PythonRDD, so we can
    utilize the relational query api exposed by Spark SQL.

    For normal L{pyspark.rdd.RDD} operations (map, count, etc.) the
    L{SchemaRDD} is not operated on directly, as it's underlying
    implementation is an RDD composed of Java objects. Instead it is
    converted to a PythonRDD in the JVM, on which Python operations can
    be done.

    This class receives raw tuples from Java but assigns a class to it in
    all its data-collection methods (mapPartitionsWithIndex, collect, take,
    etc) so that PySpark sees them as Row objects with named fields.
    """

    def __init__(self, jschema_rdd, sql_ctx):
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx._sc
        clsName = jschema_rdd.getClass().getName()
        assert clsName.endswith("JavaSchemaRDD"), "jschema_rdd must be JavaSchemaRDD"
        self._jschema_rdd = jschema_rdd
        self._id = None
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = self.sql_ctx._sc
        # the _jrdd is created by javaToPython(), serialized by pickle
        self._jrdd_deserializer = AutoBatchedSerializer(PickleSerializer())

    @property
    def _jrdd(self):
        """Lazy evaluation of PythonRDD object.

        Only done when a user calls methods defined by the
        L{pyspark.rdd.RDD} super class (map, filter, etc.).
        """
        if not hasattr(self, '_lazy_jrdd'):
            self._lazy_jrdd = self._jschema_rdd.baseSchemaRDD().javaToPython()
        return self._lazy_jrdd

    def id(self):
        if self._id is None:
            self._id = self._jrdd.id()
        return self._id

    def limit(self, num):
        """Limit the result count to the number specified.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.limit(2).collect()
        [Row(field1=1, field2=u'row1'), Row(field1=2, field2=u'row2')]
        >>> srdd.limit(0).collect()
        []
        """
        rdd = self._jschema_rdd.baseSchemaRDD().limit(num).toJavaSchemaRDD()
        return SchemaRDD(rdd, self.sql_ctx)

    def toJSON(self, use_unicode=False):
        """Convert a SchemaRDD into a MappedRDD of JSON documents; one document per row.

        >>> srdd1 = sqlCtx.jsonRDD(json)
        >>> sqlCtx.registerRDDAsTable(srdd1, "table1")
        >>> srdd2 = sqlCtx.sql( "SELECT * from table1")
        >>> srdd2.toJSON().take(1)[0] == '{"field1":1,"field2":"row1","field3":{"field4":11}}'
        True
        >>> srdd3 = sqlCtx.sql( "SELECT field3.field4 from table1")
        >>> srdd3.toJSON().collect() == ['{"field4":11}', '{"field4":22}', '{"field4":33}']
        True
        """
        rdd = self._jschema_rdd.baseSchemaRDD().toJSON()
        return RDD(rdd.toJavaRDD(), self._sc, UTF8Deserializer(use_unicode))

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

    def registerTempTable(self, name):
        """Registers this RDD as a temporary table using the given name.

        The lifetime of this temporary table is tied to the L{SQLContext}
        that was used to create this SchemaRDD.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.registerTempTable("test")
        >>> srdd2 = sqlCtx.sql("select * from test")
        >>> sorted(srdd.collect()) == sorted(srdd2.collect())
        True
        """
        self._jschema_rdd.registerTempTable(name)

    def registerAsTable(self, name):
        """DEPRECATED: use registerTempTable() instead"""
        warnings.warn("Use registerTempTable instead of registerAsTable.", DeprecationWarning)
        self.registerTempTable(name)

    def insertInto(self, tableName, overwrite=False):
        """Inserts the contents of this SchemaRDD into the specified table.

        Optionally overwriting any existing data.
        """
        self._jschema_rdd.insertInto(tableName, overwrite)

    def saveAsTable(self, tableName):
        """Creates a new table with the contents of this SchemaRDD."""
        self._jschema_rdd.saveAsTable(tableName)

    def schema(self):
        """Returns the schema of this SchemaRDD (represented by
        a L{StructType})."""
        return _parse_datatype_json_string(self._jschema_rdd.baseSchemaRDD().schema().json())

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
        """Return a list that contains all of the rows in this RDD.

        Each object in the list is a Row, the fields can be accessed as
        attributes.

        Unlike the base RDD implementation of collect, this implementation
        leverages the query optimizer to perform a collect on the SchemaRDD,
        which supports features such as filter pushdown.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.collect()
        [Row(field1=1, field2=u'row1'), ..., Row(field1=3, field2=u'row3')]
        """
        with SCCallSiteSync(self.context) as css:
            bytesInJava = self._jschema_rdd.baseSchemaRDD().collectToPython().iterator()
        cls = _create_cls(self.schema())
        return map(cls, self._collect_iterator_through_file(bytesInJava))

    def take(self, num):
        """Take the first num rows of the RDD.

        Each object in the list is a Row, the fields can be accessed as
        attributes.

        Unlike the base RDD implementation of take, this implementation
        leverages the query optimizer to perform a collect on a SchemaRDD,
        which supports features such as filter pushdown.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.take(2)
        [Row(field1=1, field2=u'row1'), Row(field1=2, field2=u'row2')]
        """
        return self.limit(num).collect()

    # Convert each object in the RDD to a Row with the right class
    # for this SchemaRDD, so that fields can be accessed as attributes.
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

        schema = self.schema()

        def applySchema(_, it):
            cls = _create_cls(schema)
            return itertools.imap(cls, it)

        objrdd = rdd.mapPartitionsWithIndex(applySchema, preservesPartitioning)
        return objrdd.mapPartitionsWithIndex(f, preservesPartitioning)

    # We override the default cache/persist/checkpoint behavior
    # as we want to cache the underlying SchemaRDD object in the JVM,
    # not the PythonRDD checkpointed by the super class
    def cache(self):
        self.is_cached = True
        self._jschema_rdd.cache()
        return self

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY_SER):
        self.is_cached = True
        javaStorageLevel = self.ctx._getJavaStorageLevel(storageLevel)
        self._jschema_rdd.persist(javaStorageLevel)
        return self

    def unpersist(self, blocking=True):
        self.is_cached = False
        self._jschema_rdd.unpersist(blocking)
        return self

    def checkpoint(self):
        self.is_checkpointed = True
        self._jschema_rdd.checkpoint()

    def isCheckpointed(self):
        return self._jschema_rdd.isCheckpointed()

    def getCheckpointFile(self):
        checkpointFile = self._jschema_rdd.getCheckpointFile()
        if checkpointFile.isPresent():
            return checkpointFile.get()

    def coalesce(self, numPartitions, shuffle=False):
        rdd = self._jschema_rdd.coalesce(numPartitions, shuffle)
        return SchemaRDD(rdd, self.sql_ctx)

    def distinct(self, numPartitions=None):
        if numPartitions is None:
            rdd = self._jschema_rdd.distinct()
        else:
            rdd = self._jschema_rdd.distinct(numPartitions)
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
                rdd = self._jschema_rdd.subtract(other._jschema_rdd,
                                                 numPartitions)
            return SchemaRDD(rdd, self.sql_ctx)
        else:
            raise ValueError("Can only subtract another SchemaRDD")

    def sample(self, withReplacement, fraction, seed=None):
        """
        Return a sampled subset of this SchemaRDD.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.sample(False, 0.5, 97).count()
        2L
        """
        assert fraction >= 0.0, "Negative fraction value: %s" % fraction
        seed = seed if seed is not None else random.randint(0, sys.maxint)
        rdd = self._jschema_rdd.sample(withReplacement, fraction, long(seed))
        return SchemaRDD(rdd, self.sql_ctx)

    def takeSample(self, withReplacement, num, seed=None):
        """Return a fixed-size sampled subset of this SchemaRDD.

        >>> srdd = sqlCtx.inferSchema(rdd)
        >>> srdd.takeSample(False, 2, 97)
        [Row(field1=3, field2=u'row3'), Row(field1=1, field2=u'row1')]
        """
        seed = seed if seed is not None else random.randint(0, sys.maxint)
        with SCCallSiteSync(self.context) as css:
            bytesInJava = self._jschema_rdd.baseSchemaRDD() \
                .takeSampleToPython(withReplacement, num, long(seed)) \
                .iterator()
        cls = _create_cls(self.schema())
        return map(cls, self._collect_iterator_through_file(bytesInJava))


def _test():
    import doctest
    from pyspark.context import SparkContext
    # let doctest run in pyspark.sql, so DataTypes can be picklable
    import pyspark.sql
    from pyspark.sql import Row, SQLContext
    from pyspark.tests import ExamplePoint, ExamplePointUDT
    globs = pyspark.sql.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlCtx'] = SQLContext(sc)
    globs['rdd'] = sc.parallelize(
        [Row(field1=1, field2="row1"),
         Row(field1=2, field2="row2"),
         Row(field1=3, field2="row3")]
    )
    globs['ExamplePoint'] = ExamplePoint
    globs['ExamplePointUDT'] = ExamplePointUDT
    jsonStrings = [
        '{"field1": 1, "field2": "row1", "field3":{"field4":11}}',
        '{"field1" : 2, "field3":{"field4":22, "field5": [10, 11]},'
        '"field6":[{"field7": "row2"}]}',
        '{"field1" : null, "field2": "row3", '
        '"field3":{"field4":33, "field5": []}}'
    ]
    globs['jsonStrings'] = jsonStrings
    globs['json'] = sc.parallelize(jsonStrings)
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql, globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
