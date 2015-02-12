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

import decimal
import datetime
import keyword
import warnings
import json
import re
from array import array
from operator import itemgetter


__all__ = [
    "DataType", "NullType", "StringType", "BinaryType", "BooleanType", "DateType",
    "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType",
    "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType", ]


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

    def simpleString(self):
        return self.typeName()

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

    def simpleString(self):
        if self.hasPrecisionInfo:
            return "decimal(%d,%d)" % (self.precision, self.scale)
        else:
            return "decimal(10,0)"

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
    def simpleString(self):
        return 'tinyint'


class IntegerType(PrimitiveType):

    """Spark SQL IntegerType

    The data type representing int values.
    """
    def simpleString(self):
        return 'int'


class LongType(PrimitiveType):

    """Spark SQL LongType

    The data type representing long values. If the any value is
    beyond the range of [-9223372036854775808, 9223372036854775807],
    please use DecimalType.
    """
    def simpleString(self):
        return 'bigint'


class ShortType(PrimitiveType):

    """Spark SQL ShortType

    The data type representing int values with 2 signed bytes.
    """
    def simpleString(self):
        return 'smallint'


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

    def simpleString(self):
        return 'array<%s>' % self.elementType.simpleString()

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

    def simpleString(self):
        return 'map<%s,%s>' % (self.keyType.simpleString(), self.valueType.simpleString())

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

    def simpleString(self):
        return '%s:%s' % (self.name, self.dataType.simpleString())

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

    def simpleString(self):
        return 'struct<%s>' % (','.join(f.simpleString() for f in self.fields))

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

    def simpleString(self):
        return 'null'

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
    Fill the dataType with types inferred from obj

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

        """ Row in DataFrame """
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


def _create_row(fields, values):
    row = Row(*values)
    row.__FIELDS__ = fields
    return row


class Row(tuple):

    """
    A row in L{DataFrame}. The fields in it can be accessed like attributes.

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


def _test():
    import doctest
    from pyspark.context import SparkContext
    # let doctest run in pyspark.sql.types, so DataTypes can be picklable
    import pyspark.sql.types
    from pyspark.sql import Row, SQLContext
    from pyspark.sql.tests import ExamplePoint, ExamplePointUDT
    globs = pyspark.sql.types.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlCtx'] = sqlCtx = SQLContext(sc)
    globs['ExamplePoint'] = ExamplePoint
    globs['ExamplePointUDT'] = ExamplePointUDT
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.types, globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
