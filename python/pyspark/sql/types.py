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
import decimal
import time
import datetime
import calendar
import json
import re
import base64
from array import array

if sys.version >= "3":
    long = int
    basestring = unicode = str

from py4j.protocol import register_input_converter
from py4j.java_gateway import JavaClass

from pyspark.serializers import CloudPickleSerializer

__all__ = [
    "DataType", "NullType", "StringType", "BinaryType", "BooleanType", "DateType",
    "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType",
    "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType"]


class DataType(object):
    """Base class for data types."""

    def __repr__(self):
        return self.__class__.__name__

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

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

    def needConversion(self):
        """
        Does this type need to conversion between Python object and internal SQL object.

        This is used to avoid the unnecessary conversion for ArrayType/MapType/StructType.
        """
        return False

    def toInternal(self, obj):
        """
        Converts a Python object into an internal SQL object.
        """
        return obj

    def fromInternal(self, obj):
        """
        Converts an internal SQL object into a native Python object.
        """
        return obj


# This singleton pattern does not work with pickle, you will get
# another object after pickle and unpickle
class DataTypeSingleton(type):
    """Metaclass for DataType"""

    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super(DataTypeSingleton, cls).__call__()
        return cls._instances[cls]


class NullType(DataType):
    """Null type.

    The data type representing None, used for the types that cannot be inferred.
    """

    __metaclass__ = DataTypeSingleton


class AtomicType(DataType):
    """An internal type used to represent everything that is not
    null, UDTs, arrays, structs, and maps."""


class NumericType(AtomicType):
    """Numeric data types.
    """


class IntegralType(NumericType):
    """Integral data types.
    """

    __metaclass__ = DataTypeSingleton


class FractionalType(NumericType):
    """Fractional data types.
    """


class StringType(AtomicType):
    """String data type.
    """

    __metaclass__ = DataTypeSingleton


class BinaryType(AtomicType):
    """Binary (byte array) data type.
    """

    __metaclass__ = DataTypeSingleton


class BooleanType(AtomicType):
    """Boolean data type.
    """

    __metaclass__ = DataTypeSingleton


class DateType(AtomicType):
    """Date (datetime.date) data type.
    """

    __metaclass__ = DataTypeSingleton

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def needConversion(self):
        return True

    def toInternal(self, d):
        if d is not None:
            return d.toordinal() - self.EPOCH_ORDINAL

    def fromInternal(self, v):
        if v is not None:
            return datetime.date.fromordinal(v + self.EPOCH_ORDINAL)


class TimestampType(AtomicType):
    """Timestamp (datetime.datetime) data type.
    """

    __metaclass__ = DataTypeSingleton

    def needConversion(self):
        return True

    def toInternal(self, dt):
        if dt is not None:
            seconds = (calendar.timegm(dt.utctimetuple()) if dt.tzinfo
                       else time.mktime(dt.timetuple()))
            return int(seconds) * 1000000 + dt.microsecond

    def fromInternal(self, ts):
        if ts is not None:
            # using int to avoid precision loss in float
            return datetime.datetime.fromtimestamp(ts // 1000000).replace(microsecond=ts % 1000000)


class DecimalType(FractionalType):
    """Decimal (decimal.Decimal) data type.

    The DecimalType must have fixed precision (the maximum total number of digits)
    and scale (the number of digits on the right of dot). For example, (5, 2) can
    support the value from [-999.99 to 999.99].

    The precision can be up to 38, the scale must less or equal to precision.

    When create a DecimalType, the default precision and scale is (10, 0). When infer
    schema from decimal.Decimal objects, it will be DecimalType(38, 18).

    :param precision: the maximum total number of digits (default: 10)
    :param scale: the number of digits on right side of dot. (default: 0)
    """

    def __init__(self, precision=10, scale=0):
        self.precision = precision
        self.scale = scale
        self.hasPrecisionInfo = True  # this is public API

    def simpleString(self):
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def jsonValue(self):
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def __repr__(self):
        return "DecimalType(%d,%d)" % (self.precision, self.scale)


class DoubleType(FractionalType):
    """Double data type, representing double precision floats.
    """

    __metaclass__ = DataTypeSingleton


class FloatType(FractionalType):
    """Float data type, representing single precision floats.
    """

    __metaclass__ = DataTypeSingleton


class ByteType(IntegralType):
    """Byte data type, i.e. a signed integer in a single byte.
    """
    def simpleString(self):
        return 'tinyint'


class IntegerType(IntegralType):
    """Int data type, i.e. a signed 32-bit integer.
    """
    def simpleString(self):
        return 'int'


class LongType(IntegralType):
    """Long data type, i.e. a signed 64-bit integer.

    If the values are beyond the range of [-9223372036854775808, 9223372036854775807],
    please use :class:`DecimalType`.
    """
    def simpleString(self):
        return 'bigint'


class ShortType(IntegralType):
    """Short data type, i.e. a signed 16-bit integer.
    """
    def simpleString(self):
        return 'smallint'


class ArrayType(DataType):
    """Array data type.

    :param elementType: :class:`DataType` of each element in the array.
    :param containsNull: boolean, whether the array can contain null (None) values.
    """

    def __init__(self, elementType, containsNull=True):
        """
        >>> ArrayType(StringType()) == ArrayType(StringType(), True)
        True
        >>> ArrayType(StringType(), False) == ArrayType(StringType())
        False
        """
        assert isinstance(elementType, DataType), "elementType should be DataType"
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

    def needConversion(self):
        return self.elementType.needConversion()

    def toInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and [self.elementType.toInternal(v) for v in obj]

    def fromInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and [self.elementType.fromInternal(v) for v in obj]


class MapType(DataType):
    """Map data type.

    :param keyType: :class:`DataType` of the keys in the map.
    :param valueType: :class:`DataType` of the values in the map.
    :param valueContainsNull: indicates whether values can contain null (None) values.

    Keys in a map data type are not allowed to be null (None).
    """

    def __init__(self, keyType, valueType, valueContainsNull=True):
        """
        >>> (MapType(StringType(), IntegerType())
        ...        == MapType(StringType(), IntegerType(), True))
        True
        >>> (MapType(StringType(), IntegerType(), False)
        ...        == MapType(StringType(), FloatType()))
        False
        """
        assert isinstance(keyType, DataType), "keyType should be DataType"
        assert isinstance(valueType, DataType), "valueType should be DataType"
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

    def needConversion(self):
        return self.keyType.needConversion() or self.valueType.needConversion()

    def toInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and dict((self.keyType.toInternal(k), self.valueType.toInternal(v))
                            for k, v in obj.items())

    def fromInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and dict((self.keyType.fromInternal(k), self.valueType.fromInternal(v))
                            for k, v in obj.items())


class StructField(DataType):
    """A field in :class:`StructType`.

    :param name: string, name of the field.
    :param dataType: :class:`DataType` of the field.
    :param nullable: boolean, whether the field can be null (None) or not.
    :param metadata: a dict from string to simple type that can be toInternald to JSON automatically
    """

    def __init__(self, name, dataType, nullable=True, metadata=None):
        """
        >>> (StructField("f1", StringType(), True)
        ...      == StructField("f1", StringType(), True))
        True
        >>> (StructField("f1", StringType(), True)
        ...      == StructField("f2", StringType(), True))
        False
        """
        assert isinstance(dataType, DataType), "dataType should be DataType"
        assert isinstance(name, basestring), "field name should be string"
        if not isinstance(name, str):
            name = name.encode('utf-8')
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

    def needConversion(self):
        return self.dataType.needConversion()

    def toInternal(self, obj):
        return self.dataType.toInternal(obj)

    def fromInternal(self, obj):
        return self.dataType.fromInternal(obj)


class StructType(DataType):
    """Struct type, consisting of a list of :class:`StructField`.

    This is the data type representing a :class:`Row`.

    Iterating a :class:`StructType` will iterate its :class:`StructField`s.
    A contained :class:`StructField` can be accessed by name or position.

    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct1["f1"]
    StructField(f1,StringType,true)
    >>> struct1[0]
    StructField(f1,StringType,true)
    """
    def __init__(self, fields=None):
        """
        >>> struct1 = StructType([StructField("f1", StringType(), True)])
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType([StructField("f1", StringType(), True)])
        >>> struct2 = StructType([StructField("f1", StringType(), True),
        ...     StructField("f2", IntegerType(), False)])
        >>> struct1 == struct2
        False
        """
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]
            assert all(isinstance(f, StructField) for f in fields),\
                "fields should be a list of StructField"
        self._needSerializeAnyField = any(f.needConversion() for f in self)

    def add(self, field, data_type=None, nullable=True, metadata=None):
        """
        Construct a StructType by adding new elements to it to define the schema. The method accepts
        either:

            a) A single parameter which is a StructField object.
            b) Between 2 and 4 parameters as (name, data_type, nullable (optional),
               metadata(optional). The data_type parameter may be either a String or a
               DataType object.

        >>> struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        >>> struct2 = StructType([StructField("f1", StringType(), True), \\
        ...     StructField("f2", StringType(), True, None)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType().add(StructField("f1", StringType(), True))
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType().add("f1", "string", True)
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct3 = StructType().add(u"f1", u"string", True)
        >>> struct1 == struct2
        True
        >>> struct1 == struct3
        True

        :param field: Either the name of the field or a StructField object
        :param data_type: If present, the DataType of the StructField to create
        :param nullable: Whether the field to add should be nullable (default True)
        :param metadata: Any additional metadata (default None)
        :return: a new updated StructType
        """
        if isinstance(field, StructField):
            self.fields.append(field)
            self.names.append(field.name)
        else:
            if isinstance(field, basestring) and data_type is None:
                raise ValueError("Must specify DataType if passing name of struct_field to create.")

            if isinstance(data_type, basestring):
                data_type_f = _parse_datatype_json_value(data_type)
            else:
                data_type_f = data_type
            self.fields.append(StructField(field, data_type_f, nullable, metadata))
            self.names.append(field)
        self._needSerializeAnyField = any(f.needConversion() for f in self)
        return self

    def __iter__(self):
        """Iterate the fields"""
        return iter(self.fields)

    def __len__(self):
        """Return the number of fields."""
        return len(self.fields)

    def __getitem__(self, key):
        """Access fields by name or slice."""
        if isinstance(key, basestring):
            for field in self:
                if field.name == key:
                    return field
            raise KeyError('No StructField named {0}'.format(key))
        elif isinstance(key, int):
            try:
                return self.fields[key]
            except IndexError:
                raise IndexError('StructType index out of range')
        elif isinstance(key, slice):
            return StructType(self.fields[key])
        else:
            raise TypeError('StructType keys should be strings, integers or slices')

    def simpleString(self):
        return 'struct<%s>' % (','.join(f.simpleString() for f in self))

    def __repr__(self):
        return ("StructType(List(%s))" %
                ",".join(str(field) for field in self))

    def jsonValue(self):
        return {"type": self.typeName(),
                "fields": [f.jsonValue() for f in self]}

    @classmethod
    def fromJson(cls, json):
        return StructType([StructField.fromJson(f) for f in json["fields"]])

    def needConversion(self):
        # We need convert Row()/namedtuple into tuple()
        return True

    def toInternal(self, obj):
        if obj is None:
            return

        if self._needSerializeAnyField:
            if isinstance(obj, dict):
                return tuple(f.toInternal(obj.get(n)) for n, f in zip(self.names, self.fields))
            elif isinstance(obj, (tuple, list)):
                return tuple(f.toInternal(v) for f, v in zip(self.fields, obj))
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(f.toInternal(d.get(n)) for n, f in zip(self.names, self.fields))
            else:
                raise ValueError("Unexpected tuple %r with StructType" % obj)
        else:
            if isinstance(obj, dict):
                return tuple(obj.get(n) for n in self.names)
            elif isinstance(obj, Row) and getattr(obj, "__from_dict__", False):
                return tuple(obj[n] for n in self.names)
            elif isinstance(obj, (list, tuple)):
                return tuple(obj)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(d.get(n) for n in self.names)
            else:
                raise ValueError("Unexpected tuple %r with StructType" % obj)

    def fromInternal(self, obj):
        if obj is None:
            return
        if isinstance(obj, Row):
            # it's already converted by pickler
            return obj
        if self._needSerializeAnyField:
            values = [f.fromInternal(v) for f, v in zip(self.fields, obj)]
        else:
            values = obj
        return _create_row(self.names, values)


class UserDefinedType(DataType):
    """User-defined type (UDT).

    .. note:: WARN: Spark Internal Use Only
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
        The class name of the paired Scala UDT (could be '', if there
        is no corresponding one).
        """
        return ''

    def needConversion(self):
        return True

    @classmethod
    def _cachedSqlType(cls):
        """
        Cache the sqlType() into class, because it's heavy used in `toInternal`.
        """
        if not hasattr(cls, "_cached_sql_type"):
            cls._cached_sql_type = cls.sqlType()
        return cls._cached_sql_type

    def toInternal(self, obj):
        if obj is not None:
            return self._cachedSqlType().toInternal(self.serialize(obj))

    def fromInternal(self, obj):
        v = self._cachedSqlType().fromInternal(obj)
        if v is not None:
            return self.deserialize(v)

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        raise NotImplementedError("UDT must implement toInternal().")

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        raise NotImplementedError("UDT must implement fromInternal().")

    def simpleString(self):
        return 'udt'

    def json(self):
        return json.dumps(self.jsonValue(), separators=(',', ':'), sort_keys=True)

    def jsonValue(self):
        if self.scalaUDT():
            assert self.module() != '__main__', 'UDT in __main__ cannot work with ScalaUDT'
            schema = {
                "type": "udt",
                "class": self.scalaUDT(),
                "pyClass": "%s.%s" % (self.module(), type(self).__name__),
                "sqlType": self.sqlType().jsonValue()
            }
        else:
            ser = CloudPickleSerializer()
            b = ser.dumps(type(self))
            schema = {
                "type": "udt",
                "pyClass": "%s.%s" % (self.module(), type(self).__name__),
                "serializedClass": base64.b64encode(b).decode('utf8'),
                "sqlType": self.sqlType().jsonValue()
            }
        return schema

    @classmethod
    def fromJson(cls, json):
        pyUDT = str(json["pyClass"])  # convert unicode to str
        split = pyUDT.rfind(".")
        pyModule = pyUDT[:split]
        pyClass = pyUDT[split+1:]
        m = __import__(pyModule, globals(), locals(), [pyClass])
        if not hasattr(m, pyClass):
            s = base64.b64decode(json['serializedClass'].encode('utf-8'))
            UDT = CloudPickleSerializer().loads(s)
        else:
            UDT = getattr(m, pyClass)
        return UDT()

    def __eq__(self, other):
        return type(self) == type(other)


_atomic_types = [StringType, BinaryType, BooleanType, DecimalType, FloatType, DoubleType,
                 ByteType, ShortType, IntegerType, LongType, DateType, TimestampType, NullType]
_all_atomic_types = dict((t.typeName(), t) for t in _atomic_types)
_all_complex_types = dict((v.typeName(), v)
                          for v in [ArrayType, MapType, StructType])


_FIXED_DECIMAL = re.compile("decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)")


_BRACKETS = {'(': ')', '[': ']', '{': '}'}


def _parse_basic_datatype_string(s):
    if s in _all_atomic_types.keys():
        return _all_atomic_types[s]()
    elif s == "int":
        return IntegerType()
    elif _FIXED_DECIMAL.match(s):
        m = _FIXED_DECIMAL.match(s)
        return DecimalType(int(m.group(1)), int(m.group(2)))
    else:
        raise ValueError("Could not parse datatype: %s" % s)


def _ignore_brackets_split(s, separator):
    """
    Splits the given string by given separator, but ignore separators inside brackets pairs, e.g.
    given "a,b" and separator ",", it will return ["a", "b"], but given "a<b,c>, d", it will return
    ["a<b,c>", "d"].
    """
    parts = []
    buf = ""
    level = 0
    for c in s:
        if c in _BRACKETS.keys():
            level += 1
            buf += c
        elif c in _BRACKETS.values():
            if level == 0:
                raise ValueError("Brackets are not correctly paired: %s" % s)
            level -= 1
            buf += c
        elif c == separator and level > 0:
            buf += c
        elif c == separator:
            parts.append(buf)
            buf = ""
        else:
            buf += c

    if len(buf) == 0:
        raise ValueError("The %s cannot be the last char: %s" % (separator, s))
    parts.append(buf)
    return parts


def _parse_struct_fields_string(s):
    parts = _ignore_brackets_split(s, ",")
    fields = []
    for part in parts:
        name_and_type = _ignore_brackets_split(part, ":")
        if len(name_and_type) != 2:
            raise ValueError("The strcut field string format is: 'field_name:field_type', " +
                             "but got: %s" % part)
        field_name = name_and_type[0].strip()
        field_type = _parse_datatype_string(name_and_type[1])
        fields.append(StructField(field_name, field_type))
    return StructType(fields)


def _parse_datatype_string(s):
    """
    Parses the given data type string to a :class:`DataType`. The data type string format equals
    to :class:`DataType.simpleString`, except that top level struct type can omit
    the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use ``byte`` instead
    of ``tinyint`` for :class:`ByteType`. We can also use ``int`` as a short name
    for :class:`IntegerType`.

    >>> _parse_datatype_string("int ")
    IntegerType
    >>> _parse_datatype_string("a: byte, b: decimal(  16 , 8   ) ")
    StructType(List(StructField(a,ByteType,true),StructField(b,DecimalType(16,8),true)))
    >>> _parse_datatype_string("a: array< short>")
    StructType(List(StructField(a,ArrayType(ShortType,true),true)))
    >>> _parse_datatype_string(" map<string , string > ")
    MapType(StringType,StringType,true)

    >>> # Error cases
    >>> _parse_datatype_string("blabla") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _parse_datatype_string("a: int,") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _parse_datatype_string("array<int") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _parse_datatype_string("map<int, boolean>>") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    """
    s = s.strip()
    if s.startswith("array<"):
        if s[-1] != ">":
            raise ValueError("'>' should be the last char, but got: %s" % s)
        return ArrayType(_parse_datatype_string(s[6:-1]))
    elif s.startswith("map<"):
        if s[-1] != ">":
            raise ValueError("'>' should be the last char, but got: %s" % s)
        parts = _ignore_brackets_split(s[4:-1], ",")
        if len(parts) != 2:
            raise ValueError("The map type string format is: 'map<key_type,value_type>', " +
                             "but got: %s" % s)
        kt = _parse_datatype_string(parts[0])
        vt = _parse_datatype_string(parts[1])
        return MapType(kt, vt)
    elif s.startswith("struct<"):
        if s[-1] != ">":
            raise ValueError("'>' should be the last char, but got: %s" % s)
        return _parse_struct_fields_string(s[7:-1])
    elif ":" in s:
        return _parse_struct_fields_string(s)
    else:
        return _parse_basic_datatype_string(s)


def _parse_datatype_json_string(json_string):
    """Parses the given data type JSON string.
    >>> import pickle
    >>> def check_datatype(datatype):
    ...     pickled = pickle.loads(pickle.dumps(datatype))
    ...     assert datatype == pickled
    ...     scala_datatype = spark._jsparkSession.parseDataType(datatype.json())
    ...     python_datatype = _parse_datatype_json_string(scala_datatype.json())
    ...     assert datatype == python_datatype
    >>> for cls in _all_atomic_types.values():
    ...     check_datatype(cls())

    >>> # Simple ArrayType.
    >>> simple_arraytype = ArrayType(StringType(), True)
    >>> check_datatype(simple_arraytype)

    >>> # Simple MapType.
    >>> simple_maptype = MapType(StringType(), LongType())
    >>> check_datatype(simple_maptype)

    >>> # Simple StructType.
    >>> simple_structtype = StructType([
    ...     StructField("a", DecimalType(), False),
    ...     StructField("b", BooleanType(), True),
    ...     StructField("c", LongType(), True),
    ...     StructField("d", BinaryType(), False)])
    >>> check_datatype(simple_structtype)

    >>> # Complex StructType.
    >>> complex_structtype = StructType([
    ...     StructField("simpleArray", simple_arraytype, True),
    ...     StructField("simpleMap", simple_maptype, True),
    ...     StructField("simpleStruct", simple_structtype, True),
    ...     StructField("boolean", BooleanType(), False),
    ...     StructField("withMeta", DoubleType(), False, {"name": "age"})])
    >>> check_datatype(complex_structtype)

    >>> # Complex ArrayType.
    >>> complex_arraytype = ArrayType(complex_structtype, True)
    >>> check_datatype(complex_arraytype)

    >>> # Complex MapType.
    >>> complex_maptype = MapType(complex_structtype,
    ...                           complex_arraytype, False)
    >>> check_datatype(complex_maptype)
    """
    return _parse_datatype_json_value(json.loads(json_string))


def _parse_datatype_json_value(json_value):
    if not isinstance(json_value, dict):
        if json_value in _all_atomic_types.keys():
            return _all_atomic_types[json_value]()
        elif json_value == 'decimal':
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
    int: LongType,
    float: DoubleType,
    str: StringType,
    bytearray: BinaryType,
    decimal.Decimal: DecimalType,
    datetime.date: DateType,
    datetime.datetime: TimestampType,
    datetime.time: TimestampType,
}

if sys.version < "3":
    _type_mappings.update({
        unicode: StringType,
        long: LongType,
    })


def _infer_type(obj):
    """Infer the DataType from obj
    """
    if obj is None:
        return NullType()

    if hasattr(obj, '__UDT__'):
        return obj.__UDT__

    dataType = _type_mappings.get(type(obj))
    if dataType is DecimalType:
        # the precision and scale of `obj` may be different from row to row.
        return DecimalType(38, 18)
    elif dataType is not None:
        return dataType()

    if isinstance(obj, dict):
        for key, value in obj.items():
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
        except TypeError:
            raise TypeError("not supported type: %s" % type(obj))


def _infer_schema(row):
    """Infer the schema from dict/namedtuple/object"""
    if isinstance(row, dict):
        items = sorted(row.items())

    elif isinstance(row, (tuple, list)):
        if hasattr(row, "__fields__"):  # Row
            items = zip(row.__fields__, tuple(row))
        elif hasattr(row, "_fields"):  # namedtuple
            items = zip(row._fields, tuple(row))
        else:
            names = ['_%d' % i for i in range(1, len(row) + 1)]
            items = zip(names, row)

    elif hasattr(row, "__dict__"):  # object
        items = sorted(row.__dict__.items())

    else:
        raise TypeError("Can not infer schema for type: %s" % type(row))

    fields = [StructField(k, _infer_type(v), True) for k, v in items]
    return StructType(fields)


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
        raise TypeError("Can not merge type %s and %s" % (type(a), type(b)))

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


def _need_converter(dataType):
    if isinstance(dataType, StructType):
        return True
    elif isinstance(dataType, ArrayType):
        return _need_converter(dataType.elementType)
    elif isinstance(dataType, MapType):
        return _need_converter(dataType.keyType) or _need_converter(dataType.valueType)
    elif isinstance(dataType, NullType):
        return True
    else:
        return False


def _create_converter(dataType):
    """Create a converter to drop the names of fields in obj """
    if not _need_converter(dataType):
        return lambda x: x

    if isinstance(dataType, ArrayType):
        conv = _create_converter(dataType.elementType)
        return lambda row: [conv(v) for v in row]

    elif isinstance(dataType, MapType):
        kconv = _create_converter(dataType.keyType)
        vconv = _create_converter(dataType.valueType)
        return lambda row: dict((kconv(k), vconv(v)) for k, v in row.items())

    elif isinstance(dataType, NullType):
        return lambda x: None

    elif not isinstance(dataType, StructType):
        return lambda x: x

    # dataType must be StructType
    names = [f.name for f in dataType.fields]
    converters = [_create_converter(f.dataType) for f in dataType.fields]
    convert_fields = any(_need_converter(f.dataType) for f in dataType.fields)

    def convert_struct(obj):
        if obj is None:
            return

        if isinstance(obj, (tuple, list)):
            if convert_fields:
                return tuple(conv(v) for v, conv in zip(obj, converters))
            else:
                return tuple(obj)

        if isinstance(obj, dict):
            d = obj
        elif hasattr(obj, "__dict__"):  # object
            d = obj.__dict__
        else:
            raise TypeError("Unexpected obj type: %s" % type(obj))

        if convert_fields:
            return tuple([conv(d.get(name)) for name, conv in zip(names, converters)])
        else:
            return tuple([d.get(name) for name in names])

    return convert_struct


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
    StructField(a,NullType,true)
    >>> _parse_field_abstract("b(c d)")
    StructField(b,StructType(...c,NullType,true),StructField(d...
    >>> _parse_field_abstract("a[]")
    StructField(a,ArrayType(NullType,true),true)
    >>> _parse_field_abstract("a{[]}")
    StructField(a,MapType(NullType,ArrayType(NullType,true),true),true)
    """
    if set(_BRACKETS.keys()) & set(s):
        idx = min((s.index(c) for c in _BRACKETS if c in s))
        name = s[:idx]
        return StructField(name, _parse_schema_abstract(s[idx:]), True)
    else:
        return StructField(s, NullType(), True)


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
    StructField(b,StructType(List(StructField(t,NullType,true))),true)
    """
    s = s.strip()
    if not s:
        return NullType()

    elif s.startswith('('):
        return _parse_schema_abstract(s[1:-1])

    elif s.startswith('['):
        return ArrayType(_parse_schema_abstract(s[1:-1]), True)

    elif s.startswith('{'):
        return MapType(NullType(), _parse_schema_abstract(s[1:-1]))

    parts = _split_schema_abstract(s)
    fields = [_parse_field_abstract(p) for p in parts]
    return StructType(fields)


def _infer_schema_type(obj, dataType):
    """
    Fill the dataType with types inferred from obj

    >>> schema = _parse_schema_abstract("a b c d")
    >>> row = (1, 1.0, "str", datetime.date(2014, 10, 10))
    >>> _infer_schema_type(row, schema)
    StructType...LongType...DoubleType...StringType...DateType...
    >>> row = [[1], {"key": (1, 2.0)}]
    >>> schema = _parse_schema_abstract("a[] b{c d}")
    >>> _infer_schema_type(row, schema)
    StructType...a,ArrayType...b,MapType(StringType,...c,LongType...
    """
    if isinstance(dataType, NullType):
        return _infer_type(obj)

    if not obj:
        return NullType()

    if isinstance(dataType, ArrayType):
        eType = _infer_schema_type(obj[0], dataType.elementType)
        return ArrayType(eType, True)

    elif isinstance(dataType, MapType):
        k, v = next(iter(obj.items()))
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
        raise TypeError("Unexpected dataType: %s" % type(dataType))


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
    DateType: (datetime.date, datetime.datetime),
    TimestampType: (datetime.datetime,),
    ArrayType: (list, tuple, array),
    MapType: (dict,),
    StructType: (tuple, list, dict),
}


def _verify_type(obj, dataType, nullable=True):
    """
    Verify the type of obj against dataType, raise a TypeError if they do not match.

    Also verify the value of obj against datatype, raise a ValueError if it's not within the allowed
    range, e.g. using 128 as ByteType will overflow. Note that, Python float is not checked, so it
    will become infinity when cast to Java float if it overflows.

    >>> _verify_type(None, StructType([]))
    >>> _verify_type("", StringType())
    >>> _verify_type(0, LongType())
    >>> _verify_type(list(range(3)), ArrayType(ShortType()))
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
    >>> # Check if numeric values are within the allowed range.
    >>> _verify_type(12, ByteType())
    >>> _verify_type(1234, ByteType()) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _verify_type(None, ByteType(), False) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _verify_type([1, None], ArrayType(ShortType(), False)) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _verify_type({None: 1}, MapType(StringType(), IntegerType()))
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> schema = StructType().add("a", IntegerType()).add("b", StringType(), False)
    >>> _verify_type((1, None), schema) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    """
    if obj is None:
        if nullable:
            return
        else:
            raise ValueError("This field is not nullable, but got None")

    # StringType can work with any types
    if isinstance(dataType, StringType):
        return

    if isinstance(dataType, UserDefinedType):
        if not (hasattr(obj, '__UDT__') and obj.__UDT__ == dataType):
            raise ValueError("%r is not an instance of type %r" % (obj, dataType))
        _verify_type(dataType.toInternal(obj), dataType.sqlType())
        return

    _type = type(dataType)
    assert _type in _acceptable_types, "unknown datatype: %s for object %r" % (dataType, obj)

    if _type is StructType:
        # check the type and fields later
        pass
    else:
        # subclass of them can not be fromInternal in JVM
        if type(obj) not in _acceptable_types[_type]:
            raise TypeError("%s can not accept object %r in type %s" % (dataType, obj, type(obj)))

    if isinstance(dataType, ByteType):
        if obj < -128 or obj > 127:
            raise ValueError("object of ByteType out of range, got: %s" % obj)

    elif isinstance(dataType, ShortType):
        if obj < -32768 or obj > 32767:
            raise ValueError("object of ShortType out of range, got: %s" % obj)

    elif isinstance(dataType, IntegerType):
        if obj < -2147483648 or obj > 2147483647:
            raise ValueError("object of IntegerType out of range, got: %s" % obj)

    elif isinstance(dataType, ArrayType):
        for i in obj:
            _verify_type(i, dataType.elementType, dataType.containsNull)

    elif isinstance(dataType, MapType):
        for k, v in obj.items():
            _verify_type(k, dataType.keyType, False)
            _verify_type(v, dataType.valueType, dataType.valueContainsNull)

    elif isinstance(dataType, StructType):
        if isinstance(obj, dict):
            for f in dataType.fields:
                _verify_type(obj.get(f.name), f.dataType, f.nullable)
        elif isinstance(obj, Row) and getattr(obj, "__from_dict__", False):
            # the order in obj could be different than dataType.fields
            for f in dataType.fields:
                _verify_type(obj[f.name], f.dataType, f.nullable)
        elif isinstance(obj, (tuple, list)):
            if len(obj) != len(dataType.fields):
                raise ValueError("Length of object (%d) does not match with "
                                 "length of fields (%d)" % (len(obj), len(dataType.fields)))
            for v, f in zip(obj, dataType.fields):
                _verify_type(v, f.dataType, f.nullable)
        elif hasattr(obj, "__dict__"):
            d = obj.__dict__
            for f in dataType.fields:
                _verify_type(d.get(f.name), f.dataType, f.nullable)
        else:
            raise TypeError("StructType can not accept object %r in type %s" % (obj, type(obj)))


# This is used to unpickle a Row from JVM
def _create_row_inbound_converter(dataType):
    return lambda *a: dataType.fromInternal(a)


def _create_row(fields, values):
    row = Row(*values)
    row.__fields__ = fields
    return row


class Row(tuple):

    """
    A row in L{DataFrame}.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments,
    the fields will be sorted by names.

    >>> row = Row(name="Alice", age=11)
    >>> row
    Row(age=11, name='Alice')
    >>> row['name'], row['age']
    ('Alice', 11)
    >>> row.name, row.age
    ('Alice', 11)
    >>> 'name' in row
    True
    >>> 'wrong_key' in row
    False

    Row also can be used to create another Row like class, then it
    could be used to create Row objects, such as

    >>> Person = Row("name", "age")
    >>> Person
    <Row(name, age)>
    >>> 'name' in Person
    True
    >>> 'wrong_key' in Person
    False
    >>> Person("Alice", 11)
    Row(name='Alice', age=11)
    """

    def __new__(self, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if kwargs:
            # create row objects
            names = sorted(kwargs.keys())
            row = tuple.__new__(self, [kwargs[n] for n in names])
            row.__fields__ = names
            row.__from_dict__ = True
            return row

        else:
            # create row class or objects
            return tuple.__new__(self, args)

    def asDict(self, recursive=False):
        """
        Return as an dict

        :param recursive: turns the nested Row as dict (default: False).

        >>> Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}
        True
        >>> row = Row(key=1, value=Row(name='a', age=2))
        >>> row.asDict() == {'key': 1, 'value': Row(age=2, name='a')}
        True
        >>> row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
        True
        """
        if not hasattr(self, "__fields__"):
            raise TypeError("Cannot convert a Row class into dict")

        if recursive:
            def conv(obj):
                if isinstance(obj, Row):
                    return obj.asDict(True)
                elif isinstance(obj, list):
                    return [conv(o) for o in obj]
                elif isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                else:
                    return obj
            return dict(zip(self.__fields__, (conv(o) for o in self)))
        else:
            return dict(zip(self.__fields__, self))

    def __contains__(self, item):
        if hasattr(self, "__fields__"):
            return item in self.__fields__
        else:
            return super(Row, self).__contains__(item)

    # let object acts like class
    def __call__(self, *args):
        """create new Row object"""
        return _create_row(self, args)

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return super(Row, self).__getitem__(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return super(Row, self).__getitem__(idx)
        except IndexError:
            raise KeyError(item)
        except ValueError:
            raise ValueError(item)

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return self[idx]
        except IndexError:
            raise AttributeError(item)
        except ValueError:
            raise AttributeError(item)

    def __setattr__(self, key, value):
        if key != '__fields__' and key != "__from_dict__":
            raise Exception("Row is read-only")
        self.__dict__[key] = value

    def __reduce__(self):
        """Returns a tuple so Python knows how to pickle Row."""
        if hasattr(self, "__fields__"):
            return (_create_row, (self.__fields__, tuple(self)))
        else:
            return tuple.__reduce__(self)

    def __repr__(self):
        """Printable representation of Row used in Python REPL."""
        if hasattr(self, "__fields__"):
            return "Row(%s)" % ", ".join("%s=%r" % (k, v)
                                         for k, v in zip(self.__fields__, tuple(self)))
        else:
            return "<Row(%s)>" % ", ".join(self)


class DateConverter(object):
    def can_convert(self, obj):
        return isinstance(obj, datetime.date)

    def convert(self, obj, gateway_client):
        Date = JavaClass("java.sql.Date", gateway_client)
        return Date.valueOf(obj.strftime("%Y-%m-%d"))


class DatetimeConverter(object):
    def can_convert(self, obj):
        return isinstance(obj, datetime.datetime)

    def convert(self, obj, gateway_client):
        Timestamp = JavaClass("java.sql.Timestamp", gateway_client)
        seconds = (calendar.timegm(obj.utctimetuple()) if obj.tzinfo
                   else time.mktime(obj.timetuple()))
        t = Timestamp(int(seconds) * 1000)
        t.setNanos(obj.microsecond * 1000)
        return t

# datetime is a subclass of date, we should register DatetimeConverter first
register_input_converter(DatetimeConverter())
register_input_converter(DateConverter())


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession
    globs = globals()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['spark'] = SparkSession.builder.getOrCreate()
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
