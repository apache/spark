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
import ctypes

if sys.version >= "3":
    long = int
    basestring = unicode = str

from py4j.protocol import register_input_converter
from py4j.java_gateway import JavaClass

from pyspark import SparkContext
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

    The precision can be up to 38, the scale must be less or equal to precision.

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
        assert isinstance(elementType, DataType),\
            "elementType %s should be an instance of %s" % (elementType, DataType)
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
        assert isinstance(keyType, DataType),\
            "keyType %s should be an instance of %s" % (keyType, DataType)
        assert isinstance(valueType, DataType),\
            "valueType %s should be an instance of %s" % (valueType, DataType)
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
        assert isinstance(dataType, DataType),\
            "dataType %s should be an instance of %s" % (dataType, DataType)
        assert isinstance(name, basestring), "field name %s should be string" % (name)
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

    def typeName(self):
        raise TypeError(
            "StructField does not have typeName. "
            "Use typeName on its type explicitly instead.")


class StructType(DataType):
    """Struct type, consisting of a list of :class:`StructField`.

    This is the data type representing a :class:`Row`.

    Iterating a :class:`StructType` will iterate its :class:`StructField`\\s.
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
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)

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
        >>> struct1 == struct2
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
            if isinstance(field, str) and data_type is None:
                raise ValueError("Must specify DataType if passing name of struct_field to create.")

            if isinstance(data_type, str):
                data_type_f = _parse_datatype_json_value(data_type)
            else:
                data_type_f = data_type
            self.fields.append(StructField(field, data_type_f, nullable, metadata))
            self.names.append(field)
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)
        return self

    def __iter__(self):
        """Iterate the fields"""
        return iter(self.fields)

    def __len__(self):
        """Return the number of fields."""
        return len(self.fields)

    def __getitem__(self, key):
        """Access fields by name or slice."""
        if isinstance(key, str):
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

    def fieldNames(self):
        """
        Returns all field names in a list.

        >>> struct = StructType([StructField("f1", StringType(), True)])
        >>> struct.fieldNames()
        ['f1']
        """
        return list(self.names)

    def needConversion(self):
        # We need convert Row()/namedtuple into tuple()
        return True

    def toInternal(self, obj):
        if obj is None:
            return

        if self._needSerializeAnyField:
            # Only calling toInternal function for fields that need conversion
            if isinstance(obj, dict):
                return tuple(f.toInternal(obj.get(n)) if c else obj.get(n)
                             for n, f, c in zip(self.names, self.fields, self._needConversion))
            elif isinstance(obj, (tuple, list)):
                return tuple(f.toInternal(v) if c else v
                             for f, v, c in zip(self.fields, obj, self._needConversion))
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(f.toInternal(d.get(n)) if c else d.get(n)
                             for n, f, c in zip(self.names, self.fields, self._needConversion))
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
            # Only calling fromInternal function for fields that need conversion
            values = [f.fromInternal(v) if c else v
                      for f, v, c in zip(self.fields, obj, self._needConversion)]
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


_FIXED_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)")


def _parse_datatype_string(s):
    """
    Parses the given data type string to a :class:`DataType`. The data type string format equals
    to :class:`DataType.simpleString`, except that top level struct type can omit
    the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use ``byte`` instead
    of ``tinyint`` for :class:`ByteType`. We can also use ``int`` as a short name
    for :class:`IntegerType`. Since Spark 2.3, this also supports a schema in a DDL-formatted
    string and case-insensitive strings.

    >>> _parse_datatype_string("int ")
    IntegerType
    >>> _parse_datatype_string("INT ")
    IntegerType
    >>> _parse_datatype_string("a: byte, b: decimal(  16 , 8   ) ")
    StructType(List(StructField(a,ByteType,true),StructField(b,DecimalType(16,8),true)))
    >>> _parse_datatype_string("a DOUBLE, b STRING")
    StructType(List(StructField(a,DoubleType,true),StructField(b,StringType,true)))
    >>> _parse_datatype_string("a: array< short>")
    StructType(List(StructField(a,ArrayType(ShortType,true),true)))
    >>> _parse_datatype_string(" map<string , string > ")
    MapType(StringType,StringType,true)

    >>> # Error cases
    >>> _parse_datatype_string("blabla") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("a: int,") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("array<int") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("map<int, boolean>>") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    """
    sc = SparkContext._active_spark_context

    def from_ddl_schema(type_str):
        return _parse_datatype_json_string(
            sc._jvm.org.apache.spark.sql.types.StructType.fromDDL(type_str).json())

    def from_ddl_datatype(type_str):
        return _parse_datatype_json_string(
            sc._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.parseDataType(type_str).json())

    try:
        # DDL format, "fieldname datatype, fieldname datatype".
        return from_ddl_schema(s)
    except Exception as e:
        try:
            # For backwards compatibility, "integer", "struct<fieldname: datatype>" and etc.
            return from_ddl_datatype(s)
        except:
            try:
                # For backwards compatibility, "fieldname: datatype, fieldname: datatype" case.
                return from_ddl_datatype("struct<%s>" % s.strip())
            except:
                raise e


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

# Mapping Python array types to Spark SQL DataType
# We should be careful here. The size of these types in python depends on C
# implementation. We need to make sure that this conversion does not lose any
# precision. Also, JVM only support signed types, when converting unsigned types,
# keep in mind that it required 1 more bit when stored as singed types.
#
# Reference for C integer size, see:
# ISO/IEC 9899:201x specification, chapter 5.2.4.2.1 Sizes of integer types <limits.h>.
# Reference for python array typecode, see:
# https://docs.python.org/2/library/array.html
# https://docs.python.org/3.6/library/array.html
# Reference for JVM's supported integral types:
# http://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.3.1

_array_signed_int_typecode_ctype_mappings = {
    'b': ctypes.c_byte,
    'h': ctypes.c_short,
    'i': ctypes.c_int,
    'l': ctypes.c_long,
}

_array_unsigned_int_typecode_ctype_mappings = {
    'B': ctypes.c_ubyte,
    'H': ctypes.c_ushort,
    'I': ctypes.c_uint,
    'L': ctypes.c_ulong
}


def _int_size_to_type(size):
    """
    Return the Catalyst datatype from the size of integers.
    """
    if size <= 8:
        return ByteType
    if size <= 16:
        return ShortType
    if size <= 32:
        return IntegerType
    if size <= 64:
        return LongType

# The list of all supported array typecodes is stored here
_array_type_mappings = {
    # Warning: Actual properties for float and double in C is not specified in C.
    # On almost every system supported by both python and JVM, they are IEEE 754
    # single-precision binary floating-point format and IEEE 754 double-precision
    # binary floating-point format. And we do assume the same thing here for now.
    'f': FloatType,
    'd': DoubleType
}

# compute array typecode mappings for signed integer types
for _typecode in _array_signed_int_typecode_ctype_mappings.keys():
    size = ctypes.sizeof(_array_signed_int_typecode_ctype_mappings[_typecode]) * 8
    dt = _int_size_to_type(size)
    if dt is not None:
        _array_type_mappings[_typecode] = dt

# compute array typecode mappings for unsigned integer types
for _typecode in _array_unsigned_int_typecode_ctype_mappings.keys():
    # JVM does not have unsigned types, so use signed types that is at least 1
    # bit larger to store
    size = ctypes.sizeof(_array_unsigned_int_typecode_ctype_mappings[_typecode]) * 8 + 1
    dt = _int_size_to_type(size)
    if dt is not None:
        _array_type_mappings[_typecode] = dt

# Type code 'u' in Python's array is deprecated since version 3.3, and will be
# removed in version 4.0. See: https://docs.python.org/3/library/array.html
if sys.version_info[0] < 4:
    _array_type_mappings['u'] = StringType

# Type code 'c' are only available at python 2
if sys.version_info[0] < 3:
    _array_type_mappings['c'] = StringType

# SPARK-21465:
# In python2, array of 'L' happened to be mistakenly partially supported. To
# avoid breaking user's code, we should keep this partial support. Below is a
# dirty hacking to keep this partial support and make the unit test passes
import platform
if sys.version_info[0] < 3 and platform.python_implementation() != 'PyPy':
    if 'L' not in _array_type_mappings.keys():
        _array_type_mappings['L'] = LongType
        _array_unsigned_int_typecode_ctype_mappings['L'] = ctypes.c_uint


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
    elif isinstance(obj, list):
        for v in obj:
            if v is not None:
                return ArrayType(_infer_type(obj[0]), True)
        else:
            return ArrayType(NullType(), True)
    elif isinstance(obj, array):
        if obj.typecode in _array_type_mappings:
            return ArrayType(_array_type_mappings[obj.typecode](), False)
        else:
            raise TypeError("not supported type: array(%s)" % obj.typecode)
    else:
        try:
            return _infer_schema(obj)
        except TypeError:
            raise TypeError("not supported type: %s" % type(obj))


def _infer_schema(row, names=None):
    """Infer the schema from dict/namedtuple/object"""
    if isinstance(row, dict):
        items = sorted(row.items())

    elif isinstance(row, (tuple, list)):
        if hasattr(row, "__fields__"):  # Row
            items = zip(row.__fields__, tuple(row))
        elif hasattr(row, "_fields"):  # namedtuple
            items = zip(row._fields, tuple(row))
        else:
            if names is None:
                names = ['_%d' % i for i in range(1, len(row) + 1)]
            elif len(names) < len(row):
                names.extend('_%d' % i for i in range(len(names) + 1, len(row) + 1))
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


def _merge_type(a, b, name=None):
    if name is None:
        new_msg = lambda msg: msg
        new_name = lambda n: "field %s" % n
    else:
        new_msg = lambda msg: "%s: %s" % (name, msg)
        new_name = lambda n: "field %s in %s" % (n, name)

    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif type(a) is not type(b):
        # TODO: type cast (such as int -> long)
        raise TypeError(new_msg("Can not merge type %s and %s" % (type(a), type(b))))

    # same type
    if isinstance(a, StructType):
        nfs = dict((f.name, f.dataType) for f in b.fields)
        fields = [StructField(f.name, _merge_type(f.dataType, nfs.get(f.name, NullType()),
                                                  name=new_name(f.name)))
                  for f in a.fields]
        names = set([f.name for f in fields])
        for n in nfs:
            if n not in names:
                fields.append(StructField(n, nfs[n]))
        return StructType(fields)

    elif isinstance(a, ArrayType):
        return ArrayType(_merge_type(a.elementType, b.elementType,
                                     name='element in array %s' % name), True)

    elif isinstance(a, MapType):
        return MapType(_merge_type(a.keyType, b.keyType, name='key of map %s' % name),
                       _merge_type(a.valueType, b.valueType, name='value of map %s' % name),
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


def _make_type_verifier(dataType, nullable=True, name=None):
    """
    Make a verifier that checks the type of obj against dataType and raises a TypeError if they do
    not match.

    This verifier also checks the value of obj against datatype and raises a ValueError if it's not
    within the allowed range, e.g. using 128 as ByteType will overflow. Note that, Python float is
    not checked, so it will become infinity when cast to Java float if it overflows.

    >>> _make_type_verifier(StructType([]))(None)
    >>> _make_type_verifier(StringType())("")
    >>> _make_type_verifier(LongType())(0)
    >>> _make_type_verifier(ArrayType(ShortType()))(list(range(3)))
    >>> _make_type_verifier(ArrayType(StringType()))(set()) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError:...
    >>> _make_type_verifier(MapType(StringType(), IntegerType()))({})
    >>> _make_type_verifier(StructType([]))(())
    >>> _make_type_verifier(StructType([]))([])
    >>> _make_type_verifier(StructType([]))([1]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> # Check if numeric values are within the allowed range.
    >>> _make_type_verifier(ByteType())(12)
    >>> _make_type_verifier(ByteType())(1234) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _make_type_verifier(ByteType(), False)(None) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _make_type_verifier(
    ...     ArrayType(ShortType(), False))([1, None]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> _make_type_verifier(MapType(StringType(), IntegerType()))({None: 1})
    Traceback (most recent call last):
        ...
    ValueError:...
    >>> schema = StructType().add("a", IntegerType()).add("b", StringType(), False)
    >>> _make_type_verifier(schema)((1, None)) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
    """

    if name is None:
        new_msg = lambda msg: msg
        new_name = lambda n: "field %s" % n
    else:
        new_msg = lambda msg: "%s: %s" % (name, msg)
        new_name = lambda n: "field %s in %s" % (n, name)

    def verify_nullability(obj):
        if obj is None:
            if nullable:
                return True
            else:
                raise ValueError(new_msg("This field is not nullable, but got None"))
        else:
            return False

    _type = type(dataType)

    def assert_acceptable_types(obj):
        assert _type in _acceptable_types, \
            new_msg("unknown datatype: %s for object %r" % (dataType, obj))

    def verify_acceptable_types(obj):
        # subclass of them can not be fromInternal in JVM
        if type(obj) not in _acceptable_types[_type]:
            raise TypeError(new_msg("%s can not accept object %r in type %s"
                                    % (dataType, obj, type(obj))))

    if isinstance(dataType, StringType):
        # StringType can work with any types
        verify_value = lambda _: _

    elif isinstance(dataType, UserDefinedType):
        verifier = _make_type_verifier(dataType.sqlType(), name=name)

        def verify_udf(obj):
            if not (hasattr(obj, '__UDT__') and obj.__UDT__ == dataType):
                raise ValueError(new_msg("%r is not an instance of type %r" % (obj, dataType)))
            verifier(dataType.toInternal(obj))

        verify_value = verify_udf

    elif isinstance(dataType, ByteType):
        def verify_byte(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -128 or obj > 127:
                raise ValueError(new_msg("object of ByteType out of range, got: %s" % obj))

        verify_value = verify_byte

    elif isinstance(dataType, ShortType):
        def verify_short(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -32768 or obj > 32767:
                raise ValueError(new_msg("object of ShortType out of range, got: %s" % obj))

        verify_value = verify_short

    elif isinstance(dataType, IntegerType):
        def verify_integer(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -2147483648 or obj > 2147483647:
                raise ValueError(
                    new_msg("object of IntegerType out of range, got: %s" % obj))

        verify_value = verify_integer

    elif isinstance(dataType, ArrayType):
        element_verifier = _make_type_verifier(
            dataType.elementType, dataType.containsNull, name="element in array %s" % name)

        def verify_array(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            for i in obj:
                element_verifier(i)

        verify_value = verify_array

    elif isinstance(dataType, MapType):
        key_verifier = _make_type_verifier(dataType.keyType, False, name="key of map %s" % name)
        value_verifier = _make_type_verifier(
            dataType.valueType, dataType.valueContainsNull, name="value of map %s" % name)

        def verify_map(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            for k, v in obj.items():
                key_verifier(k)
                value_verifier(v)

        verify_value = verify_map

    elif isinstance(dataType, StructType):
        verifiers = []
        for f in dataType.fields:
            verifier = _make_type_verifier(f.dataType, f.nullable, name=new_name(f.name))
            verifiers.append((f.name, verifier))

        def verify_struct(obj):
            assert_acceptable_types(obj)

            if isinstance(obj, dict):
                for f, verifier in verifiers:
                    verifier(obj.get(f))
            elif isinstance(obj, Row) and getattr(obj, "__from_dict__", False):
                # the order in obj could be different than dataType.fields
                for f, verifier in verifiers:
                    verifier(obj[f])
            elif isinstance(obj, (tuple, list)):
                if len(obj) != len(verifiers):
                    raise ValueError(
                        new_msg("Length of object (%d) does not match with "
                                "length of fields (%d)" % (len(obj), len(verifiers))))
                for v, (_, verifier) in zip(obj, verifiers):
                    verifier(v)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                for f, verifier in verifiers:
                    verifier(d.get(f))
            else:
                raise TypeError(new_msg("StructType can not accept object %r in type %s"
                                        % (obj, type(obj))))
        verify_value = verify_struct

    else:
        def verify_default(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)

        verify_value = verify_default

    def verify(obj):
        if not verify_nullability(obj):
            verify_value(obj)

    return verify


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
    the fields will be sorted by names. It is not allowed to omit
    a named argument to represent the value is None or missing. This should be
    explicitly set to None in this case.

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
        if len(args) > len(self):
            raise ValueError("Can not create Row with fields %s, expected %d values "
                             "but got %s" % (self, len(self), args))
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


def to_arrow_type(dt):
    """ Convert Spark data type to pyarrow type
    """
    from distutils.version import LooseVersion
    import pyarrow as pa
    if type(dt) == BooleanType:
        arrow_type = pa.bool_()
    elif type(dt) == ByteType:
        arrow_type = pa.int8()
    elif type(dt) == ShortType:
        arrow_type = pa.int16()
    elif type(dt) == IntegerType:
        arrow_type = pa.int32()
    elif type(dt) == LongType:
        arrow_type = pa.int64()
    elif type(dt) == FloatType:
        arrow_type = pa.float32()
    elif type(dt) == DoubleType:
        arrow_type = pa.float64()
    elif type(dt) == DecimalType:
        arrow_type = pa.decimal128(dt.precision, dt.scale)
    elif type(dt) == StringType:
        arrow_type = pa.string()
    elif type(dt) == BinaryType:
        # TODO: remove version check once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            raise TypeError("Unsupported type in conversion to Arrow: " + str(dt) +
                            "\nPlease install pyarrow >= 0.10.0 for BinaryType support.")
        arrow_type = pa.binary()
    elif type(dt) == DateType:
        arrow_type = pa.date32()
    elif type(dt) == TimestampType:
        # Timestamps should be in UTC, JVM Arrow timestamps require a timezone to be read
        arrow_type = pa.timestamp('us', tz='UTC')
    elif type(dt) == ArrayType:
        if type(dt.elementType) == TimestampType:
            raise TypeError("Unsupported type in conversion to Arrow: " + str(dt))
        arrow_type = pa.list_(to_arrow_type(dt.elementType))
    else:
        raise TypeError("Unsupported type in conversion to Arrow: " + str(dt))
    return arrow_type


def to_arrow_schema(schema):
    """ Convert a schema from Spark to Arrow
    """
    import pyarrow as pa
    fields = [pa.field(field.name, to_arrow_type(field.dataType), nullable=field.nullable)
              for field in schema]
    return pa.schema(fields)


def from_arrow_type(at):
    """ Convert pyarrow type to Spark data type.
    """
    from distutils.version import LooseVersion
    import pyarrow as pa
    import pyarrow.types as types
    if types.is_boolean(at):
        spark_type = BooleanType()
    elif types.is_int8(at):
        spark_type = ByteType()
    elif types.is_int16(at):
        spark_type = ShortType()
    elif types.is_int32(at):
        spark_type = IntegerType()
    elif types.is_int64(at):
        spark_type = LongType()
    elif types.is_float32(at):
        spark_type = FloatType()
    elif types.is_float64(at):
        spark_type = DoubleType()
    elif types.is_decimal(at):
        spark_type = DecimalType(precision=at.precision, scale=at.scale)
    elif types.is_string(at):
        spark_type = StringType()
    elif types.is_binary(at):
        # TODO: remove version check once minimum pyarrow version is 0.10.0
        if LooseVersion(pa.__version__) < LooseVersion("0.10.0"):
            raise TypeError("Unsupported type in conversion from Arrow: " + str(at) +
                            "\nPlease install pyarrow >= 0.10.0 for BinaryType support.")
        spark_type = BinaryType()
    elif types.is_date32(at):
        spark_type = DateType()
    elif types.is_timestamp(at):
        spark_type = TimestampType()
    elif types.is_list(at):
        if types.is_timestamp(at.value_type):
            raise TypeError("Unsupported type in conversion from Arrow: " + str(at))
        spark_type = ArrayType(from_arrow_type(at.value_type))
    else:
        raise TypeError("Unsupported type in conversion from Arrow: " + str(at))
    return spark_type


def from_arrow_schema(arrow_schema):
    """ Convert schema from Arrow to Spark.
    """
    return StructType(
        [StructField(field.name, from_arrow_type(field.type), nullable=field.nullable)
         for field in arrow_schema])


def _check_series_convert_date(series, data_type):
    """
    Cast the series to datetime.date if it's a date type, otherwise returns the original series.

    :param series: pandas.Series
    :param data_type: a Spark data type for the series
    """
    if type(data_type) == DateType:
        return series.dt.date
    else:
        return series


def _check_dataframe_convert_date(pdf, schema):
    """ Correct date type value to use datetime.date.

    Pandas DataFrame created from PyArrow uses datetime64[ns] for date type values, but we should
    use datetime.date to match the behavior with when Arrow optimization is disabled.

    :param pdf: pandas.DataFrame
    :param schema: a Spark schema of the pandas.DataFrame
    """
    for field in schema:
        pdf[field.name] = _check_series_convert_date(pdf[field.name], field.dataType)
    return pdf


def _get_local_timezone():
    """ Get local timezone using pytz with environment variable, or dateutil.

    If there is a 'TZ' environment variable, pass it to pandas to use pytz and use it as timezone
    string, otherwise use the special word 'dateutil/:' which means that pandas uses dateutil and
    it reads system configuration to know the system local timezone.

    See also:
    - https://github.com/pandas-dev/pandas/blob/0.19.x/pandas/tslib.pyx#L1753
    - https://github.com/dateutil/dateutil/blob/2.6.1/dateutil/tz/tz.py#L1338
    """
    import os
    return os.environ.get('TZ', 'dateutil/:')


def _check_series_localize_timestamps(s, timezone):
    """
    Convert timezone aware timestamps to timezone-naive in the specified timezone or local timezone.

    If the input series is not a timestamp series, then the same series is returned. If the input
    series is a timestamp series, then a converted series is returned.

    :param s: pandas.Series
    :param timezone: the timezone to convert. if None then use local timezone
    :return pandas.Series that have been converted to tz-naive
    """
    from pyspark.sql.utils import require_minimum_pandas_version
    require_minimum_pandas_version()

    from pandas.api.types import is_datetime64tz_dtype
    tz = timezone or _get_local_timezone()
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert(tz).dt.tz_localize(None)
    else:
        return s


def _check_dataframe_localize_timestamps(pdf, timezone):
    """
    Convert timezone aware timestamps to timezone-naive in the specified timezone or local timezone

    :param pdf: pandas.DataFrame
    :param timezone: the timezone to convert. if None then use local timezone
    :return pandas.DataFrame where any timezone aware columns have been converted to tz-naive
    """
    from pyspark.sql.utils import require_minimum_pandas_version
    require_minimum_pandas_version()

    for column, series in pdf.iteritems():
        pdf[column] = _check_series_localize_timestamps(series, timezone)
    return pdf


def _check_series_convert_timestamps_internal(s, timezone):
    """
    Convert a tz-naive timestamp in the specified timezone or local timezone to UTC normalized for
    Spark internal storage

    :param s: a pandas.Series
    :param timezone: the timezone to convert. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been UTC normalized without a time zone
    """
    from pyspark.sql.utils import require_minimum_pandas_version
    require_minimum_pandas_version()

    from pandas.api.types import is_datetime64_dtype, is_datetime64tz_dtype
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64_dtype(s.dtype):
        # When tz_localize a tz-naive timestamp, the result is ambiguous if the tz-naive
        # timestamp is during the hour when the clock is adjusted backward during due to
        # daylight saving time (dst).
        # E.g., for America/New_York, the clock is adjusted backward on 2015-11-01 2:00 to
        # 2015-11-01 1:00 from dst-time to standard time, and therefore, when tz_localize
        # a tz-naive timestamp 2015-11-01 1:30 with America/New_York timezone, it can be either
        # dst time (2015-01-01 1:30-0400) or standard time (2015-11-01 1:30-0500).
        #
        # Here we explicit choose to use standard time. This matches the default behavior of
        # pytz.
        #
        # Here are some code to help understand this behavior:
        # >>> import datetime
        # >>> import pandas as pd
        # >>> import pytz
        # >>>
        # >>> t = datetime.datetime(2015, 11, 1, 1, 30)
        # >>> ts = pd.Series([t])
        # >>> tz = pytz.timezone('America/New_York')
        # >>>
        # >>> ts.dt.tz_localize(tz, ambiguous=True)
        # 0   2015-11-01 01:30:00-04:00
        # dtype: datetime64[ns, America/New_York]
        # >>>
        # >>> ts.dt.tz_localize(tz, ambiguous=False)
        # 0   2015-11-01 01:30:00-05:00
        # dtype: datetime64[ns, America/New_York]
        # >>>
        # >>> str(tz.localize(t))
        # '2015-11-01 01:30:00-05:00'
        tz = timezone or _get_local_timezone()
        return s.dt.tz_localize(tz, ambiguous=False).dt.tz_convert('UTC')
    elif is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert('UTC')
    else:
        return s


def _check_series_convert_timestamps_localize(s, from_timezone, to_timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param from_timezone: the timezone to convert from. if None then use local timezone
    :param to_timezone: the timezone to convert to. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    from pyspark.sql.utils import require_minimum_pandas_version
    require_minimum_pandas_version()

    import pandas as pd
    from pandas.api.types import is_datetime64tz_dtype, is_datetime64_dtype
    from_tz = from_timezone or _get_local_timezone()
    to_tz = to_timezone or _get_local_timezone()
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert(to_tz).dt.tz_localize(None)
    elif is_datetime64_dtype(s.dtype) and from_tz != to_tz:
        # `s.dt.tz_localize('tzlocal()')` doesn't work properly when including NaT.
        return s.apply(
            lambda ts: ts.tz_localize(from_tz, ambiguous=False).tz_convert(to_tz).tz_localize(None)
            if ts is not pd.NaT else pd.NaT)
    else:
        return s


def _check_series_convert_timestamps_local_tz(s, timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param timezone: the timezone to convert to. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, None, timezone)


def _check_series_convert_timestamps_tz_local(s, timezone):
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    :param s: a pandas.Series
    :param timezone: the timezone to convert from. if None then use local timezone
    :return pandas.Series where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, timezone, None)


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
        sys.exit(-1)


if __name__ == "__main__":
    _test()
