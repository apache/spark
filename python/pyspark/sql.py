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
import itertools
import warnings
import decimal
import datetime
import keyword
import warnings
from array import array
from operator import itemgetter

from pyspark.rdd import RDD, PipelinedRDD
from pyspark.serializers import BatchedSerializer, PickleSerializer, CloudPickleSerializer
from pyspark.storagelevel import StorageLevel

from itertools import chain, ifilter, imap

from py4j.protocol import Py4JError
from py4j.java_collections import ListConverter, MapConverter


__all__ = [
    "StringType", "BinaryType", "BooleanType", "TimestampType", "DecimalType",
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


class TimestampType(PrimitiveType):

    """Spark SQL TimestampType

    The data type representing datetime.datetime values.
    """


class DecimalType(PrimitiveType):

    """Spark SQL DecimalType

    The data type representing decimal.Decimal values.
    """


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

    def __str__(self):
        return "ArrayType(%s,%s)" % (self.elementType,
                                     str(self.containsNull).lower())


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

    def __init__(self, name, dataType, nullable):
        """Creates a StructField
        :param name: the name of this field.
        :param dataType: the data type of this field.
        :param nullable: indicates whether values of this field
                         can be null.

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

    def __repr__(self):
        return "StructField(%s,%s,%s)" % (self.name, self.dataType,
                                          str(self.nullable).lower())


class StructType(DataType):

    """Spark SQL StructType

    The data type representing rows.
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
        return ("StructType(List(%s))" %
                ",".join(str(field) for field in self.fields))


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


_all_primitive_types = dict((k, v) for k, v in globals().iteritems()
                            if type(v) is PrimitiveTypeSingleton and v.__base__ == PrimitiveType)


def _parse_datatype_string(datatype_string):
    """Parses the given data type string.

    >>> def check_datatype(datatype):
    ...     scala_datatype = sqlCtx._ssql_ctx.parseDataType(str(datatype))
    ...     python_datatype = _parse_datatype_string(
    ...                          scala_datatype.toString())
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
    ...     StructField("boolean", BooleanType(), False)])
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
    """
    index = datatype_string.find("(")
    if index == -1:
        # It is a primitive type.
        index = len(datatype_string)
    type_or_field = datatype_string[:index]
    rest_part = datatype_string[index + 1:len(datatype_string) - 1].strip()

    if type_or_field in _all_primitive_types:
        return _all_primitive_types[type_or_field]()

    elif type_or_field == "ArrayType":
        last_comma_index = rest_part.rfind(",")
        containsNull = True
        if rest_part[last_comma_index + 1:].strip().lower() == "false":
            containsNull = False
        elementType = _parse_datatype_string(
            rest_part[:last_comma_index].strip())
        return ArrayType(elementType, containsNull)

    elif type_or_field == "MapType":
        last_comma_index = rest_part.rfind(",")
        valueContainsNull = True
        if rest_part[last_comma_index + 1:].strip().lower() == "false":
            valueContainsNull = False
        keyType, valueType = _parse_datatype_list(
            rest_part[:last_comma_index].strip())
        return MapType(keyType, valueType, valueContainsNull)

    elif type_or_field == "StructField":
        first_comma_index = rest_part.find(",")
        name = rest_part[:first_comma_index].strip()
        last_comma_index = rest_part.rfind(",")
        nullable = True
        if rest_part[last_comma_index + 1:].strip().lower() == "false":
            nullable = False
        dataType = _parse_datatype_string(
            rest_part[first_comma_index + 1:last_comma_index].strip())
        return StructField(name, dataType, nullable)

    elif type_or_field == "StructType":
        # rest_part should be in the format like
        # List(StructField(field1,IntegerType,false)).
        field_list_string = rest_part[rest_part.find("(") + 1:-1]
        fields = _parse_datatype_list(field_list_string)
        return StructType(fields)


# Mapping Python types to Spark SQL DateType
_type_mappings = {
    bool: BooleanType,
    int: IntegerType,
    long: LongType,
    float: DoubleType,
    str: StringType,
    unicode: StringType,
    decimal.Decimal: DecimalType,
    datetime.datetime: TimestampType,
    datetime.date: TimestampType,
    datetime.time: TimestampType,
}


def _infer_type(obj):
    """Infer the DataType from obj"""
    if obj is None:
        raise ValueError("Can not infer type for None")

    dataType = _type_mappings.get(type(obj))
    if dataType is not None:
        return dataType()

    if isinstance(obj, dict):
        if not obj:
            raise ValueError("Can not infer type for empty dict")
        key, value = obj.iteritems().next()
        return MapType(_infer_type(key), _infer_type(value), True)
    elif isinstance(obj, (list, array)):
        if not obj:
            raise ValueError("Can not infer type for empty list/array")
        return ArrayType(_infer_type(obj[0]), True)
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


def _create_converter(obj, dataType):
    """Create an converter to drop the names of fields in obj """
    if isinstance(dataType, ArrayType):
        conv = _create_converter(obj[0], dataType.elementType)
        return lambda row: map(conv, row)

    elif isinstance(dataType, MapType):
        value = obj.values()[0]
        conv = _create_converter(value, dataType.valueType)
        return lambda row: dict((k, conv(v)) for k, v in row.iteritems())

    elif not isinstance(dataType, StructType):
        return lambda x: x

    # dataType must be StructType
    names = [f.name for f in dataType.fields]

    if isinstance(obj, dict):
        conv = lambda o: tuple(o.get(n) for n in names)

    elif isinstance(obj, tuple):
        if hasattr(obj, "_fields"):  # namedtuple
            conv = tuple
        elif hasattr(obj, "__FIELDS__"):
            conv = tuple
        elif all(isinstance(x, tuple) and len(x) == 2 for x in obj):
            conv = lambda o: tuple(v for k, v in o)
        else:
            raise ValueError("unexpected tuple")

    elif hasattr(obj, "__dict__"):  # object
        conv = lambda o: [o.__dict__.get(n, None) for n in names]

    if all(isinstance(f.dataType, PrimitiveType) for f in dataType.fields):
        return conv

    row = conv(obj)
    convs = [_create_converter(v, f.dataType)
             for v, f in zip(row, dataType.fields)]

    def nested_conv(row):
        return tuple(f(v) for f, v in zip(convs, conv(row)))

    return nested_conv


def _drop_schema(rows, schema):
    """ all the names of fields, becoming tuples"""
    iterator = iter(rows)
    row = iterator.next()
    converter = _create_converter(row, schema)
    yield converter(row)
    for i in iterator:
        yield converter(i)


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

    >>> schema = _parse_schema_abstract("a b c")
    >>> row = (1, 1.0, "str")
    >>> _infer_schema_type(row, schema)
    StructType...IntegerType...DoubleType...StringType...
    >>> row = [[1], {"key": (1, 2.0)}]
    >>> schema = _parse_schema_abstract("a[] b{c d}")
    >>> _infer_schema_type(row, schema)
    StructType...a,ArrayType...b,MapType(StringType,...c,IntegerType...
    """
    if dataType is None:
        return _infer_type(obj)

    if not obj:
        raise ValueError("Can not infer type from empty value")

    if isinstance(dataType, ArrayType):
        eType = _infer_schema_type(obj[0], dataType.elementType)
        return ArrayType(eType, True)

    elif isinstance(dataType, MapType):
        k, v = obj.iteritems().next()
        return MapType(_infer_type(k),
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
    LongType: (long,),
    FloatType: (float,),
    DoubleType: (float,),
    DecimalType: (decimal.Decimal,),
    StringType: (str, unicode),
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
    """
    # all objects are nullable
    if obj is None:
        return

    _type = type(dataType)
    if _type not in _acceptable_types:
        return

    if type(obj) not in _acceptable_types[_type]:
        raise TypeError("%s can not accept abject in type %s"
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
    # same object in mose cases.
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
    return cls(v) if v is not None else v


def _create_getter(dt, i):
    """ Create a getter for item `i` with schema """
    cls = _create_cls(dt)

    def getter(self):
        return _create_object(cls, self[i])

    return getter


def _has_struct(dt):
    """Return whether `dt` is or has StructType in it"""
    if isinstance(dt, StructType):
        return True
    elif isinstance(dt, ArrayType):
        return _has_struct(dt.elementType)
    elif isinstance(dt, MapType):
        return _has_struct(dt.valueType)
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
        if _has_struct(f.dataType):
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
    """

    if isinstance(dataType, ArrayType):
        cls = _create_cls(dataType.elementType)

        class List(list):

            def __getitem__(self, i):
                # create object with datetype
                return _create_object(cls, list.__getitem__(self, i))

            def __repr__(self):
                # call collect __repr__ for nested objects
                return "[%s]" % (", ".join(repr(self[i])
                                           for i in range(len(self))))

            def __reduce__(self):
                return list.__reduce__(self)

        return List

    elif isinstance(dataType, MapType):
        vcls = _create_cls(dataType.valueType)

        class Dict(dict):

            def __getitem__(self, k):
                # create object with datetype
                return _create_object(vcls, dict.__getitem__(self, k))

            def __repr__(self):
                # call collect __repr__ for nested objects
                return "{%s}" % (", ".join("%r: %r" % (k, self[k])
                                           for k in self))

            def __reduce__(self):
                return dict.__reduce__(self)

        return Dict

    elif not isinstance(dataType, StructType):
        raise Exception("unexpected data type: %s" % dataType)

    class Row(tuple):

        """ Row in SchemaRDD """
        __DATATYPE__ = dataType
        __FIELDS__ = tuple(f.name for f in dataType.fields)
        __slots__ = ()

        # create property for fast access
        locals().update(_create_properties(dataType.fields))

        def __repr__(self):
            # call collect __repr__ for nested objects
            return ("Row(%s)" % ", ".join("%s=%r" % (n, getattr(self, n))
                                          for n in self.__FIELDS__))

        def __reduce__(self):
            return (_restore_object, (self.__DATATYPE__, tuple(self)))

    return Row


class SQLContext(object):

    """Main entry point for Spark SQL functionality.

    A SQLContext can be used create L{SchemaRDD}s, register L{SchemaRDD}s as
    tables, execute SQL over tables, cache tables, and read parquet files.
    """

    def __init__(self, sparkContext, sqlContext=None):
        """Create a new SQLContext.

        @param sparkContext: The SparkContext to wrap.
        @param sqlContext: An optional JVM Scala SQLContext. If set, we do not instatiate a new
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
        self._pythonToJava = self._jvm.PythonRDD.pythonToJavaArray
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
        command = (func,
                   BatchedSerializer(PickleSerializer(), 1024),
                   BatchedSerializer(PickleSerializer(), 1024))
        pickled_command = CloudPickleSerializer().dumps(command)
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
                                      str(returnType))

    def inferSchema(self, rdd):
        """Infer and apply a schema to an RDD of L{Row}s.

        We peek at the first row of the RDD to determine the fields' names
        and types. Nested collections are supported, which include array,
        dict, list, Row, tuple, namedtuple, or object.

        All the rows in `rdd` should have the same type with the first one,
        or it will cause runtime exceptions.

        Each row could be L{pyspark.sql.Row} object or namedtuple or objects,
        using dict is deprecated.

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

        schema = _infer_schema(first)
        rdd = rdd.mapPartitions(lambda rows: _drop_schema(rows, schema))
        return self.applySchema(rdd, schema)

    def applySchema(self, rdd, schema):
        """
        Applies the given schema to the given RDD of L{tuple} or L{list}s.

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

        >>> from datetime import datetime
        >>> rdd = sc.parallelize([(127, -128L, -32768, 32767, 2147483647L, 1.0,
        ...     datetime(2010, 1, 1, 1, 1, 1),
        ...     {"a": 1}, (2,), [1, 2, 3], None)])
        >>> schema = StructType([
        ...     StructField("byte1", ByteType(), False),
        ...     StructField("byte2", ByteType(), False),
        ...     StructField("short1", ShortType(), False),
        ...     StructField("short2", ShortType(), False),
        ...     StructField("int", IntegerType(), False),
        ...     StructField("float", FloatType(), False),
        ...     StructField("time", TimestampType(), False),
        ...     StructField("map",
        ...         MapType(StringType(), IntegerType(), False), False),
        ...     StructField("struct",
        ...         StructType([StructField("b", ShortType(), False)]), False),
        ...     StructField("list", ArrayType(ByteType(), False), False),
        ...     StructField("null", DoubleType(), True)])
        >>> srdd = sqlCtx.applySchema(rdd, schema)
        >>> results = srdd.map(
        ...     lambda x: (x.byte1, x.byte2, x.short1, x.short2, x.int, x.float, x.time,
        ...         x.map["a"], x.struct.b, x.list, x.null))
        >>> results.collect()[0]
        (127, -128, -32768, 32767, 2147483647, 1.0, ...(2010, 1, 1, 1, 1, 1), 1, 2, [1, 2, 3], None)

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
        for row in rows:
            _verify_type(row, schema)

        batched = isinstance(rdd._jrdd_deserializer, BatchedSerializer)
        jrdd = self._pythonToJava(rdd._jrdd, batched)
        srdd = self._ssql_ctx.applySchemaToPythonRDD(jrdd.rdd(), str(schema))
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
        """
        Loads a text file storing one JSON object per line as a
        L{SchemaRDD}.

        If the schema is provided, applies the given schema to this
        JSON dataset.

        Otherwise, it goes through the entire dataset once to determine
        the schema.

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
            jschema_rdd = self._ssql_ctx.jsonFile(path)
        else:
            scala_datatype = self._ssql_ctx.parseDataType(str(schema))
            jschema_rdd = self._ssql_ctx.jsonFile(path, scala_datatype)
        return SchemaRDD(jschema_rdd, self)

    def jsonRDD(self, rdd, schema=None):
        """Loads an RDD storing one JSON object per string as a L{SchemaRDD}.

        If the schema is provided, applies the given schema to this
        JSON dataset.

        Otherwise, it goes through the entire dataset once to determine
        the schema.

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
            jschema_rdd = self._ssql_ctx.jsonRDD(jrdd.rdd())
        else:
            scala_datatype = self._ssql_ctx.parseDataType(str(schema))
            jschema_rdd = self._ssql_ctx.jsonRDD(jrdd.rdd(), scala_datatype)
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

    def __init__(self, sparkContext, hiveContext=None):
        """Create a new HiveContext.

        @param sparkContext: The SparkContext to wrap.
        @param hiveContext: An optional JVM Scala HiveContext. If set, we do not instatiate a new
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
                            "sbt/sbt assembly", e)

    def _get_hive_ctx(self):
        return self._jvm.HiveContext(self._jsc.sc())

    def hiveql(self, hqlQuery):
        """
        DEPRECATED: Use sql()
        """
        warnings.warn("hiveql() is deprecated as the sql function now parses using HiveQL by" +
                      "default. The SQL dialect for parsing can be set using 'spark.sql.dialect'",
                      DeprecationWarning)
        return SchemaRDD(self._ssql_ctx.hiveql(hqlQuery), self)

    def hql(self, hqlQuery):
        """
        DEPRECATED: Use sql()
        """
        warnings.warn("hql() is deprecated as the sql function now parses using HiveQL by" +
                      "default. The SQL dialect for parsing can be set using 'spark.sql.dialect'",
                      DeprecationWarning)
        return self.hiveql(hqlQuery)


class LocalHiveContext(HiveContext):

    """Starts up an instance of hive where metadata is stored locally.

    An in-process metadata data is created with data stored in ./metadata.
    Warehouse data is stored in in ./warehouse.

    >>> import os
    >>> hiveCtx = LocalHiveContext(sc)
    >>> try:
    ...     supress = hiveCtx.sql("DROP TABLE src")
    ... except Exception:
    ...     pass
    >>> kv1 = os.path.join(os.environ["SPARK_HOME"],
    ...        'examples/src/main/resources/kv1.txt')
    >>> supress = hiveCtx.sql(
    ...     "CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    >>> supress = hiveCtx.sql("LOAD DATA LOCAL INPATH '%s' INTO TABLE src"
    ...        % kv1)
    >>> results = hiveCtx.sql("FROM src SELECT value"
    ...      ).map(lambda r: int(r.value.split('_')[1]))
    >>> num = results.count()
    >>> reduce_sum = results.reduce(lambda x, y: x + y)
    >>> num
    500
    >>> reduce_sum
    130091
    """

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
        self._jschema_rdd = jschema_rdd
        self._id = None
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

    def id(self):
        if self._id is None:
            self._id = self._jrdd.id()
        return self._id

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

    def collect(self):
        """
        Return a list that contains all of the rows in this RDD.

        Each object in the list is on Row, the fields can be accessed as
        attributes.
        """
        rows = RDD.collect(self)
        cls = _create_cls(self.schema())
        return map(cls, rows)

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
        import pickle
        pickle.loads(pickle.dumps(schema))

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
                rdd = self._jschema_rdd.subtract(other._jschema_rdd,
                                                 numPartitions)
            return SchemaRDD(rdd, self.sql_ctx)
        else:
            raise ValueError("Can only subtract another SchemaRDD")


def _test():
    import doctest
    from array import array
    from pyspark.context import SparkContext
    # let doctest run in pyspark.sql, so DataTypes can be picklable
    import pyspark.sql
    from pyspark.sql import Row, SQLContext
    globs = pyspark.sql.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext('local[4]', 'PythonTest', batchSize=2)
    globs['sc'] = sc
    globs['sqlCtx'] = SQLContext(sc)
    globs['rdd'] = sc.parallelize(
        [Row(field1=1, field2="row1"),
         Row(field1=2, field2="row2"),
         Row(field1=3, field2="row3")]
    )
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
