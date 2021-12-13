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
import math
import datetime
import calendar
import json
import re
import base64
from array import array
import ctypes
from collections.abc import Iterable
from typing import (
    cast,
    overload,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    Tuple,
    Type,
    TypeVar,
)

from py4j.protocol import register_input_converter  # type: ignore[import]
from py4j.java_gateway import JavaClass, JavaGateway, JavaObject  # type: ignore[import]

from pyspark.serializers import CloudPickleSerializer

T = TypeVar("T")
U = TypeVar("U")

__all__ = [
    "DataType",
    "NullType",
    "StringType",
    "BinaryType",
    "BooleanType",
    "DateType",
    "TimestampType",
    "TimestampNTZType",
    "DecimalType",
    "DoubleType",
    "FloatType",
    "ByteType",
    "IntegerType",
    "LongType",
    "DayTimeIntervalType",
    "Row",
    "ShortType",
    "ArrayType",
    "MapType",
    "StructField",
    "StructType",
]


class DataType:
    """Base class for data types."""

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    @classmethod
    def typeName(cls) -> str:
        return cls.__name__[:-4].lower()

    def simpleString(self) -> str:
        return self.typeName()

    def jsonValue(self) -> Union[str, Dict[str, Any]]:
        return self.typeName()

    def json(self) -> str:
        return json.dumps(self.jsonValue(), separators=(",", ":"), sort_keys=True)

    def needConversion(self) -> bool:
        """
        Does this type needs conversion between Python object and internal SQL object.

        This is used to avoid the unnecessary conversion for ArrayType/MapType/StructType.
        """
        return False

    def toInternal(self, obj: Any) -> Any:
        """
        Converts a Python object into an internal SQL object.
        """
        return obj

    def fromInternal(self, obj: Any) -> Any:
        """
        Converts an internal SQL object into a native Python object.
        """
        return obj


# This singleton pattern does not work with pickle, you will get
# another object after pickle and unpickle
class DataTypeSingleton(type):
    """Metaclass for DataType"""

    _instances: ClassVar[Dict[Type["DataTypeSingleton"], "DataTypeSingleton"]] = {}

    def __call__(cls: Type[T]) -> T:  # type: ignore[override]
        if cls not in cls._instances:  # type: ignore[attr-defined]
            cls._instances[cls] = super(  # type: ignore[misc, attr-defined]
                DataTypeSingleton, cls
            ).__call__()
        return cls._instances[cls]  # type: ignore[attr-defined]


class NullType(DataType, metaclass=DataTypeSingleton):
    """Null type.

    The data type representing None, used for the types that cannot be inferred.
    """

    @classmethod
    def typeName(cls) -> str:
        return "void"


class AtomicType(DataType):
    """An internal type used to represent everything that is not
    null, UDTs, arrays, structs, and maps."""


class NumericType(AtomicType):
    """Numeric data types."""


class IntegralType(NumericType, metaclass=DataTypeSingleton):
    """Integral data types."""

    pass


class FractionalType(NumericType):
    """Fractional data types."""


class StringType(AtomicType, metaclass=DataTypeSingleton):
    """String data type."""

    pass


class BinaryType(AtomicType, metaclass=DataTypeSingleton):
    """Binary (byte array) data type."""

    pass


class BooleanType(AtomicType, metaclass=DataTypeSingleton):
    """Boolean data type."""

    pass


class DateType(AtomicType, metaclass=DataTypeSingleton):
    """Date (datetime.date) data type."""

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def needConversion(self) -> bool:
        return True

    def toInternal(self, d: datetime.date) -> int:
        if d is not None:
            return d.toordinal() - self.EPOCH_ORDINAL

    def fromInternal(self, v: int) -> datetime.date:
        if v is not None:
            return datetime.date.fromordinal(v + self.EPOCH_ORDINAL)


class TimestampType(AtomicType, metaclass=DataTypeSingleton):
    """Timestamp (datetime.datetime) data type."""

    def needConversion(self) -> bool:
        return True

    def toInternal(self, dt: datetime.datetime) -> int:
        if dt is not None:
            seconds = (
                calendar.timegm(dt.utctimetuple()) if dt.tzinfo else time.mktime(dt.timetuple())
            )
            return int(seconds) * 1000000 + dt.microsecond

    def fromInternal(self, ts: int) -> datetime.datetime:
        if ts is not None:
            # using int to avoid precision loss in float
            return datetime.datetime.fromtimestamp(ts // 1000000).replace(microsecond=ts % 1000000)


class TimestampNTZType(AtomicType, metaclass=DataTypeSingleton):
    """Timestamp (datetime.datetime) data type without timezone information."""

    def needConversion(self) -> bool:
        return True

    @classmethod
    def typeName(cls) -> str:
        return "timestamp_ntz"

    def toInternal(self, dt: datetime.datetime) -> int:
        if dt is not None:
            seconds = calendar.timegm(dt.timetuple())
            return int(seconds) * 1000000 + dt.microsecond

    def fromInternal(self, ts: int) -> datetime.datetime:
        if ts is not None:
            # using int to avoid precision loss in float
            return datetime.datetime.utcfromtimestamp(ts // 1000000).replace(
                microsecond=ts % 1000000
            )


class DecimalType(FractionalType):
    """Decimal (decimal.Decimal) data type.

    The DecimalType must have fixed precision (the maximum total number of digits)
    and scale (the number of digits on the right of dot). For example, (5, 2) can
    support the value from [-999.99 to 999.99].

    The precision can be up to 38, the scale must be less or equal to precision.

    When creating a DecimalType, the default precision and scale is (10, 0). When inferring
    schema from decimal.Decimal objects, it will be DecimalType(38, 18).

    Parameters
    ----------
    precision : int, optional
        the maximum (i.e. total) number of digits (default: 10)
    scale : int, optional
        the number of digits on right side of dot. (default: 0)
    """

    def __init__(self, precision: int = 10, scale: int = 0):
        self.precision = precision
        self.scale = scale
        self.hasPrecisionInfo = True  # this is a public API

    def simpleString(self) -> str:
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def jsonValue(self) -> str:
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def __repr__(self) -> str:
        return "DecimalType(%d,%d)" % (self.precision, self.scale)


class DoubleType(FractionalType, metaclass=DataTypeSingleton):
    """Double data type, representing double precision floats."""

    pass


class FloatType(FractionalType, metaclass=DataTypeSingleton):
    """Float data type, representing single precision floats."""

    pass


class ByteType(IntegralType):
    """Byte data type, i.e. a signed integer in a single byte."""

    def simpleString(self) -> str:
        return "tinyint"


class IntegerType(IntegralType):
    """Int data type, i.e. a signed 32-bit integer."""

    def simpleString(self) -> str:
        return "int"


class LongType(IntegralType):
    """Long data type, i.e. a signed 64-bit integer.

    If the values are beyond the range of [-9223372036854775808, 9223372036854775807],
    please use :class:`DecimalType`.
    """

    def simpleString(self) -> str:
        return "bigint"


class DayTimeIntervalType(AtomicType):
    """DayTimeIntervalType (datetime.timedelta)."""

    DAY = 0
    HOUR = 1
    MINUTE = 2
    SECOND = 3

    _fields = {
        DAY: "day",
        HOUR: "hour",
        MINUTE: "minute",
        SECOND: "second",
    }

    _inverted_fields = dict(zip(_fields.values(), _fields.keys()))

    def __init__(self, startField: Optional[int] = None, endField: Optional[int] = None):
        if startField is None and endField is None:
            # Default matched to scala side.
            startField = DayTimeIntervalType.DAY
            endField = DayTimeIntervalType.SECOND
        elif startField is not None and endField is None:
            endField = startField

        fields = DayTimeIntervalType._fields
        if startField not in fields.keys() or endField not in fields.keys():
            raise RuntimeError("interval %s to %s is invalid" % (startField, endField))
        self.startField = cast(int, startField)
        self.endField = cast(int, endField)

    def _str_repr(self) -> str:
        fields = DayTimeIntervalType._fields
        start_field_name = fields[self.startField]
        end_field_name = fields[self.endField]
        if start_field_name == end_field_name:
            return "interval %s" % start_field_name
        else:
            return "interval %s to %s" % (start_field_name, end_field_name)

    simpleString = _str_repr

    jsonValue = _str_repr

    def __repr__(self) -> str:
        return "%s(%d,%d)" % (type(self).__name__, self.startField, self.endField)

    def needConversion(self) -> bool:
        return True

    def toInternal(self, dt: datetime.timedelta) -> Optional[int]:
        if dt is not None:
            return (math.floor(dt.total_seconds()) * 1000000) + dt.microseconds

    def fromInternal(self, micros: int) -> Optional[datetime.timedelta]:
        if micros is not None:
            return datetime.timedelta(microseconds=micros)


class ShortType(IntegralType):
    """Short data type, i.e. a signed 16-bit integer."""

    def simpleString(self) -> str:
        return "smallint"


class ArrayType(DataType):
    """Array data type.

    Parameters
    ----------
    elementType : :class:`DataType`
        :class:`DataType` of each element in the array.
    containsNull : bool, optional
        whether the array can contain null (None) values.

    Examples
    --------
    >>> ArrayType(StringType()) == ArrayType(StringType(), True)
    True
    >>> ArrayType(StringType(), False) == ArrayType(StringType())
    False
    """

    def __init__(self, elementType: DataType, containsNull: bool = True):
        assert isinstance(elementType, DataType), "elementType %s should be an instance of %s" % (
            elementType,
            DataType,
        )
        self.elementType = elementType
        self.containsNull = containsNull

    def simpleString(self) -> str:
        return "array<%s>" % self.elementType.simpleString()

    def __repr__(self) -> str:
        return "ArrayType(%s,%s)" % (self.elementType, str(self.containsNull).lower())

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "type": self.typeName(),
            "elementType": self.elementType.jsonValue(),
            "containsNull": self.containsNull,
        }

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> "ArrayType":
        return ArrayType(_parse_datatype_json_value(json["elementType"]), json["containsNull"])

    def needConversion(self) -> bool:
        return self.elementType.needConversion()

    def toInternal(self, obj: List[Optional[T]]) -> List[Optional[T]]:
        if not self.needConversion():
            return obj
        return obj and [self.elementType.toInternal(v) for v in obj]

    def fromInternal(self, obj: List[Optional[T]]) -> List[Optional[T]]:
        if not self.needConversion():
            return obj
        return obj and [self.elementType.fromInternal(v) for v in obj]


class MapType(DataType):
    """Map data type.

    Parameters
    ----------
    keyType : :class:`DataType`
        :class:`DataType` of the keys in the map.
    valueType : :class:`DataType`
        :class:`DataType` of the values in the map.
    valueContainsNull : bool, optional
        indicates whether values can contain null (None) values.

    Notes
    -----
    Keys in a map data type are not allowed to be null (None).

    Examples
    --------
    >>> (MapType(StringType(), IntegerType())
    ...        == MapType(StringType(), IntegerType(), True))
    True
    >>> (MapType(StringType(), IntegerType(), False)
    ...        == MapType(StringType(), FloatType()))
    False
    """

    def __init__(self, keyType: DataType, valueType: DataType, valueContainsNull: bool = True):
        assert isinstance(keyType, DataType), "keyType %s should be an instance of %s" % (
            keyType,
            DataType,
        )
        assert isinstance(valueType, DataType), "valueType %s should be an instance of %s" % (
            valueType,
            DataType,
        )
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def simpleString(self) -> str:
        return "map<%s,%s>" % (self.keyType.simpleString(), self.valueType.simpleString())

    def __repr__(self) -> str:
        return "MapType(%s,%s,%s)" % (
            self.keyType,
            self.valueType,
            str(self.valueContainsNull).lower(),
        )

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "type": self.typeName(),
            "keyType": self.keyType.jsonValue(),
            "valueType": self.valueType.jsonValue(),
            "valueContainsNull": self.valueContainsNull,
        }

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> "MapType":
        return MapType(
            _parse_datatype_json_value(json["keyType"]),
            _parse_datatype_json_value(json["valueType"]),
            json["valueContainsNull"],
        )

    def needConversion(self) -> bool:
        return self.keyType.needConversion() or self.valueType.needConversion()

    def toInternal(self, obj: Dict[T, Optional[U]]) -> Dict[T, Optional[U]]:
        if not self.needConversion():
            return obj
        return obj and dict(
            (self.keyType.toInternal(k), self.valueType.toInternal(v)) for k, v in obj.items()
        )

    def fromInternal(self, obj: Dict[T, Optional[U]]) -> Dict[T, Optional[U]]:
        if not self.needConversion():
            return obj
        return obj and dict(
            (self.keyType.fromInternal(k), self.valueType.fromInternal(v)) for k, v in obj.items()
        )


class StructField(DataType):
    """A field in :class:`StructType`.

    Parameters
    ----------
    name : str
        name of the field.
    dataType : :class:`DataType`
        :class:`DataType` of the field.
    nullable : bool, optional
        whether the field can be null (None) or not.
    metadata : dict, optional
        a dict from string to simple type that can be toInternald to JSON automatically

    Examples
    --------
    >>> (StructField("f1", StringType(), True)
    ...      == StructField("f1", StringType(), True))
    True
    >>> (StructField("f1", StringType(), True)
    ...      == StructField("f2", StringType(), True))
    False
    """

    def __init__(
        self,
        name: str,
        dataType: DataType,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        assert isinstance(dataType, DataType), "dataType %s should be an instance of %s" % (
            dataType,
            DataType,
        )
        assert isinstance(name, str), "field name %s should be a string" % (name)
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def simpleString(self) -> str:
        return "%s:%s" % (self.name, self.dataType.simpleString())

    def __repr__(self) -> str:
        return "StructField(%s,%s,%s)" % (self.name, self.dataType, str(self.nullable).lower())

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.dataType.jsonValue(),
            "nullable": self.nullable,
            "metadata": self.metadata,
        }

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> "StructField":
        return StructField(
            json["name"],
            _parse_datatype_json_value(json["type"]),
            json["nullable"],
            json["metadata"],
        )

    def needConversion(self) -> bool:
        return self.dataType.needConversion()

    def toInternal(self, obj: T) -> T:
        return self.dataType.toInternal(obj)

    def fromInternal(self, obj: T) -> T:
        return self.dataType.fromInternal(obj)

    def typeName(self) -> str:  # type: ignore[override]
        raise TypeError(
            "StructField does not have typeName. " "Use typeName on its type explicitly instead."
        )


class StructType(DataType):
    """Struct type, consisting of a list of :class:`StructField`.

    This is the data type representing a :class:`Row`.

    Iterating a :class:`StructType` will iterate over its :class:`StructField`\\s.
    A contained :class:`StructField` can be accessed by its name or position.

    Examples
    --------
    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct1["f1"]
    StructField(f1,StringType,true)
    >>> struct1[0]
    StructField(f1,StringType,true)

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

    def __init__(self, fields: Optional[List[StructField]] = None):
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]
            assert all(
                isinstance(f, StructField) for f in fields
            ), "fields should be a list of StructField"
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)

    @overload
    def add(
        self,
        field: str,
        data_type: Union[str, DataType],
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "StructType":
        ...

    @overload
    def add(self, field: StructField) -> "StructType":
        ...

    def add(
        self,
        field: Union[str, StructField],
        data_type: Optional[Union[str, DataType]] = None,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "StructType":
        """
        Construct a :class:`StructType` by adding new elements to it, to define the schema.
        The method accepts either:

            a) A single parameter which is a :class:`StructField` object.
            b) Between 2 and 4 parameters as (name, data_type, nullable (optional),
               metadata(optional). The data_type parameter may be either a String or a
               :class:`DataType` object.

        Parameters
        ----------
        field : str or :class:`StructField`
            Either the name of the field or a :class:`StructField` object
        data_type : :class:`DataType`, optional
            If present, the DataType of the :class:`StructField` to create
        nullable : bool, optional
            Whether the field to add should be nullable (default True)
        metadata : dict, optional
            Any additional metadata (default None)

        Returns
        -------
        :class:`StructType`

        Examples
        --------
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

    def __iter__(self) -> Iterator[StructField]:
        """Iterate the fields"""
        return iter(self.fields)

    def __len__(self) -> int:
        """Return the number of fields."""
        return len(self.fields)

    def __getitem__(self, key: Union[str, int]) -> StructField:
        """Access fields by name or slice."""
        if isinstance(key, str):
            for field in self:
                if field.name == key:
                    return field
            raise KeyError("No StructField named {0}".format(key))
        elif isinstance(key, int):
            try:
                return self.fields[key]
            except IndexError:
                raise IndexError("StructType index out of range")
        elif isinstance(key, slice):
            return StructType(self.fields[key])
        else:
            raise TypeError("StructType keys should be strings, integers or slices")

    def simpleString(self) -> str:
        return "struct<%s>" % (",".join(f.simpleString() for f in self))

    def __repr__(self) -> str:
        return "StructType(List(%s))" % ",".join(str(field) for field in self)

    def jsonValue(self) -> Dict[str, Any]:
        return {"type": self.typeName(), "fields": [f.jsonValue() for f in self]}

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> "StructType":
        return StructType([StructField.fromJson(f) for f in json["fields"]])

    def fieldNames(self) -> List[str]:
        """
        Returns all field names in a list.

        Examples
        --------
        >>> struct = StructType([StructField("f1", StringType(), True)])
        >>> struct.fieldNames()
        ['f1']
        """
        return list(self.names)

    def needConversion(self) -> bool:
        # We need convert Row()/namedtuple into tuple()
        return True

    def toInternal(self, obj: Tuple) -> Tuple:
        if obj is None:
            return

        if self._needSerializeAnyField:
            # Only calling toInternal function for fields that need conversion
            if isinstance(obj, dict):
                return tuple(
                    f.toInternal(obj.get(n)) if c else obj.get(n)
                    for n, f, c in zip(self.names, self.fields, self._needConversion)
                )
            elif isinstance(obj, (tuple, list)):
                return tuple(
                    f.toInternal(v) if c else v
                    for f, v, c in zip(self.fields, obj, self._needConversion)
                )
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(
                    f.toInternal(d.get(n)) if c else d.get(n)
                    for n, f, c in zip(self.names, self.fields, self._needConversion)
                )
            else:
                raise ValueError("Unexpected tuple %r with StructType" % obj)
        else:
            if isinstance(obj, dict):
                return tuple(obj.get(n) for n in self.names)
            elif isinstance(obj, (list, tuple)):
                return tuple(obj)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(d.get(n) for n in self.names)
            else:
                raise ValueError("Unexpected tuple %r with StructType" % obj)

    def fromInternal(self, obj: Tuple) -> "Row":
        if obj is None:
            return
        if isinstance(obj, Row):
            # it's already converted by pickler
            return obj

        values: Union[Tuple, List]
        if self._needSerializeAnyField:
            # Only calling fromInternal function for fields that need conversion
            values = [
                f.fromInternal(v) if c else v
                for f, v, c in zip(self.fields, obj, self._needConversion)
            ]
        else:
            values = obj
        return _create_row(self.names, values)


class UserDefinedType(DataType):
    """User-defined type (UDT).

    .. note:: WARN: Spark Internal Use Only
    """

    @classmethod
    def typeName(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def sqlType(cls) -> DataType:
        """
        Underlying SQL storage type for this UDT.
        """
        raise NotImplementedError("UDT must implement sqlType().")

    @classmethod
    def module(cls) -> str:
        """
        The Python module of the UDT.
        """
        raise NotImplementedError("UDT must implement module().")

    @classmethod
    def scalaUDT(cls) -> str:
        """
        The class name of the paired Scala UDT (could be '', if there
        is no corresponding one).
        """
        return ""

    def needConversion(self) -> bool:
        return True

    @classmethod
    def _cachedSqlType(cls) -> DataType:
        """
        Cache the sqlType() into class, because it's heavily used in `toInternal`.
        """
        if not hasattr(cls, "_cached_sql_type"):
            cls._cached_sql_type = cls.sqlType()  # type: ignore[attr-defined]
        return cls._cached_sql_type  # type: ignore[attr-defined]

    def toInternal(self, obj: Any) -> Any:
        if obj is not None:
            return self._cachedSqlType().toInternal(self.serialize(obj))

    def fromInternal(self, obj: Any) -> Any:
        v = self._cachedSqlType().fromInternal(obj)
        if v is not None:
            return self.deserialize(v)

    def serialize(self, obj: Any) -> Any:
        """
        Converts a user-type object into a SQL datum.
        """
        raise NotImplementedError("UDT must implement toInternal().")

    def deserialize(self, datum: Any) -> Any:
        """
        Converts a SQL datum into a user-type object.
        """
        raise NotImplementedError("UDT must implement fromInternal().")

    def simpleString(self) -> str:
        return "udt"

    def json(self) -> str:
        return json.dumps(self.jsonValue(), separators=(",", ":"), sort_keys=True)

    def jsonValue(self) -> Dict[str, Any]:
        if self.scalaUDT():
            assert self.module() != "__main__", "UDT in __main__ cannot work with ScalaUDT"
            schema = {
                "type": "udt",
                "class": self.scalaUDT(),
                "pyClass": "%s.%s" % (self.module(), type(self).__name__),
                "sqlType": self.sqlType().jsonValue(),
            }
        else:
            ser = CloudPickleSerializer()
            b = ser.dumps(type(self))
            schema = {
                "type": "udt",
                "pyClass": "%s.%s" % (self.module(), type(self).__name__),
                "serializedClass": base64.b64encode(b).decode("utf8"),
                "sqlType": self.sqlType().jsonValue(),
            }
        return schema

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> "UserDefinedType":
        pyUDT = str(json["pyClass"])  # convert unicode to str
        split = pyUDT.rfind(".")
        pyModule = pyUDT[:split]
        pyClass = pyUDT[split + 1 :]
        m = __import__(pyModule, globals(), locals(), [pyClass])
        if not hasattr(m, pyClass):
            s = base64.b64decode(json["serializedClass"].encode("utf-8"))
            UDT = CloudPickleSerializer().loads(s)
        else:
            UDT = getattr(m, pyClass)
        return UDT()

    def __eq__(self, other: Any) -> bool:
        return type(self) == type(other)


_atomic_types: List[Type[DataType]] = [
    StringType,
    BinaryType,
    BooleanType,
    DecimalType,
    FloatType,
    DoubleType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    DateType,
    TimestampType,
    TimestampNTZType,
    NullType,
]
_all_atomic_types: Dict[str, Type[DataType]] = dict((t.typeName(), t) for t in _atomic_types)

_complex_types: List[Type[Union[ArrayType, MapType, StructType]]] = [ArrayType, MapType, StructType]
_all_complex_types: Dict[str, Type[Union[ArrayType, MapType, StructType]]] = dict(
    (v.typeName(), v) for v in _complex_types
)


_FIXED_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(-?\d+)\s*\)")
_INTERVAL_DAYTIME = re.compile(r"interval (day|hour|minute|second)( to (day|hour|minute|second))?")


def _parse_datatype_string(s: str) -> DataType:
    """
    Parses the given data type string to a :class:`DataType`. The data type string format equals
    :class:`DataType.simpleString`, except that the top level struct type can omit
    the ``struct<>``. Since Spark 2.3, this also supports a schema in a DDL-formatted
    string and case-insensitive strings.

    Examples
    --------
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
    from pyspark import SparkContext

    sc = SparkContext._active_spark_context  # type: ignore[attr-defined]

    def from_ddl_schema(type_str: str) -> DataType:
        return _parse_datatype_json_string(
            sc._jvm.org.apache.spark.sql.types.StructType.fromDDL(type_str).json()
        )

    def from_ddl_datatype(type_str: str) -> DataType:
        return _parse_datatype_json_string(
            sc._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.parseDataType(type_str).json()
        )

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


def _parse_datatype_json_string(json_string: str) -> DataType:
    """Parses the given data type JSON string.

    Examples
    --------
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


def _parse_datatype_json_value(json_value: Union[dict, str]) -> DataType:
    if not isinstance(json_value, dict):
        if json_value in _all_atomic_types.keys():
            return _all_atomic_types[json_value]()
        elif json_value == "decimal":
            return DecimalType()
        elif _FIXED_DECIMAL.match(json_value):
            m = _FIXED_DECIMAL.match(json_value)
            return DecimalType(int(m.group(1)), int(m.group(2)))  # type: ignore[union-attr]
        elif _INTERVAL_DAYTIME.match(json_value):
            m = _INTERVAL_DAYTIME.match(json_value)
            inverted_fields = DayTimeIntervalType._inverted_fields
            first_field = inverted_fields.get(m.group(1))  # type: ignore[union-attr]
            second_field = inverted_fields.get(m.group(3))  # type: ignore[union-attr]
            if first_field is not None and second_field is None:
                return DayTimeIntervalType(first_field)
            return DayTimeIntervalType(first_field, second_field)
        else:
            raise ValueError("Could not parse datatype: %s" % json_value)
    else:
        tpe = json_value["type"]
        if tpe in _all_complex_types:
            return _all_complex_types[tpe].fromJson(json_value)
        elif tpe == "udt":
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
    datetime.datetime: TimestampType,  # can be TimestampNTZType
    datetime.time: TimestampType,  # can be TimestampNTZType
    datetime.timedelta: DayTimeIntervalType,
    bytes: BinaryType,
}

# Mapping Python array types to Spark SQL DataType
# We should be careful here. The size of these types in python depends on C
# implementation. We need to make sure that this conversion does not lose any
# precision. Also, JVM only support signed types, when converting unsigned types,
# keep in mind that it require 1 more bit when stored as signed types.
#
# Reference for C integer size, see:
# ISO/IEC 9899:201x specification, chapter 5.2.4.2.1 Sizes of integer types <limits.h>.
# Reference for python array typecode, see:
# https://docs.python.org/2/library/array.html
# https://docs.python.org/3.6/library/array.html
# Reference for JVM's supported integral types:
# http://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.3.1

_array_signed_int_typecode_ctype_mappings = {
    "b": ctypes.c_byte,
    "h": ctypes.c_short,
    "i": ctypes.c_int,
    "l": ctypes.c_long,
}

_array_unsigned_int_typecode_ctype_mappings = {
    "B": ctypes.c_ubyte,
    "H": ctypes.c_ushort,
    "I": ctypes.c_uint,
    "L": ctypes.c_ulong,
}


def _int_size_to_type(
    size: int,
) -> Optional[Union[Type[ByteType], Type[ShortType], Type[IntegerType], Type[LongType]]]:
    """
    Return the Catalyst datatype from the size of integers.
    """
    if size <= 8:
        return ByteType
    elif size <= 16:
        return ShortType
    elif size <= 32:
        return IntegerType
    elif size <= 64:
        return LongType
    else:
        return None


# The list of all supported array typecodes, is stored here
_array_type_mappings: Dict[str, Type[DataType]] = {
    # Warning: Actual properties for float and double in C is not specified in C.
    # On almost every system supported by both python and JVM, they are IEEE 754
    # single-precision binary floating-point format and IEEE 754 double-precision
    # binary floating-point format. And we do assume the same thing here for now.
    "f": FloatType,
    "d": DoubleType,
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
    _array_type_mappings["u"] = StringType


def _infer_type(
    obj: Any,
    infer_dict_as_struct: bool = False,
    prefer_timestamp_ntz: bool = False,
) -> DataType:
    """Infer the DataType from obj"""
    if obj is None:
        return NullType()

    if hasattr(obj, "__UDT__"):
        return obj.__UDT__

    dataType = _type_mappings.get(type(obj))
    if dataType is DecimalType:
        # the precision and scale of `obj` may be different from row to row.
        return DecimalType(38, 18)
    if dataType is TimestampType and prefer_timestamp_ntz and obj.tzinfo is None:
        return TimestampNTZType()
    if dataType is DayTimeIntervalType:
        return DayTimeIntervalType()
    elif dataType is not None:
        return dataType()

    if isinstance(obj, dict):
        if infer_dict_as_struct:
            struct = StructType()
            for key, value in obj.items():
                if key is not None and value is not None:
                    struct.add(
                        key, _infer_type(value, infer_dict_as_struct, prefer_timestamp_ntz), True
                    )
            return struct
        else:
            for key, value in obj.items():
                if key is not None and value is not None:
                    return MapType(
                        _infer_type(key, infer_dict_as_struct, prefer_timestamp_ntz),
                        _infer_type(value, infer_dict_as_struct, prefer_timestamp_ntz),
                        True,
                    )
            return MapType(NullType(), NullType(), True)
    elif isinstance(obj, list):
        for v in obj:
            if v is not None:
                return ArrayType(
                    _infer_type(obj[0], infer_dict_as_struct, prefer_timestamp_ntz), True
                )
        return ArrayType(NullType(), True)
    elif isinstance(obj, array):
        if obj.typecode in _array_type_mappings:
            return ArrayType(_array_type_mappings[obj.typecode](), False)
        else:
            raise TypeError("not supported type: array(%s)" % obj.typecode)
    else:
        try:
            return _infer_schema(obj, infer_dict_as_struct=infer_dict_as_struct)
        except TypeError:
            raise TypeError("not supported type: %s" % type(obj))


def _infer_schema(
    row: Any,
    names: Optional[List[str]] = None,
    infer_dict_as_struct: bool = False,
    prefer_timestamp_ntz: bool = False,
) -> StructType:
    """Infer the schema from dict/namedtuple/object"""
    items: Iterable[Tuple[str, Any]]
    if isinstance(row, dict):
        items = sorted(row.items())

    elif isinstance(row, (tuple, list)):
        if hasattr(row, "__fields__"):  # Row
            items = zip(row.__fields__, tuple(row))  # type: ignore[union-attr]
        elif hasattr(row, "_fields"):  # namedtuple
            items = zip(row._fields, tuple(row))  # type: ignore[union-attr]
        else:
            if names is None:
                names = ["_%d" % i for i in range(1, len(row) + 1)]
            elif len(names) < len(row):
                names.extend("_%d" % i for i in range(len(names) + 1, len(row) + 1))
            items = zip(names, row)

    elif hasattr(row, "__dict__"):  # object
        items = sorted(row.__dict__.items())

    else:
        raise TypeError("Can not infer schema for type: %s" % type(row))

    fields = []
    for k, v in items:
        try:
            fields.append(
                StructField(k, _infer_type(v, infer_dict_as_struct, prefer_timestamp_ntz), True)
            )
        except TypeError as e:
            raise TypeError("Unable to infer the type of the field {}.".format(k)) from e
    return StructType(fields)


def _has_nulltype(dt: DataType) -> bool:
    """Return whether there is a NullType in `dt` or not"""
    if isinstance(dt, StructType):
        return any(_has_nulltype(f.dataType) for f in dt.fields)
    elif isinstance(dt, ArrayType):
        return _has_nulltype((dt.elementType))
    elif isinstance(dt, MapType):
        return _has_nulltype(dt.keyType) or _has_nulltype(dt.valueType)
    else:
        return isinstance(dt, NullType)


@overload
def _merge_type(a: StructType, b: StructType, name: Optional[str] = None) -> StructType:
    ...


@overload
def _merge_type(a: ArrayType, b: ArrayType, name: Optional[str] = None) -> ArrayType:
    ...


@overload
def _merge_type(a: MapType, b: MapType, name: Optional[str] = None) -> MapType:
    ...


@overload
def _merge_type(a: DataType, b: DataType, name: Optional[str] = None) -> DataType:
    ...


def _merge_type(
    a: Union[StructType, ArrayType, MapType, DataType],
    b: Union[StructType, ArrayType, MapType, DataType],
    name: Optional[str] = None,
) -> Union[StructType, ArrayType, MapType, DataType]:
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
    elif isinstance(a, TimestampType) and isinstance(b, TimestampNTZType):
        return a
    elif isinstance(a, TimestampNTZType) and isinstance(b, TimestampType):
        return b
    elif type(a) is not type(b):
        # TODO: type cast (such as int -> long)
        raise TypeError(new_msg("Can not merge type %s and %s" % (type(a), type(b))))

    # same type
    if isinstance(a, StructType):
        nfs = dict((f.name, f.dataType) for f in cast(StructType, b).fields)
        fields = [
            StructField(
                f.name, _merge_type(f.dataType, nfs.get(f.name, NullType()), name=new_name(f.name))
            )
            for f in a.fields
        ]
        names = set([f.name for f in fields])
        for n in nfs:
            if n not in names:
                fields.append(StructField(n, nfs[n]))
        return StructType(fields)

    elif isinstance(a, ArrayType):
        return ArrayType(
            _merge_type(
                a.elementType, cast(ArrayType, b).elementType, name="element in array %s" % name
            ),
            True,
        )

    elif isinstance(a, MapType):
        return MapType(
            _merge_type(a.keyType, cast(MapType, b).keyType, name="key of map %s" % name),
            _merge_type(a.valueType, cast(MapType, b).valueType, name="value of map %s" % name),
            True,
        )
    else:
        return a


def _need_converter(dataType: DataType) -> bool:
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


def _create_converter(dataType: DataType) -> Callable:
    """Create a converter to drop the names of fields in obj"""
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

    def convert_struct(obj: Any) -> Optional[Tuple]:
        if obj is None:
            return None

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
    ByteType: (int,),
    ShortType: (int,),
    IntegerType: (int,),
    LongType: (int,),
    FloatType: (float,),
    DoubleType: (float,),
    DecimalType: (decimal.Decimal,),
    StringType: (str,),
    BinaryType: (bytearray, bytes),
    DateType: (datetime.date, datetime.datetime),
    TimestampType: (datetime.datetime,),
    TimestampNTZType: (datetime.datetime,),
    DayTimeIntervalType: (datetime.timedelta,),
    ArrayType: (list, tuple, array),
    MapType: (dict,),
    StructType: (tuple, list, dict),
}


def _make_type_verifier(
    dataType: DataType,
    nullable: bool = True,
    name: Optional[str] = None,
) -> Callable:
    """
    Make a verifier that checks the type of obj against dataType and raises a TypeError if they do
    not match.

    This verifier also checks the value of obj against datatype and raises a ValueError if it's not
    within the allowed range, e.g. using 128 as ByteType will overflow. Note that, Python float is
    not checked, so it will become infinity when cast to Java float, if it overflows.

    Examples
    --------
    >>> _make_type_verifier(StructType([]))(None)
    >>> _make_type_verifier(StringType())("")
    >>> _make_type_verifier(LongType())(0)
    >>> _make_type_verifier(LongType())(1 << 64) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError:...
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

    def verify_nullability(obj: Any) -> bool:
        if obj is None:
            if nullable:
                return True
            else:
                raise ValueError(new_msg("This field is not nullable, but got None"))
        else:
            return False

    _type = type(dataType)

    def assert_acceptable_types(obj: Any) -> None:
        assert _type in _acceptable_types, new_msg(
            "unknown datatype: %s for object %r" % (dataType, obj)
        )

    def verify_acceptable_types(obj: Any) -> None:
        # subclass of them can not be fromInternal in JVM
        if type(obj) not in _acceptable_types[_type]:  # type: ignore[operator]
            raise TypeError(
                new_msg("%s can not accept object %r in type %s" % (dataType, obj, type(obj)))
            )

    if isinstance(dataType, StringType):
        # StringType can work with any types
        verify_value = lambda _: _

    elif isinstance(dataType, UserDefinedType):
        verifier = _make_type_verifier(dataType.sqlType(), name=name)

        def verify_udf(obj: Any) -> None:
            if not (hasattr(obj, "__UDT__") and obj.__UDT__ == dataType):
                raise ValueError(new_msg("%r is not an instance of type %r" % (obj, dataType)))
            verifier(dataType.toInternal(obj))

        verify_value = verify_udf

    elif isinstance(dataType, ByteType):

        def verify_byte(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -128 or obj > 127:
                raise ValueError(new_msg("object of ByteType out of range, got: %s" % obj))

        verify_value = verify_byte

    elif isinstance(dataType, ShortType):

        def verify_short(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -32768 or obj > 32767:
                raise ValueError(new_msg("object of ShortType out of range, got: %s" % obj))

        verify_value = verify_short

    elif isinstance(dataType, IntegerType):

        def verify_integer(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -2147483648 or obj > 2147483647:
                raise ValueError(new_msg("object of IntegerType out of range, got: %s" % obj))

        verify_value = verify_integer

    elif isinstance(dataType, LongType):

        def verify_long(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -9223372036854775808 or obj > 9223372036854775807:
                raise ValueError(new_msg("object of LongType out of range, got: %s" % obj))

        verify_value = verify_long

    elif isinstance(dataType, ArrayType):
        element_verifier = _make_type_verifier(
            dataType.elementType, dataType.containsNull, name="element in array %s" % name
        )

        def verify_array(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            for i in obj:
                element_verifier(i)

        verify_value = verify_array

    elif isinstance(dataType, MapType):
        key_verifier = _make_type_verifier(dataType.keyType, False, name="key of map %s" % name)
        value_verifier = _make_type_verifier(
            dataType.valueType, dataType.valueContainsNull, name="value of map %s" % name
        )

        def verify_map(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            for k, v in obj.items():
                key_verifier(k)
                value_verifier(v)

        verify_value = verify_map

    elif isinstance(dataType, StructType):
        verifiers = []
        for f in dataType.fields:
            verifier = _make_type_verifier(
                f.dataType, f.nullable, name=new_name(f.name)
            )  # type: ignore[arg-type]
            verifiers.append((f.name, verifier))

        def verify_struct(obj: Any) -> None:
            assert_acceptable_types(obj)

            if isinstance(obj, dict):
                for f, verifier in verifiers:
                    verifier(obj.get(f))
            elif isinstance(obj, (tuple, list)):
                if len(obj) != len(verifiers):
                    raise ValueError(
                        new_msg(
                            "Length of object (%d) does not match with "
                            "length of fields (%d)" % (len(obj), len(verifiers))
                        )
                    )
                for v, (_, verifier) in zip(obj, verifiers):
                    verifier(v)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                for f, verifier in verifiers:
                    verifier(d.get(f))
            else:
                raise TypeError(
                    new_msg("StructType can not accept object %r in type %s" % (obj, type(obj)))
                )

        verify_value = verify_struct

    else:

        def verify_default(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)

        verify_value = verify_default

    def verify(obj: Any) -> None:
        if not verify_nullability(obj):
            verify_value(obj)

    return verify


# This is used to unpickle a Row from JVM
def _create_row_inbound_converter(dataType: DataType) -> Callable:
    return lambda *a: dataType.fromInternal(a)


def _create_row(
    fields: Union["Row", List[str]], values: Union[Tuple[Any, ...], List[Any]]
) -> "Row":
    row = Row(*values)
    row.__fields__ = fields
    return row


class Row(tuple):

    """
    A row in :class:`DataFrame`.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments.
    It is not allowed to omit a named argument to represent that the value is
    None or missing. This should be explicitly set to None in this case.

    .. versionchanged:: 3.0.0
        Rows created from named arguments no longer have
        field names sorted alphabetically and will be ordered in the position as
        entered.

    Examples
    --------
    >>> row = Row(name="Alice", age=11)
    >>> row
    Row(name='Alice', age=11)
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
    <Row('name', 'age')>
    >>> 'name' in Person
    True
    >>> 'wrong_key' in Person
    False
    >>> Person("Alice", 11)
    Row(name='Alice', age=11)

    This form can also be used to create rows as tuple values, i.e. with unnamed
    fields.

    >>> row1 = Row("Alice", 11)
    >>> row2 = Row(name="Alice", age=11)
    >>> row1 == row2
    True
    """

    @overload
    def __new__(cls, *args: str) -> "Row":
        ...

    @overload
    def __new__(cls, **kwargs: Any) -> "Row":
        ...

    def __new__(cls, *args: Optional[str], **kwargs: Optional[Any]) -> "Row":
        if args and kwargs:
            raise ValueError("Can not use both args " "and kwargs to create Row")
        if kwargs:
            # create row objects
            row = tuple.__new__(cls, list(kwargs.values()))
            row.__fields__ = list(kwargs.keys())
            return row
        else:
            # create row class or objects
            return tuple.__new__(cls, args)

    def asDict(self, recursive: bool = False) -> Dict[str, Any]:
        """
        Return as a dict

        Parameters
        ----------
        recursive : bool, optional
            turns the nested Rows to dict (default: False).

        Notes
        -----
        If a row contains duplicate field names, e.g., the rows of a join
        between two :class:`DataFrame` that both have the fields of same names,
        one of the duplicate fields will be selected by ``asDict``. ``__getitem__``
        will also return one of the duplicate fields, however returned value might
        be different to ``asDict``.

        Examples
        --------
        >>> Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}
        True
        >>> row = Row(key=1, value=Row(name='a', age=2))
        >>> row.asDict() == {'key': 1, 'value': Row(name='a', age=2)}
        True
        >>> row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
        True
        """
        if not hasattr(self, "__fields__"):
            raise TypeError("Cannot convert a Row class into dict")

        if recursive:

            def conv(obj: Any) -> Any:
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

    def __contains__(self, item: Any) -> bool:
        if hasattr(self, "__fields__"):
            return item in self.__fields__
        else:
            return super(Row, self).__contains__(item)

    # let object acts like class
    def __call__(self, *args: Any) -> "Row":
        """create new Row object"""
        if len(args) > len(self):
            raise ValueError(
                "Can not create Row with fields %s, expected %d values "
                "but got %s" % (self, len(self), args)
            )
        return _create_row(self, args)

    def __getitem__(self, item: Any) -> Any:
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

    def __getattr__(self, item: str) -> Any:
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

    def __setattr__(self, key: Any, value: Any) -> None:
        if key != "__fields__":
            raise RuntimeError("Row is read-only")
        self.__dict__[key] = value

    def __reduce__(
        self,
    ) -> Union[str, Tuple[Any, ...]]:
        """Returns a tuple so Python knows how to pickle Row."""
        if hasattr(self, "__fields__"):
            return (_create_row, (self.__fields__, tuple(self)))
        else:
            return tuple.__reduce__(self)

    def __repr__(self) -> str:
        """Printable representation of Row used in Python REPL."""
        if hasattr(self, "__fields__"):
            return "Row(%s)" % ", ".join(
                "%s=%r" % (k, v) for k, v in zip(self.__fields__, tuple(self))
            )
        else:
            return "<Row(%s)>" % ", ".join("%r" % field for field in self)


class DateConverter:
    def can_convert(self, obj: Any) -> bool:
        return isinstance(obj, datetime.date)

    def convert(self, obj: datetime.date, gateway_client: JavaGateway) -> JavaObject:
        Date = JavaClass("java.sql.Date", gateway_client)
        return Date.valueOf(obj.strftime("%Y-%m-%d"))


class DatetimeConverter:
    def can_convert(self, obj: Any) -> bool:
        return isinstance(obj, datetime.datetime)

    def convert(self, obj: datetime.datetime, gateway_client: JavaGateway) -> JavaObject:
        Timestamp = JavaClass("java.sql.Timestamp", gateway_client)
        seconds = (
            calendar.timegm(obj.utctimetuple()) if obj.tzinfo else time.mktime(obj.timetuple())
        )
        t = Timestamp(int(seconds) * 1000)
        t.setNanos(obj.microsecond * 1000)
        return t


class DatetimeNTZConverter:
    def can_convert(self, obj: Any) -> bool:
        from pyspark.sql.utils import is_timestamp_ntz_preferred

        return (
            isinstance(obj, datetime.datetime)
            and obj.tzinfo is None
            and is_timestamp_ntz_preferred()
        )

    def convert(self, obj: datetime.datetime, gateway_client: JavaGateway) -> JavaObject:
        from pyspark import SparkContext

        seconds = calendar.timegm(obj.utctimetuple())
        jvm = SparkContext._jvm  # type: ignore[attr-defined]
        return jvm.org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToLocalDateTime(
            int(seconds) * 1000000 + obj.microsecond
        )


class DayTimeIntervalTypeConverter:
    def can_convert(self, obj: Any) -> bool:
        return isinstance(obj, datetime.timedelta)

    def convert(self, obj: datetime.timedelta, gateway_client: JavaGateway) -> JavaObject:
        from pyspark import SparkContext

        jvm = SparkContext._jvm  # type: ignore[attr-defined]
        return jvm.org.apache.spark.sql.catalyst.util.IntervalUtils.microsToDuration(
            (math.floor(obj.total_seconds()) * 1000000) + obj.microseconds
        )


# datetime is a subclass of date, we should register DatetimeConverter first
register_input_converter(DatetimeNTZConverter())
register_input_converter(DatetimeConverter())
register_input_converter(DateConverter())
register_input_converter(DayTimeIntervalTypeConverter())


def _test() -> None:
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession

    globs = globals()
    sc = SparkContext("local[4]", "PythonTest")
    globs["sc"] = sc
    globs["spark"] = SparkSession.builder.getOrCreate()
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs["sc"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
