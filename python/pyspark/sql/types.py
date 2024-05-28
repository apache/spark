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
from functools import reduce
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
    TYPE_CHECKING,
)

from pyspark.util import is_remote_only, JVM_INT_MAX
from pyspark.serializers import CloudPickleSerializer
from pyspark.sql.utils import (
    has_numpy,
    get_active_spark_context,
    escape_meta_characters,
    StringConcat,
)
from pyspark.sql.variant_utils import VariantUtils
from pyspark.errors import (
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
    PySparkIndexError,
    PySparkRuntimeError,
    PySparkAttributeError,
    PySparkKeyError,
)

if has_numpy:
    import numpy as np

if TYPE_CHECKING:
    import numpy as np
    from py4j.java_gateway import GatewayClient, JavaGateway, JavaClass

T = TypeVar("T")
U = TypeVar("U")

__all__ = [
    "DataType",
    "NullType",
    "CharType",
    "StringType",
    "VarcharType",
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
    "YearMonthIntervalType",
    "CalendarIntervalType",
    "Row",
    "ShortType",
    "ArrayType",
    "MapType",
    "StructField",
    "StructType",
    "VariantType",
    "VariantVal",
]


class DataType:
    """Base class for data types."""

    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            self_dict = {k: v for k, v in self.__dict__.items() if k != "typeName"}
            other_dict = {k: v for k, v in other.__dict__.items() if k != "typeName"}
            return self_dict == other_dict
        return False

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    @classmethod
    def typeName(cls) -> str:
        return cls.__name__[:-4].lower()

    # The classmethod 'typeName' is not always consistent with the Scala side, e.g.
    # DecimalType(10, 2): 'decimal' vs 'decimal(10, 2)'
    # This method is used in subclass initializer to replace 'typeName' if they are different.
    def _type_name(self) -> str:
        return self.__class__.__name__.removesuffix("Type").removesuffix("UDT").lower()

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

    def _as_nullable(self) -> "DataType":
        return self

    @classmethod
    def fromDDL(cls, ddl: str) -> "DataType":
        """
        Creates :class:`DataType` for a given DDL-formatted string.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        ddl : str
            DDL-formatted string representation of types, e.g.
            :class:`pyspark.sql.types.DataType.simpleString`, except that top level struct
            type can omit the ``struct<>`` for the compatibility reason with
            ``spark.createDataFrame`` and Python UDFs.

        Returns
        -------
        :class:`DataType`

        Examples
        --------
        Create a StructType by the corresponding DDL formatted string.

        >>> from pyspark.sql.types import DataType
        >>> DataType.fromDDL("b string, a int")
        StructType([StructField('b', StringType(), True), StructField('a', IntegerType(), True)])

        Create a single DataType by the corresponding DDL formatted string.

        >>> DataType.fromDDL("decimal(10,10)")
        DecimalType(10,10)

        Create a StructType by the legacy string format.

        >>> DataType.fromDDL("b: string, a: int")
        StructType([StructField('b', StringType(), True), StructField('a', IntegerType(), True)])
        """
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import udf

        # Intentionally uses SparkSession so one implementation can be shared with/without
        # Spark Connect.
        schema = (
            SparkSession.active().range(0).select(udf(lambda x: x, returnType=ddl)("id")).schema
        )
        assert len(schema) == 1
        return schema[0].dataType

    @classmethod
    def _data_type_build_formatted_string(
        cls,
        dataType: "DataType",
        prefix: str,
        stringConcat: StringConcat,
        maxDepth: int,
    ) -> None:
        if isinstance(dataType, (ArrayType, StructType, MapType)):
            dataType._build_formatted_string(prefix, stringConcat, maxDepth - 1)


# This singleton pattern does not work with pickle, you will get
# another object after pickle and unpickle
class DataTypeSingleton(type):
    """Metaclass for DataType"""

    _instances: ClassVar[Dict[Type["DataTypeSingleton"], "DataTypeSingleton"]] = {}

    def __call__(cls: Type[T]) -> T:
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


class StringType(AtomicType):
    """String data type.

    Parameters
    ----------
    collation : str
        name of the collation, default is UTF8_BINARY.
    """

    collationNames = ["UTF8_BINARY", "UTF8_BINARY_LCASE", "UNICODE", "UNICODE_CI"]
    providerSpark = "spark"
    providerICU = "icu"
    providers = [providerSpark, providerICU]

    def __init__(self, collation: Optional[str] = None):
        self.typeName = self._type_name  # type: ignore[method-assign]
        self.collationId = 0 if collation is None else self.collationNameToId(collation)

    @classmethod
    def fromCollationId(self, collationId: int) -> "StringType":
        return StringType(StringType.collationNames[collationId])

    @classmethod
    def collationIdToName(cls, collationId: int) -> str:
        return StringType.collationNames[collationId]

    @classmethod
    def collationNameToId(cls, collationName: str) -> int:
        return StringType.collationNames.index(collationName)

    @classmethod
    def collationProvider(cls, collationName: str) -> str:
        # TODO: do this properly like on the scala side
        if collationName.startswith("UTF8"):
            return StringType.providerSpark
        return StringType.providerICU

    def _type_name(self) -> str:
        if self.isUTF8BinaryCollation():
            return "string"

        return f"string collate ${self.collationIdToName(self.collationId)}"

    # For backwards compatibility and compatibility with other readers all string types
    # are serialized in json as regular strings and the collation info is written to
    # struct field metadata
    def jsonValue(self) -> str:
        return "string"

    def __repr__(self) -> str:
        return (
            "StringType('%s')" % StringType.collationNames[self.collationId]
            if self.collationId != 0
            else "StringType()"
        )

    def isUTF8BinaryCollation(self) -> bool:
        return self.collationId == 0


class CharType(AtomicType):
    """Char data type

    Parameters
    ----------
    length : int
        the length limitation.
    """

    def __init__(self, length: int):
        self.typeName = self._type_name  # type: ignore[method-assign]
        self.length = length

    def _type_name(self) -> str:
        return "char(%d)" % (self.length)

    def __repr__(self) -> str:
        return "CharType(%d)" % (self.length)


class VarcharType(AtomicType):
    """Varchar data type

    Parameters
    ----------
    length : int
        the length limitation.
    """

    def __init__(self, length: int):
        self.typeName = self._type_name  # type: ignore[method-assign]
        self.length = length

    def _type_name(self) -> str:
        return "varchar(%d)" % (self.length)

    def __repr__(self) -> str:
        return "VarcharType(%d)" % (self.length)


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
        self.typeName = self._type_name  # type: ignore[method-assign]
        self.precision = precision
        self.scale = scale
        self.hasPrecisionInfo = True  # this is a public API

    def _type_name(self) -> str:
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
    """Byte data type, representing signed 8-bit integers."""

    def simpleString(self) -> str:
        return "tinyint"


class IntegerType(IntegralType):
    """Int data type, representing signed 32-bit integers."""

    def simpleString(self) -> str:
        return "int"


class LongType(IntegralType):
    """Long data type, representing signed 64-bit integers.

    If the values are beyond the range of [-9223372036854775808, 9223372036854775807],
    please use :class:`DecimalType`.
    """

    def simpleString(self) -> str:
        return "bigint"


class ShortType(IntegralType):
    """Short data type, representing signed 16-bit integers."""

    def simpleString(self) -> str:
        return "smallint"


class AnsiIntervalType(AtomicType):
    """The interval type which conforms to the ANSI SQL standard."""

    pass


class DayTimeIntervalType(AnsiIntervalType):
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
        self.typeName = self._type_name  # type: ignore[method-assign]
        if startField is None and endField is None:
            # Default matched to scala side.
            startField = DayTimeIntervalType.DAY
            endField = DayTimeIntervalType.SECOND
        elif startField is not None and endField is None:
            endField = startField

        fields = DayTimeIntervalType._fields
        if startField not in fields.keys() or endField not in fields.keys():
            raise PySparkRuntimeError(
                error_class="INVALID_INTERVAL_CASTING",
                message_parameters={"start_field": str(startField), "end_field": str(endField)},
            )
        self.startField = startField
        self.endField = endField

    def _type_name(self) -> str:
        fields = DayTimeIntervalType._fields
        start_field_name = fields[self.startField]
        end_field_name = fields[self.endField]
        if start_field_name == end_field_name:
            return "interval %s" % start_field_name
        else:
            return "interval %s to %s" % (start_field_name, end_field_name)

    def __repr__(self) -> str:
        return "%s(%d, %d)" % (type(self).__name__, self.startField, self.endField)

    def needConversion(self) -> bool:
        return True

    def toInternal(self, dt: datetime.timedelta) -> Optional[int]:
        if dt is not None:
            return (((dt.days * 86400) + dt.seconds) * 1_000_000) + dt.microseconds

    def fromInternal(self, micros: int) -> Optional[datetime.timedelta]:
        if micros is not None:
            return datetime.timedelta(microseconds=micros)


class YearMonthIntervalType(AnsiIntervalType):
    """YearMonthIntervalType, represents year-month intervals of the SQL standard"""

    YEAR = 0
    MONTH = 1

    _fields = {
        YEAR: "year",
        MONTH: "month",
    }

    _inverted_fields = dict(zip(_fields.values(), _fields.keys()))

    def __init__(self, startField: Optional[int] = None, endField: Optional[int] = None):
        self.typeName = self._type_name  # type: ignore[method-assign]
        if startField is None and endField is None:
            # Default matched to scala side.
            startField = YearMonthIntervalType.YEAR
            endField = YearMonthIntervalType.MONTH
        elif startField is not None and endField is None:
            endField = startField

        fields = YearMonthIntervalType._fields
        if startField not in fields.keys() or endField not in fields.keys():
            raise PySparkRuntimeError(
                error_class="INVALID_INTERVAL_CASTING",
                message_parameters={"start_field": str(startField), "end_field": str(endField)},
            )
        self.startField = startField
        self.endField = endField

    def _type_name(self) -> str:
        fields = YearMonthIntervalType._fields
        start_field_name = fields[self.startField]
        end_field_name = fields[self.endField]
        if start_field_name == end_field_name:
            return "interval %s" % start_field_name
        else:
            return "interval %s to %s" % (start_field_name, end_field_name)

    def __repr__(self) -> str:
        return "%s(%d, %d)" % (type(self).__name__, self.startField, self.endField)


class CalendarIntervalType(DataType, metaclass=DataTypeSingleton):
    """The data type representing calendar intervals.

    The calendar interval is stored internally in three components:
    - an integer value representing the number of `months` in this interval.
    - an integer value representing the number of `days` in this interval.
    - a long value representing the number of `microseconds` in this interval.
    """

    @classmethod
    def typeName(cls) -> str:
        return "interval"


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
    >>> from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    The below example demonstrates how to create class:`ArrayType`:

    >>> arr = ArrayType(StringType())

    The array can contain null (None) values by default:

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

    def _as_nullable(self) -> "ArrayType":
        return ArrayType(self.elementType._as_nullable(), containsNull=True)

    def toNullable(self) -> "ArrayType":
        """
        Returns the same data type but set all nullability fields are true
        (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).

        .. versionadded:: 4.0.0

        Returns
        -------
        :class:`ArrayType`

        Examples
        --------
        Example 1: Simple nullability conversion

        >>> ArrayType(IntegerType(), containsNull=False).toNullable()
        ArrayType(IntegerType(), True)

        Example 2: Nested nullability conversion

        >>> ArrayType(
        ...     StructType([
        ...         StructField("b", IntegerType(), nullable=False),
        ...         StructField("c", ArrayType(IntegerType(), containsNull=False))
        ...     ]),
        ...     containsNull=False
        ... ).toNullable()
        ArrayType(StructType([StructField('b', IntegerType(), True),
        StructField('c', ArrayType(IntegerType(), True), True)]), True)
        """
        return self._as_nullable()

    def __repr__(self) -> str:
        return "ArrayType(%s, %s)" % (self.elementType, str(self.containsNull))

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "type": self.typeName(),
            "elementType": self.elementType.jsonValue(),
            "containsNull": self.containsNull,
        }

    @classmethod
    def fromJson(
        cls,
        json: Dict[str, Any],
        fieldPath: str,
        collationsMap: Optional[Dict[str, str]],
    ) -> "ArrayType":
        elementType = _parse_datatype_json_value(
            json["elementType"], fieldPath + ".element", collationsMap
        )
        return ArrayType(elementType, json["containsNull"])

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

    def _build_formatted_string(
        self,
        prefix: str,
        stringConcat: StringConcat,
        maxDepth: int = JVM_INT_MAX,
    ) -> None:
        if maxDepth > 0:
            stringConcat.append(
                f"{prefix}-- element: {self.elementType.typeName()} "
                + f"(containsNull = {str(self.containsNull).lower()})\n"
            )
            DataType._data_type_build_formatted_string(
                self.elementType, f"{prefix}    |", stringConcat, maxDepth
            )


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
    >>> from pyspark.sql.types import IntegerType, FloatType, MapType, StringType

    The below example demonstrates how to create class:`MapType`:

    >>> map_type = MapType(StringType(), IntegerType())

    The values of the map can contain null (``None``) values by default:

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

    def _as_nullable(self) -> "MapType":
        return MapType(
            self.keyType._as_nullable(), self.valueType._as_nullable(), valueContainsNull=True
        )

    def toNullable(self) -> "MapType":
        """
        Returns the same data type but set all nullability fields are true
        (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).

        .. versionadded:: 4.0.0

        Returns
        -------
        :class:`MapType`

        Examples
        --------
        Example 1: Simple nullability conversion

        >>> MapType(IntegerType(), StringType(), valueContainsNull=False).toNullable()
        MapType(IntegerType(), StringType(), True)

        Example 2: Nested nullability conversion

        >>> MapType(
        ...     StringType(),
        ...     MapType(
        ...         IntegerType(),
        ...         ArrayType(IntegerType(), containsNull=False),
        ...         valueContainsNull=False
        ...     ),
        ...     valueContainsNull=False
        ... ).toNullable()
        MapType(StringType(), MapType(IntegerType(), ArrayType(IntegerType(), True), True), True)
        """
        return self._as_nullable()

    def __repr__(self) -> str:
        return "MapType(%s, %s, %s)" % (self.keyType, self.valueType, str(self.valueContainsNull))

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "type": self.typeName(),
            "keyType": self.keyType.jsonValue(),
            "valueType": self.valueType.jsonValue(),
            "valueContainsNull": self.valueContainsNull,
        }

    @classmethod
    def fromJson(
        cls,
        json: Dict[str, Any],
        fieldPath: str,
        collationsMap: Optional[Dict[str, str]],
    ) -> "MapType":
        keyType = _parse_datatype_json_value(json["keyType"], fieldPath + ".key", collationsMap)
        valueType = _parse_datatype_json_value(
            json["valueType"], fieldPath + ".value", collationsMap
        )
        return MapType(
            keyType,
            valueType,
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

    def _build_formatted_string(
        self,
        prefix: str,
        stringConcat: StringConcat,
        maxDepth: int = JVM_INT_MAX,
    ) -> None:
        if maxDepth > 0:
            stringConcat.append(f"{prefix}-- key: {self.keyType.typeName()}\n")
            DataType._data_type_build_formatted_string(
                self.keyType, f"{prefix}    |", stringConcat, maxDepth
            )
            stringConcat.append(
                f"{prefix}-- value: {self.valueType.typeName()} "
                + f"(valueContainsNull = {str(self.valueContainsNull).lower()})\n"
            )
            DataType._data_type_build_formatted_string(
                self.valueType, f"{prefix}    |", stringConcat, maxDepth
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
    >>> from pyspark.sql.types import StringType, StructField
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
        return "StructField('%s', %s, %s)" % (self.name, self.dataType, str(self.nullable))

    def jsonValue(self) -> Dict[str, Any]:
        collationMetadata = self.getCollationMetadata()
        metadata = (
            self.metadata
            if not collationMetadata
            else {**self.metadata, _COLLATIONS_METADATA_KEY: collationMetadata}
        )

        return {
            "name": self.name,
            "type": self.dataType.jsonValue(),
            "nullable": self.nullable,
            "metadata": metadata,
        }

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> "StructField":
        metadata = json.get("metadata")
        collationsMap = {}
        if metadata and _COLLATIONS_METADATA_KEY in metadata:
            collationsMap = metadata[_COLLATIONS_METADATA_KEY]
            for key, value in collationsMap.items():
                nameParts = value.split(".")
                assert len(nameParts) == 2
                provider, name = nameParts[0], nameParts[1]
                _assert_valid_collation_provider(provider)
                collationsMap[key] = name

            metadata = {
                key: value for key, value in metadata.items() if key != _COLLATIONS_METADATA_KEY
            }

        return StructField(
            json["name"],
            _parse_datatype_json_value(json["type"], json["name"], collationsMap),
            json.get("nullable", True),
            metadata,
        )

    def getCollationsMap(self, metadata: Dict[str, Any]) -> Dict[str, str]:
        if not metadata or _COLLATIONS_METADATA_KEY not in metadata:
            return {}

        collationMetadata: Dict[str, str] = metadata[_COLLATIONS_METADATA_KEY]
        collationsMap: Dict[str, str] = {}

        for key, value in collationMetadata.items():
            nameParts = value.split(".")
            assert len(nameParts) == 2
            provider, name = nameParts[0], nameParts[1]
            _assert_valid_collation_provider(provider)
            collationsMap[key] = name

        return collationsMap

    def getCollationMetadata(self) -> Dict[str, str]:
        def visitRecursively(dt: DataType, fieldPath: str) -> None:
            if isinstance(dt, ArrayType):
                processDataType(dt.elementType, fieldPath + ".element")
            elif isinstance(dt, MapType):
                processDataType(dt.keyType, fieldPath + ".key")
                processDataType(dt.valueType, fieldPath + ".value")
            elif isinstance(dt, StringType) and self._isCollatedString(dt):
                collationMetadata[fieldPath] = self.schemaCollationValue(dt)

        def processDataType(dt: DataType, fieldPath: str) -> None:
            if self._isCollatedString(dt):
                collationMetadata[fieldPath] = self.schemaCollationValue(dt)
            else:
                visitRecursively(dt, fieldPath)

        collationMetadata: Dict[str, str] = {}
        visitRecursively(self.dataType, self.name)
        return collationMetadata

    def _isCollatedString(self, dt: DataType) -> bool:
        return isinstance(dt, StringType) and not dt.isUTF8BinaryCollation()

    def schemaCollationValue(self, dt: DataType) -> str:
        assert isinstance(dt, StringType)
        collationName = StringType.collationIdToName(dt.collationId)
        provider = StringType.collationProvider(collationName)
        return f"{provider}.{collationName}"

    def needConversion(self) -> bool:
        return self.dataType.needConversion()

    def toInternal(self, obj: T) -> T:
        return self.dataType.toInternal(obj)

    def fromInternal(self, obj: T) -> T:
        return self.dataType.fromInternal(obj)

    def typeName(self) -> str:  # type: ignore[override]
        raise PySparkTypeError(
            error_class="INVALID_TYPENAME_CALL",
            message_parameters={},
        )

    def _build_formatted_string(
        self,
        prefix: str,
        stringConcat: StringConcat,
        maxDepth: int = JVM_INT_MAX,
    ) -> None:
        if maxDepth > 0:
            stringConcat.append(
                f"{prefix}-- {escape_meta_characters(self.name)}: {self.dataType.typeName()} "
                + f"(nullable = {str(self.nullable).lower()})\n"
            )
            DataType._data_type_build_formatted_string(
                self.dataType, f"{prefix}    |", stringConcat, maxDepth
            )


class StructType(DataType):
    """Struct type, consisting of a list of :class:`StructField`.

    This is the data type representing a :class:`Row`.

    Iterating a :class:`StructType` will iterate over its :class:`StructField`\\s.
    A contained :class:`StructField` can be accessed by its name or position.

    Examples
    --------
    >>> from pyspark.sql.types import *
    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct1["f1"]
    StructField('f1', StringType(), True)
    >>> struct1[0]
    StructField('f1', StringType(), True)

    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct2 = StructType([StructField("f1", StringType(), True)])
    >>> struct1 == struct2
    True
    >>> struct1 = StructType([StructField("f1", CharType(10), True)])
    >>> struct2 = StructType([StructField("f1", CharType(10), True)])
    >>> struct1 == struct2
    True
    >>> struct1 = StructType([StructField("f1", VarcharType(10), True)])
    >>> struct2 = StructType([StructField("f1", VarcharType(10), True)])
    >>> struct1 == struct2
    True
    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct2 = StructType([StructField("f1", StringType(), True),
    ...     StructField("f2", IntegerType(), False)])
    >>> struct1 == struct2
    False

    The below example demonstrates how to create a DataFrame based on a struct created
    using class:`StructType` and class:`StructField`:

    >>> data = [("Alice", ["Java", "Scala"]), ("Bob", ["Python", "Scala"])]
    >>> schema = StructType([
    ...     StructField("name", StringType()),
    ...     StructField("languagesSkills", ArrayType(StringType())),
    ... ])
    >>> df = spark.createDataFrame(data=data, schema=schema)
    >>> df.printSchema()
    root
     |-- name: string (nullable = true)
     |-- languagesSkills: array (nullable = true)
     |    |-- element: string (containsNull = true)
    >>> df.show()
    +-----+---------------+
    | name|languagesSkills|
    +-----+---------------+
    |Alice|  [Java, Scala]|
    |  Bob|[Python, Scala]|
    +-----+---------------+
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
        >>> from pyspark.sql.types import IntegerType, StringType, StructField, StructType
        >>> struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        >>> struct2 = StructType([StructField("f1", StringType(), True),
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
                raise PySparkValueError(
                    error_class="ARGUMENT_REQUIRED",
                    message_parameters={
                        "arg_name": "data_type",
                        "condition": "passing name of struct_field to create",
                    },
                )

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
            raise PySparkKeyError(
                error_class="KEY_NOT_EXISTS", message_parameters={"key": str(key)}
            )
        elif isinstance(key, int):
            try:
                return self.fields[key]
            except IndexError:
                raise PySparkIndexError(
                    error_class="INDEX_OUT_OF_RANGE",
                    message_parameters={"arg_name": "StructType", "index": str(key)},
                )
        elif isinstance(key, slice):
            return StructType(self.fields[key])
        else:
            raise PySparkTypeError(
                error_class="NOT_INT_OR_SLICE_OR_STR",
                message_parameters={"arg_name": "key", "arg_type": type(key).__name__},
            )

    def simpleString(self) -> str:
        return "struct<%s>" % (",".join(f.simpleString() for f in self))

    def _as_nullable(self) -> "StructType":
        fields = []
        for field in self.fields:
            fields.append(
                StructField(
                    field.name,
                    field.dataType._as_nullable(),
                    nullable=True,
                    metadata=field.metadata,
                )
            )
        return StructType(fields)

    def toNullable(self) -> "StructType":
        """
        Returns the same data type but set all nullability fields are true
        (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).

        .. versionadded:: 4.0.0

        Returns
        -------
        :class:`StructType`

        Examples
        --------
        Example 1: Simple nullability conversion

        >>> StructType([StructField("a", IntegerType(), nullable=False)]).toNullable()
        StructType([StructField('a', IntegerType(), True)])

        Example 2: Nested nullability conversion

        >>> StructType([
        ...     StructField("a",
        ...         StructType([
        ...             StructField("b", IntegerType(), nullable=False),
        ...             StructField("c", StructType([
        ...                 StructField("d", IntegerType(), nullable=False)
        ...             ]))
        ...         ]),
        ...         nullable=False)
        ... ]).toNullable()
        StructType([StructField('a', StructType([StructField('b', IntegerType(), True),
        StructField('c', StructType([StructField('d', IntegerType(), True)]), True)]), True)])
        """
        return self._as_nullable()

    def __repr__(self) -> str:
        return "StructType([%s])" % ", ".join(str(field) for field in self)

    def jsonValue(self) -> Dict[str, Any]:
        return {"type": self.typeName(), "fields": [f.jsonValue() for f in self]}

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> "StructType":
        """
        Constructs :class:`StructType` from a schema defined in JSON format.

        Below is a JSON schema it must adhere to::

            {
              "title":"StructType",
              "description":"Schema of StructType in json format",
              "type":"object",
              "properties":{
                 "fields":{
                    "description":"Array of struct fields",
                    "type":"array",
                    "items":{
                        "type":"object",
                        "properties":{
                           "name":{
                              "description":"Name of the field",
                              "type":"string"
                           },
                           "type":{
                              "description": "Type of the field. Can either be
                                              another nested StructType or primitive type",
                              "type":"object/string"
                           },
                           "nullable":{
                              "description":"If nulls are allowed",
                              "type":"boolean"
                           },
                           "metadata":{
                              "description":"Additional metadata to supply",
                              "type":"object"
                           },
                           "required":[
                              "name",
                              "type",
                              "nullable",
                              "metadata"
                           ]
                        }
                   }
                }
             }
           }

        Parameters
        ----------
        json : dict or a dict-like object e.g. JSON object
            This "dict" must have "fields" key that returns an array of fields
            each of which must have specific keys (name, type, nullable, metadata).

        Returns
        -------
        :class:`StructType`

        Examples
        --------
        >>> json_str = '''
        ...  {
        ...      "fields": [
        ...          {
        ...              "metadata": {},
        ...              "name": "Person",
        ...              "nullable": true,
        ...              "type": {
        ...                  "fields": [
        ...                      {
        ...                          "metadata": {},
        ...                          "name": "name",
        ...                          "nullable": false,
        ...                          "type": "string"
        ...                      },
        ...                      {
        ...                          "metadata": {},
        ...                          "name": "surname",
        ...                          "nullable": false,
        ...                          "type": "string"
        ...                      }
        ...                  ],
        ...                  "type": "struct"
        ...              }
        ...          }
        ...      ],
        ...      "type": "struct"
        ...  }
        ...  '''
        >>> import json
        >>> scheme = StructType.fromJson(json.loads(json_str))
        >>> scheme.simpleString()
        'struct<Person:struct<name:string,surname:string>>'
        """
        return StructType([StructField.fromJson(f) for f in json["fields"]])

    def fieldNames(self) -> List[str]:
        """
        Returns all field names in a list.

        Examples
        --------
        >>> from pyspark.sql.types import StringType, StructField, StructType
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
                raise PySparkValueError(
                    error_class="UNEXPECTED_TUPLE_WITH_STRUCT",
                    message_parameters={"tuple": str(obj)},
                )
        else:
            if isinstance(obj, dict):
                return tuple(obj.get(n) for n in self.names)
            elif isinstance(obj, (list, tuple)):
                return tuple(obj)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(d.get(n) for n in self.names)
            else:
                raise PySparkValueError(
                    error_class="UNEXPECTED_TUPLE_WITH_STRUCT",
                    message_parameters={"tuple": str(obj)},
                )

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

    def _build_formatted_string(
        self,
        prefix: str,
        stringConcat: StringConcat,
        maxDepth: int = JVM_INT_MAX,
    ) -> None:
        for field in self.fields:
            field._build_formatted_string(prefix, stringConcat, maxDepth)

    def treeString(self, maxDepth: int = JVM_INT_MAX) -> str:
        stringConcat = StringConcat()
        stringConcat.append("root\n")
        prefix = " |"
        depth = maxDepth if maxDepth > 0 else JVM_INT_MAX
        for field in self.fields:
            field._build_formatted_string(prefix, stringConcat, depth)
        return stringConcat.toString()


class VariantType(AtomicType):
    """
    Variant data type, representing semi-structured values.

    .. versionadded:: 4.0.0
    """

    def needConversion(self) -> bool:
        return True

    def fromInternal(self, obj: Dict) -> Optional["VariantVal"]:
        if obj is None or not all(key in obj for key in ["value", "metadata"]):
            return None
        return VariantVal(obj["value"], obj["metadata"])


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
        raise PySparkNotImplementedError(
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "sqlType()"},
        )

    @classmethod
    def module(cls) -> str:
        """
        The Python module of the UDT.
        """
        raise PySparkNotImplementedError(
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "module()"},
        )

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
        raise PySparkNotImplementedError(
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "toInternal()"},
        )

    def deserialize(self, datum: Any) -> Any:
        """
        Converts a SQL datum into a user-type object.
        """
        raise PySparkNotImplementedError(
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "fromInternal()"},
        )

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


class VariantVal:
    """
    A class to represent a Variant value in Python.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    value : bytes
        The bytes representing the value component of the Variant.
    metadata : bytes
        The bytes representing the metadata component of the Variant.

    Methods
    -------
    toPython()
        Convert the VariantVal to a Python data structure.

    Examples
    --------
    >>> from pyspark.sql.functions import *
    >>> df = spark.createDataFrame([ {'json': '''{ "a" : 1 }'''} ])
    >>> v = df.select(parse_json(df.json).alias("var")).collect()[0].var
    >>> v.toPython()
    {'a': 1}
    """

    def __init__(self, value: bytes, metadata: bytes):
        self.value = value
        self.metadata = metadata

    def __str__(self) -> str:
        return VariantUtils.to_json(self.value, self.metadata)

    def __repr__(self) -> str:
        return "VariantVal(%r, %r)" % (self.value, self.metadata)

    def toPython(self) -> Any:
        """
        Convert the VariantVal to a Python data structure.

        Returns
        -------
        Any
            A Python object that represents the Variant.
        """
        return VariantUtils.to_python(self.value, self.metadata)

    def toJson(self, zone_id: str = "UTC") -> str:
        """
        Convert the VariantVal to a JSON string. The zone ID represents the time zone that the
        timestamp should be printed in. It is defaulted to UTC. The list of valid zone IDs can be
        found by importing the `zoneinfo` module and running :code:`zoneinfo.available_timezones()`.

        Returns
        -------
        str
            A JSON string that represents the Variant.
        """
        return VariantUtils.to_json(self.value, self.metadata, zone_id)


_atomic_types: List[Type[DataType]] = [
    StringType,
    CharType,
    VarcharType,
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
    VariantType,
    YearMonthIntervalType,
    DayTimeIntervalType,
]

_complex_types: List[Type[Union[ArrayType, MapType, StructType]]] = [
    ArrayType,
    MapType,
    StructType,
]
_all_complex_types: Dict[str, Type[Union[ArrayType, MapType, StructType]]] = {
    "array": ArrayType,
    "map": MapType,
    "struct": StructType,
}

# Datatypes that can be directly parsed by mapping a json string without regex.
# This dict should be only used in json parsing.
# Note that:
# 1, CharType and VarcharType are not listed here, since they need regex;
# 2, DecimalType can be parsed by both mapping ('decimal') and regex ('decimal(10, 2)');
# 3, CalendarIntervalType is not an atomic type, but can be mapped by 'interval';
_all_mappable_types: Dict[str, Type[DataType]] = {
    "string": StringType,
    "binary": BinaryType,
    "boolean": BooleanType,
    "decimal": DecimalType,
    "float": FloatType,
    "double": DoubleType,
    "byte": ByteType,
    "short": ShortType,
    "integer": IntegerType,
    "long": LongType,
    "date": DateType,
    "timestamp": TimestampType,
    "timestamp_ntz": TimestampNTZType,
    "void": NullType,
    "variant": VariantType,
    "interval": CalendarIntervalType,
}

_LENGTH_CHAR = re.compile(r"char\(\s*(\d+)\s*\)")
_LENGTH_VARCHAR = re.compile(r"varchar\(\s*(\d+)\s*\)")
_FIXED_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(-?\d+)\s*\)")
_INTERVAL_DAYTIME = re.compile(r"interval (day|hour|minute|second)( to (day|hour|minute|second))?")
_INTERVAL_YEARMONTH = re.compile(r"interval (year|month)( to (year|month))?")

_COLLATIONS_METADATA_KEY = "__COLLATIONS"


def _drop_metadata(d: Union[DataType, StructField]) -> Union[DataType, StructField]:
    assert isinstance(d, (DataType, StructField))
    if isinstance(d, StructField):
        return StructField(d.name, _drop_metadata(d.dataType), d.nullable, None)
    elif isinstance(d, StructType):
        return StructType([cast(StructField, _drop_metadata(f)) for f in d.fields])
    elif isinstance(d, ArrayType):
        return ArrayType(_drop_metadata(d.elementType), d.containsNull)
    elif isinstance(d, MapType):
        return MapType(_drop_metadata(d.keyType), _drop_metadata(d.valueType), d.valueContainsNull)
    return d


def _parse_datatype_string(s: str) -> DataType:
    """
    Parses the given data type string to a :class:`DataType`. The data type string format equals
    :class:`DataType.simpleString`, except that the top level struct type can omit
    the ``struct<>``. Since Spark 2.3, this also supports a schema in a DDL-formatted
    string and case-insensitive strings.

    Examples
    --------
    >>> _parse_datatype_string("int ")
    IntegerType()
    >>> _parse_datatype_string("INT ")
    IntegerType()
    >>> _parse_datatype_string("a: byte, b: decimal(  16 , 8   ) ")
    StructType([StructField('a', ByteType(), True), StructField('b', DecimalType(16,8), True)])
    >>> _parse_datatype_string("a DOUBLE, b STRING")
    StructType([StructField('a', DoubleType(), True), StructField('b', StringType(), True)])
    >>> _parse_datatype_string("a DOUBLE, b CHAR( 50 )")
    StructType([StructField('a', DoubleType(), True), StructField('b', CharType(50), True)])
    >>> _parse_datatype_string("a DOUBLE, b VARCHAR( 50 )")
    StructType([StructField('a', DoubleType(), True), StructField('b', VarcharType(50), True)])
    >>> _parse_datatype_string("a: array< short>")
    StructType([StructField('a', ArrayType(ShortType(), True), True)])
    >>> _parse_datatype_string(" map<string , string > ")
    MapType(StringType(), StringType(), True)

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
    from pyspark.sql.utils import is_remote

    if is_remote():
        from pyspark.sql.connect.session import SparkSession

        return cast(
            DataType,
            SparkSession.active()._client._analyze(method="ddl_parse", ddl_string=s).parsed,
        )

    else:
        from py4j.java_gateway import JVMView

        sc = get_active_spark_context()

        def from_ddl_schema(type_str: str) -> DataType:
            return _parse_datatype_json_string(
                cast(JVMView, sc._jvm)
                .org.apache.spark.sql.types.StructType.fromDDL(type_str)
                .json()
            )

        def from_ddl_datatype(type_str: str) -> DataType:
            return _parse_datatype_json_string(
                cast(JVMView, sc._jvm)
                .org.apache.spark.sql.api.python.PythonSQLUtils.parseDataType(type_str)
                .json()
            )

        try:
            # DDL format, "fieldname datatype, fieldname datatype".
            return from_ddl_schema(s)
        except Exception as e:
            try:
                # For backwards compatibility, "integer", "struct<fieldname: datatype>" and etc.
                return from_ddl_datatype(s)
            except BaseException:
                try:
                    # For backwards compatibility, "fieldname: datatype, fieldname: datatype" case.
                    return from_ddl_datatype("struct<%s>" % s.strip())
                except BaseException:
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
    ...
    >>> for cls in _all_mappable_types.values():
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
    ...     StructField("chars", CharType(10), False),
    ...     StructField("words", VarcharType(10), False),
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


def _parse_datatype_json_value(
    json_value: Union[dict, str],
    fieldPath: str = "",
    collationsMap: Optional[Dict[str, str]] = None,
) -> DataType:
    if not isinstance(json_value, dict):
        if json_value in _all_mappable_types.keys():
            if collationsMap is not None and fieldPath in collationsMap:
                _assert_valid_type_for_collation(fieldPath, json_value, collationsMap)
                collation_name = collationsMap[fieldPath]
                return StringType(collation_name)
            return _all_mappable_types[json_value]()
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
        elif _INTERVAL_YEARMONTH.match(json_value):
            m = _INTERVAL_YEARMONTH.match(json_value)
            inverted_fields = YearMonthIntervalType._inverted_fields
            first_field = inverted_fields.get(m.group(1))  # type: ignore[union-attr]
            second_field = inverted_fields.get(m.group(3))  # type: ignore[union-attr]
            if first_field is not None and second_field is None:
                return YearMonthIntervalType(first_field)
            return YearMonthIntervalType(first_field, second_field)
        elif _LENGTH_CHAR.match(json_value):
            m = _LENGTH_CHAR.match(json_value)
            return CharType(int(m.group(1)))  # type: ignore[union-attr]
        elif _LENGTH_VARCHAR.match(json_value):
            m = _LENGTH_VARCHAR.match(json_value)
            return VarcharType(int(m.group(1)))  # type: ignore[union-attr]
        else:
            raise PySparkValueError(
                error_class="CANNOT_PARSE_DATATYPE",
                message_parameters={"error": str(json_value)},
            )
    else:
        tpe = json_value["type"]
        if tpe in _all_complex_types:
            if collationsMap is not None and fieldPath in collationsMap:
                _assert_valid_type_for_collation(fieldPath, tpe, collationsMap)

            complex_type = _all_complex_types[tpe]
            if complex_type is ArrayType:
                return ArrayType.fromJson(json_value, fieldPath, collationsMap)
            elif complex_type is MapType:
                return MapType.fromJson(json_value, fieldPath, collationsMap)
            return StructType.fromJson(json_value)
        elif tpe == "udt":
            return UserDefinedType.fromJson(json_value)
        else:
            raise PySparkValueError(
                error_class="UNSUPPORTED_DATA_TYPE",
                message_parameters={"data_type": str(tpe)},
            )


def _assert_valid_type_for_collation(
    fieldPath: str, fieldType: Any, collationMap: Dict[str, str]
) -> None:
    if fieldPath in collationMap and fieldType != "string":
        raise PySparkTypeError(
            error_class="INVALID_JSON_DATA_TYPE_FOR_COLLATIONS",
            message_parameters={"jsonType": fieldType},
        )


def _assert_valid_collation_provider(provider: str) -> None:
    if provider.lower() not in StringType.providers:
        raise PySparkValueError(
            error_class="COLLATION_INVALID_PROVIDER",
            message_parameters={
                "provider": provider,
                "supportedProviders": ", ".join(StringType.providers),
            },
        )


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


def _from_numpy_type(nt: "np.dtype") -> Optional[DataType]:
    """Convert NumPy type to Spark data type."""
    import numpy as np

    if nt == np.dtype("int8"):
        return ByteType()
    elif nt == np.dtype("int16"):
        return ShortType()
    elif nt == np.dtype("int32"):
        return IntegerType()
    elif nt == np.dtype("int64"):
        return LongType()
    elif nt == np.dtype("float32"):
        return FloatType()
    elif nt == np.dtype("float64"):
        return DoubleType()

    return None


def _infer_type(
    obj: Any,
    infer_dict_as_struct: bool = False,
    infer_array_from_first_element: bool = False,
    infer_map_from_first_pair: bool = False,
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
    if dataType is YearMonthIntervalType:
        return YearMonthIntervalType()
    if dataType is CalendarIntervalType:
        return CalendarIntervalType()
    elif dataType is not None:
        return dataType()

    if isinstance(obj, dict):
        if infer_dict_as_struct:
            struct = StructType()
            for key, value in obj.items():
                if key is not None and value is not None:
                    struct.add(
                        key,
                        _infer_type(
                            value,
                            infer_dict_as_struct,
                            infer_array_from_first_element,
                            infer_map_from_first_pair,
                            prefer_timestamp_ntz,
                        ),
                        True,
                    )
            return struct
        elif infer_map_from_first_pair:
            for key, value in obj.items():
                if key is not None and value is not None:
                    return MapType(
                        _infer_type(
                            key,
                            infer_dict_as_struct,
                            infer_array_from_first_element,
                            infer_map_from_first_pair,
                            prefer_timestamp_ntz,
                        ),
                        _infer_type(
                            value,
                            infer_dict_as_struct,
                            infer_array_from_first_element,
                            infer_map_from_first_pair,
                            prefer_timestamp_ntz,
                        ),
                        True,
                    )
            return MapType(NullType(), NullType(), True)
        else:
            key_type: DataType = NullType()
            value_type: DataType = NullType()
            for key, value in obj.items():
                if key is not None:
                    key_type = _merge_type(
                        key_type,
                        _infer_type(
                            key,
                            infer_dict_as_struct,
                            infer_array_from_first_element,
                            infer_map_from_first_pair,
                            prefer_timestamp_ntz,
                        ),
                    )
                if value is not None:
                    value_type = _merge_type(
                        value_type,
                        _infer_type(
                            value,
                            infer_dict_as_struct,
                            infer_array_from_first_element,
                            infer_map_from_first_pair,
                            prefer_timestamp_ntz,
                        ),
                    )

            return MapType(key_type, value_type, True)
    elif isinstance(obj, list):
        if len(obj) > 0:
            if infer_array_from_first_element:
                return ArrayType(
                    _infer_type(
                        obj[0],
                        infer_dict_as_struct,
                        infer_array_from_first_element,
                        prefer_timestamp_ntz,
                    ),
                    True,
                )
            else:
                return ArrayType(
                    reduce(
                        _merge_type,
                        (
                            _infer_type(
                                v,
                                infer_dict_as_struct,
                                infer_array_from_first_element,
                                prefer_timestamp_ntz,
                            )
                            for v in obj
                        ),
                    ),
                    True,
                )
        return ArrayType(NullType(), True)
    elif isinstance(obj, array):
        if obj.typecode in _array_type_mappings:
            return ArrayType(_array_type_mappings[obj.typecode](), False)
        else:
            raise PySparkTypeError(
                error_class="UNSUPPORTED_DATA_TYPE",
                message_parameters={"data_type": f"array({obj.typecode})"},
            )
    else:
        try:
            return _infer_schema(
                obj,
                infer_dict_as_struct=infer_dict_as_struct,
                infer_array_from_first_element=infer_array_from_first_element,
                prefer_timestamp_ntz=prefer_timestamp_ntz,
            )
        except TypeError:
            raise PySparkTypeError(
                error_class="UNSUPPORTED_DATA_TYPE",
                message_parameters={"data_type": type(obj).__name__},
            )


def _infer_schema(
    row: Any,
    names: Optional[List[str]] = None,
    infer_dict_as_struct: bool = False,
    infer_array_from_first_element: bool = False,
    infer_map_from_first_pair: bool = False,
    prefer_timestamp_ntz: bool = False,
) -> StructType:
    """Infer the schema from dict/namedtuple/object"""
    items: Iterable[Tuple[str, Any]]
    if isinstance(row, dict):
        items = sorted(row.items())

    elif isinstance(row, (tuple, list)):
        if hasattr(row, "__fields__"):  # Row
            items = zip(row.__fields__, tuple(row))
        elif hasattr(row, "_fields"):  # namedtuple
            items = zip(row._fields, tuple(row))
        else:
            if names is None:
                names = ["_%d" % i for i in range(1, len(row) + 1)]
            elif len(names) < len(row):
                names.extend("_%d" % i for i in range(len(names) + 1, len(row) + 1))
            items = zip(names, row)

    elif hasattr(row, "__dict__"):  # object
        items = sorted(row.__dict__.items())

    else:
        raise PySparkTypeError(
            error_class="CANNOT_INFER_SCHEMA_FOR_TYPE",
            message_parameters={"data_type": type(row).__name__},
        )

    fields = []
    for k, v in items:
        try:
            fields.append(
                StructField(
                    k,
                    _infer_type(
                        v,
                        infer_dict_as_struct,
                        infer_array_from_first_element,
                        infer_map_from_first_pair,
                        prefer_timestamp_ntz,
                    ),
                    True,
                )
            )
        except TypeError:
            raise PySparkTypeError(
                error_class="CANNOT_INFER_TYPE_FOR_FIELD",
                message_parameters={"field_name": k},
            )
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


def _has_type(dt: DataType, dts: Union[type, Tuple[type, ...]]) -> bool:
    """Return whether there are specified types"""
    if isinstance(dt, dts):
        return True
    elif isinstance(dt, StructType):
        return any(_has_type(f.dataType, dts) for f in dt.fields)
    elif isinstance(dt, ArrayType):
        return _has_type(dt.elementType, dts)
    elif isinstance(dt, MapType):
        return _has_type(dt.keyType, dts) or _has_type(dt.valueType, dts)
    else:
        return False


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

        def new_msg(msg: str) -> str:
            return msg

        def new_name(n: str) -> str:
            return "field %s" % n

    else:

        def new_msg(msg: str) -> str:
            return "%s: %s" % (name, msg)

        def new_name(n: str) -> str:
            return "field %s in %s" % (n, name)

    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif isinstance(a, TimestampType) and isinstance(b, TimestampNTZType):
        return a
    elif isinstance(a, TimestampNTZType) and isinstance(b, TimestampType):
        return b
    elif isinstance(a, AtomicType) and isinstance(b, StringType):
        return b
    elif isinstance(a, StringType) and isinstance(b, AtomicType):
        return a
    elif type(a) is not type(b):
        # TODO: type cast (such as int -> long)
        raise PySparkTypeError(
            error_class="CANNOT_MERGE_TYPE",
            message_parameters={"data_type1": type(a).__name__, "data_type2": type(b).__name__},
        )

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
            raise PySparkTypeError(
                error_class="UNSUPPORTED_DATA_TYPE",
                message_parameters={"data_type": type(obj).__name__},
            )

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
    CharType: (str,),
    VarcharType: (str,),
    BinaryType: (bytearray, bytes),
    DateType: (datetime.date, datetime.datetime),
    TimestampType: (datetime.datetime,),
    TimestampNTZType: (datetime.datetime,),
    DayTimeIntervalType: (datetime.timedelta,),
    ArrayType: (list, tuple, array),
    MapType: (dict,),
    StructType: (tuple, list, dict),
    VariantType: (
        bool,
        int,
        float,
        decimal.Decimal,
        str,
        bytearray,
        bytes,
        datetime.date,
        datetime.datetime,
        datetime.timedelta,
        tuple,
        list,
        dict,
        array,
    ),
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
    pyspark.errors.exceptions.base.PySparkValueError:...
    >>> _make_type_verifier(ArrayType(ShortType()))(list(range(3)))
    >>> _make_type_verifier(ArrayType(StringType()))(set()) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    pyspark.errors.exceptions.base.PySparkTypeError:...
    >>> _make_type_verifier(MapType(StringType(), IntegerType()))({})
    >>> _make_type_verifier(StructType([]))(())
    >>> _make_type_verifier(StructType([]))([])
    >>> _make_type_verifier(StructType([]))([1]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    pyspark.errors.exceptions.base.PySparkValueError:...
    >>> # Check if numeric values are within the allowed range.
    >>> _make_type_verifier(ByteType())(12)
    >>> _make_type_verifier(ByteType())(1234) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    pyspark.errors.exceptions.base.PySparkValueError:...
    >>> _make_type_verifier(ByteType(), False)(None) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    pyspark.errors.exceptions.base.PySparkValueError:...
    >>> _make_type_verifier(
    ...     ArrayType(ShortType(), False))([1, None]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    pyspark.errors.exceptions.base.PySparkValueError:...
    >>> _make_type_verifier(  # doctest: +IGNORE_EXCEPTION_DETAIL
    ...     MapType(StringType(), IntegerType())
    ...     )({None: 1})
    Traceback (most recent call last):
        ...
    pyspark.errors.exceptions.base.PySparkValueError:...
    >>> schema = StructType().add("a", IntegerType()).add("b", StringType(), False)
    >>> _make_type_verifier(schema)((1, None)) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    pyspark.errors.exceptions.base.PySparkValueError:...
    """

    if name is None:

        def new_msg(msg: str) -> str:
            return msg

        def new_name(n: str) -> str:
            return "field %s" % n

    else:

        def new_msg(msg: str) -> str:
            return "%s: %s" % (name, msg)

        def new_name(n: str) -> str:
            return "field %s in %s" % (n, name)

    def verify_nullability(obj: Any) -> bool:
        if obj is None:
            if nullable:
                return True
            else:
                if name is not None:
                    raise PySparkValueError(
                        error_class="FIELD_NOT_NULLABLE_WITH_NAME",
                        message_parameters={
                            "field_name": str(name),
                        },
                    )
                raise PySparkValueError(
                    error_class="FIELD_NOT_NULLABLE",
                    message_parameters={},
                )
        else:
            return False

    _type = type(dataType)

    def assert_acceptable_types(obj: Any) -> None:
        assert _type in _acceptable_types, new_msg(
            "unknown datatype: %s for object %r" % (dataType, obj)
        )

    def verify_acceptable_types(obj: Any) -> None:
        # subclass of them can not be fromInternal in JVM
        if type(obj) not in _acceptable_types[_type]:
            if name is not None:
                raise PySparkTypeError(
                    error_class="FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME",
                    message_parameters={
                        "field_name": str(name),
                        "data_type": str(dataType),
                        "obj": repr(obj),
                        "obj_type": str(type(obj)),
                    },
                )
            raise PySparkTypeError(
                error_class="FIELD_DATA_TYPE_UNACCEPTABLE",
                message_parameters={
                    "data_type": str(dataType),
                    "obj": repr(obj),
                    "obj_type": str(type(obj)),
                },
            )

    if isinstance(dataType, (StringType, CharType, VarcharType)):
        # StringType, CharType and VarcharType can work with any types
        def verify_value(obj: Any) -> None:
            pass

    elif isinstance(dataType, UserDefinedType):
        verifier = _make_type_verifier(dataType.sqlType(), name=name)

        def verify_udf(obj: Any) -> None:
            if not (hasattr(obj, "__UDT__") and obj.__UDT__ == dataType):
                if name is not None:
                    raise PySparkValueError(
                        error_class="FIELD_TYPE_MISMATCH_WITH_NAME",
                        message_parameters={
                            "field_name": str(name),
                            "obj": str(obj),
                            "data_type": str(dataType),
                        },
                    )
                raise PySparkValueError(
                    error_class="FIELD_TYPE_MISMATCH",
                    message_parameters={
                        "obj": str(obj),
                        "data_type": str(dataType),
                    },
                )
            verifier(dataType.toInternal(obj))

        verify_value = verify_udf

    elif isinstance(dataType, ByteType):

        def verify_byte(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            lower_bound = -128
            upper_bound = 127
            if obj < lower_bound or obj > upper_bound:
                raise PySparkValueError(
                    error_class="VALUE_OUT_OF_BOUNDS",
                    message_parameters={
                        "arg_name": "obj",
                        "lower_bound": str(lower_bound),
                        "upper_bound": str(upper_bound),
                        "actual": str(obj),
                    },
                )

        verify_value = verify_byte

    elif isinstance(dataType, ShortType):

        def verify_short(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            lower_bound = -32768
            upper_bound = 32767
            if obj < lower_bound or obj > upper_bound:
                raise PySparkValueError(
                    error_class="VALUE_OUT_OF_BOUNDS",
                    message_parameters={
                        "arg_name": "obj",
                        "lower_bound": str(lower_bound),
                        "upper_bound": str(upper_bound),
                        "actual": str(obj),
                    },
                )

        verify_value = verify_short

    elif isinstance(dataType, IntegerType):

        def verify_integer(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            lower_bound = -2147483648
            upper_bound = 2147483647
            if obj < lower_bound or obj > upper_bound:
                raise PySparkValueError(
                    error_class="VALUE_OUT_OF_BOUNDS",
                    message_parameters={
                        "arg_name": "obj",
                        "lower_bound": str(lower_bound),
                        "upper_bound": str(upper_bound),
                        "actual": str(obj),
                    },
                )

        verify_value = verify_integer

    elif isinstance(dataType, LongType):

        def verify_long(obj: Any) -> None:
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            lower_bound = -9223372036854775808
            upper_bound = 9223372036854775807
            if obj < lower_bound or obj > upper_bound:
                raise PySparkValueError(
                    error_class="VALUE_OUT_OF_BOUNDS",
                    message_parameters={
                        "arg_name": "obj",
                        "lower_bound": str(lower_bound),
                        "upper_bound": str(upper_bound),
                        "actual": str(obj),
                    },
                )

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
            verifier = _make_type_verifier(f.dataType, f.nullable, name=new_name(f.name))
            verifiers.append((f.name, verifier))

        def verify_struct(obj: Any) -> None:
            assert_acceptable_types(obj)

            if isinstance(obj, dict):
                for f, verifier in verifiers:
                    verifier(obj.get(f))
            elif isinstance(obj, (tuple, list)):
                if len(obj) != len(verifiers):
                    if name is not None:
                        raise PySparkValueError(
                            error_class="FIELD_STRUCT_LENGTH_MISMATCH_WITH_NAME",
                            message_parameters={
                                "field_name": str(name),
                                "object_length": str(len(obj)),
                                "field_length": str(len(verifiers)),
                            },
                        )
                    raise PySparkValueError(
                        error_class="FIELD_STRUCT_LENGTH_MISMATCH",
                        message_parameters={
                            "object_length": str(len(obj)),
                            "field_length": str(len(verifiers)),
                        },
                    )
                for v, (_, verifier) in zip(obj, verifiers):
                    verifier(v)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                for f, verifier in verifiers:
                    verifier(d.get(f))
            else:
                if name is not None:
                    raise PySparkTypeError(
                        error_class="FIELD_DATA_TYPE_UNACCEPTABLE_WITH_NAME",
                        message_parameters={
                            "field_name": str(name),
                            "data_type": str(dataType),
                            "obj": repr(obj),
                            "obj_type": str(type(obj)),
                        },
                    )
                raise PySparkTypeError(
                    error_class="FIELD_DATA_TYPE_UNACCEPTABLE",
                    message_parameters={
                        "data_type": str(dataType),
                        "obj": repr(obj),
                        "obj_type": str(type(obj)),
                    },
                )

        verify_value = verify_struct

    elif isinstance(dataType, VariantType):

        def verify_variant(obj: Any) -> None:
            # The variant data type can take in any type.
            pass

        verify_value = verify_variant

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
    >>> from pyspark.sql import Row
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
            raise PySparkValueError(
                error_class="CANNOT_SET_TOGETHER",
                message_parameters={"arg_list": "args and kwargs"},
            )
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
        >>> from pyspark.sql import Row
        >>> Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}
        True
        >>> row = Row(key=1, value=Row(name='a', age=2))
        >>> row.asDict() == {'key': 1, 'value': Row(name='a', age=2)}
        True
        >>> row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
        True
        """
        if not hasattr(self, "__fields__"):
            raise PySparkTypeError(
                error_class="CANNOT_CONVERT_TYPE",
                message_parameters={
                    "from_type": "Row",
                    "to_type": "dict",
                },
            )

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
            raise PySparkValueError(
                error_class="TOO_MANY_VALUES",
                message_parameters={
                    "expected": str(len(self)),
                    "item": "fields",
                    "actual": str(len(args)),
                },
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
            raise PySparkKeyError(
                error_class="KEY_NOT_EXISTS", message_parameters={"key": str(item)}
            )
        except ValueError:
            raise PySparkValueError(item)

    def __getattr__(self, item: str) -> Any:
        if item.startswith("__"):
            raise PySparkAttributeError(
                error_class="ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": item}
            )
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return self[idx]
        except IndexError:
            raise PySparkAttributeError(
                error_class="ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": item}
            )
        except ValueError:
            raise PySparkAttributeError(
                error_class="ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": item}
            )

    def __setattr__(self, key: Any, value: Any) -> None:
        if key != "__fields__":
            raise PySparkRuntimeError(
                error_class="READ_ONLY",
                message_parameters={"object": "Row"},
            )
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
            return "<Row(%s)>" % ", ".join(repr(field) for field in self)


class DateConverter:
    def can_convert(self, obj: Any) -> bool:
        return isinstance(obj, datetime.date)

    def convert(self, obj: datetime.date, gateway_client: "GatewayClient") -> "JavaGateway":
        from py4j.java_gateway import JavaClass

        Date = JavaClass("java.sql.Date", gateway_client)
        return Date.valueOf(obj.strftime("%Y-%m-%d"))


class DatetimeConverter:
    def can_convert(self, obj: Any) -> bool:
        return isinstance(obj, datetime.datetime)

    def convert(self, obj: datetime.datetime, gateway_client: "GatewayClient") -> "JavaGateway":
        from py4j.java_gateway import JavaClass

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

    def convert(self, obj: datetime.datetime, gateway_client: "GatewayClient") -> "JavaGateway":
        from py4j.java_gateway import JavaClass

        seconds = calendar.timegm(obj.utctimetuple())
        DateTimeUtils = JavaClass(
            "org.apache.spark.sql.catalyst.util.DateTimeUtils",
            gateway_client,
        )
        return DateTimeUtils.microsToLocalDateTime(int(seconds) * 1000000 + obj.microsecond)


class DayTimeIntervalTypeConverter:
    def can_convert(self, obj: Any) -> bool:
        return isinstance(obj, datetime.timedelta)

    def convert(self, obj: datetime.timedelta, gateway_client: "GatewayClient") -> "JavaGateway":
        from py4j.java_gateway import JavaClass

        IntervalUtils = JavaClass(
            "org.apache.spark.sql.catalyst.util.IntervalUtils",
            gateway_client,
        )
        return IntervalUtils.microsToDuration(
            (math.floor(obj.total_seconds()) * 1000000) + obj.microseconds
        )


class NumpyScalarConverter:
    def can_convert(self, obj: Any) -> bool:
        return has_numpy and isinstance(obj, np.generic)

    def convert(self, obj: "np.generic", gateway_client: "GatewayClient") -> Any:
        return obj.item()


class NumpyArrayConverter:
    def _from_numpy_type_to_java_type(
        self, nt: "np.dtype", gateway: "JavaGateway"
    ) -> Optional["JavaClass"]:
        """Convert NumPy type to Py4J Java type."""
        if nt in [np.dtype("int8"), np.dtype("int16")]:
            # Mapping int8 to gateway.jvm.byte causes
            #   TypeError: 'bytes' object does not support item assignment
            return gateway.jvm.short
        elif nt == np.dtype("int32"):
            return gateway.jvm.int
        elif nt == np.dtype("int64"):
            return gateway.jvm.long
        elif nt == np.dtype("float32"):
            return gateway.jvm.float
        elif nt == np.dtype("float64"):
            return gateway.jvm.double
        elif nt == np.dtype("bool"):
            return gateway.jvm.boolean

        return None

    def can_convert(self, obj: Any) -> bool:
        return has_numpy and isinstance(obj, np.ndarray) and obj.ndim == 1

    def convert(self, obj: "np.ndarray", gateway_client: "GatewayClient") -> "JavaGateway":
        from pyspark import SparkContext

        gateway = SparkContext._gateway
        assert gateway is not None
        plist = obj.tolist()

        if len(obj) > 0 and isinstance(plist[0], str):
            jtpe = gateway.jvm.String
        else:
            jtpe = self._from_numpy_type_to_java_type(obj.dtype, gateway)
            if jtpe is None:
                raise PySparkTypeError(
                    error_class="UNSUPPORTED_NUMPY_ARRAY_SCALAR",
                    message_parameters={"dtype": str(obj.dtype)},
                )
        jarr = gateway.new_array(jtpe, len(obj))
        for i in range(len(plist)):
            jarr[i] = plist[i]
        return jarr


if not is_remote_only():
    from py4j.protocol import register_input_converter

    # datetime is a subclass of date, we should register DatetimeConverter first
    register_input_converter(DatetimeNTZConverter())
    register_input_converter(DatetimeConverter())
    register_input_converter(DateConverter())
    register_input_converter(DayTimeIntervalTypeConverter())
    register_input_converter(NumpyScalarConverter())
    # NumPy array satisfies py4j.java_collections.ListConverter,
    # so prepend NumpyArrayConverter
    register_input_converter(NumpyArrayConverter(), prepend=True)


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession

    globs = globals()
    globs["spark"] = SparkSession.builder.getOrCreate()
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
