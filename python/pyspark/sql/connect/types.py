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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__, __file__)

import json
import re

import pyarrow as pa

from typing import Final, List, Optional, Pattern

from pyspark.errors import ParseException
from pyspark.sql.types import (
    DataType,
    ByteType,
    ShortType,
    IntegerType,
    FloatType,
    DateType,
    TimestampType,
    TimestampNTZType,
    DayTimeIntervalType,
    MapType,
    StringType,
    CharType,
    VarcharType,
    StructType,
    StructField,
    ArrayType,
    DoubleType,
    LongType,
    DecimalType,
    BinaryType,
    BooleanType,
    NullType,
    UserDefinedType,
)

import pyspark.sql.connect.proto as pb2


JVM_BYTE_MIN: int = -(1 << 7)
JVM_BYTE_MAX: int = (1 << 7) - 1
JVM_SHORT_MIN: int = -(1 << 15)
JVM_SHORT_MAX: int = (1 << 15) - 1
JVM_INT_MIN: int = -(1 << 31)
JVM_INT_MAX: int = (1 << 31) - 1
JVM_LONG_MIN: int = -(1 << 63)
JVM_LONG_MAX: int = (1 << 63) - 1


def pyspark_types_to_proto_types(data_type: DataType) -> pb2.DataType:
    ret = pb2.DataType()
    if isinstance(data_type, NullType):
        ret.null.CopyFrom(pb2.DataType.NULL())
    elif isinstance(data_type, StringType):
        ret.string.CopyFrom(pb2.DataType.String())
    elif isinstance(data_type, BooleanType):
        ret.boolean.CopyFrom(pb2.DataType.Boolean())
    elif isinstance(data_type, BinaryType):
        ret.binary.CopyFrom(pb2.DataType.Binary())
    elif isinstance(data_type, ByteType):
        ret.byte.CopyFrom(pb2.DataType.Byte())
    elif isinstance(data_type, ShortType):
        ret.short.CopyFrom(pb2.DataType.Short())
    elif isinstance(data_type, IntegerType):
        ret.integer.CopyFrom(pb2.DataType.Integer())
    elif isinstance(data_type, LongType):
        ret.long.CopyFrom(pb2.DataType.Long())
    elif isinstance(data_type, FloatType):
        ret.float.CopyFrom(pb2.DataType.Float())
    elif isinstance(data_type, DoubleType):
        ret.double.CopyFrom(pb2.DataType.Double())
    elif isinstance(data_type, DecimalType):
        ret.decimal.scale = data_type.scale
        ret.decimal.precision = data_type.precision
    elif isinstance(data_type, DateType):
        ret.date.CopyFrom(pb2.DataType.Date())
    elif isinstance(data_type, TimestampType):
        ret.timestamp.CopyFrom(pb2.DataType.Timestamp())
    elif isinstance(data_type, TimestampNTZType):
        ret.timestamp_ntz.CopyFrom(pb2.DataType.TimestampNTZ())
    elif isinstance(data_type, DayTimeIntervalType):
        ret.day_time_interval.start_field = data_type.startField
        ret.day_time_interval.end_field = data_type.endField
    elif isinstance(data_type, StructType):
        for field in data_type.fields:
            struct_field = pb2.DataType.StructField()
            struct_field.name = field.name
            struct_field.data_type.CopyFrom(pyspark_types_to_proto_types(field.dataType))
            struct_field.nullable = field.nullable
            if field.metadata is not None and len(field.metadata) > 0:
                struct_field.metadata = json.dumps(field.metadata)
            ret.struct.fields.append(struct_field)
    elif isinstance(data_type, MapType):
        ret.map.key_type.CopyFrom(pyspark_types_to_proto_types(data_type.keyType))
        ret.map.value_type.CopyFrom(pyspark_types_to_proto_types(data_type.valueType))
        ret.map.value_contains_null = data_type.valueContainsNull
    elif isinstance(data_type, ArrayType):
        ret.array.element_type.CopyFrom(pyspark_types_to_proto_types(data_type.elementType))
        ret.array.contains_null = data_type.containsNull
    elif isinstance(data_type, UserDefinedType):
        json_value = data_type.jsonValue()
        ret.udt.type = "udt"
        if "class" in json_value:
            # Scala/Java UDT
            ret.udt.jvm_class = json_value["class"]
        else:
            # Python UDT
            ret.udt.serialized_python_class = json_value["serializedClass"]
        ret.udt.python_class = json_value["pyClass"]
        ret.udt.sql_type.CopyFrom(pyspark_types_to_proto_types(data_type.sqlType()))
    else:
        raise Exception(f"Unsupported data type {data_type}")
    return ret


def proto_schema_to_pyspark_data_type(schema: pb2.DataType) -> DataType:
    if schema.HasField("null"):
        return NullType()
    elif schema.HasField("boolean"):
        return BooleanType()
    elif schema.HasField("binary"):
        return BinaryType()
    elif schema.HasField("byte"):
        return ByteType()
    elif schema.HasField("short"):
        return ShortType()
    elif schema.HasField("integer"):
        return IntegerType()
    elif schema.HasField("long"):
        return LongType()
    elif schema.HasField("float"):
        return FloatType()
    elif schema.HasField("double"):
        return DoubleType()
    elif schema.HasField("decimal"):
        p = schema.decimal.precision if schema.decimal.HasField("precision") else 10
        s = schema.decimal.scale if schema.decimal.HasField("scale") else 0
        return DecimalType(precision=p, scale=s)
    elif schema.HasField("string"):
        return StringType()
    elif schema.HasField("char"):
        return CharType(schema.char.length)
    elif schema.HasField("var_char"):
        return VarcharType(schema.var_char.length)
    elif schema.HasField("date"):
        return DateType()
    elif schema.HasField("timestamp"):
        return TimestampType()
    elif schema.HasField("timestamp_ntz"):
        return TimestampNTZType()
    elif schema.HasField("day_time_interval"):
        start: Optional[int] = (
            schema.day_time_interval.start_field
            if schema.day_time_interval.HasField("start_field")
            else None
        )
        end: Optional[int] = (
            schema.day_time_interval.end_field
            if schema.day_time_interval.HasField("end_field")
            else None
        )
        return DayTimeIntervalType(startField=start, endField=end)
    elif schema.HasField("array"):
        return ArrayType(
            proto_schema_to_pyspark_data_type(schema.array.element_type),
            schema.array.contains_null,
        )
    elif schema.HasField("struct"):
        fields = []
        for f in schema.struct.fields:
            if f.HasField("metadata"):
                metadata = json.loads(f.metadata)
            else:
                metadata = None
            fields.append(
                StructField(
                    f.name, proto_schema_to_pyspark_data_type(f.data_type), f.nullable, metadata
                )
            )
        return StructType(fields)
    elif schema.HasField("map"):
        return MapType(
            proto_schema_to_pyspark_data_type(schema.map.key_type),
            proto_schema_to_pyspark_data_type(schema.map.value_type),
            schema.map.value_contains_null,
        )
    elif schema.HasField("udt"):
        assert schema.udt.type == "udt"
        json_value = {}
        if schema.udt.HasField("python_class"):
            json_value["pyClass"] = schema.udt.python_class
        if schema.udt.HasField("serialized_python_class"):
            json_value["serializedClass"] = schema.udt.serialized_python_class
        return UserDefinedType.fromJson(json_value)
    else:
        raise Exception(f"Unsupported data type {schema}")


def to_arrow_type(dt: DataType) -> "pa.DataType":
    """
    Convert Spark data type to pyarrow type.

    This function refers to 'pyspark.sql.pandas.types.to_arrow_type' but relax the restriction,
    e.g. it supports nested StructType.
    """
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
        arrow_type = pa.binary()
    elif type(dt) == DateType:
        arrow_type = pa.date32()
    elif type(dt) == TimestampType:
        # Timestamps should be in UTC, JVM Arrow timestamps require a timezone to be read
        arrow_type = pa.timestamp("us", tz="UTC")
    elif type(dt) == TimestampNTZType:
        arrow_type = pa.timestamp("us", tz=None)
    elif type(dt) == DayTimeIntervalType:
        arrow_type = pa.duration("us")
    elif type(dt) == ArrayType:
        arrow_type = pa.list_(to_arrow_type(dt.elementType))
    elif type(dt) == MapType:
        arrow_type = pa.map_(to_arrow_type(dt.keyType), to_arrow_type(dt.valueType))
    elif type(dt) == StructType:
        fields = [
            pa.field(field.name, to_arrow_type(field.dataType), nullable=field.nullable)
            for field in dt
        ]
        arrow_type = pa.struct(fields)
    elif type(dt) == NullType:
        arrow_type = pa.null()
    else:
        raise TypeError("Unsupported type in conversion to Arrow: " + str(dt))
    return arrow_type


def to_arrow_schema(schema: StructType) -> "pa.Schema":
    """Convert a schema from Spark to Arrow"""
    fields = [
        pa.field(field.name, to_arrow_type(field.dataType), nullable=field.nullable)
        for field in schema
    ]
    return pa.schema(fields)


def from_arrow_type(at: "pa.DataType", prefer_timestamp_ntz: bool = False) -> DataType:
    """Convert pyarrow type to Spark data type.

    This function refers to 'pyspark.sql.pandas.types.from_arrow_type' but relax the restriction,
    e.g. it supports nested StructType, Array of TimestampType. However, Arrow DictionaryType is
    not allowed.
    """
    import pyarrow.types as types

    spark_type: DataType
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
        spark_type = BinaryType()
    elif types.is_date32(at):
        spark_type = DateType()
    elif types.is_timestamp(at) and prefer_timestamp_ntz and at.tz is None:
        spark_type = TimestampNTZType()
    elif types.is_timestamp(at):
        spark_type = TimestampType()
    elif types.is_duration(at):
        spark_type = DayTimeIntervalType()
    elif types.is_list(at):
        spark_type = ArrayType(from_arrow_type(at.value_type))
    elif types.is_map(at):
        spark_type = MapType(from_arrow_type(at.key_type), from_arrow_type(at.item_type))
    elif types.is_struct(at):
        return StructType(
            [
                StructField(field.name, from_arrow_type(field.type), nullable=field.nullable)
                for field in at
            ]
        )
    elif types.is_null(at):
        spark_type = NullType()
    else:
        raise TypeError("Unsupported type in conversion from Arrow: " + str(at))
    return spark_type


def from_arrow_schema(arrow_schema: "pa.Schema") -> StructType:
    """Convert schema from Arrow to Spark."""
    return StructType(
        [
            StructField(field.name, from_arrow_type(field.type), nullable=field.nullable)
            for field in arrow_schema
        ]
    )


def parse_data_type(data_type: str) -> DataType:
    """
    Parses the given data type string to a :class:`DataType`. The data type string format equals
    :class:`DataType.simpleString`, except that the top level struct type can omit
    the ``struct<>``. Since Spark 2.3, this also supports a schema in a DDL-formatted
    string and case-insensitive strings.

    Examples
    --------
    >>> parse_data_type("int ")
    IntegerType()
    >>> parse_data_type("INT ")
    IntegerType()
    >>> parse_data_type("a: byte, b: decimal(  16 , 8   ) ")
    StructType([StructField('a', ByteType(), True), StructField('b', DecimalType(16,8), True)])
    >>> parse_data_type("a DOUBLE, b STRING")
    StructType([StructField('a', DoubleType(), True), StructField('b', StringType(), True)])
    >>> parse_data_type("a DOUBLE, b CHAR( 50 )")
    StructType([StructField('a', DoubleType(), True), StructField('b', CharType(50), True)])
    >>> parse_data_type("a DOUBLE, b VARCHAR( 50 )")
    StructType([StructField('a', DoubleType(), True), StructField('b', VarcharType(50), True)])
    >>> parse_data_type("a: array< short>")
    StructType([StructField('a', ArrayType(ShortType(), True), True)])
    >>> parse_data_type(" map<string , string > ")
    MapType(StringType(), StringType(), True)

    >>> # Error cases
    >>> parse_data_type("blabla") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> parse_data_type("a: int,") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> parse_data_type("array<int") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> parse_data_type("map<int, boolean>>") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    """
    try:
        # DDL format, "fieldname datatype, fieldname datatype".
        return DDLSchemaParser(data_type).from_ddl_schema()
    except ParseException as e:
        try:
            # For backwards compatibility, "integer", "struct<fieldname: datatype>" and etc.
            return DDLDataTypeParser(data_type).from_ddl_datatype()
        except ParseException:
            try:
                # For backwards compatibility, "fieldname: datatype, fieldname: datatype" case.
                return DDLDataTypeParser(f"struct<{data_type}>").from_ddl_datatype()
            except ParseException:
                raise e from None


class DataTypeParserBase:
    REGEXP_IDENTIFIER: Final[Pattern] = re.compile("\\w+|`(?:``|[^`])*`", re.MULTILINE)
    REGEXP_INTEGER_VALUES: Final[Pattern] = re.compile(
        "\\(\\s*(?:-?\\s*\\d+)\\s*(?:,\\s*(?:-?\\s*\\d+)\\s*)*\\)", re.MULTILINE
    )
    REGEXP_INTERVAL_TYPE: Final[Pattern] = re.compile(
        "(day|hour|minute|second)(?:\\s+to\\s+(hour|minute|second))?", re.IGNORECASE | re.MULTILINE
    )

    def __init__(self, type_str: str):
        self._type_str = type_str
        self._pos = 0
        self._lstrip()

    def _lstrip(self) -> None:
        remaining = self._type_str[self._pos :]
        self._pos = self._pos + (len(remaining) - len(remaining.lstrip()))

    def _parse_data_type(self) -> DataType:
        type_str = self._type_str[self._pos :]
        m = self.REGEXP_IDENTIFIER.match(type_str)
        if m:
            data_type_name = m.group(0).lower().strip("`").replace("``", "`")
            self._pos = self._pos + len(m.group(0))
            self._lstrip()
            if data_type_name == "array":
                return self._parse_array_type()
            elif data_type_name == "map":
                return self._parse_map_type()
            elif data_type_name == "struct":
                return self._parse_struct_type()
            elif data_type_name == "interval":
                return self._parse_interval_type()
            else:
                return self._parse_primitive_types(data_type_name)

        raise ParseException(
            error_class="PARSE_SYNTAX_ERROR",
            message_parameters={"error": f"'{type_str}'", "hint": ""},
        )

    def _parse_array_type(self) -> ArrayType:
        type_str = self._type_str[self._pos :]
        if len(type_str) > 0 and type_str[0] == "<":
            self._pos = self._pos + 1
            self._lstrip()
            element_type = self._parse_data_type()
            remaining = self._type_str[self._pos :]
            if len(remaining) and remaining[0] == ">":
                self._pos = self._pos + 1
                self._lstrip()
                return ArrayType(element_type)
        raise ParseException(error_class="INCOMPLETE_TYPE_DEFINITION.ARRAY", message_parameters={})

    def _parse_map_type(self) -> MapType:
        type_str = self._type_str[self._pos :]
        if len(type_str) > 0 and type_str[0] == "<":
            self._pos = self._pos + 1
            self._lstrip()
            key_type = self._parse_data_type()
            remaining = self._type_str[self._pos :]
            if len(remaining) > 0 and remaining[0] == ",":
                self._pos = self._pos + 1
                self._lstrip()
                value_type = self._parse_data_type()
                remaining = self._type_str[self._pos :]
                if len(remaining) > 0 and remaining[0] == ">":
                    self._pos = self._pos + 1
                    self._lstrip()
                    return MapType(key_type, value_type)
        raise ParseException(error_class="INCOMPLETE_TYPE_DEFINITION.MAP", message_parameters={})

    def _parse_struct_type(self) -> StructType:
        type_str = self._type_str[self._pos :]
        if len(type_str) > 0 and type_str[0] == "<":
            self._pos = self._pos + 1
            self._lstrip()
            fields = self._parse_struct_fields()
            remaining = self._type_str[self._pos :]
            if len(remaining) > 0 and remaining[0] == ">":
                self._pos = self._pos + 1
                self._lstrip()
                return StructType(fields)
        raise ParseException(error_class="INCOMPLETE_TYPE_DEFINITION.STRUCT", message_parameters={})

    def _parse_struct_fields(self, sep_with_colon: bool = True) -> List[StructField]:
        type_str = self._type_str[self._pos :]
        m = self.REGEXP_IDENTIFIER.match(type_str)
        if m:
            field_name = m.group(0).lower().strip("`").replace("``", "`")
            self._pos = self._pos + len(m.group(0))
            self._lstrip()
            if sep_with_colon:
                remaining = self._type_str[self._pos :]
                if remaining[0] == ":":
                    self._pos = self._pos + 1
                    self._lstrip()
            data_type = self._parse_data_type()
            remaining = self._type_str[self._pos :]
            if len(remaining) > 0 and remaining[0] == ",":
                self._pos = self._pos + 1
                self._lstrip()
                return [StructField(field_name, data_type)] + self._parse_struct_fields(
                    sep_with_colon=sep_with_colon
                )
            else:
                return [StructField(field_name, data_type)]
        raise ParseException(error_class="INCOMPLETE_TYPE_DEFINITION.STRUCT", message_parameters={})

    def _parse_interval_type(self) -> DayTimeIntervalType:
        type_str = self._type_str[self._pos :]
        m = self.REGEXP_INTERVAL_TYPE.match(type_str)
        if m:
            start_field = DayTimeIntervalType._inverted_fields[m.group(1).lower()]
            end_field = (
                DayTimeIntervalType._inverted_fields[m.group(2).lower()]
                if m.group(2) is not None
                else None
            )
            self._pos = self._pos + len(m.group(0))
            self._lstrip()
            return DayTimeIntervalType(start_field, end_field)
        raise ParseException(
            error_class="PARSE_SYNTAX_ERROR",
            message_parameters={"error": f"'{type_str}'", "hint": f": extra input '{type_str}'"},
        )

    def _parse_primitive_types(self, data_type_name: str) -> DataType:
        type_str = self._type_str[self._pos :]
        m = self.REGEXP_INTEGER_VALUES.match(type_str)
        if m:
            integer_values = self._parse_integer_values(m.group(0))
            self._pos = self._pos + len(m.group(0))
            self._lstrip()
        else:
            integer_values = []
        len_iv = len(integer_values)
        if data_type_name == "boolean" and len_iv == 0:
            return BooleanType()
        elif data_type_name in ("tinyint", "byte") and len_iv == 0:
            return ByteType()
        elif data_type_name in ("smallint", "short") and len_iv == 0:
            return ShortType()
        elif data_type_name in ("int", "integer") and len_iv == 0:
            return IntegerType()
        elif data_type_name in ("bigint", "long") and len_iv == 0:
            return LongType()
        elif data_type_name in ("float", "real") and len_iv == 0:
            return FloatType()
        elif data_type_name == "double" and len_iv == 0:
            return DoubleType()
        elif data_type_name == "date" and len_iv == 0:
            return DateType()
        elif data_type_name == "timestamp" and len_iv == 0:
            return TimestampType()
        elif data_type_name == "timestamp_ntz" and len_iv == 0:
            return TimestampNTZType()
        elif data_type_name == "timestamp_ltz" and len_iv == 0:
            return TimestampType()
        elif data_type_name == "string" and len_iv == 0:
            return StringType()
        elif data_type_name in ("character", "char") and len_iv == 1:
            return CharType(integer_values[0])
        elif data_type_name == "varchar" and len_iv == 1:
            return VarcharType(integer_values[0])
        elif data_type_name == "binary" and len_iv == 0:
            return BinaryType()
        elif data_type_name in ("decimal", "dec", "numeric") and len_iv == 0:
            return DecimalType()
        elif data_type_name in ("decimal", "dec", "numeric") and len_iv == 1:
            return DecimalType(precision=integer_values[0])
        elif data_type_name in ("decimal", "dec", "numeric") and len_iv == 2:
            return DecimalType(precision=integer_values[0], scale=integer_values[1])
        elif data_type_name == "void" and len_iv == 0:
            return NullType()
        elif data_type_name in ("character", "char", "varchar") and len_iv == 0:
            raise ParseException(
                error_class="DATATYPE_MISSING_SIZE",
                message_parameters={"type": f'"{data_type_name}"'},
            )
        elif data_type_name == "array" and len_iv == 0:
            raise ParseException(
                error_class="INCOMPLETE_TYPE_DEFINITION.ARRAY", message_parameters={}
            )
        elif data_type_name == "map" and len_iv == 0:
            raise ParseException(
                error_class="INCOMPLETE_TYPE_DEFINITION.MAP", message_parameters={}
            )
        elif data_type_name == "struct" and len_iv == 0:
            raise ParseException(
                error_class="INCOMPLETE_TYPE_DEFINITION.STRUCT", message_parameters={}
            )
        else:
            raise ParseException(
                error_class="UNSUPPORTED_DATATYPE",
                message_parameters={"typeName": f'"{data_type_name}"'},
            )

    def _parse_integer_values(self, values: str) -> List[int]:
        values = values.strip()
        values = values[1:-1]
        integer_values = values.split(",")
        return [int(v.strip()) for v in integer_values]


class DDLSchemaParser(DataTypeParserBase):
    def __init__(self, type_str: str):
        super().__init__(type_str)
        self._data_type: Optional[StructType] = None

    def from_ddl_schema(self) -> StructType:
        if self._data_type is None:
            data_type = StructType(self._parse_struct_fields(sep_with_colon=False))
            remaining = self._type_str[self._pos :]
            if len(remaining) == 0:
                self._data_type = data_type
            else:
                raise ParseException(
                    error_class="PARSE_SYNTAX_ERROR",
                    message_parameters={
                        "error": f"'{remaining}'",
                        "hint": f": extra input '{remaining}'",
                    },
                )
        return self._data_type


class DDLDataTypeParser(DataTypeParserBase):
    def __init__(self, type_str: str):
        super().__init__(type_str)
        self._data_type: Optional[DataType] = None

    def from_ddl_datatype(self) -> DataType:
        if self._data_type is None:
            data_type = self._parse_data_type()
            remaining = self._type_str[self._pos :]
            if len(remaining) == 0:
                self._data_type = data_type
            else:
                raise ParseException(
                    error_class="PARSE_SYNTAX_ERROR",
                    message_parameters={
                        "error": f"'{remaining}'",
                        "hint": f": extra input '{remaining}'",
                    },
                )
        return self._data_type
