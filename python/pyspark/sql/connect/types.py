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

import datetime
import json

from typing import Any, Optional, Callable

from pyspark.sql.types import (
    Row,
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
        ret.decimal.CopyFrom(pb2.DataType.Decimal())
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
    else:
        raise Exception(f"Unsupported data type {schema}")


def _need_converter(dataType: DataType) -> bool:
    if isinstance(dataType, NullType):
        return True
    elif isinstance(dataType, StructType):
        return True
    elif isinstance(dataType, ArrayType):
        return _need_converter(dataType.elementType)
    elif isinstance(dataType, MapType):
        # Different from PySpark, here always needs conversion,
        # since the input from Arrow is a list of tuples.
        return True
    elif isinstance(dataType, BinaryType):
        return True
    elif isinstance(dataType, (TimestampType, TimestampNTZType)):
        # Always remove the time zone info for now
        return True
    else:
        return False


def _create_converter(dataType: DataType) -> Callable:
    assert dataType is not None and isinstance(dataType, DataType)

    if not _need_converter(dataType):
        return lambda value: value

    if isinstance(dataType, NullType):
        return lambda value: None

    elif isinstance(dataType, StructType):

        field_convs = {f.name: _create_converter(f.dataType) for f in dataType.fields}
        need_conv = any(_need_converter(f.dataType) for f in dataType.fields)

        def convert_struct(value: Any) -> Row:
            if value is None:
                return Row()
            else:
                assert isinstance(value, dict)

                if need_conv:
                    _dict = {}
                    for k, v in value.items():
                        assert isinstance(k, str)
                        _dict[k] = field_convs[k](v)
                    return Row(**_dict)
                else:
                    return Row(**value)

        return convert_struct

    elif isinstance(dataType, ArrayType):

        element_conv = _create_converter(dataType.elementType)

        def convert_array(value: Any) -> Any:
            if value is None:
                return None
            else:
                assert isinstance(value, list)
                return [element_conv(v) for v in value]

        return convert_array

    elif isinstance(dataType, MapType):

        key_conv = _create_converter(dataType.keyType)
        value_conv = _create_converter(dataType.valueType)

        def convert_map(value: Any) -> Any:
            if value is None:
                return None
            else:
                assert isinstance(value, list)
                assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                return dict((key_conv(t[0]), value_conv(t[1])) for t in value)

        return convert_map

    elif isinstance(dataType, BinaryType):

        def convert_binary(value: Any) -> Any:
            if value is None:
                return None
            else:
                assert isinstance(value, bytes)
                return bytearray(value)

        return convert_binary

    elif isinstance(dataType, (TimestampType, TimestampNTZType)):

        def convert_timestample(value: Any) -> Any:
            if value is None:
                return None
            else:
                assert isinstance(value, datetime.datetime)
                if value.tzinfo is not None:
                    # always remove the time zone for now
                    return value.replace(tzinfo=None)
                else:
                    return value

        return convert_timestample

    else:

        return lambda value: value
