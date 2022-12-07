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

from typing import Optional

import pyspark.sql.connect.proto as pb2
from pyspark.sql.types import (
    DataType,
    ByteType,
    ShortType,
    IntegerType,
    FloatType,
    DateType,
    TimestampType,
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


def pyspark_types_to_proto_types(data_type: DataType) -> pb2.DataType:
    ret = pb2.DataType()
    if isinstance(data_type, StringType):
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
    elif isinstance(data_type, DayTimeIntervalType):
        ret.day_time_interval.start_field = data_type.startField
        ret.day_time_interval.end_field = data_type.endField
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
        fields = [
            StructField(
                f.name,
                proto_schema_to_pyspark_data_type(f.data_type),
                f.nullable,
            )
            for f in schema.struct.fields
        ]
        return StructType(fields)
    elif schema.HasField("map"):
        return MapType(
            proto_schema_to_pyspark_data_type(schema.map.key_type),
            proto_schema_to_pyspark_data_type(schema.map.value_type),
            schema.map.value_contains_null,
        )
    else:
        raise Exception(f"Unsupported data type {schema}")
