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

check_dependencies(__name__)

import array
import itertools
import datetime
import decimal

import pyarrow as pa

from pyspark.sql.types import (
    _create_row,
    Row,
    DataType,
    TimestampType,
    TimestampNTZType,
    MapType,
    StructField,
    StructType,
    ArrayType,
    BinaryType,
    NullType,
    DecimalType,
    StringType,
    UserDefinedType,
    cast,
)

from pyspark.sql.connect.types import to_arrow_schema

from typing import (
    Any,
    Callable,
    Dict,
    Sequence,
    List,
)


class LocalDataToArrowConversion:
    """
    Conversion from local data (except pandas DataFrame and numpy ndarray) to Arrow.
    Currently, only :class:`SparkSession` in Spark Connect can use this class.
    """

    @staticmethod
    def _need_converter(dataType: DataType) -> bool:
        if isinstance(dataType, NullType):
            return True
        elif isinstance(dataType, StructType):
            # Struct maybe rows, should convert to dict.
            return True
        elif isinstance(dataType, ArrayType):
            return LocalDataToArrowConversion._need_converter(dataType.elementType)
        elif isinstance(dataType, MapType):
            # Different from PySpark, here always needs conversion,
            # since an Arrow Map requires a list of tuples.
            return True
        elif isinstance(dataType, BinaryType):
            return True
        elif isinstance(dataType, (TimestampType, TimestampNTZType)):
            # Always truncate
            return True
        elif isinstance(dataType, DecimalType):
            # Convert Decimal('NaN') to None
            return True
        elif isinstance(dataType, StringType):
            # Coercion to StringType is allowed
            return True
        elif isinstance(dataType, UserDefinedType):
            return True
        else:
            return False

    @staticmethod
    def _create_converter(dataType: DataType) -> Callable:
        assert dataType is not None and isinstance(dataType, DataType)

        if not LocalDataToArrowConversion._need_converter(dataType):
            return lambda value: value

        if isinstance(dataType, NullType):
            return lambda value: None

        elif isinstance(dataType, StructType):

            field_names = dataType.fieldNames()

            field_convs = [
                LocalDataToArrowConversion._create_converter(field.dataType)
                for field in dataType.fields
            ]

            def convert_struct(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, (tuple, dict)) or hasattr(
                        value, "__dict__"
                    ), f"{type(value)} {value}"

                    _dict = {}
                    if not isinstance(value, Row) and hasattr(value, "__dict__"):
                        value = value.__dict__
                    for i, field in enumerate(field_names):
                        if isinstance(value, dict):
                            v = value.get(field)
                        else:
                            v = value[i]

                        _dict[f"col_{i}"] = field_convs[i](v)

                    return _dict

            return convert_struct

        elif isinstance(dataType, ArrayType):

            element_conv = LocalDataToArrowConversion._create_converter(dataType.elementType)

            def convert_array(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, (list, array.array))
                    return [element_conv(v) for v in value]

            return convert_array

        elif isinstance(dataType, MapType):

            key_conv = LocalDataToArrowConversion._create_converter(dataType.keyType)
            value_conv = LocalDataToArrowConversion._create_converter(dataType.valueType)

            def convert_map(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, dict)

                    _tuples = []
                    for k, v in value.items():
                        _tuples.append((key_conv(k), value_conv(v)))

                    return _tuples

            return convert_map

        elif isinstance(dataType, BinaryType):

            def convert_binary(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, (bytes, bytearray))
                    return bytes(value)

            return convert_binary

        elif isinstance(dataType, TimestampType):

            def convert_timestamp(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value.astimezone(datetime.timezone.utc)

            return convert_timestamp

        elif isinstance(dataType, TimestampNTZType):

            def convert_timestamp_ntz(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, datetime.datetime) and value.tzinfo is None
                    return value

            return convert_timestamp_ntz

        elif isinstance(dataType, DecimalType):

            def convert_decimal(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, decimal.Decimal)
                    return None if value.is_nan() else value

            return convert_decimal

        elif isinstance(dataType, StringType):

            def convert_string(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    # only atomic types are supported
                    assert isinstance(
                        value,
                        (
                            bool,
                            int,
                            float,
                            str,
                            bytes,
                            bytearray,
                            decimal.Decimal,
                            datetime.date,
                            datetime.datetime,
                            datetime.timedelta,
                        ),
                    )
                    if isinstance(value, bool):
                        # To match the PySpark which convert bool to string in
                        # the JVM side (python.EvaluatePython.makeFromJava)
                        return str(value).lower()
                    else:
                        return str(value)

            return convert_string

        elif isinstance(dataType, UserDefinedType):
            udt: UserDefinedType = dataType

            conv = LocalDataToArrowConversion._create_converter(dataType.sqlType())

            def convert_udt(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    return conv(udt.serialize(value))

            return convert_udt

        else:

            return lambda value: value

    @staticmethod
    def convert(data: Sequence[Any], schema: StructType) -> "pa.Table":
        assert isinstance(data, list) and len(data) > 0

        assert schema is not None and isinstance(schema, StructType)

        column_names = schema.fieldNames()

        column_convs = [
            LocalDataToArrowConversion._create_converter(field.dataType) for field in schema.fields
        ]

        pylist: List[List] = [[] for _ in range(len(column_names))]

        for item in data:
            if not isinstance(item, Row) and hasattr(item, "__dict__"):
                item = item.__dict__
            for i, col in enumerate(column_names):
                if isinstance(item, dict):
                    value = item.get(col)
                else:
                    value = item[i]

                pylist[i].append(column_convs[i](value))

        def normalize(dt: DataType) -> DataType:
            if isinstance(dt, StructType):
                return StructType(
                    [
                        StructField(f"col_{i}", normalize(field.dataType), nullable=field.nullable)
                        for i, field in enumerate(dt.fields)
                    ]
                )
            elif isinstance(dt, ArrayType):
                return ArrayType(normalize(dt.elementType), containsNull=dt.containsNull)
            elif isinstance(dt, MapType):
                return MapType(
                    normalize(dt.keyType),
                    normalize(dt.valueType),
                    valueContainsNull=dt.valueContainsNull,
                )
            else:
                return dt

        pa_schema = to_arrow_schema(cast(StructType, normalize(schema)))

        return pa.Table.from_arrays(pylist, schema=pa_schema)


class ArrowTableToRowsConversion:
    """
    Conversion from Arrow Table to Rows.
    Currently, only :class:`DataFrame` in Spark Connect can use this class.
    """

    @staticmethod
    def _need_converter(dataType: DataType) -> bool:
        if isinstance(dataType, NullType):
            return True
        elif isinstance(dataType, StructType):
            return True
        elif isinstance(dataType, ArrayType):
            return ArrowTableToRowsConversion._need_converter(dataType.elementType)
        elif isinstance(dataType, MapType):
            # Different from PySpark, here always needs conversion,
            # since the input from Arrow is a list of tuples.
            return True
        elif isinstance(dataType, BinaryType):
            return True
        elif isinstance(dataType, (TimestampType, TimestampNTZType)):
            # Always remove the time zone info for now
            return True
        elif isinstance(dataType, UserDefinedType):
            return True
        else:
            return False

    @staticmethod
    def _create_converter(dataType: DataType) -> Callable:
        assert dataType is not None and isinstance(dataType, DataType)

        if not ArrowTableToRowsConversion._need_converter(dataType):
            return lambda value: value

        if isinstance(dataType, NullType):
            return lambda value: None

        elif isinstance(dataType, StructType):

            field_names = dataType.names

            if len(set(field_names)) == len(field_names):
                dedup_field_names = field_names
            else:
                gen_new_name: Dict[str, Callable[[], str]] = {}
                for name, group in itertools.groupby(dataType.names):
                    if len(list(group)) > 1:

                        def _gen(_name: str) -> Callable[[], str]:
                            _i = itertools.count()
                            return lambda: f"{_name}_{next(_i)}"

                    else:

                        def _gen(_name: str) -> Callable[[], str]:
                            return lambda: _name

                    gen_new_name[name] = _gen(name)
                dedup_field_names = [gen_new_name[name]() for name in dataType.names]

            field_convs = [
                ArrowTableToRowsConversion._create_converter(f.dataType) for f in dataType.fields
            ]

            def convert_struct(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, dict)

                    _values = [
                        field_convs[i](value.get(name, None))
                        for i, name in enumerate(dedup_field_names)
                    ]
                    return _create_row(field_names, _values)

            return convert_struct

        elif isinstance(dataType, ArrayType):

            element_conv = ArrowTableToRowsConversion._create_converter(dataType.elementType)

            def convert_array(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, list)
                    return [element_conv(v) for v in value]

            return convert_array

        elif isinstance(dataType, MapType):

            key_conv = ArrowTableToRowsConversion._create_converter(dataType.keyType)
            value_conv = ArrowTableToRowsConversion._create_converter(dataType.valueType)

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

        elif isinstance(dataType, TimestampType):

            def convert_timestample(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value.astimezone().replace(tzinfo=None)

            return convert_timestample

        elif isinstance(dataType, TimestampNTZType):

            def convert_timestample_ntz(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value

            return convert_timestample_ntz

        elif isinstance(dataType, UserDefinedType):
            udt: UserDefinedType = dataType

            conv = ArrowTableToRowsConversion._create_converter(dataType.sqlType())

            def convert_udt(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    return udt.deserialize(conv(value))

            return convert_udt

        else:

            return lambda value: value

    @staticmethod
    def convert(table: "pa.Table", schema: StructType) -> List[Row]:
        assert isinstance(table, pa.Table)

        assert schema is not None and isinstance(schema, StructType)

        field_converters = [
            ArrowTableToRowsConversion._create_converter(f.dataType) for f in schema.fields
        ]

        columnar_data = [column.to_pylist() for column in table.columns]

        rows: List[Row] = []
        for i in range(0, table.num_rows):
            values = [field_converters[j](columnar_data[j][i]) for j in range(table.num_columns)]
            rows.append(_create_row(fields=schema.fieldNames(), values=values))
        return rows
