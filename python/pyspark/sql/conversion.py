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

import array
import datetime
import decimal
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Sequence, Union, overload

from pyspark.errors import PySparkValueError
from pyspark.sql.pandas.types import _dedup_names, _deduplicate_field_names, to_arrow_schema
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DataType,
    DecimalType,
    GeographyType,
    Geography,
    GeometryType,
    Geometry,
    MapType,
    NullType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    UserDefinedType,
    VariantType,
    VariantVal,
    _create_row,
)

if TYPE_CHECKING:
    import pyarrow as pa


class LocalDataToArrowConversion:
    """
    Conversion from local data (except pandas DataFrame and numpy ndarray) to Arrow.
    """

    @staticmethod
    def _need_converter(
        dataType: DataType,
        nullable: bool = True,
    ) -> bool:
        if not nullable:
            # always check the nullability
            return True
        elif isinstance(dataType, NullType):
            # always check the nullability
            return True
        elif isinstance(dataType, StructType):
            # Struct maybe rows, should convert to dict.
            return True
        elif isinstance(dataType, ArrayType):
            return LocalDataToArrowConversion._need_converter(
                dataType.elementType, dataType.containsNull
            )
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
            # Rescale Decimal values
            return True
        elif isinstance(dataType, StringType):
            # Coercion to StringType is allowed
            return True
        elif isinstance(dataType, UserDefinedType):
            return True
        elif isinstance(dataType, VariantType):
            return True
        elif isinstance(dataType, GeometryType):
            return True
        elif isinstance(dataType, GeographyType):
            return True
        else:
            return False

    @overload
    @staticmethod
    def _create_converter(
        dataType: DataType, nullable: bool = True, *, int_to_decimal_coercion_enabled: bool = False
    ) -> Callable:
        pass

    @overload
    @staticmethod
    def _create_converter(
        dataType: DataType,
        nullable: bool = True,
        *,
        none_on_identity: bool = True,
        int_to_decimal_coercion_enabled: bool = False,
    ) -> Optional[Callable]:
        pass

    @staticmethod
    def _create_converter(
        dataType: DataType,
        nullable: bool = True,
        *,
        none_on_identity: bool = False,
        int_to_decimal_coercion_enabled: bool = False,
    ) -> Optional[Callable]:
        assert dataType is not None and isinstance(dataType, DataType)
        assert isinstance(nullable, bool)

        if not LocalDataToArrowConversion._need_converter(dataType, nullable):
            if none_on_identity:
                return None
            else:
                return lambda value: value

        if isinstance(dataType, NullType):

            def convert_null(value: Any) -> Any:
                if value is not None:
                    raise PySparkValueError(f"input for {dataType} must be None, but got {value}")
                return None

            return convert_null

        elif isinstance(dataType, StructType):
            field_names = dataType.fieldNames()
            len_field_names = len(field_names)
            dedup_field_names = _dedup_names(dataType.names)

            field_convs = [
                LocalDataToArrowConversion._create_converter(
                    field.dataType,
                    field.nullable,
                    none_on_identity=True,
                    int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
                )
                for field in dataType.fields
            ]

            def convert_struct(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    # The `value` should be tuple, dict, or have `__dict__`.
                    if isinstance(value, tuple):  # `Row` inherits `tuple`
                        if len(value) != len_field_names:
                            raise PySparkValueError(
                                errorClass="AXIS_LENGTH_MISMATCH",
                                messageParameters={
                                    "expected_length": str(len_field_names),
                                    "actual_length": str(len(value)),
                                },
                            )
                        return {
                            dedup_field_names[i]: (
                                field_convs[i](value[i])  # type: ignore[misc]
                                if field_convs[i] is not None
                                else value[i]
                            )
                            for i in range(len_field_names)
                        }
                    elif isinstance(value, dict):
                        return {
                            dedup_field_names[i]: (
                                field_convs[i](value.get(field))  # type: ignore[misc]
                                if field_convs[i] is not None
                                else value.get(field)
                            )
                            for i, field in enumerate(field_names)
                        }
                    else:
                        assert hasattr(value, "__dict__"), f"{type(value)} {value}"
                        value = value.__dict__
                        return {
                            dedup_field_names[i]: (
                                field_convs[i](value.get(field))  # type: ignore[misc]
                                if field_convs[i] is not None
                                else value.get(field)
                            )
                            for i, field in enumerate(field_names)
                        }

            return convert_struct

        elif isinstance(dataType, ArrayType):
            element_conv = LocalDataToArrowConversion._create_converter(
                dataType.elementType,
                dataType.containsNull,
                none_on_identity=True,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            )

            if element_conv is None:

                def convert_array(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        assert isinstance(value, (list, array.array))
                        return list(value)

            else:

                def convert_array(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        assert isinstance(value, (list, array.array))
                        return [element_conv(v) for v in value]

            return convert_array

        elif isinstance(dataType, MapType):
            key_conv = LocalDataToArrowConversion._create_converter(
                dataType.keyType,
                nullable=False,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            )
            value_conv = LocalDataToArrowConversion._create_converter(
                dataType.valueType,
                dataType.valueContainsNull,
                none_on_identity=True,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            )

            if value_conv is None:

                def convert_map(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        assert isinstance(value, dict)
                        return [(key_conv(k), v) for k, v in value.items()]

            else:

                def convert_map(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        assert isinstance(value, dict)
                        return [(key_conv(k), value_conv(v)) for k, v in value.items()]

            return convert_map

        elif isinstance(dataType, BinaryType):

            def convert_binary(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    assert isinstance(value, (bytes, bytearray))
                    return bytes(value)

            return convert_binary

        elif isinstance(dataType, TimestampType):

            def convert_timestamp(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value.astimezone(datetime.timezone.utc)

            return convert_timestamp

        elif isinstance(dataType, TimestampNTZType):

            def convert_timestamp_ntz(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    assert isinstance(value, datetime.datetime) and value.tzinfo is None
                    return value

            return convert_timestamp_ntz

        elif isinstance(dataType, DecimalType):
            exp = decimal.Decimal(f"1E-{dataType.scale}")
            ctx = decimal.Context(prec=dataType.precision, rounding=decimal.ROUND_HALF_EVEN)

            def convert_decimal(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    if int_to_decimal_coercion_enabled and isinstance(value, int):
                        value = decimal.Decimal(value)

                    assert isinstance(value, decimal.Decimal)
                    if value.is_nan():
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None

                    return value.quantize(exp, context=ctx)

            return convert_decimal

        elif isinstance(dataType, StringType):

            def convert_string(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    if isinstance(value, bool):
                        # To match the PySpark Classic which convert bool to string in
                        # the JVM side (python.EvaluatePython.makeFromJava)
                        return str(value).lower()
                    else:
                        return str(value)

            return convert_string

        elif isinstance(dataType, UserDefinedType):
            udt: UserDefinedType = dataType

            conv = LocalDataToArrowConversion._create_converter(
                udt.sqlType(),
                nullable=nullable,
                none_on_identity=True,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            )

            if conv is None:

                def convert_udt(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        return udt.serialize(value)

            else:

                def convert_udt(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        return conv(udt.serialize(value))

            return convert_udt

        elif isinstance(dataType, VariantType):

            def convert_variant(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                elif isinstance(value, VariantVal):
                    return VariantType().toInternal(value)
                else:
                    raise PySparkValueError(errorClass="MALFORMED_VARIANT")

            return convert_variant

        elif isinstance(dataType, GeographyType):

            def convert_geography(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                elif isinstance(value, Geography):
                    return dataType.toInternal(value)
                else:
                    raise PySparkValueError(errorClass="MALFORMED_GEOGRAPHY")

            return convert_geography

        elif isinstance(dataType, GeometryType):

            def convert_geometry(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                elif isinstance(value, Geometry):
                    return dataType.toInternal(value)
                else:
                    raise PySparkValueError(errorClass="MALFORMED_GEOMETRY")

            return convert_geometry

        elif not nullable:

            def convert_other(value: Any) -> Any:
                if value is None:
                    raise PySparkValueError(f"input for {dataType} must not be None")
                return value

            return convert_other
        else:
            if none_on_identity:
                return None
            else:
                return lambda value: value

    @staticmethod
    def convert(data: Sequence[Any], schema: StructType, use_large_var_types: bool) -> "pa.Table":
        require_minimum_pyarrow_version()
        import pyarrow as pa

        assert isinstance(data, list) and len(data) > 0

        assert schema is not None and isinstance(schema, StructType)

        column_names = schema.fieldNames()
        len_column_names = len(column_names)

        def to_row(item: Any) -> tuple:
            if item is None:
                return tuple([None] * len_column_names)
            elif isinstance(item, tuple):  # `Row` inherits `tuple`
                if len(item) != len_column_names:
                    raise PySparkValueError(
                        errorClass="AXIS_LENGTH_MISMATCH",
                        messageParameters={
                            "expected_length": str(len_column_names),
                            "actual_length": str(len(item)),
                        },
                    )
                return tuple(item)
            elif isinstance(item, dict):
                return tuple([item.get(col) for col in column_names])
            elif isinstance(item, VariantVal):
                raise PySparkValueError("Rows cannot be of type VariantVal")
            elif hasattr(item, "__dict__"):
                item = item.__dict__
                return tuple([item.get(col) for col in column_names])
            else:
                if len(item) != len_column_names:
                    raise PySparkValueError(
                        errorClass="AXIS_LENGTH_MISMATCH",
                        messageParameters={
                            "expected_length": str(len_column_names),
                            "actual_length": str(len(item)),
                        },
                    )
                return tuple(item)

        rows = [to_row(item) for item in data]

        if len_column_names > 0:
            column_convs = [
                LocalDataToArrowConversion._create_converter(
                    field.dataType,
                    field.nullable,
                    none_on_identity=True,
                    # Default to False for general data conversion
                    int_to_decimal_coercion_enabled=False,
                )
                for field in schema.fields
            ]

            pylist = [
                [conv(row[i]) for row in rows] if conv is not None else [row[i] for row in rows]
                for i, conv in enumerate(column_convs)
            ]

            pa_schema = to_arrow_schema(
                StructType(
                    [
                        StructField(
                            field.name, _deduplicate_field_names(field.dataType), field.nullable
                        )
                        for field in schema.fields
                    ]
                ),
                prefers_large_types=use_large_var_types,
            )

            return pa.Table.from_arrays(pylist, schema=pa_schema)
        else:
            return pa.Table.from_struct_array(pa.array([{}] * len(rows)))


class ArrowTableToRowsConversion:
    """
    Conversion from Arrow Table to Rows.
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
        elif isinstance(dataType, VariantType):
            return True
        elif isinstance(dataType, GeographyType):
            return True
        elif isinstance(dataType, GeometryType):
            return True
        else:
            return False

    @overload
    @staticmethod
    def _create_converter(dataType: DataType) -> Callable:
        pass

    @overload
    @staticmethod
    def _create_converter(
        dataType: DataType, *, none_on_identity: bool = True, binary_as_bytes: bool = True
    ) -> Optional[Callable]:
        pass

    @staticmethod
    def _create_converter(
        dataType: DataType, *, none_on_identity: bool = False, binary_as_bytes: bool = True
    ) -> Optional[Callable]:
        assert dataType is not None and isinstance(dataType, DataType)

        if not ArrowTableToRowsConversion._need_converter(dataType):
            if none_on_identity:
                return None
            else:
                return lambda value: value

        if isinstance(dataType, NullType):
            return lambda value: None

        elif isinstance(dataType, StructType):
            field_names = dataType.names
            dedup_field_names = _dedup_names(field_names)

            field_convs = [
                ArrowTableToRowsConversion._create_converter(
                    f.dataType, none_on_identity=True, binary_as_bytes=binary_as_bytes
                )
                for f in dataType.fields
            ]

            def convert_struct(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, dict)

                    _values = [
                        field_convs[i](value.get(name, None))  # type: ignore[misc]
                        if field_convs[i] is not None
                        else value.get(name, None)
                        for i, name in enumerate(dedup_field_names)
                    ]
                    return _create_row(field_names, _values)

            return convert_struct

        elif isinstance(dataType, ArrayType):
            element_conv = ArrowTableToRowsConversion._create_converter(
                dataType.elementType, none_on_identity=True, binary_as_bytes=binary_as_bytes
            )

            if element_conv is None:

                def convert_array(value: Any) -> Any:
                    if value is None:
                        return None
                    else:
                        assert isinstance(value, list)
                        return value

            else:

                def convert_array(value: Any) -> Any:
                    if value is None:
                        return None
                    else:
                        assert isinstance(value, list)
                        return [element_conv(v) for v in value]

            return convert_array

        elif isinstance(dataType, MapType):
            key_conv = ArrowTableToRowsConversion._create_converter(
                dataType.keyType, none_on_identity=True, binary_as_bytes=binary_as_bytes
            )
            value_conv = ArrowTableToRowsConversion._create_converter(
                dataType.valueType, none_on_identity=True, binary_as_bytes=binary_as_bytes
            )

            if key_conv is None:
                if value_conv is None:

                    def convert_map(value: Any) -> Any:
                        if value is None:
                            return None
                        else:
                            assert isinstance(value, list)
                            assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                            return dict(value)

                else:

                    def convert_map(value: Any) -> Any:
                        if value is None:
                            return None
                        else:
                            assert isinstance(value, list)
                            assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                            return dict((t[0], value_conv(t[1])) for t in value)

            else:
                if value_conv is None:

                    def convert_map(value: Any) -> Any:
                        if value is None:
                            return None
                        else:
                            assert isinstance(value, list)
                            assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                            return dict((key_conv(t[0]), t[1]) for t in value)

                else:

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
                    return value if binary_as_bytes else bytearray(value)

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

            conv = ArrowTableToRowsConversion._create_converter(
                udt.sqlType(), none_on_identity=True, binary_as_bytes=binary_as_bytes
            )

            if conv is None:

                def convert_udt(value: Any) -> Any:
                    if value is None:
                        return None
                    else:
                        return udt.deserialize(value)

            else:

                def convert_udt(value: Any) -> Any:
                    if value is None:
                        return None
                    else:
                        return udt.deserialize(conv(value))

            return convert_udt

        elif isinstance(dataType, VariantType):

            def convert_variant(value: Any) -> Any:
                if value is None:
                    return None
                elif (
                    isinstance(value, dict)
                    and all(key in value for key in ["value", "metadata"])
                    and all(isinstance(value[key], bytes) for key in ["value", "metadata"])
                ):
                    return VariantVal(value["value"], value["metadata"])
                else:
                    raise PySparkValueError(errorClass="MALFORMED_VARIANT")

            return convert_variant

        elif isinstance(dataType, GeographyType):

            def convert_geography(value: Any) -> Any:
                if value is None:
                    return None
                elif (
                    isinstance(value, dict)
                    and all(key in value for key in ["wkb", "srid"])
                    and isinstance(value["wkb"], bytes)
                    and isinstance(value["srid"], int)
                ):
                    return Geography.fromWKB(value["wkb"], value["srid"])
                else:
                    raise PySparkValueError(errorClass="MALFORMED_GEOGRAPHY")

            return convert_geography

        elif isinstance(dataType, GeometryType):

            def convert_geometry(value: Any) -> Any:
                if value is None:
                    return None
                elif (
                    isinstance(value, dict)
                    and all(key in value for key in ["wkb", "srid"])
                    and isinstance(value["wkb"], bytes)
                    and isinstance(value["srid"], int)
                ):
                    return Geometry.fromWKB(value["wkb"], value["srid"])
                else:
                    raise PySparkValueError(errorClass="MALFORMED_GEOMETRY")

            return convert_geometry

        else:
            if none_on_identity:
                return None
            else:
                return lambda value: value

    @overload
    @staticmethod
    def convert(table: "pa.Table", schema: StructType) -> List[Row]:
        pass

    @overload
    @staticmethod
    def convert(table: "pa.Table", schema: StructType, *, binary_as_bytes: bool) -> List[Row]:
        pass

    @overload
    @staticmethod
    def convert(table: "pa.Table", schema: StructType, *, return_as_tuples: bool) -> List[tuple]:
        pass

    @staticmethod  # type: ignore[misc]
    def convert(
        table: "pa.Table",
        schema: StructType,
        *,
        return_as_tuples: bool = False,
        binary_as_bytes: bool = True,
    ) -> List[Union[Row, tuple]]:
        require_minimum_pyarrow_version()
        import pyarrow as pa

        assert isinstance(table, pa.Table)

        assert schema is not None and isinstance(schema, StructType)

        fields = schema.fieldNames()

        if len(fields) > 0:
            field_converters = [
                ArrowTableToRowsConversion._create_converter(
                    f.dataType, none_on_identity=True, binary_as_bytes=binary_as_bytes
                )
                for f in schema.fields
            ]

            columnar_data = [
                [conv(v) for v in column.to_pylist()] if conv is not None else column.to_pylist()
                for column, conv in zip(table.columns, field_converters)
            ]

            if return_as_tuples:
                rows = [tuple(cols) for cols in zip(*columnar_data)]
            else:
                rows = [_create_row(fields, tuple(cols)) for cols in zip(*columnar_data)]
            assert len(rows) == table.num_rows, f"{len(rows)}, {table.num_rows}"
            return rows
        else:
            if return_as_tuples:
                return [tuple()] * table.num_rows
            else:
                return [_create_row(fields, tuple())] * table.num_rows
