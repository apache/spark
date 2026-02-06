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
from pyspark.sql.pandas.types import (
    _dedup_names,
    _deduplicate_field_names,
    _create_converter_to_pandas,
    to_arrow_schema,
)
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    DataType,
    FloatType,
    DoubleType,
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
    DateType,
    TimeType,
    TimestampNTZType,
    TimestampType,
    DayTimeIntervalType,
    YearMonthIntervalType,
    UserDefinedType,
    VariantType,
    VariantVal,
    _create_row,
)

if TYPE_CHECKING:
    import pyarrow as pa
    import pandas as pd


class ArrowBatchTransformer:
    """
    Pure functions that transform RecordBatch -> RecordBatch.
    They should have no side effects (no I/O, no writing to streams).
    """

    @staticmethod
    def flatten_struct(batch: "pa.RecordBatch", column_index: int = 0) -> "pa.RecordBatch":
        """
        Flatten a struct column at given index into a RecordBatch.

        Used by:
            - ArrowStreamUDFSerializer.load_stream
            - SQL_GROUPED_MAP_ARROW_UDF mapper
            - SQL_GROUPED_MAP_ARROW_ITER_UDF mapper
        """
        import pyarrow as pa

        struct = batch.column(column_index)
        return pa.RecordBatch.from_arrays(struct.flatten(), schema=pa.schema(struct.type))

    @staticmethod
    def wrap_struct(batch: "pa.RecordBatch") -> "pa.RecordBatch":
        """
        Wrap a RecordBatch's columns into a single struct column.

        Used by: ArrowStreamUDFSerializer.dump_stream
        """
        import pyarrow as pa

        if batch.num_columns == 0:
            # When batch has no column, it should still create
            # an empty batch with the number of rows set.
            struct = pa.array([{}] * batch.num_rows)
        else:
            struct = pa.StructArray.from_arrays(batch.columns, fields=pa.struct(list(batch.schema)))
        return pa.RecordBatch.from_arrays([struct], ["_0"])

    @classmethod
    def to_pandas(
        cls,
        batch: Union["pa.RecordBatch", "pa.Table"],
        timezone: str,
        schema: Optional["StructType"] = None,
        struct_in_pandas: str = "dict",
        ndarray_as_list: bool = False,
        df_for_struct: bool = False,
    ) -> List[Union["pd.Series", "pd.DataFrame"]]:
        """
        Convert a RecordBatch or Table to a list of pandas Series.

        Parameters
        ----------
        batch : pa.RecordBatch or pa.Table
            The Arrow RecordBatch or Table to convert.
        timezone : str
            Timezone for timestamp conversion.
        schema : StructType, optional
            Spark schema for type conversion. If None, types are inferred from Arrow.
        struct_in_pandas : str
            How to represent struct in pandas ("dict", "row", etc.)
        ndarray_as_list : bool
            Whether to convert ndarray as list.
        df_for_struct : bool
            If True, convert struct columns to DataFrame instead of Series.

        Returns
        -------
        List[Union[pd.Series, pd.DataFrame]]
            List of pandas Series (or DataFrame if df_for_struct=True), one for each column.
        """
        import pandas as pd

        import pyspark
        from pyspark.sql.pandas.types import from_arrow_type

        if batch.num_columns == 0:
            return [pd.Series([pyspark._NoValue] * batch.num_rows)]

        return [
            ArrowArrayToPandasConversion.convert(
                batch.column(i),
                schema[i].dataType if schema is not None else from_arrow_type(batch.column(i).type),
                timezone=timezone,
                struct_in_pandas=struct_in_pandas,
                ndarray_as_list=ndarray_as_list,
                df_for_struct=df_for_struct,
            )
            for i in range(batch.num_columns)
        ]


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
        none_on_identity: bool = False,
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
        else:  # pragma: no cover
            assert False, f"Need converter for {dataType} but failed to find one."

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
                timezone="UTC",
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

            assert (
                element_conv is not None
            ), f"_need_converter() returned True for ArrayType of {dataType.elementType}"

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

        else:  # pragma: no cover
            assert False, f"Need converter for {dataType} but failed to find one."

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
    def convert(
        table: "pa.Table", schema: StructType, *, return_as_tuples: bool
    ) -> List[Row | tuple]:
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


class ArrowArrayConversion:
    @classmethod
    def check_conversion(
        cls,
        pa_type: "pa.DataType",
        check_type: Callable[["pa.DataType"], bool],
    ) -> bool:
        import pyarrow.types as types

        if check_type(pa_type):
            return True
        elif (
            types.is_list(pa_type)
            or types.is_large_list(pa_type)
            or types.is_fixed_size_list(pa_type)
            or types.is_dictionary(pa_type)
        ):
            return cls.check_conversion(pa_type.value_type, check_type)
        elif types.is_map(pa_type):
            return any(
                cls.check_conversion(at, check_type)
                for at in [
                    pa_type.key_type,
                    pa_type.item_type,
                ]
            )
        elif types.is_struct(pa_type):
            return any(cls.check_conversion(field.type, check_type) for field in pa_type)
        else:
            return False

    @classmethod
    def convert_array(
        cls,
        arr: "pa.Array",
        check_type: Callable[["pa.DataType"], bool],
        convert: Callable[["pa.Array"], "pa.Array"],
    ) -> "pa.Array":
        import pyarrow as pa
        import pyarrow.types as types

        assert isinstance(arr, pa.Array)

        pa_type = arr.type
        # fastpath
        if not cls.check_conversion(pa_type, check_type):
            return arr

        if check_type(pa_type):
            converted = convert(arr)
            assert len(converted) == len(arr), f"array length changed: {arr} -> {converted}"
            return converted
        elif types.is_list(pa_type):
            return pa.ListArray.from_arrays(
                offsets=arr.offsets,
                values=cls.convert_array(arr.values, check_type, convert),
            )
        elif types.is_large_list(pa_type):
            return pa.LargeListType.from_arrays(
                offsets=arr.offsets,
                values=cls.convert_array(arr.values, check_type, convert),
            )
        elif types.is_fixed_size_list(pa_type):
            return pa.FixedSizeListArray.from_arrays(
                values=cls.convert_array(arr.values, check_type, convert),
            )
        elif types.is_dictionary(pa_type):
            return pa.DictionaryArray.from_arrays(
                indices=arr.indices,
                dictionary=cls.convert_array(arr.dictionary, check_type, convert),
            )
        elif types.is_map(pa_type):
            return pa.MapArray.from_arrays(
                offsets=arr.offsets,
                keys=cls.convert_array(arr.keys, check_type, convert),
                items=cls.convert_array(arr.items, check_type, convert),
            )
        elif types.is_struct(pa_type):
            return pa.StructArray.from_arrays(
                arrays=[
                    cls.convert_array(arr.field(i), check_type, convert)
                    for i in range(len(arr.type))
                ],
                names=arr.type.names,
            )
        else:  # pragma: no cover
            assert False, f"Need converter for {pa_type} but failed to find one."

    @classmethod
    def convert(
        cls,
        arr: Union["pa.Array", "pa.ChunkedArray"],
        check_type: Callable[["pa.DataType"], bool],
        convert: Callable[["pa.Array"], "pa.Array"],
    ) -> Union["pa.Array", "pa.ChunkedArray"]:
        import pyarrow as pa

        assert isinstance(arr, (pa.Array, pa.ChunkedArray))

        # fastpath
        if not cls.check_conversion(arr.type, check_type):
            return arr

        if isinstance(arr, pa.Array):
            return cls.convert_array(arr, check_type, convert)
        else:
            return pa.chunked_array(
                (cls.convert_array(a, check_type, convert) for a in arr.iterchunks())
            )

    @classmethod
    def localize_tz(
        cls,
        arr: Union["pa.Array", "pa.ChunkedArray"],
    ) -> Union["pa.Array", "pa.ChunkedArray"]:
        """
        Convert Arrow timezone-aware timestamps to timezone-naive in the specified timezone.
        This function works on Arrow Arrays, and it recurses to convert nested types.
        This function is dedicated for Pandas UDF execution.

        Differences from _create_converter_to_pandas + _check_series_convert_timestamps_local_tz:
        1, respect the timezone field in pyarrow timestamp type;
        2, do not use local time at any time;
        3, handle nested types in a consistent way. (_create_converter_to_pandas handles
        simple timestamp series with session timezone, but handles nested series with
        datetime.timezone.utc)

        Differences from _check_arrow_array_timestamps_localize:
        1, respect the timezone field in pyarrow timestamp type;
        2, do not handle timezone-naive timestamp;
        3, do not support unit coercion which won't happen in UDF execution.

        Parameters
        ----------
        arr : :class:`pyarrow.Array`

        Returns
        -------
        :class:`pyarrow.Array`

        Notes
        -----
        Arrow UDF (@arrow_udf/mapInArrow/etc) always preserve the original timezone, and thus
        doesn't need this conversion.
        """
        import pyarrow as pa
        import pyarrow.types as types
        import pyarrow.compute as pc

        def check_type_func(pa_type: pa.DataType) -> bool:
            # match timezone-aware TimestampType
            return types.is_timestamp(pa_type) and pa_type.tz is not None

        def convert_func(arr: pa.Array) -> pa.Array:
            assert isinstance(arr, pa.TimestampArray)

            # import datetime
            # from zoneinfo import ZoneInfo
            # ts = datetime.datetime(2022, 1, 5, 15, 0, 1, tzinfo=ZoneInfo('Asia/Singapore'))
            # arr = pa.array([ts])
            # arr[0]
            # <pyarrow.TimestampScalar: '2022-01-05T15:00:01.000000+0800'>
            # arr = pc.local_timestamp(arr)
            # arr[0]
            # <pyarrow.TimestampScalar: '2022-01-05T15:00:01.000000'>
            return pc.local_timestamp(arr)

        return cls.convert(
            arr,
            check_type=check_type_func,
            convert=convert_func,
        )

    @classmethod
    def preprocess_time(
        cls,
        arr: Union["pa.Array", "pa.ChunkedArray"],
    ) -> Union["pa.Array", "pa.ChunkedArray"]:
        """
        1, always drop the timezone from TimestampType;
        2, coerce_temporal_nanoseconds: coerce timestamp time units to nanoseconds
        """
        import pyarrow as pa
        import pyarrow.types as types
        import pyarrow.compute as pc

        def check_type_func(pa_type: pa.DataType) -> bool:
            return types.is_timestamp(pa_type) and (pa_type.unit != "ns" or pa_type.tz is not None)

        def convert_func(arr: pa.Array) -> pa.Array:
            assert isinstance(arr, pa.TimestampArray)

            pa_type = arr.type

            if pa_type.tz is not None:
                arr = pc.local_timestamp(arr)
            if pa_type.unit != "ns":
                arr = pc.cast(arr, target_type=pa.timestamp("ns", tz=None))
            return arr

        return cls.convert(
            arr,
            check_type=check_type_func,
            convert=convert_func,
        )


class ArrowArrayToPandasConversion:
    """
    Conversion utilities for converting PyArrow Arrays and ChunkedArrays to pandas.

    This class provides methods to convert PyArrow columnar data structures to pandas
    Series or DataFrames, with support for Spark-specific type handling and conversions.

    The class is primarily used by PySpark's Arrow-based serializers for UDF execution,
    where Arrow data needs to be converted to pandas for Python UDF processing.
    """

    @classmethod
    def convert(
        cls,
        arrow_column: Union["pa.Array", "pa.ChunkedArray"],
        target_type: DataType,
        *,
        timezone: Optional[str] = None,
        struct_in_pandas: str = "dict",
        ndarray_as_list: bool = False,
        df_for_struct: bool = False,
    ) -> Union["pd.Series", "pd.DataFrame"]:
        """
        Convert a PyArrow Array or ChunkedArray to a pandas Series or DataFrame.

        Parameters
        ----------
        arrow_column : pa.Array or pa.ChunkedArray
            The Arrow column to convert.
        target_type : DataType
            The target Spark type for the column to be converted to.
        timezone : str, optional
            Timezone for timestamp conversion. Required if the data contains timestamp types.
        struct_in_pandas : str, optional
            How to represent struct types in pandas. Valid values are "dict", "row", or "legacy".
            Default is "dict".
        ndarray_as_list : bool, optional
            Whether to convert numpy ndarrays to Python lists. Default is False.
        df_for_struct : bool, optional
            If True, convert struct columns to a DataFrame with columns corresponding
            to struct fields instead of a Series. Default is False.

        Returns
        -------
        pd.Series or pd.DataFrame
            Converted pandas Series. If df_for_struct is True and the type is StructType,
            returns a DataFrame with columns corresponding to struct fields.
        """
        if cls._prefer_convert_numpy(target_type, df_for_struct):
            return cls.convert_numpy(
                arrow_column,
                target_type,
                timezone=timezone,
                struct_in_pandas=struct_in_pandas,
                ndarray_as_list=ndarray_as_list,
                df_for_struct=df_for_struct,
            )

        return cls.convert_legacy(
            arrow_column,
            target_type,
            timezone=timezone,
            struct_in_pandas=struct_in_pandas,
            ndarray_as_list=ndarray_as_list,
            df_for_struct=df_for_struct,
        )

    @classmethod
    def convert_legacy(
        cls,
        arr: Union["pa.Array", "pa.ChunkedArray"],
        spark_type: DataType,
        *,
        timezone: Optional[str] = None,
        struct_in_pandas: Optional[str] = None,
        ndarray_as_list: bool = False,
        df_for_struct: bool = False,
    ) -> Union["pd.Series", "pd.DataFrame"]:
        """
        Convert a PyArrow Array or ChunkedArray to a pandas Series or DataFrame.

        This is the lower-level conversion method that requires explicit Spark type
        specification. For a more convenient API, see :meth:`convert`.

        Parameters
        ----------
        arr : pa.Array or pa.ChunkedArray
            The arrow column to convert.
        spark_type : DataType
            Target Spark type. Must be specified and should match the Arrow array type.
        timezone : str, optional
            The timezone to use for timestamp conversion. Required if the data contains
            timestamp types.
        struct_in_pandas : str, optional
            How to handle struct types in pandas. Valid values are "dict", "row", or "legacy".
            Required if the data contains struct types.
        ndarray_as_list : bool, optional
            Whether to convert numpy ndarrays to Python lists. Default is False.
        df_for_struct : bool, optional
            If True and spark_type is a StructType, return a DataFrame with columns
            corresponding to struct fields instead of a Series. Default is False.

        Returns
        -------
        pd.Series or pd.DataFrame
            Converted pandas Series. If df_for_struct is True and spark_type is StructType,
            returns a DataFrame with columns corresponding to struct fields.

        Notes
        -----
        This method handles date type columns specially to avoid overflow issues with
        datetime64[ns] intermediate representations.
        """
        import pyarrow as pa
        import pandas as pd

        assert isinstance(arr, (pa.Array, pa.ChunkedArray))

        if df_for_struct and isinstance(spark_type, StructType):
            import pyarrow.types as types

            assert types.is_struct(arr.type)
            assert len(spark_type.names) == len(arr.type.names), (
                f"Schema mismatch: spark_type has {len(spark_type.names)} fields, "
                f"but arrow type has {len(arr.type.names)} fields. "
                f"spark_type={spark_type}, arrow_type={arr.type}"
            )

            series = [
                cls.convert_legacy(
                    field_arr,
                    spark_type=field.dataType,
                    timezone=timezone,
                    struct_in_pandas=struct_in_pandas,
                    ndarray_as_list=ndarray_as_list,
                    df_for_struct=False,  # always False for child fields
                )
                for field_arr, field in zip(arr.flatten(), spark_type)
            ]
            pdf = pd.concat(series, axis=1)
            pdf.columns = spark_type.names  # type: ignore[assignment]
            return pdf

        # Convert Arrow array to pandas Series with specific options:
        # - date_as_object: Convert date types to Python datetime.date objects directly
        #   instead of datetime64[ns] to avoid overflow issues
        # - coerce_temporal_nanoseconds: Handle nanosecond precision timestamps correctly
        # - integer_object_nulls: Use object dtype for integer arrays with nulls
        pandas_options = {
            "date_as_object": True,
            "coerce_temporal_nanoseconds": True,
            "integer_object_nulls": True,
        }
        ser = arr.to_pandas(**pandas_options)

        converter = _create_converter_to_pandas(
            data_type=spark_type,
            nullable=True,
            timezone=timezone,
            struct_in_pandas=struct_in_pandas,
            error_on_duplicated_field_names=True,
            ndarray_as_list=ndarray_as_list,
            integer_object_nulls=True,
        )
        return converter(ser)

    @classmethod
    def _prefer_convert_numpy(
        cls,
        spark_type: DataType,
        df_for_struct: bool,
    ) -> bool:
        supported_types = (
            NullType,
            BinaryType,
            BooleanType,
            FloatType,
            DoubleType,
            ByteType,
            ShortType,
            IntegerType,
            LongType,
            TimestampType,
            TimestampNTZType,
        )
        if df_for_struct and isinstance(spark_type, StructType):
            return all(isinstance(f.dataType, supported_types) for f in spark_type.fields)
        else:
            return isinstance(spark_type, supported_types)

    @classmethod
    def convert_numpy(
        cls,
        arr: Union["pa.Array", "pa.ChunkedArray"],
        spark_type: DataType,
        *,
        timezone: Optional[str] = None,
        struct_in_pandas: Optional[str] = None,
        ndarray_as_list: bool = False,
        df_for_struct: bool = False,
    ) -> Union["pd.Series", "pd.DataFrame"]:
        import pyarrow as pa
        import pandas as pd

        assert isinstance(arr, (pa.Array, pa.ChunkedArray))

        if df_for_struct and isinstance(spark_type, StructType):
            import pyarrow.types as types

            assert types.is_struct(arr.type)
            assert len(spark_type.names) == len(arr.type.names), f"{spark_type} {arr.type} "

            pdf: pd.DataFrame = pd.concat(
                [
                    cls.convert_numpy(
                        field_arr,
                        spark_type=field.dataType,
                        timezone=timezone,
                        struct_in_pandas=struct_in_pandas,
                        ndarray_as_list=ndarray_as_list,
                        df_for_struct=False,  # always False for child fields
                    )
                    for field_arr, field in zip(arr.flatten(), spark_type)
                ],
                axis=1,
            )
            pdf.columns = spark_type.names  # type: ignore[assignment]
            return pdf

        # Arrow array from batch.column(idx) contains name,
        # and this name will be used to rename the pandas series
        # returned by array.to_pandas().
        # Right now, the name is dropped in arrow conversions.
        # TODO: should make convert_numpy explicitly pass the expected series name.
        name = arr._name
        arr = ArrowArrayConversion.preprocess_time(arr)

        series: pd.Series

        # TODO(SPARK-55332): Create benchmark for pa.array -> pd.series integer conversion
        # 1, benchmark a nullable integral array
        # a = pa.array(list(range(10000000)) + [9223372036854775707, None], type=pa.int64())
        # %timeit a.to_pandas(types_mapper=pd.ArrowDtype)
        # 11.9 μs ± 407 ns per loop (mean ± std. dev. of 7 runs, 100,000 loops each)
        # %timeit a.to_pandas(types_mapper=pd.ArrowDtype).astype(pd.Int64Dtype())
        # 589 ms ± 9.35 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
        # %timeit pd.Series(a.to_pylist(), dtype=pd.Int64Dtype())
        # 2.94 s ± 19.1 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
        # %timeit a.to_pandas(integer_object_nulls=True).astype(pd.Int64Dtype())
        # 2.05 s ± 22.9 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
        # pd.Series(a, dtype=pd.Int64Dtype())
        # fails due to internal np.float64 coercion
        # OverflowError: Python int too large to convert to C long
        #
        # 2, benchmark a nullable integral array
        # b = pa.array(list(range(10000000)) + [9223372036854775707, 1], type=pa.int64())
        # %timeit b.to_pandas(types_mapper=pd.ArrowDtype).astype(np.int64)
        # 30.2 μs ± 831 ns per loop (mean ± std. dev. of 7 runs, 10,000 loops each)
        # %timeit pd.Series(b.to_pandas(types_mapper=pd.ArrowDtype), dtype=np.int64)
        # 33.3 μs ± 928 ns per loop (mean ± std. dev. of 7 runs, 10,000 loops each)
        # %timeit pd.Series(b, dtype=np.int64) <- lose the name
        # 11.9 μs ± 125 ns per loop (mean ± std. dev. of 7 runs, 100,000 loops each)
        # %timeit b.to_pandas()
        # 7.56 μs ± 96.5 ns per loop (mean ± std. dev. of 7 runs, 100,000 loops each)
        # %timeit b.to_pandas().astype(np.int64) <- astype is non-trivial
        # 19.1 μs ± 242 ns per loop (mean ± std. dev. of 7 runs, 100,000 loops each)
        if isinstance(spark_type, ByteType):
            if arr.null_count > 0:
                series = arr.to_pandas(types_mapper=pd.ArrowDtype).astype(pd.Int8Dtype())
            else:
                series = arr.to_pandas()
        elif isinstance(spark_type, ShortType):
            if arr.null_count > 0:
                series = arr.to_pandas(types_mapper=pd.ArrowDtype).astype(pd.Int16Dtype())
            else:
                series = arr.to_pandas()
        elif isinstance(spark_type, IntegerType):
            if arr.null_count > 0:
                series = arr.to_pandas(types_mapper=pd.ArrowDtype).astype(pd.Int32Dtype())
            else:
                series = arr.to_pandas()
        elif isinstance(spark_type, LongType):
            if arr.null_count > 0:
                series = arr.to_pandas(types_mapper=pd.ArrowDtype).astype(pd.Int64Dtype())
            else:
                series = arr.to_pandas()
        elif isinstance(
            spark_type,
            (
                NullType,
                BinaryType,
                BooleanType,
                FloatType,
                DoubleType,
                DecimalType,
                StringType,
                DateType,
                TimeType,
                TimestampType,
                TimestampNTZType,
                DayTimeIntervalType,
                YearMonthIntervalType,
            ),
        ):
            # TODO(SPARK-55333): Revisit date_as_object in arrow->pandas conversion
            # If the given column is a date type column, creates a series of datetime.date directly
            # instead of creating datetime64[ns] as intermediate data to avoid overflow caused by
            # datetime64[ns] type handling.
            pandas_options = {
                "date_as_object": True,
            }
            series = arr.to_pandas(**pandas_options)
        # elif isinstance(
        #     spark_type,
        #     (
        #         ArrayType,
        #         MapType,
        #         StructType,
        #         UserDefinedType,
        #         VariantType,
        #         GeographyType,
        #         GeometryType,
        #     ),
        # ):
        # TODO(SPARK-55324): Support complex types
        else:  # pragma: no cover
            assert False, f"Need converter for {spark_type} but failed to find one."

        return series.rename(name)
