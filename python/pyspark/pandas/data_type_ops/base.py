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

import numbers
from abc import ABCMeta
from typing import Any, Optional, Union
from itertools import chain

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
from pandas.core.dtypes.common import is_numeric_dtype

from pyspark.sql import functions as F, Column as PySparkColumn
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    FractionalType,
    IntegralType,
    MapType,
    NullType,
    NumericType,
    StringType,
    StructType,
    TimestampType,
    TimestampNTZType,
    UserDefinedType,
)
from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.typedef import extension_dtypes
from pyspark.pandas.typedef.typehints import (
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
    spark_type_to_pandas_dtype,
)
from pyspark.pandas.utils import is_ansi_mode_enabled

if extension_dtypes_available:
    from pandas import Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype

if extension_float_dtypes_available:
    from pandas import Float32Dtype, Float64Dtype

if extension_object_dtypes_available:
    from pandas import BooleanDtype, StringDtype


def is_valid_operand_for_numeric_arithmetic(operand: Any, *, allow_bool: bool = True) -> bool:
    """Check whether the `operand` is valid for arithmetic operations against numerics."""
    from pyspark.pandas.base import IndexOpsMixin

    if isinstance(operand, numbers.Number):
        return not isinstance(operand, bool) or allow_bool
    elif isinstance(operand, IndexOpsMixin):
        if isinstance(operand.dtype, CategoricalDtype):
            return False
        else:
            return isinstance(operand.spark.data_type, NumericType) or (
                allow_bool and isinstance(operand.spark.data_type, BooleanType)
            )
    else:
        return False


def transform_boolean_operand_to_numeric(
    operand: Any, *, spark_type: Optional[DataType] = None
) -> Any:
    """Transform boolean operand to numeric.

    If the `operand` is:
        - a boolean IndexOpsMixin, transform the `operand` to the `spark_type`.
        - a boolean literal, transform to the int value.
    Otherwise, return the operand as it is.
    """
    from pyspark.pandas.base import IndexOpsMixin

    if isinstance(operand, IndexOpsMixin) and isinstance(operand.spark.data_type, BooleanType):
        assert spark_type, "spark_type must be provided if the operand is a boolean IndexOpsMixin"
        assert isinstance(spark_type, NumericType), "spark_type must be NumericType"
        dtype = spark_type_to_pandas_dtype(
            spark_type, use_extension_dtypes=operand._internal.data_fields[0].is_extension_dtype
        )
        return operand._with_new_scol(
            operand.spark.column.cast(spark_type),
            field=operand._internal.data_fields[0].copy(dtype=dtype, spark_type=spark_type),
        )
    elif isinstance(operand, bool):
        return int(operand)
    else:
        return operand


def _should_return_all_false(left: IndexOpsLike, right: Any) -> bool:
    """
    Determine if binary comparison should short-circuit to all False,
    based on incompatible dtypes: non-numeric vs. numeric (including bools).
    """
    from pyspark.pandas.base import IndexOpsMixin
    from pandas.api.types import is_list_like  # type: ignore[attr-defined]

    def are_both_numeric(left_dtype: Dtype, right_dtype: Dtype) -> bool:
        return is_numeric_dtype(left_dtype) and is_numeric_dtype(right_dtype)

    left_dtype = left.dtype

    if isinstance(right, IndexOpsMixin):
        right_dtype = right.dtype
    elif isinstance(right, (list, tuple)):
        right_dtype = pd.Series(right).dtype
    else:
        assert not is_list_like(right), (
            "Only ps.Series, ps.Index, list, tuple, or scalar is supported as the "
            "right-hand operand."
        )
        right_dtype = pd.Series([right]).dtype

    return left_dtype != right_dtype and not are_both_numeric(left_dtype, right_dtype)


def _as_categorical_type(
    index_ops: IndexOpsLike, dtype: CategoricalDtype, spark_type: DataType
) -> IndexOpsLike:
    """Cast `index_ops` to categorical dtype, given `dtype` and `spark_type`."""
    assert isinstance(dtype, CategoricalDtype)
    if dtype.categories is None:
        codes, uniques = index_ops.factorize()
        categories = uniques.astype(index_ops.dtype)
        return codes._with_new_scol(
            codes.spark.column,
            field=codes._internal.data_fields[0].copy(
                dtype=CategoricalDtype(categories=categories)
            ),
        )
    else:
        categories = dtype.categories
        if len(categories) == 0:
            scol = F.lit(-1)
        else:
            kvs = chain(
                *[(F.lit(category), F.lit(code)) for code, category in enumerate(categories)]
            )
            map_scol = F.create_map(*kvs)
            scol = F.coalesce(map_scol[index_ops.spark.column], F.lit(-1))

        return index_ops._with_new_scol(
            scol.cast(spark_type),
            field=index_ops._internal.data_fields[0].copy(
                dtype=dtype, spark_type=spark_type, nullable=False
            ),
        )


def _as_bool_type(index_ops: IndexOpsLike, dtype: Dtype) -> IndexOpsLike:
    """Cast `index_ops` to BooleanType Spark type, given `dtype`."""
    spark_type = BooleanType()
    if isinstance(dtype, extension_dtypes):
        scol = index_ops.spark.column.cast(spark_type)
    else:
        null_value = (
            F.lit(True) if isinstance(index_ops.spark.data_type, DecimalType) else F.lit(False)
        )
        scol = F.when(index_ops.spark.column.isNull(), null_value).otherwise(
            index_ops.spark.column.cast(spark_type)
        )
    return index_ops._with_new_scol(
        scol, field=index_ops._internal.data_fields[0].copy(dtype=dtype, spark_type=spark_type)
    )


def _as_string_type(
    index_ops: IndexOpsLike, dtype: Dtype, *, null_str: str = str(None)
) -> IndexOpsLike:
    """Cast `index_ops` to StringType Spark type, given `dtype` and `null_str`,
    representing null Spark column. Note that `null_str` is for non-extension dtypes only.
    """
    spark_type = StringType()
    if isinstance(dtype, extension_dtypes):
        scol = index_ops.spark.column.cast(spark_type)
    else:
        casted = index_ops.spark.column.cast(spark_type)
        scol = F.when(index_ops.spark.column.isNull(), null_str).otherwise(casted)
    return index_ops._with_new_scol(
        scol, field=index_ops._internal.data_fields[0].copy(dtype=dtype, spark_type=spark_type)
    )


def _as_other_type(index_ops: IndexOpsLike, dtype: Dtype, spark_type: DataType) -> IndexOpsLike:
    """Cast `index_ops` to a `dtype` (`spark_type`) that needs no pre-processing.

    Destination types that need pre-processing: CategoricalDtype, BooleanType, and StringType.
    """
    from pyspark.pandas.internal import InternalField

    need_pre_process = (
        isinstance(dtype, CategoricalDtype)
        or isinstance(spark_type, BooleanType)
        or isinstance(spark_type, StringType)
    )
    assert not need_pre_process, "Pre-processing is needed before the type casting."

    scol = index_ops.spark.column.cast(spark_type)
    return index_ops._with_new_scol(scol, field=InternalField(dtype=dtype))


def _sanitize_list_like(operand: Any) -> None:
    """Raise TypeError if operand is list-like."""
    if isinstance(operand, (list, tuple, dict, set)):
        raise TypeError("The operation can not be applied to %s." % type(operand).__name__)


def _is_valid_for_logical_operator(right: Any) -> bool:
    from pyspark.pandas.base import IndexOpsMixin

    return isinstance(right, (int, bool)) or (
        isinstance(right, IndexOpsMixin)
        and (
            isinstance(right.spark.data_type, BooleanType)
            or isinstance(right.spark.data_type, IntegralType)
        )
    )


def _is_boolean_type(right: Any) -> bool:
    from pyspark.pandas.base import IndexOpsMixin

    return isinstance(right, bool) or (
        isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, BooleanType)
    )


def _is_extension_dtypes(object: Any) -> bool:
    """
    Check whether the type of given object is extension dtype or not.
    Extention dtype includes Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype, BooleanDtype,
    StringDtype, Float32Dtype and Float64Dtype.
    """
    return isinstance(getattr(object, "dtype", None), extension_dtypes)


class DataTypeOps(object, metaclass=ABCMeta):
    """The base class for binary operations of pandas-on-Spark objects (of different data types)."""

    def __new__(cls, dtype: Dtype, spark_type: DataType) -> "DataTypeOps":
        from pyspark.pandas.data_type_ops.binary_ops import BinaryOps
        from pyspark.pandas.data_type_ops.boolean_ops import BooleanOps, BooleanExtensionOps
        from pyspark.pandas.data_type_ops.categorical_ops import CategoricalOps
        from pyspark.pandas.data_type_ops.complex_ops import ArrayOps, MapOps, StructOps
        from pyspark.pandas.data_type_ops.date_ops import DateOps
        from pyspark.pandas.data_type_ops.datetime_ops import DatetimeOps, DatetimeNTZOps
        from pyspark.pandas.data_type_ops.null_ops import NullOps
        from pyspark.pandas.data_type_ops.num_ops import (
            DecimalOps,
            FractionalExtensionOps,
            FractionalOps,
            IntegralExtensionOps,
            IntegralOps,
        )
        from pyspark.pandas.data_type_ops.string_ops import StringOps, StringExtensionOps
        from pyspark.pandas.data_type_ops.timedelta_ops import TimedeltaOps
        from pyspark.pandas.data_type_ops.udt_ops import UDTOps

        if isinstance(dtype, CategoricalDtype):
            return object.__new__(CategoricalOps)
        elif isinstance(spark_type, DecimalType):
            return object.__new__(DecimalOps)
        elif isinstance(spark_type, FractionalType):
            if extension_float_dtypes_available and type(dtype) in [Float32Dtype, Float64Dtype]:
                return object.__new__(FractionalExtensionOps)
            else:
                return object.__new__(FractionalOps)
        elif isinstance(spark_type, IntegralType):
            if extension_dtypes_available and type(dtype) in [
                Int8Dtype,
                Int16Dtype,
                Int32Dtype,
                Int64Dtype,
            ]:
                return object.__new__(IntegralExtensionOps)
            else:
                return object.__new__(IntegralOps)
        elif isinstance(spark_type, StringType):
            if extension_object_dtypes_available and isinstance(dtype, StringDtype):
                return object.__new__(StringExtensionOps)
            else:
                return object.__new__(StringOps)
        elif isinstance(spark_type, BooleanType):
            if extension_object_dtypes_available and isinstance(dtype, BooleanDtype):
                return object.__new__(BooleanExtensionOps)
            else:
                return object.__new__(BooleanOps)
        elif isinstance(spark_type, TimestampType):
            return object.__new__(DatetimeOps)
        elif isinstance(spark_type, TimestampNTZType):
            return object.__new__(DatetimeNTZOps)
        elif isinstance(spark_type, DateType):
            return object.__new__(DateOps)
        elif isinstance(spark_type, DayTimeIntervalType):
            return object.__new__(TimedeltaOps)
        elif isinstance(spark_type, BinaryType):
            return object.__new__(BinaryOps)
        elif isinstance(spark_type, ArrayType):
            return object.__new__(ArrayOps)
        elif isinstance(spark_type, MapType):
            return object.__new__(MapOps)
        elif isinstance(spark_type, StructType):
            return object.__new__(StructOps)
        elif isinstance(spark_type, NullType):
            return object.__new__(NullOps)
        elif isinstance(spark_type, UserDefinedType):
            return object.__new__(UDTOps)
        else:
            raise TypeError("Type %s was not understood." % dtype)

    def __init__(self, dtype: Dtype, spark_type: DataType):
        self.dtype = dtype
        self.spark_type = spark_type

    @property
    def pretty_name(self) -> str:
        raise NotImplementedError()

    def add(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Addition can not be applied to %s." % self.pretty_name)

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Subtraction can not be applied to %s." % self.pretty_name)

    def mul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Multiplication can not be applied to %s." % self.pretty_name)

    def truediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("True division can not be applied to %s." % self.pretty_name)

    def floordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Floor division can not be applied to %s." % self.pretty_name)

    def mod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Modulo can not be applied to %s." % self.pretty_name)

    def pow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Exponentiation can not be applied to %s." % self.pretty_name)

    def radd(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Addition can not be applied to %s." % self.pretty_name)

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Subtraction can not be applied to %s." % self.pretty_name)

    def rmul(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Multiplication can not be applied to %s." % self.pretty_name)

    def rtruediv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("True division can not be applied to %s." % self.pretty_name)

    def rfloordiv(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Floor division can not be applied to %s." % self.pretty_name)

    def rmod(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Modulo can not be applied to %s." % self.pretty_name)

    def rpow(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Exponentiation can not be applied to %s." % self.pretty_name)

    def __and__(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Bitwise and can not be applied to %s." % self.pretty_name)

    def xor(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Bitwise xor can not be applied to %s." % self.pretty_name)

    def __or__(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("Bitwise or can not be applied to %s." % self.pretty_name)

    def rand(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return left.__and__(right)

    def rxor(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return left ^ right

    def ror(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        _sanitize_list_like(right)
        return left.__or__(right)

    def neg(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary - can not be applied to %s." % self.pretty_name)

    def abs(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("abs() can not be applied to %s." % self.pretty_name)

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("< can not be applied to %s." % self.pretty_name)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("<= can not be applied to %s." % self.pretty_name)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError("> can not be applied to %s." % self.pretty_name)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        raise TypeError(">= can not be applied to %s." % self.pretty_name)

    def eq(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        if is_ansi_mode_enabled(left._internal.spark_frame.sparkSession):
            if _should_return_all_false(left, right):
                return left._with_new_scol(F.lit(False)).rename(None)  # type: ignore[attr-defined]

        if isinstance(right, (list, tuple)):
            from pyspark.pandas.series import first_series, scol_for
            from pyspark.pandas.frame import DataFrame
            from pyspark.pandas.internal import NATURAL_ORDER_COLUMN_NAME, InternalField

            if len(left) != len(right):
                raise ValueError("Lengths must be equal")

            sdf = left._internal.spark_frame
            structed_scol = F.struct(
                sdf[NATURAL_ORDER_COLUMN_NAME],
                *left._internal.index_spark_columns,
                left.spark.column,
            )
            # The size of the list is expected to be small.
            collected_structed_scol = F.collect_list(structed_scol)
            # Sort the array by NATURAL_ORDER_COLUMN so that we can guarantee the order.
            collected_structed_scol = F.array_sort(collected_structed_scol)
            right_values_scol = F.array(*(F.lit(x) for x in right))
            index_scol_names = left._internal.index_spark_column_names
            scol_name = left._internal.spark_column_name_for(left._internal.column_labels[0])
            # Compare the values of left and right by using zip_with function.
            cond = F.zip_with(
                collected_structed_scol,
                right_values_scol,
                lambda x, y: F.struct(
                    *[
                        x[index_scol_name].alias(index_scol_name)
                        for index_scol_name in index_scol_names
                    ],
                    F.when(x[scol_name].isNull() | y.isNull(), False)
                    .otherwise(
                        x[scol_name] == y,
                    )
                    .alias(scol_name),
                ),
            ).alias(scol_name)
            # 1. `sdf_new` here looks like the below (the first field of each set is Index):
            # +----------------------------------------------------------+
            # |0                                                         |
            # +----------------------------------------------------------+
            # |[{0, false}, {1, true}, {2, false}, {3, true}, {4, false}]|
            # +----------------------------------------------------------+
            sdf_new = sdf.select(cond)
            # 2. `sdf_new` after the explode looks like the below:
            # +----------+
            # |       col|
            # +----------+
            # |{0, false}|
            # | {1, true}|
            # |{2, false}|
            # | {3, true}|
            # |{4, false}|
            # +----------+
            sdf_new = sdf_new.select(F.explode(scol_name))
            # 3. Here, the final `sdf_new` looks like the below:
            # +-----------------+-----+
            # |__index_level_0__|    0|
            # +-----------------+-----+
            # |                0|false|
            # |                1| true|
            # |                2|false|
            # |                3| true|
            # |                4|false|
            # +-----------------+-----+
            sdf_new = sdf_new.select("col.*")

            index_spark_columns = [
                scol_for(sdf_new, index_scol_name) for index_scol_name in index_scol_names
            ]
            data_spark_columns = [scol_for(sdf_new, scol_name)]

            internal = left._internal.copy(
                spark_frame=sdf_new,
                index_spark_columns=index_spark_columns,
                data_spark_columns=data_spark_columns,
                index_fields=[
                    InternalField.from_struct_field(index_field)
                    for index_field in sdf_new.select(index_spark_columns).schema.fields
                ],
                data_fields=[
                    InternalField.from_struct_field(
                        sdf_new.select(data_spark_columns).schema.fields[0]
                    )
                ],
            )
            return first_series(DataFrame(internal))
        else:
            from pyspark.pandas.base import column_op

            return column_op(PySparkColumn.__eq__)(left, right)

    def ne(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        from pyspark.pandas.base import column_op

        _sanitize_list_like(right)

        return column_op(PySparkColumn.__ne__)(left, right)

    def invert(self, operand: IndexOpsLike) -> IndexOpsLike:
        raise TypeError("Unary ~ can not be applied to %s." % self.pretty_name)

    def restore(self, col: pd.Series) -> pd.Series:
        """Restore column when to_pandas."""
        return col

    def prepare(self, col: pd.Series) -> pd.Series:
        """Prepare column when from_pandas."""
        return col.replace({np.nan: None})

    def isnull(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        return index_ops._with_new_scol(
            index_ops.spark.column.isNull(),
            field=index_ops._internal.data_fields[0].copy(
                dtype=np.dtype("bool"), spark_type=BooleanType(), nullable=False
            ),
        )

    def nan_to_null(self, index_ops: IndexOpsLike) -> IndexOpsLike:
        return index_ops.copy()

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        raise TypeError("astype can not be applied to %s." % self.pretty_name)
