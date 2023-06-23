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

"""
Utilities to deal with types. This is mostly focused on python3.
"""
import datetime
import decimal
import sys
import typing
from collections.abc import Iterable
from distutils.version import LooseVersion
from inspect import isclass
from typing import Any, Callable, Generic, List, Tuple, Union, Type, get_type_hints

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype, pandas_dtype  # type: ignore[attr-defined]
from pandas.api.extensions import ExtensionDtype

extension_dtypes: Tuple[type, ...]
try:
    from pandas import Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype

    extension_dtypes_available = True
    extension_dtypes = (Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype)

    try:
        from pandas import BooleanDtype, StringDtype

        extension_object_dtypes_available = True
        extension_dtypes += (BooleanDtype, StringDtype)
    except ImportError:
        extension_object_dtypes_available = False

    try:
        from pandas import Float32Dtype, Float64Dtype

        extension_float_dtypes_available = True
        extension_dtypes += (Float32Dtype, Float64Dtype)
    except ImportError:
        extension_float_dtypes_available = False

except ImportError:
    extension_dtypes_available = False
    extension_object_dtypes_available = False
    extension_float_dtypes_available = False
    extension_dtypes = ()

import pyarrow as pa
import pyspark.sql.types as types
from pyspark.sql.pandas.types import to_arrow_type, from_arrow_type

# For running doctests and reference resolution in PyCharm.
from pyspark import pandas as ps  # noqa: F401
from pyspark.pandas._typing import Dtype, T

if typing.TYPE_CHECKING:
    from pyspark.pandas.internal import InternalField


# A column of data, with the data type.
class SeriesType(Generic[T]):
    def __init__(self, dtype: Dtype, spark_type: types.DataType):
        self.dtype = dtype
        self.spark_type = spark_type

    def __repr__(self) -> str:
        return "SeriesType[{}]".format(self.spark_type)


class DataFrameType:
    def __init__(
        self,
        index_fields: List["InternalField"],
        data_fields: List["InternalField"],
    ):
        self.index_fields = index_fields
        self.data_fields = data_fields
        self.fields = index_fields + data_fields

    @property
    def dtypes(self) -> List[Dtype]:
        return [field.dtype for field in self.fields]

    @property
    def spark_type(self) -> types.StructType:
        return types.StructType([field.struct_field for field in self.fields])

    def __repr__(self) -> str:
        return "DataFrameType[{}]".format(self.spark_type)


# The type is a scalar type that is furthermore understood by Spark.
class ScalarType:
    def __init__(self, dtype: Dtype, spark_type: types.DataType):
        self.dtype = dtype
        self.spark_type = spark_type

    def __repr__(self) -> str:
        return "ScalarType[{}]".format(self.spark_type)


# The type is left unspecified or we do not know about this type.
class UnknownType:
    def __init__(self, tpe: Any):
        self.tpe = tpe

    def __repr__(self) -> str:
        return "UnknownType[{}]".format(self.tpe)


class IndexNameTypeHolder:
    name = None
    tpe = None
    short_name = "IndexNameType"


class NameTypeHolder:
    name = None
    tpe = None
    short_name = "NameType"


def as_spark_type(
    tpe: Union[str, type, Dtype], *, raise_error: bool = True, prefer_timestamp_ntz: bool = False
) -> types.DataType:
    """
    Given a Python type, returns the equivalent spark type.
    Accepts:
    - the built-in types in Python
    - the built-in types in numpy
    - list of pairs of (field_name, type)
    - dictionaries of field_name -> type
    - Python3's typing system
    """
    # For NumPy typing, NumPy version should be 1.21+ and Python version should be 3.8+
    if sys.version_info >= (3, 8) and LooseVersion(np.__version__) >= LooseVersion("1.21"):
        if (
            hasattr(tpe, "__origin__")
            and tpe.__origin__ is np.ndarray  # type: ignore[union-attr]
            and hasattr(tpe, "__args__")
            and len(tpe.__args__) > 1  # type: ignore[union-attr]
        ):
            # numpy.typing.NDArray
            return types.ArrayType(
                as_spark_type(
                    tpe.__args__[1].__args__[0], raise_error=raise_error  # type: ignore[union-attr]
                )
            )

    if isinstance(tpe, np.dtype) and tpe == np.dtype("object"):
        pass
    # ArrayType
    elif tpe in (np.ndarray,):
        return types.ArrayType(types.StringType())
    elif hasattr(tpe, "__origin__") and issubclass(
        tpe.__origin__, list  # type: ignore[union-attr]
    ):
        element_type = as_spark_type(
            tpe.__args__[0], raise_error=raise_error  # type: ignore[union-attr]
        )
        if element_type is None:
            return None
        return types.ArrayType(element_type)
    # BinaryType
    elif tpe in (bytes, np.character, np.bytes_, np.string_):
        return types.BinaryType()
    # BooleanType
    elif tpe in (bool, np.bool_, "bool", "?"):
        return types.BooleanType()
    # DateType
    elif tpe in (datetime.date,):
        return types.DateType()
    # NumericType
    elif tpe in (np.int8, np.byte, "int8", "byte", "b"):
        return types.ByteType()
    elif tpe in (decimal.Decimal,):
        # TODO: considering the precision & scale for decimal type.
        return types.DecimalType(38, 18)
    elif tpe in (float, np.float_, np.float64, "float", "float64", "double"):
        return types.DoubleType()
    elif tpe in (np.float32, "float32", "f"):
        return types.FloatType()
    elif tpe in (np.int32, "int32", "i"):
        return types.IntegerType()
    elif tpe in (int, np.int64, "int", "int64", "long"):
        return types.LongType()
    elif tpe in (np.int16, "int16", "short"):
        return types.ShortType()
    # StringType
    elif tpe in (str, np.unicode_, "str", "U"):
        return types.StringType()
    # TimestampType or TimestampNTZType if timezone is not specified.
    elif tpe in (datetime.datetime, np.datetime64, "datetime64[ns]", "M"):
        return types.TimestampNTZType() if prefer_timestamp_ntz else types.TimestampType()

    # DayTimeIntervalType
    elif tpe in (datetime.timedelta, np.timedelta64, "timedelta64[ns]"):
        return types.DayTimeIntervalType()

    # categorical types
    elif isinstance(tpe, CategoricalDtype) or (isinstance(tpe, str) and tpe == "category"):
        return types.LongType()

    # extension types
    elif extension_dtypes_available:
        # IntegralType
        if isinstance(tpe, Int8Dtype) or (isinstance(tpe, str) and tpe == "Int8"):
            return types.ByteType()
        elif isinstance(tpe, Int16Dtype) or (isinstance(tpe, str) and tpe == "Int16"):
            return types.ShortType()
        elif isinstance(tpe, Int32Dtype) or (isinstance(tpe, str) and tpe == "Int32"):
            return types.IntegerType()
        elif isinstance(tpe, Int64Dtype) or (isinstance(tpe, str) and tpe == "Int64"):
            return types.LongType()

        if extension_object_dtypes_available:
            # BooleanType
            if isinstance(tpe, BooleanDtype) or (isinstance(tpe, str) and tpe == "boolean"):
                return types.BooleanType()
            # StringType
            elif isinstance(tpe, StringDtype) or (isinstance(tpe, str) and tpe == "string"):
                return types.StringType()

        if extension_float_dtypes_available:
            # FractionalType
            if isinstance(tpe, Float32Dtype) or (isinstance(tpe, str) and tpe == "Float32"):
                return types.FloatType()
            elif isinstance(tpe, Float64Dtype) or (isinstance(tpe, str) and tpe == "Float64"):
                return types.DoubleType()

    if raise_error:
        raise TypeError("Type %s was not understood." % tpe)
    else:
        return None


def spark_type_to_pandas_dtype(
    spark_type: types.DataType, *, use_extension_dtypes: bool = False
) -> Dtype:
    """Return the given Spark DataType to pandas dtype."""

    if use_extension_dtypes and extension_dtypes_available:
        # IntegralType
        if isinstance(spark_type, types.ByteType):
            return Int8Dtype()
        elif isinstance(spark_type, types.ShortType):
            return Int16Dtype()
        elif isinstance(spark_type, types.IntegerType):
            return Int32Dtype()
        elif isinstance(spark_type, types.LongType):
            return Int64Dtype()

        if extension_object_dtypes_available:
            # BooleanType
            if isinstance(spark_type, types.BooleanType):
                return BooleanDtype()
            # StringType
            elif isinstance(spark_type, types.StringType):
                return StringDtype()

        # FractionalType
        if extension_float_dtypes_available:
            if isinstance(spark_type, types.FloatType):
                return Float32Dtype()
            elif isinstance(spark_type, types.DoubleType):
                return Float64Dtype()

    if isinstance(
        spark_type,
        (
            types.DateType,
            types.NullType,
            types.ArrayType,
            types.MapType,
            types.StructType,
            types.UserDefinedType,
        ),
    ):
        return np.dtype("object")
    elif isinstance(spark_type, types.TimestampType):
        return np.dtype("datetime64[ns]")
    else:
        return np.dtype(to_arrow_type(spark_type).to_pandas_dtype())


def pandas_on_spark_type(tpe: Union[str, type, Dtype]) -> Tuple[Dtype, types.DataType]:
    """
    Convert input into a pandas only dtype object or a numpy dtype object,
    and its corresponding Spark DataType.

    Parameters
    ----------
    tpe : object to be converted

    Returns
    -------
    tuple of np.dtype or a pandas dtype, and Spark DataType

    Raises
    ------
    TypeError if not a dtype

    Examples
    --------
    >>> pandas_on_spark_type(int)
    (dtype('int64'), LongType())
    >>> pandas_on_spark_type(str)
    (dtype('<U'), StringType())
    >>> pandas_on_spark_type(datetime.date)
    (dtype('O'), DateType())
    >>> pandas_on_spark_type(datetime.datetime)
    (dtype('<M8[ns]'), TimestampType())
    >>> pandas_on_spark_type(datetime.timedelta)
    (dtype('<m8[ns]'), DayTimeIntervalType(0, 3))
    >>> pandas_on_spark_type(List[bool])
    (dtype('O'), ArrayType(BooleanType(), True))
    """
    try:
        dtype = pandas_dtype(tpe)
        spark_type = as_spark_type(dtype)
    except TypeError:
        spark_type = as_spark_type(tpe)
        dtype = spark_type_to_pandas_dtype(spark_type)
    return dtype, spark_type


def infer_pd_series_spark_type(
    pser: pd.Series, dtype: Dtype, prefer_timestamp_ntz: bool = False
) -> types.DataType:
    """Infer Spark DataType from pandas Series dtype.

    :param pser: :class:`pandas.Series` to be inferred
    :param dtype: the Series' dtype
    :param prefer_timestamp_ntz: if true, infers datetime without timezone as
        TimestampNTZType type. If false, infers it as TimestampType.
    :return: the inferred Spark data type
    """
    if dtype == np.dtype("object"):
        if len(pser) == 0 or pser.isnull().all():
            return types.NullType()
        elif hasattr(pser.iloc[0], "__UDT__"):
            return pser.iloc[0].__UDT__
        else:
            return from_arrow_type(pa.Array.from_pandas(pser).type, prefer_timestamp_ntz)
    elif isinstance(dtype, CategoricalDtype):
        if isinstance(pser.dtype, CategoricalDtype):
            return as_spark_type(pser.cat.codes.dtype, prefer_timestamp_ntz=prefer_timestamp_ntz)
        else:
            # `pser` must already be converted to codes.
            return as_spark_type(pser.dtype, prefer_timestamp_ntz=prefer_timestamp_ntz)
    else:
        return as_spark_type(dtype, prefer_timestamp_ntz=prefer_timestamp_ntz)


def infer_return_type(f: Callable) -> Union[SeriesType, DataFrameType, ScalarType, UnknownType]:
    """
    Infer the return type from the return type annotation of the given function.

    The returned type class indicates both dtypes (a pandas only dtype object
    or a numpy dtype object) and its corresponding Spark DataType.

    >>> def func() -> int:
    ...    pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtype
    dtype('int64')
    >>> inferred.spark_type
    LongType()

    >>> def func() -> ps.Series[int]:
    ...    pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtype
    dtype('int64')
    >>> inferred.spark_type
    LongType()

    >>> def func() -> ps.DataFrame[float, str]:
    ...    pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('float64'), dtype('<U')]
    >>> inferred.spark_type
    StructType([StructField('c0', DoubleType(), True), StructField('c1', StringType(), True)])

    >>> def func() -> ps.DataFrame[float]:
    ...    pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('float64')]
    >>> inferred.spark_type
    StructType([StructField('c0', DoubleType(), True)])

    >>> def func() -> 'int':
    ...    pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtype
    dtype('int64')
    >>> inferred.spark_type
    LongType()

    >>> def func() -> 'ps.Series[int]':
    ...    pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtype
    dtype('int64')
    >>> inferred.spark_type
    LongType()

    >>> def func() -> 'ps.DataFrame[float, str]':
    ...    pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('float64'), dtype('<U')]
    >>> inferred.spark_type
    StructType([StructField('c0', DoubleType(), True), StructField('c1', StringType(), True)])

    >>> def func() -> 'ps.DataFrame[float]':
    ...    pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('float64')]
    >>> inferred.spark_type
    StructType([StructField('c0', DoubleType(), True)])

    >>> def func() -> ps.DataFrame['a': float, 'b': int]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('float64'), dtype('int64')]
    >>> inferred.spark_type
    StructType([StructField('a', DoubleType(), True), StructField('b', LongType(), True)])

    >>> def func() -> "ps.DataFrame['a': float, 'b': int]":
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('float64'), dtype('int64')]
    >>> inferred.spark_type
    StructType([StructField('a', DoubleType(), True), StructField('b', LongType(), True)])

    >>> pdf = pd.DataFrame({"a": [1, 2, 3], "b": [3, 4, 5]})
    >>> def func() -> ps.DataFrame[pdf.dtypes]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('int64'), dtype('int64')]
    >>> inferred.spark_type
    StructType([StructField('c0', LongType(), True), StructField('c1', LongType(), True)])

    >>> pdf = pd.DataFrame({"a": [1, 2, 3], "b": [3, 4, 5]})
    >>> def func() -> ps.DataFrame[zip(pdf.columns, pdf.dtypes)]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('int64'), dtype('int64')]
    >>> inferred.spark_type
    StructType([StructField('a', LongType(), True), StructField('b', LongType(), True)])

    >>> pdf = pd.DataFrame({("x", "a"): [1, 2, 3], ("y", "b"): [3, 4, 5]})
    >>> def func() -> ps.DataFrame[zip(pdf.columns, pdf.dtypes)]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('int64'), dtype('int64')]
    >>> inferred.spark_type
    StructType([StructField('(x, a)', LongType(), True), StructField('(y, b)', LongType(), True)])

    >>> pdf = pd.DataFrame({"a": [1, 2, 3], "b": pd.Categorical([3, 4, 5])})
    >>> def func() -> ps.DataFrame[pdf.dtypes]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('int64'), CategoricalDtype(categories=[3, 4, 5], ordered=False)]
    >>> inferred.spark_type
    StructType([StructField('c0', LongType(), True), StructField('c1', LongType(), True)])

    >>> def func() -> ps.DataFrame[zip(pdf.columns, pdf.dtypes)]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('int64'), CategoricalDtype(categories=[3, 4, 5], ordered=False)]
    >>> inferred.spark_type
    StructType([StructField('a', LongType(), True), StructField('b', LongType(), True)])

    >>> def func() -> ps.Series[pdf.b.dtype]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtype
    CategoricalDtype(categories=[3, 4, 5], ordered=False)
    >>> inferred.spark_type
    LongType()

    >>> def func() -> ps.DataFrame[int, [int, int]]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('int64'), dtype('int64'), dtype('int64')]
    >>> inferred.spark_type.simpleString()
    'struct<__index_level_0__:bigint,c0:bigint,c1:bigint>'
    >>> inferred.index_fields
    [InternalField(dtype=int64, struct_field=StructField('__index_level_0__', LongType(), True))]

    >>> def func() -> ps.DataFrame[pdf.index.dtype, pdf.dtypes]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('int64'), dtype('int64'), CategoricalDtype(categories=[3, 4, 5], ordered=False)]
    >>> inferred.spark_type.simpleString()
    'struct<__index_level_0__:bigint,c0:bigint,c1:bigint>'
    >>> inferred.index_fields
    [InternalField(dtype=int64, struct_field=StructField('__index_level_0__', LongType(), True))]

    >>> def func() -> ps.DataFrame[
    ...     ("index", CategoricalDtype(categories=[3, 4, 5], ordered=False)),
    ...     [("id", int), ("A", int)]]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [CategoricalDtype(categories=[3, 4, 5], ordered=False), dtype('int64'), dtype('int64')]
    >>> inferred.spark_type.simpleString()
    'struct<index:bigint,id:bigint,A:bigint>'
    >>> inferred.index_fields
    [InternalField(dtype=category, struct_field=StructField('index', LongType(), True))]

    >>> def func() -> ps.DataFrame[
    ...         (pdf.index.name, pdf.index.dtype), zip(pdf.columns, pdf.dtypes)]:
    ...     pass
    >>> inferred = infer_return_type(func)
    >>> inferred.dtypes
    [dtype('int64'), dtype('int64'), CategoricalDtype(categories=[3, 4, 5], ordered=False)]
    >>> inferred.spark_type.simpleString()
    'struct<__index_level_0__:bigint,a:bigint,b:bigint>'
    >>> inferred.index_fields
    [InternalField(dtype=int64, struct_field=StructField('__index_level_0__', LongType(), True))]
    """
    # We should re-import to make sure the class 'SeriesType' is not treated as a class
    # within this module locally. See Series.__class_getitem__ which imports this class
    # canonically.
    from pyspark.pandas.internal import InternalField, SPARK_INDEX_NAME_FORMAT
    from pyspark.pandas.typedef import SeriesType, NameTypeHolder, IndexNameTypeHolder
    from pyspark.pandas.utils import name_like_string

    tpe = get_type_hints(f).get("return", None)

    if tpe is None:
        raise ValueError("A return value is required for the input function")

    if hasattr(tpe, "__origin__") and issubclass(tpe.__origin__, SeriesType):
        tpe = tpe.__args__[0]
        if issubclass(tpe, NameTypeHolder):
            tpe = tpe.tpe
        dtype, spark_type = pandas_on_spark_type(tpe)
        return SeriesType(dtype, spark_type)

    # Note that, DataFrame type hints will create a Tuple.
    # Tuple has _name but other types have __name__
    name = getattr(tpe, "_name", getattr(tpe, "__name__", None))
    # Check if the name is Tuple.
    if name == "Tuple":
        tuple_type = tpe
        parameters = getattr(tuple_type, "__args__")

        index_parameters = [
            p for p in parameters if isclass(p) and issubclass(p, IndexNameTypeHolder)
        ]
        data_parameters = [p for p in parameters if p not in index_parameters]
        assert len(data_parameters) > 0, "Type hints for data must not be empty."

        index_fields = []
        if len(index_parameters) >= 1:
            for level, index_parameter in enumerate(index_parameters):
                index_name = index_parameter.name
                index_dtype, index_spark_type = pandas_on_spark_type(index_parameter.tpe)
                index_fields.append(
                    InternalField(
                        dtype=index_dtype,
                        struct_field=types.StructField(
                            name=index_name
                            if index_name is not None
                            else SPARK_INDEX_NAME_FORMAT(level),
                            dataType=index_spark_type,
                        ),
                    )
                )
        else:
            # No type hint for index.
            assert len(index_parameters) == 0

        data_dtypes, data_spark_types = zip(
            *(
                pandas_on_spark_type(p.tpe)
                if isclass(p) and issubclass(p, NameTypeHolder)
                else pandas_on_spark_type(p)
                for p in data_parameters
            )
        )
        data_names = [
            p.name if isclass(p) and issubclass(p, NameTypeHolder) else None
            for p in data_parameters
        ]
        data_fields = []
        for i, (data_name, data_dtype, data_spark_type) in enumerate(
            zip(data_names, data_dtypes, data_spark_types)
        ):
            data_fields.append(
                InternalField(
                    dtype=data_dtype,
                    struct_field=types.StructField(
                        name=name_like_string(data_name) if data_name is not None else ("c%s" % i),
                        dataType=data_spark_type,
                    ),
                )
            )

        return DataFrameType(index_fields=index_fields, data_fields=data_fields)

    tpes = pandas_on_spark_type(tpe)
    if tpes is None:
        return UnknownType(tpe)
    else:
        return ScalarType(*tpes)


# TODO: once pandas exposes a typing module like numpy.typing, we should deprecate
#   this logic and migrate to it by implementing the typing module in pandas API on Spark.


def create_type_for_series_type(param: Any) -> Type[SeriesType]:
    """
    Supported syntax:

    >>> str(ps.Series[float]).endswith("SeriesType[float]")
    True
    """
    from pyspark.pandas.typedef import NameTypeHolder

    new_class: Type[NameTypeHolder]
    if isinstance(param, ExtensionDtype):
        new_class = type(NameTypeHolder.short_name, (NameTypeHolder,), {})
        new_class.tpe = param  # type: ignore[assignment]
    else:
        new_class = param.type if isinstance(param, np.dtype) else param

    return SeriesType[new_class]  # type: ignore[valid-type]


# TODO: Remove this variadic-generic hack by tuple once ww drop Python up to 3.9.
#   See also PEP 646. One problem is that pandas doesn't inherits Generic[T]
#   so we might have to leave this hack only for monkey-patching pandas DataFrame.
def create_tuple_for_frame_type(params: Any) -> object:
    """
    This is a workaround to support variadic generic in DataFrame.

    See https://github.com/python/typing/issues/193
    we always wraps the given type hints by a tuple to mimic the variadic generic.

    Supported syntax:

    >>> import pandas as pd
    >>> pdf = pd.DataFrame({'a': range(1)})

    Typing data columns only:

        >>> ps.DataFrame[float, float]  # doctest: +ELLIPSIS
        typing.Tuple[...NameType, ...NameType]
        >>> ps.DataFrame[pdf.dtypes]  # doctest: +ELLIPSIS
        typing.Tuple[...NameType]
        >>> ps.DataFrame["id": int, "A": int]  # doctest: +ELLIPSIS
        typing.Tuple[...NameType, ...NameType]
        >>> ps.DataFrame[zip(pdf.columns, pdf.dtypes)]  # doctest: +ELLIPSIS
        typing.Tuple[...NameType]

    Typing data columns with an index:

        >>> ps.DataFrame[int, [int, int]]  # doctest: +ELLIPSIS
        typing.Tuple[...IndexNameType, ...NameType, ...NameType]
        >>> ps.DataFrame[pdf.index.dtype, pdf.dtypes]  # doctest: +ELLIPSIS
        typing.Tuple[...IndexNameType, ...NameType]
        >>> ps.DataFrame[("index", int), [("id", int), ("A", int)]]  # doctest: +ELLIPSIS
        typing.Tuple[...IndexNameType, ...NameType, ...NameType]
        >>> ps.DataFrame[(pdf.index.name, pdf.index.dtype), zip(pdf.columns, pdf.dtypes)]
        ... # doctest: +ELLIPSIS
        typing.Tuple[...IndexNameType, ...NameType]

    Typing data columns with an Multi-index:
        >>> arrays = [[1, 1, 2], ['red', 'blue', 'red']]
        >>> idx = pd.MultiIndex.from_arrays(arrays, names=('number', 'color'))
        >>> pdf = pd.DataFrame({'a': range(3)}, index=idx)
        >>> ps.DataFrame[[int, int], [int, int]]  # doctest: +ELLIPSIS
        typing.Tuple[...IndexNameType, ...IndexNameType, ...NameType, ...NameType]
        >>> ps.DataFrame[pdf.index.dtypes, pdf.dtypes]  # doctest: +ELLIPSIS, +SKIP
        typing.Tuple[...IndexNameType, ...NameType]
        >>> ps.DataFrame[[("index-1", int), ("index-2", int)], [("id", int), ("A", int)]]
        ... # doctest: +ELLIPSIS
        typing.Tuple[...IndexNameType, ...IndexNameType, ...NameType, ...NameType]
        >>> ps.DataFrame[zip(pdf.index.names, pdf.index.dtypes), zip(pdf.columns, pdf.dtypes)]
        ... # doctest: +ELLIPSIS, +SKIP
        typing.Tuple[...IndexNameType, ...NameType]
    """
    return Tuple[_to_type_holders(params)]


def _to_type_holders(params: Any) -> Tuple:
    from pyspark.pandas.typedef import NameTypeHolder, IndexNameTypeHolder

    is_with_index = (
        isinstance(params, tuple)
        and len(params) == 2
        and isinstance(params[1], (zip, list, pd.Series))
    )

    if is_with_index:
        # With index
        #   DataFrame[index_type, [type, ...]]
        #   DataFrame[dtype instance, dtypes instance]
        #   DataFrame[[index_type, ...], [type, ...]]
        #   DataFrame[dtypes instance, dtypes instance]
        #   DataFrame[(index_name, index_type), [(name, type), ...]]
        #   DataFrame[(index_name, index_type), zip(names, types)]
        #   DataFrame[[(index_name, index_type), ...], [(name, type), ...]]
        #   DataFrame[zip(index_names, index_types), zip(names, types)]
        def is_list_of_pairs(p: Any) -> bool:
            return (
                isinstance(p, list)
                and len(p) >= 1
                and all(isinstance(param, tuple) and (len(param) == 2) for param in p)
            )

        index_params = params[0]
        if isinstance(index_params, tuple) and len(index_params) == 2:
            # DataFrame[("index", int), ...]
            index_params = [index_params]

        if is_list_of_pairs(index_params):
            # DataFrame[[("index", int), ("index-2", int)], ...]
            index_params = tuple(slice(name, tpe) for name, tpe in index_params)

        index_types = _new_type_holders(index_params, IndexNameTypeHolder)

        data_types = params[1]
        if is_list_of_pairs(data_types):
            # DataFrame[..., [("id", int), ("A", int)]]
            data_types = tuple(slice(*data_type) for data_type in data_types)

        data_types = _new_type_holders(data_types, NameTypeHolder)

        return index_types + data_types
    else:
        # Without index
        #   DataFrame[type, type, ...]
        #   DataFrame[name: type, name: type, ...]
        #   DataFrame[dtypes instance]
        #   DataFrame[zip(names, types)]
        return _new_type_holders(params, NameTypeHolder)


def _new_type_holders(
    params: Any, holder_clazz: Type[Union[NameTypeHolder, IndexNameTypeHolder]]
) -> Tuple:
    if isinstance(params, zip):
        #   DataFrame[zip(names, types)]
        params = tuple(slice(name, tpe) for name, tpe in params)  # type: ignore[misc, has-type]

    if isinstance(params, Iterable):
        #   DataFrame[type, type, ...]
        #   DataFrame[name: type, name: type, ...]
        #   DataFrame[dtypes instance]
        params = tuple(params)
    else:
        #   DataFrame[type, type]
        #   DataFrame[name: type]
        params = (params,)

    is_named_params = all(
        isinstance(param, slice) and param.step is None and param.stop is not None
        for param in params
    )
    is_unnamed_params = all(
        not isinstance(param, slice) and not isinstance(param, Iterable) for param in params
    )

    if is_named_params:
        # DataFrame["id": int, "A": int]
        new_params = []
        for param in params:
            new_param: Type[Union[NameTypeHolder, IndexNameTypeHolder]] = type(
                holder_clazz.short_name, (holder_clazz,), {}
            )
            new_param.name = param.start
            if isinstance(param.stop, ExtensionDtype):
                new_param.tpe = param.stop  # type: ignore[assignment]
            else:
                # When the given argument is a numpy's dtype instance.
                new_param.tpe = param.stop.type if isinstance(param.stop, np.dtype) else param.stop
            new_params.append(new_param)
        return tuple(new_params)
    elif is_unnamed_params:
        # DataFrame[float, float]
        new_types = []
        for param in params:
            new_type: Type[Union[NameTypeHolder, IndexNameTypeHolder]] = type(
                holder_clazz.short_name, (holder_clazz,), {}
            )
            if isinstance(param, ExtensionDtype):
                new_type.tpe = param  # type: ignore[assignment]
            else:
                new_type.tpe = param.type if isinstance(param, np.dtype) else param
            new_types.append(new_type)
        return tuple(new_types)
    else:
        raise TypeError(
            """Type hints should be specified as one of:
  - DataFrame[type, type, ...]
  - DataFrame[name: type, name: type, ...]
  - DataFrame[dtypes instance]
  - DataFrame[zip(names, types)]
  - DataFrame[index_type, [type, ...]]
  - DataFrame[(index_name, index_type), [(name, type), ...]]
  - DataFrame[dtype instance, dtypes instance]
  - DataFrame[(index_name, index_type), zip(names, types)]
  - DataFrame[[index_type, ...], [type, ...]]
  - DataFrame[[(index_name, index_type), ...], [(name, type), ...]]
  - DataFrame[dtypes instance, dtypes instance]
  - DataFrame[zip(index_names, index_types), zip(names, types)]\n"""
            + "However, got %s." % str(params)
        )


def _test() -> None:
    import doctest
    import sys
    import pyspark.pandas.typedef.typehints

    globs = pyspark.pandas.typedef.typehints.__dict__.copy()
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.typedef.typehints,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
