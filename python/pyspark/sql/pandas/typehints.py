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
from inspect import Signature
from typing import Any, Callable, Dict, Optional, Union, TYPE_CHECKING, get_type_hints
from inspect import getfullargspec, signature

from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from pyspark.errors import PySparkNotImplementedError, PySparkValueError

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import (
        PandasScalarUDFType,
        PandasScalarIterUDFType,
        PandasGroupedAggUDFType,
        ArrowScalarUDFType,
        ArrowScalarIterUDFType,
        ArrowGroupedAggUDFType,
        ArrowGroupedMapIterUDFType,
        ArrowGroupedMapUDFType,
        ArrowGroupedMapFunction,
        PandasGroupedMapFunction,
        PandasGroupedMapUDFType,
        PandasGroupedMapIterUDFType,
    )


def infer_pandas_eval_type(
    sig: Signature,
    type_hints: Dict[str, Any],
) -> Optional[Union["PandasScalarUDFType", "PandasScalarIterUDFType", "PandasGroupedAggUDFType"]]:
    """
    Infers the evaluation type in :class:`pyspark.util.PythonEvalType` from
    :class:`inspect.Signature` instance and type hints.
    """
    from pyspark.sql.pandas.functions import PandasUDFType

    require_minimum_pandas_version()

    import pandas as pd

    annotations = {}
    for param in sig.parameters.values():
        if param.annotation is not param.empty:
            annotations[param.name] = type_hints.get(param.name, param.annotation)

    # Check if all arguments have type hints
    parameters_sig = [
        annotations[parameter] for parameter in sig.parameters if parameter in annotations
    ]
    if len(parameters_sig) != len(sig.parameters):
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={"target": "all parameters", "sig": str(sig)},
        )

    # Check if the return has a type hint
    return_annotation = type_hints.get("return", sig.return_annotation)
    if sig.empty is return_annotation:
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={"target": "the return type", "sig": str(sig)},
        )

    # Series, Frame or Union[DataFrame, Series], ... -> Series or Frame
    is_series_or_frame = all(
        a == pd.Series
        or a == pd.DataFrame  # Series
        or check_union_annotation(  # DataFrame  # Union[DataFrame, Series]
            a, parameter_check_func=lambda na: na == pd.Series or na == pd.DataFrame
        )
        for a in parameters_sig
    ) and (return_annotation == pd.Series or return_annotation == pd.DataFrame)
    if is_series_or_frame:
        return PandasUDFType.SCALAR

    # Iterator[Tuple[Series, Frame or Union[DataFrame, Series], ...] -> Iterator[Series or Frame]
    is_iterator_tuple_series_or_frame = (
        len(parameters_sig) == 1
        and check_iterator_annotation(  # Iterator
            parameters_sig[0],
            parameter_check_func=lambda a: check_tuple_annotation(  # Tuple
                a,
                parameter_check_func=lambda ta: (
                    ta == Ellipsis
                    or ta == pd.Series  # ...
                    or ta == pd.DataFrame  # Series
                    or check_union_annotation(  # DataFrame  # Union[DataFrame, Series]
                        ta, parameter_check_func=lambda na: (na == pd.Series or na == pd.DataFrame)
                    )
                ),
            ),
        )
        and check_iterator_annotation(
            return_annotation, parameter_check_func=lambda a: a == pd.DataFrame or a == pd.Series
        )
    )
    if is_iterator_tuple_series_or_frame:
        return PandasUDFType.SCALAR_ITER

    # Iterator[Series, Frame or Union[DataFrame, Series]] -> Iterator[Series or Frame]
    is_iterator_series_or_frame = (
        len(parameters_sig) == 1
        and check_iterator_annotation(
            parameters_sig[0],
            parameter_check_func=lambda a: (
                a == pd.Series
                or a == pd.DataFrame  # Series
                or check_union_annotation(  # DataFrame  # Union[DataFrame, Series]
                    a, parameter_check_func=lambda ua: ua == pd.Series or ua == pd.DataFrame
                )
            ),
        )
        and check_iterator_annotation(
            return_annotation, parameter_check_func=lambda a: a == pd.DataFrame or a == pd.Series
        )
    )
    if is_iterator_series_or_frame:
        return PandasUDFType.SCALAR_ITER

    # Series, Frame or Union[DataFrame, Series], ... -> Any
    is_series_or_frame_agg = all(
        a == pd.Series
        or a == pd.DataFrame  # Series
        or check_union_annotation(  # DataFrame  # Union[DataFrame, Series]
            a, parameter_check_func=lambda ua: ua == pd.Series or ua == pd.DataFrame
        )
        for a in parameters_sig
    ) and (
        # It's tricky to include only types which pd.Series constructor can take.
        # Simply exclude common types used here for now (which becomes object
        # types Spark can't recognize).
        return_annotation != pd.Series
        and return_annotation != pd.DataFrame
        and not check_iterator_annotation(return_annotation)
        and not check_tuple_annotation(return_annotation)
    )
    if is_series_or_frame_agg:
        return PandasUDFType.GROUPED_AGG

    return None


def infer_arrow_eval_type(
    sig: Signature, type_hints: Dict[str, Any]
) -> Optional[Union["ArrowScalarUDFType", "ArrowScalarIterUDFType", "ArrowGroupedAggUDFType"]]:
    """
    Infers the evaluation type in :class:`pyspark.util.PythonEvalType` from
    :class:`inspect.Signature` instance and type hints.
    """
    from pyspark.sql.pandas.functions import ArrowUDFType

    require_minimum_pyarrow_version()

    import pyarrow as pa

    annotations = {}
    for param in sig.parameters.values():
        if param.annotation is not param.empty:
            annotations[param.name] = type_hints.get(param.name, param.annotation)

    # Check if all arguments have type hints
    parameters_sig = [
        annotations[parameter] for parameter in sig.parameters if parameter in annotations
    ]
    if len(parameters_sig) != len(sig.parameters):
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={"target": "all parameters", "sig": str(sig)},
        )

    # Check if the return has a type hint
    return_annotation = type_hints.get("return", sig.return_annotation)
    if sig.empty is return_annotation:
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={"target": "the return type", "sig": str(sig)},
        )

    # pa.Array, ... -> pa.Array
    is_arrow_array = all(a == pa.Array for a in parameters_sig) and (return_annotation == pa.Array)
    if is_arrow_array:
        return ArrowUDFType.SCALAR

    # Iterator[Tuple[pa.Array, ...] -> Iterator[pa.Array]
    is_iterator_tuple_array = (
        len(parameters_sig) == 1
        and check_iterator_annotation(  # Iterator
            parameters_sig[0],
            parameter_check_func=lambda a: check_tuple_annotation(  # Tuple
                a,
                parameter_check_func=lambda ta: (ta == Ellipsis or ta == pa.Array),
            ),
        )
        and check_iterator_annotation(
            return_annotation, parameter_check_func=lambda a: a == pa.Array
        )
    )
    if is_iterator_tuple_array:
        return ArrowUDFType.SCALAR_ITER

    # Iterator[pa.Array] -> Iterator[pa.Array]
    is_iterator_array = (
        len(parameters_sig) == 1
        and check_iterator_annotation(
            parameters_sig[0],
            parameter_check_func=lambda a: a == pa.Array,
        )
        and check_iterator_annotation(
            return_annotation, parameter_check_func=lambda a: a == pa.Array
        )
    )
    if is_iterator_array:
        return ArrowUDFType.SCALAR_ITER

    # pa.Array, ... -> Any
    is_array_agg = all(a == pa.Array for a in parameters_sig) and (
        return_annotation != pa.Array
        and not check_iterator_annotation(return_annotation)
        and not check_tuple_annotation(return_annotation)
    )
    if is_array_agg:
        return ArrowUDFType.GROUPED_AGG

    return None


def infer_eval_type(
    sig: Signature,
    type_hints: Dict[str, Any],
    kind: str = "all",
) -> Union[
    "PandasScalarUDFType",
    "PandasScalarIterUDFType",
    "PandasGroupedAggUDFType",
    "ArrowScalarUDFType",
    "ArrowScalarIterUDFType",
    "ArrowGroupedAggUDFType",
]:
    """
    Infers the evaluation type in :class:`pyspark.util.PythonEvalType` from
    :class:`inspect.Signature` instance and type hints.
    """
    assert kind in ["pandas", "arrow", "all"], "kind should be either 'pandas', 'arrow' or 'all'"

    eval_type: Optional[
        Union[
            "PandasScalarUDFType",
            "PandasScalarIterUDFType",
            "PandasGroupedAggUDFType",
            "ArrowScalarUDFType",
            "ArrowScalarIterUDFType",
            "ArrowGroupedAggUDFType",
        ]
    ] = None
    if kind == "pandas":
        eval_type = infer_pandas_eval_type(sig, type_hints)
    elif kind == "arrow":
        eval_type = infer_arrow_eval_type(sig, type_hints)
    else:
        eval_type = infer_pandas_eval_type(sig, type_hints) or infer_arrow_eval_type(
            sig, type_hints
        )

    if eval_type is None:
        raise PySparkNotImplementedError(
            errorClass="UNSUPPORTED_SIGNATURE",
            messageParameters={"signature": str(sig)},
        )

    return eval_type


# infer the eval type for @udf
def infer_eval_type_for_udf(  # type: ignore[no-untyped-def]
    f,
) -> Optional[
    Union[
        "PandasScalarUDFType",
        "PandasScalarIterUDFType",
        "PandasGroupedAggUDFType",
        "ArrowScalarUDFType",
        "ArrowScalarIterUDFType",
        "ArrowGroupedAggUDFType",
    ]
]:
    argspec = getfullargspec(f)
    # different from inference of @pandas_udf/@arrow_udf, 0-arg is not allowed here
    if len(argspec.args) > 0 and len(argspec.annotations) > 0:
        try:
            type_hints = get_type_hints(f)
        except NameError:
            type_hints = {}
        return infer_eval_type(signature(f), type_hints)
    else:
        return None


def infer_group_arrow_eval_type(
    sig: Signature,
    type_hints: Dict[str, Any],
) -> Optional[Union["ArrowGroupedMapUDFType", "ArrowGroupedMapIterUDFType"]]:
    from pyspark.sql.pandas.functions import PythonEvalType

    require_minimum_pyarrow_version()

    import pyarrow as pa

    annotations = {}
    for param in sig.parameters.values():
        if param.annotation is not param.empty:
            annotations[param.name] = type_hints.get(param.name, param.annotation)

    # Check if all arguments have type hints
    parameters_sig = [
        annotations[parameter] for parameter in sig.parameters if parameter in annotations
    ]
    if len(parameters_sig) != len(sig.parameters):
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={"target": "all parameters", "sig": str(sig)},
        )

    # Check if the return has a type hint
    return_annotation = type_hints.get("return", sig.return_annotation)
    if sig.empty is return_annotation:
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={"target": "the return type", "sig": str(sig)},
        )

    # Iterator[pa.RecordBatch] -> Iterator[pa.RecordBatch]
    is_iterator_batch = (
        len(parameters_sig) == 1
        and check_iterator_annotation(  # Iterator
            parameters_sig[0],
            parameter_check_func=lambda t: t == pa.RecordBatch,
        )
        and check_iterator_annotation(
            return_annotation, parameter_check_func=lambda t: t == pa.RecordBatch
        )
    )
    if is_iterator_batch:
        return PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF

    # Tuple[pa.Scalar, ...], Iterator[pa.RecordBatch] -> Iterator[pa.RecordBatch]
    is_iterator_batch_with_keys = (
        len(parameters_sig) == 2
        and check_iterator_annotation(  # Iterator
            parameters_sig[1],
            parameter_check_func=lambda t: t == pa.RecordBatch,
        )
        and check_iterator_annotation(
            return_annotation, parameter_check_func=lambda t: t == pa.RecordBatch
        )
    )
    if is_iterator_batch_with_keys:
        return PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF

    # pa.Table -> pa.Table
    is_table = (
        len(parameters_sig) == 1 and parameters_sig[0] == pa.Table and return_annotation == pa.Table
    )
    if is_table:
        return PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF

    # Tuple[pa.Scalar, ...], pa.Table -> pa.Table
    is_table_with_keys = (
        len(parameters_sig) == 2 and parameters_sig[1] == pa.Table and return_annotation == pa.Table
    )
    if is_table_with_keys:
        return PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF

    return None


def infer_group_arrow_eval_type_from_func(
    f: "ArrowGroupedMapFunction",
) -> Optional[Union["ArrowGroupedMapUDFType", "ArrowGroupedMapIterUDFType"]]:
    argspec = getfullargspec(f)
    if len(argspec.annotations) > 0:
        try:
            type_hints = get_type_hints(f)
        except NameError:
            type_hints = {}

        return infer_group_arrow_eval_type(signature(f), type_hints)
    else:
        return None


def infer_group_pandas_eval_type(
    sig: Signature,
    type_hints: Dict[str, Any],
) -> Optional[Union["PandasGroupedMapUDFType", "PandasGroupedMapIterUDFType"]]:
    from pyspark.sql.pandas.functions import PythonEvalType

    require_minimum_pandas_version()

    import pandas as pd

    annotations = {}
    for param in sig.parameters.values():
        if param.annotation is not param.empty:
            annotations[param.name] = type_hints.get(param.name, param.annotation)

    # Check if all arguments have type hints
    parameters_sig = [
        annotations[parameter] for parameter in sig.parameters if parameter in annotations
    ]
    if len(parameters_sig) != len(sig.parameters):
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={"target": "all parameters", "sig": str(sig)},
        )

    # Check if the return has a type hint
    return_annotation = type_hints.get("return", sig.return_annotation)
    if sig.empty is return_annotation:
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={"target": "the return type", "sig": str(sig)},
        )

    # Iterator[pd.DataFrame] -> Iterator[pd.DataFrame]
    is_iterator_dataframe = (
        len(parameters_sig) == 1
        and check_iterator_annotation(  # Iterator
            parameters_sig[0],
            parameter_check_func=lambda t: t == pd.DataFrame,
        )
        and check_iterator_annotation(
            return_annotation, parameter_check_func=lambda t: t == pd.DataFrame
        )
    )
    if is_iterator_dataframe:
        return PythonEvalType.SQL_GROUPED_MAP_PANDAS_ITER_UDF

    # Tuple[Any, ...], Iterator[pd.DataFrame] -> Iterator[pd.DataFrame]
    is_iterator_dataframe_with_keys = (
        len(parameters_sig) == 2
        and check_iterator_annotation(  # Iterator
            parameters_sig[1],
            parameter_check_func=lambda t: t == pd.DataFrame,
        )
        and check_iterator_annotation(
            return_annotation, parameter_check_func=lambda t: t == pd.DataFrame
        )
    )
    if is_iterator_dataframe_with_keys:
        return PythonEvalType.SQL_GROUPED_MAP_PANDAS_ITER_UDF

    # pd.DataFrame -> pd.DataFrame
    is_dataframe = (
        len(parameters_sig) == 1
        and parameters_sig[0] == pd.DataFrame
        and return_annotation == pd.DataFrame
    )
    if is_dataframe:
        return PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF

    # Tuple[Any, ...], pd.DataFrame -> pd.DataFrame
    is_dataframe_with_keys = (
        len(parameters_sig) == 2
        and parameters_sig[1] == pd.DataFrame
        and return_annotation == pd.DataFrame
    )
    if is_dataframe_with_keys:
        return PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF

    return None


def infer_group_pandas_eval_type_from_func(
    f: "PandasGroupedMapFunction",
) -> Optional[Union["PandasGroupedMapUDFType", "PandasGroupedMapIterUDFType"]]:
    argspec = getfullargspec(f)
    if len(argspec.annotations) > 0:
        try:
            type_hints = get_type_hints(f)
        except NameError:
            type_hints = {}

        return infer_group_pandas_eval_type(signature(f), type_hints)
    else:
        return None


def check_tuple_annotation(
    annotation: Any, parameter_check_func: Optional[Callable[[Any], bool]] = None
) -> bool:
    # Tuple has _name but other types have __name__
    # Check if the name is Tuple first. After that, check the generic types.
    name = getattr(annotation, "_name", getattr(annotation, "__name__", None))
    return name in ("Tuple", "tuple") and (
        parameter_check_func is None or all(map(parameter_check_func, annotation.__args__))
    )


def check_iterator_annotation(
    annotation: Any, parameter_check_func: Optional[Callable[[Any], bool]] = None
) -> bool:
    name = getattr(annotation, "_name", getattr(annotation, "__name__", None))
    return name == "Iterator" and (
        parameter_check_func is None or all(map(parameter_check_func, annotation.__args__))
    )


def check_union_annotation(
    annotation: Any, parameter_check_func: Optional[Callable[[Any], bool]] = None
) -> bool:
    # Note that we cannot rely on '__origin__' in other type hints as it has changed from version
    # to version.
    origin = getattr(annotation, "__origin__", None)
    return origin == Union and (
        parameter_check_func is None or all(map(parameter_check_func, annotation.__args__))
    )
