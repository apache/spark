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
from typing import Any, Callable, Dict, Optional, Union, TYPE_CHECKING

from pyspark.sql.pandas.utils import require_minimum_pandas_version
from pyspark.errors import PySparkNotImplementedError

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import (
        PandasScalarUDFType,
        PandasScalarIterUDFType,
        PandasGroupedAggUDFType,
    )


def infer_eval_type(
    sig: Signature, type_hints: Dict[str, Any]
) -> Union["PandasScalarUDFType", "PandasScalarIterUDFType", "PandasGroupedAggUDFType"]:
    """
    Infers the evaluation type in :class:`pyspark.rdd.PythonEvalType` from
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
        raise ValueError("Type hints for all parameters should be specified; however, got %s" % sig)

    # Check if the return has a type hint
    return_annotation = type_hints.get("return", sig.return_annotation)
    if sig.empty is return_annotation:
        raise ValueError("Type hint for the return type should be specified; however, got %s" % sig)

    # Series, Frame or Union[DataFrame, Series], ... -> Series or Frame
    is_series_or_frame = all(
        a == pd.Series
        or a == pd.DataFrame  # Series
        or check_union_annotation(  # DataFrame  # Union[DataFrame, Series]
            a, parameter_check_func=lambda na: na == pd.Series or na == pd.DataFrame
        )
        for a in parameters_sig
    ) and (return_annotation == pd.Series or return_annotation == pd.DataFrame)

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

    if is_series_or_frame:
        return PandasUDFType.SCALAR
    elif is_iterator_tuple_series_or_frame or is_iterator_series_or_frame:
        return PandasUDFType.SCALAR_ITER
    elif is_series_or_frame_agg:
        return PandasUDFType.GROUPED_AGG
    else:
        raise PySparkNotImplementedError(
            error_class="UNSUPPORTED_SIGNATURE",
            message_parameters={"signature": str(sig)},
        )


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
    import typing

    # Note that we cannot rely on '__origin__' in other type hints as it has changed from version
    # to version.
    origin = getattr(annotation, "__origin__", None)
    return origin == typing.Union and (
        parameter_check_func is None or all(map(parameter_check_func, annotation.__args__))
    )
