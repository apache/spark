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
from pyspark.sql.pandas.utils import require_minimum_pandas_version


def infer_eval_type(sig):
    """
    Infers the evaluation type in :class:`pyspark.rdd.PythonEvalType` from
    :class:`inspect.Signature` instance.
    """
    from pyspark.sql.pandas.functions import PandasUDFType

    require_minimum_pandas_version()

    import pandas as pd

    annotations = {}
    for param in sig.parameters.values():
        if param.annotation is not param.empty:
            annotations[param.name] = param.annotation

    # Check if all arguments have type hints
    parameters_sig = [annotations[parameter] for parameter
                      in sig.parameters if parameter in annotations]
    if len(parameters_sig) != len(sig.parameters):
        raise ValueError(
            "Type hints for all parameters should be specified; however, got %s" % sig)

    # Check if the return has a type hint
    return_annotation = sig.return_annotation
    if sig.empty is return_annotation:
        raise ValueError(
            "Type hint for the return type should be specified; however, got %s" % sig)

    # Series or Frame, ... -> Series or Frame
    is_series_or_frame = (
        all(a == pd.Series or a == pd.DataFrame for a in parameters_sig) and
        (return_annotation == pd.Series or return_annotation == pd.DataFrame))

    # Iterator[Tuple[Series or Frame, ...] -> Iterator[Series or Frame]
    is_iterator_tuple_series_or_frame = (
        len(parameters_sig) == 1 and
        check_iterator_annotation(
            parameters_sig[0],
            parameter_check_func=lambda a: check_tuple_annotation(
                a,
                parameter_check_func=lambda aa: (
                    aa == pd.DataFrame or aa == pd.Series or aa == Ellipsis))) and
        check_iterator_annotation(
            return_annotation,
            parameter_check_func=lambda a: a == pd.DataFrame or a == pd.Series))

    # Iterator[Series or Frame] -> Iterator[Series or Frame]
    is_iterator_series_or_frame = (
        len(parameters_sig) == 1 and
        check_iterator_annotation(
            parameters_sig[0],
            parameter_check_func=lambda a: a == pd.DataFrame or a == pd.Series) and
        check_iterator_annotation(
            return_annotation,
            parameter_check_func=lambda a: a == pd.DataFrame or a == pd.Series))

    # Series or Frame, ... -> Any
    is_series_or_frame_agg = (
        all(a == pd.Series or a == pd.DataFrame for a in parameters_sig) and (
            # It's tricky to whitelist which types pd.Series constructor can take.
            # Simply blacklist common types used here for now (which becomes object
            # types Spark can't recognize).
            return_annotation != pd.Series and
            return_annotation != pd.DataFrame and
            not check_iterator_annotation(return_annotation) and
            not check_tuple_annotation(return_annotation)
        ))

    if is_series_or_frame:
        return PandasUDFType.SCALAR
    elif is_iterator_tuple_series_or_frame:
        return PandasUDFType.SCALAR_ITER
    elif is_iterator_series_or_frame:
        return PandasUDFType.SCALAR_ITER
    elif is_series_or_frame_agg:
        return PandasUDFType.GROUPED_AGG
    else:
        raise NotImplementedError("Unsupported signature: %s." % sig)


def check_tuple_annotation(annotation, parameter_check_func=None):
    # Python 3.6 has `__name__`. Python 3.7 and 3.8 have `_name`.
    # Check if the name is Tuple first. After that, check the generic types.
    name = getattr(annotation, "_name", getattr(annotation, "__name__", None))
    assert name is not None, "_name or __name__ not in the type annotation, %s" % annotation
    return name == "Tuple" and (
        parameter_check_func is None or all(map(parameter_check_func, annotation.__args__)))


def check_iterator_annotation(annotation, parameter_check_func=None):
    name = getattr(annotation, "_name", getattr(annotation, "__name__", None))
    assert name is not None, "_name or __name__ not in the type annotation, %s" % annotation
    return name == "Iterator" and (
        parameter_check_func is None or all(map(parameter_check_func, annotation.__args__)))
