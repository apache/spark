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

from typing import Any, cast, TypeVar, Callable, TYPE_CHECKING, Type, List, Tuple, Union, Iterable

import pandas as pd

from pyspark import cloudpickle
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf

import functools
import os

import pyspark.sql.connect.proto as pb2
from pyspark.ml.connect.serialize import serialize_ml_params, serialize, deserialize
from pyspark.sql import is_remote

if TYPE_CHECKING:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.ml.wrapper import JavaWrapper, JavaEstimator
    from pyspark.ml.util import JavaMLReadable, JavaMLWritable

FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def aggregate_dataframe(
    dataframe: Union["DataFrame", "pd.DataFrame"],
    input_col_names: List[str],
    local_agg_fn: Callable[["pd.DataFrame"], Any],
    merge_agg_state: Callable[[Any, Any], Any],
    agg_state_to_result: Callable[[Any], Any],
) -> Any:
    """
    The function can be used to run arbitrary aggregation logic on a spark dataframe
    or a pandas dataframe.

    Parameters
    ----------
    dataframe :
        A spark dataframe or a pandas dataframe

    input_col_names :
        The name of columns that are used in aggregation

    local_agg_fn :
        A user-defined function that converts a pandas dataframe to an object holding
        aggregation state. The aggregation state object must be pickle-able by
        `cloudpickle`.

    merge_agg_state :
        A user-defined function that merges 2 aggregation state objects into one and
        return the merged state. Either in-place modifying the first input state object
        and returning it or creating a new state object are acceptable.

    agg_state_to_result :
        A user-defined function that converts aggregation state object to final aggregation
        result.

    Returns
    -------
    Aggregation result.
    """

    if isinstance(dataframe, pd.DataFrame):
        dataframe = dataframe[list(input_col_names)]
        agg_state = local_agg_fn(dataframe)
        return agg_state_to_result(agg_state)

    dataframe = dataframe.select(*input_col_names)

    def compute_state(iterator: Iterable["pd.DataFrame"]) -> Iterable["pd.DataFrame"]:
        state = None

        for batch_pandas_df in iterator:
            new_batch_state = local_agg_fn(batch_pandas_df)
            if state is None:
                state = new_batch_state
            else:
                state = merge_agg_state(state, new_batch_state)

        if state is None:
            pickled_state = None
        else:
            pickled_state = cloudpickle.dumps(state)
        yield pd.DataFrame({"state": [pickled_state]})

    result_pdf = dataframe.mapInPandas(compute_state, schema="state binary").toPandas()

    merged_state = None
    for state in result_pdf.state:
        if state is None:
            continue
        state = cloudpickle.loads(state)
        if merged_state is None:
            merged_state = state
        else:
            merged_state = merge_agg_state(merged_state, state)

    return agg_state_to_result(merged_state)


def transform_dataframe_column(
    dataframe: Union["DataFrame", "pd.DataFrame"],
    input_cols: List[str],
    transform_fn: Callable[..., Any],
    output_cols: List[Tuple[str, str]],
) -> Union["DataFrame", "pd.DataFrame"]:
    """
    Transform specified column of the input spark dataframe or pandas dataframe,
    returns a new dataframe

    Parameters
    ----------
    dataframe :
        A spark dataframe or a pandas dataframe

    input_cols :
        A list of names of input columns to be transformed

    transform_fn:
        A transforming function with one or more arguments of `pandas.Series` type,
        if the transform function output is only one column data,
        return transformed result as a `pandas.Series` object,
        otherwise return transformed result as a `pandas.DataFrame` object
        with corresponding column names defined in `output_cols` argument.
        The output pandas Series/DataFrame object must have the same index
        with the input series.

    output_cols:
        a list of output transformed columns, each elements in the list
        is a tuple of (column_name, column_spark_type)

    Returns
    -------
    If it is a spark DataFrame, the result of transformation is a new spark DataFrame
    that contains all existing columns and output columns with names.
    If it is a pandas DataFrame, the input pandas dataframe is appended with output
    columns in place.
    """

    if len(output_cols) > 1:
        output_col_name = "__spark_ml_transformer_output_tmp__"
        spark_udf_return_type = ",".join(
            [f"{col_name} {col_type}" for col_name, col_type in output_cols]
        )
    else:
        output_col_name, spark_udf_return_type = output_cols[0]

    if isinstance(dataframe, pd.DataFrame):
        dataframe = dataframe.copy(deep=False)
        result_data = transform_fn(*[dataframe[col_name] for col_name in input_cols])
        if isinstance(result_data, pd.Series):
            assert len(output_cols) == 1
            result_data = pd.DataFrame({output_col_name: result_data})
        else:
            assert set(result_data.columns) == set(col_name for col_name, _ in output_cols)
            result_data = result_data

        for col_name in result_data.columns:
            dataframe.insert(len(dataframe.columns), col_name, result_data[col_name])
        return dataframe

    @pandas_udf(returnType=spark_udf_return_type)  # type: ignore[call-overload]
    def transform_fn_pandas_udf(*s: "pd.Series") -> "pd.Series":
        return transform_fn(*s)

    result_spark_df = dataframe.withColumn(output_col_name, transform_fn_pandas_udf(*input_cols))

    if len(output_cols) > 1:
        return result_spark_df.withColumns(
            {col_name: col(f"{output_col_name}.{col_name}") for col_name, _ in output_cols}
        ).drop(output_col_name)
    else:
        return result_spark_df


def _extract_id_methods(obj_identifier: str) -> Tuple[List[pb2.Fetch.Method], str]:
    """Extract the obj reference id and the methods. Eg, model.summary"""
    method_chain = obj_identifier.split(".")
    obj_ref = method_chain[0]
    methods: List[pb2.Fetch.Method] = []
    if len(method_chain) > 1:
        methods = [pb2.Fetch.Method(method=m) for m in method_chain[1:]]
    return methods, obj_ref


def try_remote_intermediate_result(f: FuncT) -> FuncT:
    """Mark the function/property that returns the intermediate result of the remote call.
    Eg, model.summary"""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            return f"{self._java_obj}.{f.__name__}"
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_attribute_relation(f: FuncT) -> FuncT:
    """Mark the function/property that returns a Relation.
    Eg, model.summary.roc"""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper", *args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            # The attribute returns a dataframe, we need to wrap it
            # in the AttributeRelation
            from pyspark.ml.connect.proto import AttributeRelation
            from pyspark.sql.connect.session import SparkSession
            from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

            session = SparkSession.getActiveSession()
            assert session is not None

            assert isinstance(self._java_obj, str)

            methods, obj_ref = _extract_id_methods(self._java_obj)
            methods.append(
                pb2.Fetch.Method(method=f.__name__, args=serialize(session.client, *args))
            )
            plan = AttributeRelation(obj_ref, methods)
            return ConnectDataFrame(plan, session)
        else:
            return f(self, *args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_fit(f: FuncT) -> FuncT:
    """Mark the function that fits a model."""

    @functools.wraps(f)
    def wrapped(self: "JavaEstimator", dataset: "ConnectDataFrame") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            client = dataset.sparkSession.client
            input = dataset._plan.plan(client)
            assert isinstance(self._java_obj, str)
            estimator = pb2.MlOperator(
                name=self._java_obj, uid=self.uid, type=pb2.MlOperator.ESTIMATOR
            )
            command = pb2.Command()
            command.ml_command.fit.CopyFrom(
                pb2.MlCommand.Fit(
                    estimator=estimator,
                    params=serialize_ml_params(self, client),
                    dataset=input,
                )
            )
            (_, properties, _) = client.execute_command(command)
            model_info = deserialize(properties)
            client.add_ml_cache(model_info.obj_ref.id)
            return model_info.obj_ref.id
        else:
            return f(self, dataset)

    return cast(FuncT, wrapped)


def try_remote_transform_relation(f: FuncT) -> FuncT:
    """Mark the function/property that returns a relation for model transform."""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper", dataset: "ConnectDataFrame") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml import Model, Transformer
            from pyspark.sql.connect.session import SparkSession
            from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

            session = SparkSession.getActiveSession()
            assert session is not None
            # Model is also a Transformer, so we much match Model first
            if isinstance(self, Model):
                params = serialize_ml_params(self, session.client)
                from pyspark.ml.connect.proto import TransformerRelation

                assert isinstance(self._java_obj, str)
                return ConnectDataFrame(
                    TransformerRelation(
                        child=dataset._plan, name=self._java_obj, ml_params=params, is_model=True
                    ),
                    session,
                )
            elif isinstance(self, Transformer):
                params = serialize_ml_params(self, session.client)
                from pyspark.ml.connect.proto import TransformerRelation

                assert isinstance(self._java_obj, str)
                return ConnectDataFrame(
                    TransformerRelation(
                        child=dataset._plan,
                        name=self._java_obj,
                        ml_params=params,
                        uid=self.uid,
                        is_model=False,
                    ),
                    session,
                )
            else:
                raise RuntimeError(f"Unsupported {self}")
        else:
            return f(self, dataset)

    return cast(FuncT, wrapped)


def try_remote_call(f: FuncT) -> FuncT:
    """Mark the function/property for the remote call.
    Eg, model.coefficients"""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper", name: str, *args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            """Launch a remote call if possible"""
            from pyspark.sql.connect.session import SparkSession

            session = SparkSession.getActiveSession()
            assert session is not None
            assert isinstance(self._java_obj, str)
            methods, obj_ref = _extract_id_methods(self._java_obj)
            methods.append(pb2.Fetch.Method(method=name, args=serialize(session.client, *args)))
            command = pb2.Command()
            command.ml_command.fetch.CopyFrom(
                pb2.Fetch(obj_ref=pb2.ObjectRef(id=obj_ref), methods=methods)
            )
            (_, properties, _) = session.client.execute_command(command)
            ml_command_result = properties["ml_command_result"]
            if ml_command_result.HasField("summary"):
                summary = ml_command_result.summary
                session.client.add_ml_cache(summary)
                return summary
            else:
                return deserialize(properties)
        else:
            return f(self, name, *args)

    return cast(FuncT, wrapped)


def try_remote_del(f: FuncT) -> FuncT:
    """Mark the function/property to delete a model on the server side."""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper") -> Any:
        try:
            in_remote = is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ
        except Exception:
            return

        if in_remote:
            # Delete the model if possible
            model_id = self._java_obj
            if model_id is not None and "." not in model_id:
                try:
                    from pyspark.sql.connect.session import SparkSession

                    session = SparkSession.getActiveSession()
                    if session is not None:
                        session.client.remove_ml_cache(model_id)
                        return
                except Exception:
                    # SparkSession's down.
                    return
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_return_java_class(f: FuncT) -> FuncT:
    """Mark the function/property that returns none."""

    @functools.wraps(f)
    def wrapped(java_class: str, *args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            return java_class
        else:
            return f(java_class, *args)

    return cast(FuncT, wrapped)


def try_remote_write(f: FuncT) -> FuncT:
    """Mark the function that write an estimator/model or evaluator"""

    @functools.wraps(f)
    def wrapped(self: "JavaMLWritable") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml.connect.readwrite import RemoteMLWriter

            return RemoteMLWriter(self)
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_read(f: FuncT) -> FuncT:
    """Mark the function to read an estimator/model or evaluator"""

    @functools.wraps(f)
    def wrapped(cls: Type["JavaMLReadable"]) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml.connect.readwrite import RemoteMLReader

            return RemoteMLReader(cls)
        else:
            return f(cls)

    return cast(FuncT, wrapped)


def try_remote_intercept(f: FuncT) -> FuncT:
    """Mark the function/property that returns none."""

    @functools.wraps(f)
    def wrapped(java_class: str, *args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            return None
        else:
            return f(java_class, *args)

    return cast(FuncT, wrapped)


def try_remote_not_supporting(f: FuncT) -> FuncT:
    """Mark the function/property that has not been supported yet"""

    @functools.wraps(f)
    def wrapped(*args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            raise NotImplementedError("")
        else:
            return f(*args)

    return cast(FuncT, wrapped)
