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
import functools
import os
from typing import Any, cast, TypeVar, Callable, TYPE_CHECKING, Type

import pyspark.sql.connect.proto as pb2
from pyspark.ml.remote.serialize import serialize_ml_params, serialize, deserialize
from pyspark.sql import is_remote
from pyspark.sql.connect.dataframe import DataFrame as RemoteDataFrame

if TYPE_CHECKING:
    from pyspark.ml.wrapper import JavaWrapper, JavaEstimator
    from pyspark.ml.util import JavaMLReadable, JavaMLWritable

FuncT = TypeVar("FuncT", bound=Callable[..., Any])


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
            # in the _ModelAttributeRelationPlan
            from pyspark.ml.remote.proto import _ModelAttributeRelationPlan
            from pyspark.sql.connect.session import SparkSession

            assert isinstance(self._java_obj, str)
            plan = _ModelAttributeRelationPlan(self._java_obj, f.__name__)
            session = SparkSession.getActiveSession()
            assert session is not None
            return RemoteDataFrame(plan, session)
        else:
            return f(self, *args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_fit(f: FuncT) -> FuncT:
    """Mark the function that fits a model."""

    @functools.wraps(f)
    def wrapped(self: "JavaEstimator", dataset: RemoteDataFrame) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            client = dataset.sparkSession.client
            input = dataset._plan.plan(client)
            assert isinstance(self._java_obj, str)
            estimator = pb2.MlOperator(
                name=self._java_obj, uid=self.uid, type=pb2.MlOperator.ESTIMATOR
            )
            fit_cmd = pb2.MlCommand.Fit(
                estimator=estimator,
                params=serialize_ml_params(self, client),
                dataset=input,
            )
            req = client._execute_plan_request_with_metadata()
            req.plan.ml_command.fit.CopyFrom(fit_cmd)
            model_info = deserialize(client.execute_ml(req))
            client.add_ml_model(model_info.model_ref.id)
            return model_info.model_ref.id
        else:
            return f(self, dataset)

    return cast(FuncT, wrapped)


def try_remote_transform_relation(f: FuncT) -> FuncT:
    """Mark the function/property that returns a relation for model transform."""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper", dataset: RemoteDataFrame) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml import Model, Transformer
            from pyspark.sql.connect.session import SparkSession

            session = SparkSession.getActiveSession()
            assert session is not None
            # Model is also a Transformer, so we much match Model first
            if isinstance(self, Model):
                params = serialize_ml_params(self, session.client)
                from pyspark.ml.remote.proto import _ModelTransformRelationPlan

                assert isinstance(self._java_obj, str)
                return RemoteDataFrame(
                    _ModelTransformRelationPlan(dataset._plan, self._java_obj, params), session
                )
            elif isinstance(self, Transformer):
                params = serialize_ml_params(self, session.client)
                from pyspark.ml.remote.proto import _TransformerRelationPlan

                assert isinstance(self._java_obj, str)
                return RemoteDataFrame(
                    _TransformerRelationPlan(dataset._plan, self._java_obj, self.uid, params),
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
            get_attribute = pb2.FetchModelAttr(
                model_ref=pb2.ModelRef(id=self._java_obj),
                method=name,
                args=serialize(session.client, *args),
            )
            req = session.client._execute_plan_request_with_metadata()
            req.plan.ml_command.fetch_model_attr.CopyFrom(get_attribute)

            return deserialize(session.client.execute_ml(req))
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
                        session.client.remove_ml_model(model_id)
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
            from pyspark.ml.remote.readwrite import RemoteMLWriter

            return RemoteMLWriter(self)
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_read(f: FuncT) -> FuncT:
    """Mark the function to read an estimator/model or evaluator"""

    @functools.wraps(f)
    def wrapped(cls: Type["JavaMLReadable"]) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml.remote.readwrite import RemoteMLReader

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
