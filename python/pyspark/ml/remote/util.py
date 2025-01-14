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
from typing import Any, cast, TypeVar, Callable, TYPE_CHECKING, Type, List, Tuple

import pyspark.sql.connect.proto as pb2
from pyspark.ml.remote.serialize import serialize_ml_params, serialize, deserialize
from pyspark.sql import is_remote

if TYPE_CHECKING:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.ml.wrapper import JavaWrapper, JavaEstimator
    from pyspark.ml.util import JavaMLReadable, JavaMLWritable

FuncT = TypeVar("FuncT", bound=Callable[..., Any])


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
            from pyspark.ml.remote.proto import AttributeRelation
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
                from pyspark.ml.remote.proto import TransformerRelation

                assert isinstance(self._java_obj, str)
                return ConnectDataFrame(
                    TransformerRelation(
                        child=dataset._plan, name=self._java_obj, ml_params=params, is_model=True
                    ),
                    session,
                )
            elif isinstance(self, Transformer):
                params = serialize_ml_params(self, session.client)
                from pyspark.ml.remote.proto import TransformerRelation

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
