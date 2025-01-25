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

import warnings
from typing import cast, Type, TYPE_CHECKING, Union, List, Dict, Any

import pyspark.sql.connect.proto as pb2
from pyspark.ml.connect.serialize import serialize_ml_params, deserialize, deserialize_param
from pyspark.ml.util import MLWriter, MLReader, RL
from pyspark.ml.wrapper import JavaWrapper

if TYPE_CHECKING:
    from pyspark.core.context import SparkContext
    from pyspark.sql.connect.session import SparkSession
    from pyspark.ml.util import JavaMLReadable, JavaMLWritable


class RemoteMLWriter(MLWriter):
    def __init__(self, instance: "JavaMLWritable") -> None:
        super().__init__()
        self._instance = instance

    @property
    def sc(self) -> "SparkContext":
        raise RuntimeError("Accessing SparkContext is not supported on Connect")

    def save(self, path: str) -> None:
        from pyspark.sql.connect.session import SparkSession

        session = SparkSession.getActiveSession()
        assert session is not None

        RemoteMLWriter.saveInstance(
            self._instance,
            path,
            session,
            self.shouldOverwrite,
            self.optionMap,
        )

    @staticmethod
    def saveInstance(
        instance: "JavaMLWritable",
        path: str,
        session: "SparkSession",
        shouldOverwrite: bool = False,
        optionMap: Dict[str, Any] = {},
    ) -> None:
        from pyspark.ml.wrapper import JavaModel, JavaEstimator, JavaTransformer
        from pyspark.ml.evaluation import JavaEvaluator
        from pyspark.ml.pipeline import Pipeline, PipelineModel

        # Spark Connect ML is built on scala Spark.ML, that means we're only
        # supporting JavaModel or JavaEstimator or JavaEvaluator
        if isinstance(instance, JavaModel):
            model = cast("JavaModel", instance)
            params = serialize_ml_params(model, session.client)
            assert isinstance(model._java_obj, str)
            writer = pb2.MlCommand.Write(
                obj_ref=pb2.ObjectRef(id=model._java_obj),
                params=params,
                path=path,
                should_overwrite=shouldOverwrite,
                options=optionMap,
            )
            command = pb2.Command()
            command.ml_command.write.CopyFrom(writer)
            session.client.execute_command(command)

        elif isinstance(instance, (JavaEstimator, JavaTransformer, JavaEvaluator)):
            operator: Union[JavaEstimator, JavaTransformer, JavaEvaluator]
            if isinstance(instance, JavaEstimator):
                ml_type = pb2.MlOperator.ESTIMATOR
                operator = cast("JavaEstimator", instance)
            elif isinstance(instance, JavaEvaluator):
                ml_type = pb2.MlOperator.EVALUATOR
                operator = cast("JavaEvaluator", instance)
            else:
                ml_type = pb2.MlOperator.TRANSFORMER
                operator = cast("JavaTransformer", instance)

            params = serialize_ml_params(operator, session.client)
            assert isinstance(operator._java_obj, str)
            writer = pb2.MlCommand.Write(
                operator=pb2.MlOperator(name=operator._java_obj, uid=operator.uid, type=ml_type),
                params=params,
                path=path,
                should_overwrite=shouldOverwrite,
                options=optionMap,
            )
            command = pb2.Command()
            command.ml_command.write.CopyFrom(writer)
            session.client.execute_command(command)

        elif isinstance(instance, (Pipeline, PipelineModel)):
            from pyspark.ml.pipeline import PipelineSharedReadWrite

            if shouldOverwrite:
                # TODO(SPARK-50954): Support client side model path overwrite
                warnings.warn("Overwrite doesn't take effect for Pipeline and PipelineModel")

            if isinstance(instance, Pipeline):
                stages = instance.getStages()  # type: ignore[attr-defined]
            else:
                stages = instance.stages

            PipelineSharedReadWrite.validateStages(stages)
            PipelineSharedReadWrite.saveImpl(
                instance,  # type: ignore[arg-type]
                stages,
                session,  # type: ignore[arg-type]
                path,
            )

        else:
            raise NotImplementedError(f"Unsupported write for {instance.__class__}")


class RemoteMLReader(MLReader[RL]):
    def __init__(self, clazz: Type["JavaMLReadable[RL]"]) -> None:
        super().__init__()
        self._clazz = clazz

    def load(self, path: str) -> RL:
        from pyspark.sql.connect.session import SparkSession

        session = SparkSession.getActiveSession()
        assert session is not None

        return RemoteMLReader.loadInstance(self._clazz, path, session)

    @staticmethod
    def loadInstance(
        clazz: Type["JavaMLReadable[RL]"],
        path: str,
        session: "SparkSession",
    ) -> RL:
        from pyspark.ml.base import Transformer
        from pyspark.ml.wrapper import JavaModel, JavaEstimator, JavaTransformer
        from pyspark.ml.evaluation import JavaEvaluator
        from pyspark.ml.pipeline import Pipeline, PipelineModel

        if (
            issubclass(clazz, JavaModel)
            or issubclass(clazz, JavaEstimator)
            or issubclass(clazz, JavaEvaluator)
            or issubclass(clazz, JavaTransformer)
        ):
            if issubclass(clazz, JavaModel):
                ml_type = pb2.MlOperator.MODEL
            elif issubclass(clazz, JavaEstimator):
                ml_type = pb2.MlOperator.ESTIMATOR
            elif issubclass(clazz, JavaEvaluator):
                ml_type = pb2.MlOperator.EVALUATOR
            else:
                ml_type = pb2.MlOperator.TRANSFORMER

            # to get the java corresponding qualified class name
            java_qualified_class_name = (
                clazz.__module__.replace("pyspark", "org.apache.spark") + "." + clazz.__name__
            )

            command = pb2.Command()
            command.ml_command.read.CopyFrom(
                pb2.MlCommand.Read(
                    operator=pb2.MlOperator(name=java_qualified_class_name, type=ml_type), path=path
                )
            )
            (_, properties, _) = session.client.execute_command(command)
            result = deserialize(properties)

            # Get the python type
            def _get_class() -> Type[RL]:
                parts = (clazz.__module__ + "." + clazz.__name__).split(".")
                module = ".".join(parts[:-1])
                m = __import__(module, fromlist=[parts[-1]])
                return getattr(m, parts[-1])

            py_type = _get_class()
            # It must be JavaWrapper, since we're passing the string to the _java_obj
            if issubclass(py_type, JavaWrapper):
                if ml_type == pb2.MlOperator.MODEL:
                    session.client.add_ml_cache(result.obj_ref.id)
                    instance = py_type(result.obj_ref.id)
                else:
                    instance = py_type()
                instance._resetUid(result.uid)
                params = {k: deserialize_param(v) for k, v in result.params.params.items()}
                instance._set(**params)
                return instance
            else:
                raise RuntimeError(f"Unsupported python type {py_type}")

        elif issubclass(clazz, Pipeline) or issubclass(clazz, PipelineModel):
            from pyspark.ml.pipeline import PipelineSharedReadWrite
            from pyspark.ml.util import DefaultParamsReader

            metadata = DefaultParamsReader.loadMetadata(path, session)
            uid, stages = PipelineSharedReadWrite.load(metadata, session, path)

            if issubclass(clazz, Pipeline):
                return Pipeline(stages=stages)._resetUid(uid)
            else:
                return PipelineModel(stages=cast(List[Transformer], stages))._resetUid(uid)

        else:
            raise RuntimeError(f"Unsupported read for {clazz}")
