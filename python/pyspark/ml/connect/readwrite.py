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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import cast, Type, TYPE_CHECKING, Union, Dict, Any

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
        from pyspark.ml.classification import OneVsRest, OneVsRestModel
        from pyspark.ml.clustering import PowerIterationClustering
        from pyspark.ml.tuning import (
            CrossValidator,
            CrossValidatorModel,
            TrainValidationSplit,
            TrainValidationSplitModel,
        )

        # Spark Connect ML is built on scala Spark.ML, that means we're only
        # supporting JavaModel or JavaEstimator or JavaEvaluator
        if isinstance(instance, JavaModel):
            from pyspark.ml.util import RemoteModelRef

            model = cast("JavaModel", instance)
            params = serialize_ml_params(model, session.client)
            assert isinstance(model._java_obj, RemoteModelRef)
            writer = pb2.MlCommand.Write(
                obj_ref=pb2.ObjectRef(id=model._java_obj.ref_id),
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
                ml_type = pb2.MlOperator.OPERATOR_TYPE_ESTIMATOR
                operator = cast("JavaEstimator", instance)
            elif isinstance(instance, JavaEvaluator):
                ml_type = pb2.MlOperator.OPERATOR_TYPE_EVALUATOR
                operator = cast("JavaEvaluator", instance)
            else:
                ml_type = pb2.MlOperator.OPERATOR_TYPE_TRANSFORMER
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

        elif isinstance(instance, Pipeline):
            from pyspark.ml.pipeline import PipelineWriter

            RemoteMLWriter.handleOverwrite(path, shouldOverwrite)
            pl_writer = PipelineWriter(instance)
            pl_writer.session(session)  # type: ignore[arg-type]
            pl_writer.save(path)
        elif isinstance(instance, PipelineModel):
            from pyspark.ml.pipeline import PipelineModelWriter

            RemoteMLWriter.handleOverwrite(path, shouldOverwrite)
            plm_writer = PipelineModelWriter(instance)
            plm_writer.session(session)  # type: ignore[arg-type]
            plm_writer.save(path)
        elif isinstance(instance, CrossValidator):
            from pyspark.ml.tuning import CrossValidatorWriter

            RemoteMLWriter.handleOverwrite(path, shouldOverwrite)
            cv_writer = CrossValidatorWriter(instance)
            cv_writer.session(session)  # type: ignore[arg-type]
            cv_writer.save(path)
        elif isinstance(instance, CrossValidatorModel):
            from pyspark.ml.tuning import CrossValidatorModelWriter

            RemoteMLWriter.handleOverwrite(path, shouldOverwrite)
            cvm_writer = CrossValidatorModelWriter(instance)
            cvm_writer.optionMap = optionMap
            cvm_writer.session(session)  # type: ignore[arg-type]
            cvm_writer.save(path)
        elif isinstance(instance, TrainValidationSplit):
            from pyspark.ml.tuning import TrainValidationSplitWriter

            RemoteMLWriter.handleOverwrite(path, shouldOverwrite)
            tvs_writer = TrainValidationSplitWriter(instance)
            tvs_writer.save(path)
        elif isinstance(instance, TrainValidationSplitModel):
            from pyspark.ml.tuning import TrainValidationSplitModelWriter

            RemoteMLWriter.handleOverwrite(path, shouldOverwrite)
            tvsm_writer = TrainValidationSplitModelWriter(instance)
            tvsm_writer.optionMap = optionMap
            tvsm_writer.session(session)  # type: ignore[arg-type]
            tvsm_writer.save(path)
        elif isinstance(instance, OneVsRest):
            from pyspark.ml.classification import OneVsRestWriter

            RemoteMLWriter.handleOverwrite(path, shouldOverwrite)
            ovr_writer = OneVsRestWriter(instance)
            ovr_writer.session(session)  # type: ignore[arg-type]
            ovr_writer.save(path)
        elif isinstance(instance, OneVsRestModel):
            from pyspark.ml.classification import OneVsRestModelWriter

            RemoteMLWriter.handleOverwrite(path, shouldOverwrite)
            ovrm_writer = OneVsRestModelWriter(instance)
            ovrm_writer.session(session)  # type: ignore[arg-type]
            ovrm_writer.save(path)

        elif isinstance(instance, PowerIterationClustering):
            transformer = JavaTransformer(
                "org.apache.spark.ml.clustering.PowerIterationClusteringWrapper"
            )
            transformer._resetUid(instance.uid)
            transformer._paramMap = instance._paramMap
            RemoteMLWriter.saveInstance(
                transformer,  # type: ignore[arg-type]
                path,
                session,
                shouldOverwrite,
                optionMap,
            )

        else:
            raise NotImplementedError(f"Unsupported write for {instance.__class__}")

    @staticmethod
    def handleOverwrite(path: str, shouldOverwrite: bool) -> None:
        from pyspark.ml.util import ML_CONNECT_HELPER_ID

        if shouldOverwrite:
            helper = JavaWrapper(java_obj=ML_CONNECT_HELPER_ID)
            helper._call_java("handleOverwrite", path, shouldOverwrite)


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
        from pyspark.ml.wrapper import JavaModel, JavaEstimator, JavaTransformer
        from pyspark.ml.evaluation import JavaEvaluator
        from pyspark.ml.pipeline import Pipeline, PipelineModel
        from pyspark.ml.classification import OneVsRest, OneVsRestModel
        from pyspark.ml.clustering import PowerIterationClustering
        from pyspark.ml.tuning import (
            CrossValidator,
            CrossValidatorModel,
            TrainValidationSplit,
            TrainValidationSplitModel,
        )

        if (
            issubclass(clazz, JavaModel)
            or issubclass(clazz, JavaEstimator)
            or issubclass(clazz, JavaEvaluator)
            or issubclass(clazz, JavaTransformer)
        ):
            if issubclass(clazz, JavaModel):
                ml_type = pb2.MlOperator.OPERATOR_TYPE_MODEL
            elif issubclass(clazz, JavaEstimator):
                ml_type = pb2.MlOperator.OPERATOR_TYPE_ESTIMATOR
            elif issubclass(clazz, JavaEvaluator):
                ml_type = pb2.MlOperator.OPERATOR_TYPE_EVALUATOR
            else:
                ml_type = pb2.MlOperator.OPERATOR_TYPE_TRANSFORMER

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
                from pyspark.ml.util import RemoteModelRef

                if ml_type == pb2.MlOperator.OPERATOR_TYPE_MODEL:
                    remote_model_ref = RemoteModelRef(result.obj_ref.id)
                    instance = py_type(remote_model_ref)
                else:
                    instance = py_type()
                instance._resetUid(result.uid)
                params = {k: deserialize_param(v) for k, v in result.params.params.items()}
                instance._set(**params)
                return instance
            else:
                raise RuntimeError(f"Unsupported python type {py_type}")

        elif issubclass(clazz, Pipeline):
            from pyspark.ml.pipeline import PipelineReader

            pl_reader = PipelineReader(Pipeline)
            pl_reader.session(session)
            return pl_reader.load(path)

        elif issubclass(clazz, PipelineModel):
            from pyspark.ml.pipeline import PipelineModelReader

            plm_reader = PipelineModelReader(PipelineModel)
            plm_reader.session(session)
            return plm_reader.load(path)

        elif issubclass(clazz, CrossValidator):
            from pyspark.ml.tuning import CrossValidatorReader

            cv_reader = CrossValidatorReader(CrossValidator)
            cv_reader.session(session)
            return cv_reader.load(path)

        elif issubclass(clazz, CrossValidatorModel):
            from pyspark.ml.tuning import CrossValidatorModelReader

            cvm_reader = CrossValidatorModelReader(CrossValidator)
            cvm_reader.session(session)
            return cvm_reader.load(path)

        elif issubclass(clazz, TrainValidationSplit):
            from pyspark.ml.tuning import TrainValidationSplitReader

            tvs_reader = TrainValidationSplitReader(TrainValidationSplit)
            tvs_reader.session(session)
            return tvs_reader.load(path)

        elif issubclass(clazz, TrainValidationSplitModel):
            from pyspark.ml.tuning import TrainValidationSplitModelReader

            tvs_reader = TrainValidationSplitModelReader(TrainValidationSplitModel)
            tvs_reader.session(session)
            return tvs_reader.load(path)

        elif issubclass(clazz, OneVsRest):
            from pyspark.ml.classification import OneVsRestReader

            ovr_reader = OneVsRestReader(OneVsRest)
            ovr_reader.session(session)
            return ovr_reader.load(path)

        elif issubclass(clazz, OneVsRestModel):
            from pyspark.ml.classification import OneVsRestModelReader

            ovrm_reader = OneVsRestModelReader(OneVsRestModel)
            ovrm_reader.session(session)
            return ovrm_reader.load(path)

        elif issubclass(clazz, PowerIterationClustering):
            java_qualified_class_name = (
                "org.apache.spark.ml.clustering.PowerIterationClusteringWrapper"
            )

            command = pb2.Command()
            command.ml_command.read.CopyFrom(
                pb2.MlCommand.Read(
                    operator=pb2.MlOperator(
                        name=java_qualified_class_name,
                        type=pb2.MlOperator.OPERATOR_TYPE_TRANSFORMER,
                    ),
                    path=path,
                )
            )
            (_, properties, _) = session.client.execute_command(command)
            result = deserialize(properties)

            instance = PowerIterationClustering()
            instance._resetUid(result.uid)
            params = {k: deserialize_param(v) for k, v in result.params.params.items()}
            instance._set(**params)
            return instance  # type: ignore[return-value]

        else:
            raise RuntimeError(f"Unsupported read for {clazz}")
