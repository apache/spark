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
import os

from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast, TYPE_CHECKING

from pyspark import keyword_only, since, SparkContext
from pyspark.ml.base import Estimator, Model, Transformer
from pyspark.ml.param import Param, Params
from pyspark.ml.util import (
    MLReadable,
    MLWritable,
    JavaMLWriter,
    JavaMLReader,
    DefaultParamsReader,
    DefaultParamsWriter,
    MLWriter,
    MLReader,
    JavaMLReadable,
    JavaMLWritable,
)
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.common import inherit_doc
from pyspark.sql.dataframe import DataFrame

if TYPE_CHECKING:
    from pyspark.ml._typing import ParamMap, PipelineStage
    from py4j.java_gateway import JavaObject  # type: ignore[import]


@inherit_doc
class Pipeline(Estimator["PipelineModel"], MLReadable["Pipeline"], MLWritable):
    """
    A simple pipeline, which acts as an estimator. A Pipeline consists
    of a sequence of stages, each of which is either an
    :py:class:`Estimator` or a :py:class:`Transformer`. When
    :py:meth:`Pipeline.fit` is called, the stages are executed in
    order. If a stage is an :py:class:`Estimator`, its
    :py:meth:`Estimator.fit` method will be called on the input
    dataset to fit a model. Then the model, which is a transformer,
    will be used to transform the dataset as the input to the next
    stage. If a stage is a :py:class:`Transformer`, its
    :py:meth:`Transformer.transform` method will be called to produce
    the dataset for the next stage. The fitted model from a
    :py:class:`Pipeline` is a :py:class:`PipelineModel`, which
    consists of fitted models and transformers, corresponding to the
    pipeline stages. If stages is an empty list, the pipeline acts as an
    identity transformer.

    .. versionadded:: 1.3.0
    """

    stages: Param[List["PipelineStage"]] = Param(
        Params._dummy(), "stages", "a list of pipeline stages"
    )

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(self, *, stages: Optional[List["PipelineStage"]] = None):
        """
        __init__(self, \\*, stages=None)
        """
        super(Pipeline, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def setStages(self, value: List["PipelineStage"]) -> "Pipeline":
        """
        Set pipeline stages.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        value : list
            of :py:class:`pyspark.ml.Transformer`
            or :py:class:`pyspark.ml.Estimator`

        Returns
        -------
        :py:class:`Pipeline`
            the pipeline instance
        """
        return self._set(stages=value)

    @since("1.3.0")
    def getStages(self) -> List["PipelineStage"]:
        """
        Get pipeline stages.
        """
        return self.getOrDefault(self.stages)

    @keyword_only
    @since("1.3.0")
    def setParams(self, *, stages: Optional[List["PipelineStage"]] = None) -> "Pipeline":
        """
        setParams(self, \\*, stages=None)
        Sets params for Pipeline.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _fit(self, dataset: DataFrame) -> "PipelineModel":
        stages = self.getStages()
        for stage in stages:
            if not (isinstance(stage, Estimator) or isinstance(stage, Transformer)):
                raise TypeError("Cannot recognize a pipeline stage of type %s." % type(stage))
        indexOfLastEstimator = -1
        for i, stage in enumerate(stages):
            if isinstance(stage, Estimator):
                indexOfLastEstimator = i
        transformers: List[Transformer] = []
        for i, stage in enumerate(stages):
            if i <= indexOfLastEstimator:
                if isinstance(stage, Transformer):
                    transformers.append(stage)
                    dataset = stage.transform(dataset)
                else:  # must be an Estimator
                    model = stage.fit(dataset)
                    transformers.append(model)
                    if i < indexOfLastEstimator:
                        dataset = model.transform(dataset)
            else:
                transformers.append(cast(Transformer, stage))
        return PipelineModel(transformers)

    def copy(self, extra: Optional["ParamMap"] = None) -> "Pipeline":
        """
        Creates a copy of this instance.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        extra : dict, optional
            extra parameters

        Returns
        -------
        :py:class:`Pipeline`
            new instance
        """
        if extra is None:
            extra = dict()
        that = Params.copy(self, extra)
        stages = [stage.copy(extra) for stage in that.getStages()]
        return that.setStages(stages)

    @since("2.0.0")
    def write(self) -> MLWriter:
        """Returns an MLWriter instance for this ML instance."""
        allStagesAreJava = PipelineSharedReadWrite.checkStagesForJava(self.getStages())
        if allStagesAreJava:
            return JavaMLWriter(self)  # type: ignore[arg-type]
        return PipelineWriter(self)

    @classmethod
    @since("2.0.0")
    def read(cls) -> "PipelineReader":
        """Returns an MLReader instance for this class."""
        return PipelineReader(cls)

    @classmethod
    def _from_java(cls, java_stage: "JavaObject") -> "Pipeline":
        """
        Given a Java Pipeline, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        # Create a new instance of this stage.
        py_stage = cls()
        # Load information from java_stage to the instance.
        py_stages: List["PipelineStage"] = [
            JavaParams._from_java(s) for s in java_stage.getStages()
        ]
        py_stage.setStages(py_stages)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self) -> "JavaObject":
        """
        Transfer this instance to a Java Pipeline.  Used for ML persistence.

        Returns
        -------
        py4j.java_gateway.JavaObject
            Java object equivalent to this instance.
        """

        gateway = SparkContext._gateway
        assert gateway is not None and SparkContext._jvm is not None

        cls = SparkContext._jvm.org.apache.spark.ml.PipelineStage
        java_stages = gateway.new_array(cls, len(self.getStages()))
        for idx, stage in enumerate(self.getStages()):
            java_stages[idx] = cast(JavaParams, stage)._to_java()

        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.Pipeline", self.uid)
        _java_obj.setStages(java_stages)

        return _java_obj


@inherit_doc
class PipelineWriter(MLWriter):
    """
    (Private) Specialization of :py:class:`MLWriter` for :py:class:`Pipeline` types
    """

    def __init__(self, instance: Pipeline):
        super(PipelineWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        stages = self.instance.getStages()
        PipelineSharedReadWrite.validateStages(stages)
        PipelineSharedReadWrite.saveImpl(self.instance, stages, self.sc, path)


@inherit_doc
class PipelineReader(MLReader[Pipeline]):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`Pipeline` types
    """

    def __init__(self, cls: Type[Pipeline]):
        super(PipelineReader, self).__init__()
        self.cls = cls

    def load(self, path: str) -> Pipeline:
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        if "language" not in metadata["paramMap"] or metadata["paramMap"]["language"] != "Python":
            return JavaMLReader(cast(Type["JavaMLReadable[Pipeline]"], self.cls)).load(path)
        else:
            uid, stages = PipelineSharedReadWrite.load(metadata, self.sc, path)
            return Pipeline(stages=stages)._resetUid(uid)


@inherit_doc
class PipelineModelWriter(MLWriter):
    """
    (Private) Specialization of :py:class:`MLWriter` for :py:class:`PipelineModel` types
    """

    def __init__(self, instance: "PipelineModel"):
        super(PipelineModelWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        stages = self.instance.stages
        PipelineSharedReadWrite.validateStages(cast(List["PipelineStage"], stages))
        PipelineSharedReadWrite.saveImpl(
            self.instance, cast(List["PipelineStage"], stages), self.sc, path
        )


@inherit_doc
class PipelineModelReader(MLReader["PipelineModel"]):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`PipelineModel` types
    """

    def __init__(self, cls: Type["PipelineModel"]):
        super(PipelineModelReader, self).__init__()
        self.cls = cls

    def load(self, path: str) -> "PipelineModel":
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        if "language" not in metadata["paramMap"] or metadata["paramMap"]["language"] != "Python":
            return JavaMLReader(cast(Type["JavaMLReadable[PipelineModel]"], self.cls)).load(path)
        else:
            uid, stages = PipelineSharedReadWrite.load(metadata, self.sc, path)
            return PipelineModel(stages=cast(List[Transformer], stages))._resetUid(uid)


@inherit_doc
class PipelineModel(Model, MLReadable["PipelineModel"], MLWritable):
    """
    Represents a compiled pipeline with transformers and fitted models.

    .. versionadded:: 1.3.0
    """

    def __init__(self, stages: List[Transformer]):
        super(PipelineModel, self).__init__()
        self.stages = stages

    def _transform(self, dataset: DataFrame) -> DataFrame:
        for t in self.stages:
            dataset = t.transform(dataset)
        return dataset

    def copy(self, extra: Optional["ParamMap"] = None) -> "PipelineModel":
        """
        Creates a copy of this instance.

        .. versionadded:: 1.4.0

        :param extra: extra parameters
        :returns: new instance
        """
        if extra is None:
            extra = dict()
        stages = [stage.copy(extra) for stage in self.stages]
        return PipelineModel(stages)

    @since("2.0.0")
    def write(self) -> MLWriter:
        """Returns an MLWriter instance for this ML instance."""
        allStagesAreJava = PipelineSharedReadWrite.checkStagesForJava(
            cast(List["PipelineStage"], self.stages)
        )
        if allStagesAreJava:
            return JavaMLWriter(self)  # type: ignore[arg-type]
        return PipelineModelWriter(self)

    @classmethod
    @since("2.0.0")
    def read(cls) -> PipelineModelReader:
        """Returns an MLReader instance for this class."""
        return PipelineModelReader(cls)

    @classmethod
    def _from_java(cls, java_stage: "JavaObject") -> "PipelineModel":
        """
        Given a Java PipelineModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        # Load information from java_stage to the instance.
        py_stages: List[Transformer] = [JavaParams._from_java(s) for s in java_stage.stages()]
        # Create a new instance of this stage.
        py_stage = cls(py_stages)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self) -> "JavaObject":
        """
        Transfer this instance to a Java PipelineModel.  Used for ML persistence.

        :return: Java object equivalent to this instance.
        """

        gateway = SparkContext._gateway
        assert gateway is not None and SparkContext._jvm is not None

        cls = SparkContext._jvm.org.apache.spark.ml.Transformer
        java_stages = gateway.new_array(cls, len(self.stages))
        for idx, stage in enumerate(self.stages):
            java_stages[idx] = cast(JavaParams, stage)._to_java()

        _java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.PipelineModel", self.uid, java_stages
        )

        return _java_obj


@inherit_doc
class PipelineSharedReadWrite:
    """
    Functions for :py:class:`MLReader` and :py:class:`MLWriter` shared between
    :py:class:`Pipeline` and :py:class:`PipelineModel`

    .. versionadded:: 2.3.0
    """

    @staticmethod
    def checkStagesForJava(stages: List["PipelineStage"]) -> bool:
        return all(isinstance(stage, JavaMLWritable) for stage in stages)

    @staticmethod
    def validateStages(stages: List["PipelineStage"]) -> None:
        """
        Check that all stages are Writable
        """
        for stage in stages:
            if not isinstance(stage, MLWritable):
                raise ValueError(
                    "Pipeline write will fail on this pipeline "
                    + "because stage %s of type %s is not MLWritable",
                    stage.uid,
                    type(stage),
                )

    @staticmethod
    def saveImpl(
        instance: Union[Pipeline, PipelineModel],
        stages: List["PipelineStage"],
        sc: SparkContext,
        path: str,
    ) -> None:
        """
        Save metadata and stages for a :py:class:`Pipeline` or :py:class:`PipelineModel`
        - save metadata to path/metadata
        - save stages to stages/IDX_UID
        """
        stageUids = [stage.uid for stage in stages]
        jsonParams = {"stageUids": stageUids, "language": "Python"}
        DefaultParamsWriter.saveMetadata(instance, path, sc, paramMap=jsonParams)
        stagesDir = os.path.join(path, "stages")
        for index, stage in enumerate(stages):
            cast(MLWritable, stage).write().save(
                PipelineSharedReadWrite.getStagePath(stage.uid, index, len(stages), stagesDir)
            )

    @staticmethod
    def load(
        metadata: Dict[str, Any], sc: SparkContext, path: str
    ) -> Tuple[str, List["PipelineStage"]]:
        """
        Load metadata and stages for a :py:class:`Pipeline` or :py:class:`PipelineModel`

        Returns
        -------
        tuple
            (UID, list of stages)
        """
        stagesDir = os.path.join(path, "stages")
        stageUids = metadata["paramMap"]["stageUids"]
        stages = []
        for index, stageUid in enumerate(stageUids):
            stagePath = PipelineSharedReadWrite.getStagePath(
                stageUid, index, len(stageUids), stagesDir
            )
            stage: "PipelineStage" = DefaultParamsReader.loadParamsInstance(stagePath, sc)
            stages.append(stage)
        return (metadata["uid"], stages)

    @staticmethod
    def getStagePath(stageUid: str, stageIdx: int, numStages: int, stagesDir: str) -> str:
        """
        Get path for saving the given stage.
        """
        stageIdxDigits = len(str(numStages))
        stageDir = str(stageIdx).zfill(stageIdxDigits) + "_" + stageUid
        stagePath = os.path.join(stagesDir, stageDir)
        return stagePath
