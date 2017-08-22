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

import sys
import os

if sys.version > '3':
    basestring = str

from pyspark import since, keyword_only, SparkContext
from pyspark.ml.base import Estimator, Model, Transformer
from pyspark.ml.param import Param, Params
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.common import inherit_doc


@inherit_doc
class Pipeline(Estimator, MLReadable, MLWritable):
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

    stages = Param(Params._dummy(), "stages", "a list of pipeline stages")

    @keyword_only
    def __init__(self, stages=None):
        """
        __init__(self, stages=None)
        """
        super(Pipeline, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @since("1.3.0")
    def setStages(self, value):
        """
        Set pipeline stages.

        :param value: a list of transformers or estimators
        :return: the pipeline instance
        """
        return self._set(stages=value)

    @since("1.3.0")
    def getStages(self):
        """
        Get pipeline stages.
        """
        return self.getOrDefault(self.stages)

    @keyword_only
    @since("1.3.0")
    def setParams(self, stages=None):
        """
        setParams(self, stages=None)
        Sets params for Pipeline.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _fit(self, dataset):
        stages = self.getStages()
        for stage in stages:
            if not (isinstance(stage, Estimator) or isinstance(stage, Transformer)):
                raise TypeError(
                    "Cannot recognize a pipeline stage of type %s." % type(stage))
        indexOfLastEstimator = -1
        for i, stage in enumerate(stages):
            if isinstance(stage, Estimator):
                indexOfLastEstimator = i
        transformers = []
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
                transformers.append(stage)
        return PipelineModel(transformers)

    @since("1.4.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance.

        :param extra: extra parameters
        :returns: new instance
        """
        if extra is None:
            extra = dict()
        that = Params.copy(self, extra)
        stages = [stage.copy(extra) for stage in that.getStages()]
        return that.setStages(stages)

    @since("2.0.0")
    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        allStagesAreJava = PipelineSharedReadWrite.checkStagesForJava(self.getStages())
        if allStagesAreJava:
            return JavaMLWriter(self)
        return PipelineWriter(self)

    @classmethod
    @since("2.0.0")
    def read(cls):
        """Returns an MLReader instance for this class."""
        return PipelineReader(cls)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java Pipeline, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        # Create a new instance of this stage.
        py_stage = cls()
        # Load information from java_stage to the instance.
        py_stages = [JavaParams._from_java(s) for s in java_stage.getStages()]
        py_stage.setStages(py_stages)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java Pipeline.  Used for ML persistence.

        :return: Java object equivalent to this instance.
        """

        gateway = SparkContext._gateway
        cls = SparkContext._jvm.org.apache.spark.ml.PipelineStage
        java_stages = gateway.new_array(cls, len(self.getStages()))
        for idx, stage in enumerate(self.getStages()):
            java_stages[idx] = stage._to_java()

        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.Pipeline", self.uid)
        _java_obj.setStages(java_stages)

        return _java_obj


@inherit_doc
class PipelineWriter(MLWriter):
    """
    (Private) Specialization of :py:class:`MLWriter` for :py:class:`Pipeline` types
    """

    def __init__(self, instance):
        super(PipelineWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path):
        stages = self.instance.getStages()
        PipelineSharedReadWrite.validateStages(stages)
        PipelineSharedReadWrite.saveImpl(self.instance, stages, self.sc, path)


@inherit_doc
class PipelineReader(MLReader):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`Pipeline` types
    """

    def __init__(self, cls):
        super(PipelineReader, self).__init__()
        self.cls = cls

    def load(self, path):
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        if 'language' not in metadata['paramMap'] or metadata['paramMap']['language'] != 'Python':
            return JavaMLReader(self.cls).load(path)
        else:
            uid, stages = PipelineSharedReadWrite.load(metadata, self.sc, path)
            return Pipeline(stages=stages)._resetUid(uid)


@inherit_doc
class PipelineModelWriter(MLWriter):
    """
    (Private) Specialization of :py:class:`MLWriter` for :py:class:`PipelineModel` types
    """

    def __init__(self, instance):
        super(PipelineModelWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path):
        stages = self.instance.stages
        PipelineSharedReadWrite.validateStages(stages)
        PipelineSharedReadWrite.saveImpl(self.instance, stages, self.sc, path)


@inherit_doc
class PipelineModelReader(MLReader):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`PipelineModel` types
    """

    def __init__(self, cls):
        super(PipelineModelReader, self).__init__()
        self.cls = cls

    def load(self, path):
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        if 'language' not in metadata['paramMap'] or metadata['paramMap']['language'] != 'Python':
            return JavaMLReader(self.cls).load(path)
        else:
            uid, stages = PipelineSharedReadWrite.load(metadata, self.sc, path)
            return PipelineModel(stages=stages)._resetUid(uid)


@inherit_doc
class PipelineModel(Model, MLReadable, MLWritable):
    """
    Represents a compiled pipeline with transformers and fitted models.

    .. versionadded:: 1.3.0
    """

    def __init__(self, stages):
        super(PipelineModel, self).__init__()
        self.stages = stages

    def _transform(self, dataset):
        for t in self.stages:
            dataset = t.transform(dataset)
        return dataset

    @since("1.4.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance.

        :param extra: extra parameters
        :returns: new instance
        """
        if extra is None:
            extra = dict()
        stages = [stage.copy(extra) for stage in self.stages]
        return PipelineModel(stages)

    @since("2.0.0")
    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        allStagesAreJava = PipelineSharedReadWrite.checkStagesForJava(self.stages)
        if allStagesAreJava:
            return JavaMLWriter(self)
        return PipelineModelWriter(self)

    @classmethod
    @since("2.0.0")
    def read(cls):
        """Returns an MLReader instance for this class."""
        return PipelineModelReader(cls)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java PipelineModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        # Load information from java_stage to the instance.
        py_stages = [JavaParams._from_java(s) for s in java_stage.stages()]
        # Create a new instance of this stage.
        py_stage = cls(py_stages)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java PipelineModel.  Used for ML persistence.

        :return: Java object equivalent to this instance.
        """

        gateway = SparkContext._gateway
        cls = SparkContext._jvm.org.apache.spark.ml.Transformer
        java_stages = gateway.new_array(cls, len(self.stages))
        for idx, stage in enumerate(self.stages):
            java_stages[idx] = stage._to_java()

        _java_obj =\
            JavaParams._new_java_obj("org.apache.spark.ml.PipelineModel", self.uid, java_stages)

        return _java_obj


@inherit_doc
class PipelineSharedReadWrite():
    """
    .. note:: DeveloperApi

    Functions for :py:class:`MLReader` and :py:class:`MLWriter` shared between
    :py:class:`Pipeline` and :py:class:`PipelineModel`

    .. versionadded:: 2.3.0
    """

    @staticmethod
    def checkStagesForJava(stages):
        return all(isinstance(stage, JavaMLWritable) for stage in stages)

    @staticmethod
    def validateStages(stages):
        """
        Check that all stages are Writable
        """
        for stage in stages:
            if not isinstance(stage, MLWritable):
                raise ValueError("Pipeline write will fail on this pipeline " +
                                 "because stage %s of type %s is not MLWritable",
                                 stage.uid, type(stage))

    @staticmethod
    def saveImpl(instance, stages, sc, path):
        """
        Save metadata and stages for a :py:class:`Pipeline` or :py:class:`PipelineModel`
        - save metadata to path/metadata
        - save stages to stages/IDX_UID
        """
        stageUids = [stage.uid for stage in stages]
        jsonParams = {'stageUids': stageUids, 'language': 'Python'}
        DefaultParamsWriter.saveMetadata(instance, path, sc, paramMap=jsonParams)
        stagesDir = os.path.join(path, "stages")
        for index, stage in enumerate(stages):
            stage.write().save(PipelineSharedReadWrite
                               .getStagePath(stage.uid, index, len(stages), stagesDir))

    @staticmethod
    def load(metadata, sc, path):
        """
        Load metadata and stages for a :py:class:`Pipeline` or :py:class:`PipelineModel`

        :return: (UID, list of stages)
        """
        stagesDir = os.path.join(path, "stages")
        stageUids = metadata['paramMap']['stageUids']
        stages = []
        for index, stageUid in enumerate(stageUids):
            stagePath = \
                PipelineSharedReadWrite.getStagePath(stageUid, index, len(stageUids), stagesDir)
            stage = DefaultParamsReader.loadParamsInstance(stagePath, sc)
            stages.append(stage)
        return (metadata['uid'], stages)

    @staticmethod
    def getStagePath(stageUid, stageIdx, numStages, stagesDir):
        """
        Get path for saving the given stage.
        """
        stageIdxDigits = len(str(numStages))
        stageDir = str(stageIdx).zfill(stageIdxDigits) + "_" + stageUid
        stagePath = os.path.join(stagesDir, stageDir)
        return stagePath
