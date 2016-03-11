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

from abc import ABCMeta, abstractmethod

from pyspark import SparkContext
from pyspark import since
from pyspark.ml.param import Param, Params
from pyspark.ml.util import keyword_only, JavaMLWriter, JavaMLReader
from pyspark.ml.wrapper import JavaWrapper
from pyspark.mllib.common import inherit_doc, _py2java, _java2py


@inherit_doc
class Estimator(Params):
    """
    Abstract class for estimators that fit models to data.

    .. versionadded:: 1.3.0
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def _fit(self, dataset):
        """
        Fits a model to the input dataset. This is called by the
        default implementation of fit.

        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.DataFrame`
        :returns: fitted model
        """
        raise NotImplementedError()

    @since("1.3.0")
    def fit(self, dataset, params=None):
        """
        Fits a model to the input dataset with optional parameters.

        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.DataFrame`
        :param params: an optional param map that overrides embedded
                       params. If a list/tuple of param maps is given,
                       this calls fit on each param map and returns a
                       list of models.
        :returns: fitted model(s)
        """
        if params is None:
            params = dict()
        if isinstance(params, (list, tuple)):
            return [self.fit(dataset, paramMap) for paramMap in params]
        elif isinstance(params, dict):
            if params:
                return self.copy(params)._fit(dataset)
            else:
                return self._fit(dataset)
        else:
            raise ValueError("Params must be either a param map or a list/tuple of param maps, "
                             "but got %s." % type(params))


@inherit_doc
class Transformer(Params):
    """
    Abstract class for transformers that transform one dataset into
    another.

    .. versionadded:: 1.3.0
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def _transform(self, dataset):
        """
        Transforms the input dataset.

        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.DataFrame`
        :returns: transformed dataset
        """
        raise NotImplementedError()

    @since("1.3.0")
    def transform(self, dataset, params=None):
        """
        Transforms the input dataset with optional parameters.

        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.DataFrame`
        :param params: an optional param map that overrides embedded
                       params.
        :returns: transformed dataset
        """
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params,)._transform(dataset)
            else:
                return self._transform(dataset)
        else:
            raise ValueError("Params must be either a param map but got %s." % type(params))


@inherit_doc
class Model(Transformer):
    """
    Abstract class for models that are fitted by estimators.

    .. versionadded:: 1.4.0
    """

    __metaclass__ = ABCMeta


def stages_java2py(java_stages):
    """
    Transforms the parameter Python stages from a list of Java stages.
    :param java_stages: An array of Java stages.
    :return: An array of Python stages.
    """

    sc = SparkContext._active_spark_context

    def __get_class(clazz):
        """
        Loads Python class from its name.
        """
        parts = clazz.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m

    def __transfer_stage_from_java(java_stage):
        stage_name = java_stage.getClass().getName().replace("org.apache.spark", "pyspark")
        # Generate a default new instance from the stage_name class.
        py_stage = __get_class(stage_name)()
        # Load information from java_stage to the instance.
        py_stage._java_obj = java_stage
        py_stage._resetUid(_java2py(sc, java_stage.uid()))
        py_stage._transfer_params_from_java()
        return py_stage

    return map(__transfer_stage_from_java, java_stages)


def stages_py2java(py_stages):
    """
    Transforms the parameter of Python stages to a Java array of Java stages.
    :param py_stages: An array of Python stages.
    :return: A Java array of Java Stages.
    """

    java_stages = map(lambda stage: stage._java_obj,
               map(lambda stage: stage._transfer_params_to_java(), py_stages))
    gateway = SparkContext._gateway
    jvm = SparkContext._jvm
    jstages = gateway.new_array(jvm.org.apache.spark.ml.PipelineStage, len(java_stages))
    for idx, java_stage in enumerate(java_stages):
        jstages[idx] = java_stage
    return jstages


@inherit_doc
class PipelineMLWriter(JavaMLWriter, JavaWrapper):
    """
    .. note:: Experimental

    Utility class that can save ML instances through their Scala implementation.

    .. versionadded:: 2.0.0
    """

    def __init__(self, instance):
        self._java_obj = self._new_java_obj("org.apache.spark.ml.Pipeline", instance.uid)
        jparam = self._java_obj.getParam(instance.stages.name)
        jstages = stages_py2java(instance.getStages())
        self._java_obj.set(jparam.w(jstages))
        self._jwrite = self._java_obj.write()


@inherit_doc
class PipelineMLReader(JavaMLReader):
    def load(self, path):
        """Load the ML instance from the input path."""
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))

        sc = SparkContext._active_spark_context
        java_obj = self._jread.load(path)
        instance = self._clazz()
        instance._resetUid(java_obj.uid())
        jparam = java_obj.getParam(instance.stages.name)
        if java_obj.isDefined(jparam):
            java_stages = _java2py(sc, java_obj.getOrDefault(jparam))
            instance._paramMap[instance.stages] = stages_java2py(java_stages)

        return instance


@inherit_doc
class Pipeline(Estimator):
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
    :py:class:`Pipeline` is an :py:class:`PipelineModel`, which
    consists of fitted models and transformers, corresponding to the
    pipeline stages. If there are no stages, the pipeline acts as an
    identity transformer.

    .. versionadded:: 1.3.0
    """

    stages = Param(Params._dummy(), "stages", "pipeline stages")

    @keyword_only
    def __init__(self, stages=None):
        """
        __init__(self, stages=None)
        """
        if stages is None:
            stages = []
        super(Pipeline, self).__init__()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @since("1.3.0")
    def setStages(self, value):
        """
        Set pipeline stages.

        :param value: a list of transformers or estimators
        :return: the pipeline instance
        """
        self._paramMap[self.stages] = value
        return self

    @since("1.3.0")
    def getStages(self):
        """
        Get pipeline stages.
        """
        if self.stages in self._paramMap:
            return self._paramMap[self.stages]

    @keyword_only
    @since("1.3.0")
    def setParams(self, stages=None):
        """
        setParams(self, stages=None)
        Sets params for Pipeline.
        """
        if stages is None:
            stages = []
        kwargs = self.setParams._input_kwargs
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

    def write(self):
        """Returns an JavaMLWriter instance for this ML instance."""
        return PipelineMLWriter(self)

    def save(self, path):
        """Save this ML instance to the given path, a shortcut of `write().save(path)`."""
        self.write().save(path)

    @classmethod
    def read(cls):
        """Returns an JavaMLReader instance for this class."""
        return PipelineMLReader(cls)

    @classmethod
    def load(cls, path):
        """Reads an ML instance from the input path, a shortcut of `read().load(path)`."""
        return cls.read().load(path)


@inherit_doc
class PipelineModelMLWriter(JavaMLWriter, JavaWrapper):
    """
    .. note:: Experimental

    Utility class that can save ML instances through their Scala implementation.

    .. versionadded:: 2.0.0
    """

    def __init__(self, instance):
        self._java_obj = self._new_java_obj("org.apache.spark.ml.PipelineModel",
                                            instance.uid,
                                            stages_py2java(instance.stages))
        self._jwrite = self._java_obj.write()


@inherit_doc
class PipelineModelMLReader(JavaMLReader):
    def load(self, path):
        """Load the ML instance from the input path."""
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))
        java_obj = self._jread.load(path)
        instance = self._clazz(stages_java2py(java_obj))
        instance._resetUid(java_obj.uid())
        return instance


@inherit_doc
class PipelineModel(Model):
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

    def write(self):
        """Returns an JavaMLWriter instance for this ML instance."""
        return PipelineModelMLWriter(self)

    def save(self, path):
        """Save this ML instance to the given path, a shortcut of `write().save(path)`."""
        self.write().save(path)

    @classmethod
    def read(cls):
        """Returns an JavaMLReader instance for this class."""
        return PipelineModelMLReader(cls)

    @classmethod
    def load(cls, path):
        """Reads an ML instance from the input path, a shortcut of `read().load(path)`."""
        return cls.read().load(path)
