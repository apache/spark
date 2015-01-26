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

from abc import ABCMeta, abstractmethod, abstractproperty

from pyspark import SparkContext
from pyspark.sql import SchemaRDD, inherit_doc  # TODO: move inherit_doc to Spark Core
from pyspark.ml.param import Param, Params
from pyspark.ml.util import Identifiable

__all__ = ["Pipeline", "Transformer", "Estimator", "param", "feature", "classification"]


def _jvm():
    """
    Returns the JVM view associated with SparkContext. Must be called
    after SparkContext is initialized.
    """
    jvm = SparkContext._jvm
    if jvm:
        return jvm
    else:
        raise AttributeError("Cannot load _jvm from SparkContext. Is SparkContext initialized?")


@inherit_doc
class PipelineStage(Params):
    """
    A stage in a pipeline, either an :py:class:`Estimator` or a
    :py:class:`Transformer`.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(PipelineStage, self).__init__()


@inherit_doc
class Estimator(PipelineStage):
    """
    Abstract class for estimators that fit models to data.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(Estimator, self).__init__()

    @abstractmethod
    def fit(self, dataset, params={}):
        """
        Fits a model to the input dataset with optional parameters.

        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.SchemaRDD`
        :param params: an optional param map that overwrites embedded
                       params
        :returns: fitted model
        """
        raise NotImplementedError()


@inherit_doc
class Transformer(PipelineStage):
    """
    Abstract class for transformers that transform one dataset into
    another.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(Transformer, self).__init__()

    @abstractmethod
    def transform(self, dataset, params={}):
        """
        Transforms the input dataset with optional parameters.

        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.SchemaRDD`
        :param params: an optional param map that overwrites embedded
                       params
        :returns: transformed dataset
        """
        raise NotImplementedError()


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
    """

    def __init__(self):
        super(Pipeline, self).__init__()
        #: Param for pipeline stages.
        self.stages = Param(self, "stages", "pipeline stages")

    def setStages(self, value):
        """
        Set pipeline stages.
        :param value: a list of transformers or estimators
        :return: the pipeline instance
        """
        self.paramMap[self.stages] = value
        return self

    def getStages(self):
        """
        Get pipeline stages.
        """
        if self.stages in self.paramMap:
            return self.paramMap[self.stages]

    def fit(self, dataset, params={}):
        paramMap = self._merge_params(params)
        stages = paramMap[self.stages]
        for stage in stages:
            if not (isinstance(stage, Estimator) or isinstance(stage, Transformer)):
                raise ValueError(
                    "Cannot recognize a pipeline stage of type %s." % type(stage).__name__)
        indexOfLastEstimator = -1
        for i, stage in enumerate(stages):
            if isinstance(stage, Estimator):
                indexOfLastEstimator = i
        transformers = []
        for i, stage in enumerate(stages):
            if i <= indexOfLastEstimator:
                if isinstance(stage, Transformer):
                    transformers.append(stage)
                    dataset = stage.transform(dataset, paramMap)
                else:  # must be an Estimator
                    model = stage.fit(dataset, paramMap)
                    transformers.append(model)
                    dataset = model.transform(dataset, paramMap)
            else:
                transformers.append(stage)
        return PipelineModel(transformers)


@inherit_doc
class PipelineModel(Transformer):
    """
    Represents a compiled pipeline with transformers and fitted models.
    """

    def __init__(self, transformers):
        super(PipelineModel, self).__init__()
        self.transformers = transformers

    def transform(self, dataset, params={}):
        paramMap = self._merge_params(params)
        for t in self.transformers:
            dataset = t.transform(dataset, paramMap)
        return dataset


@inherit_doc
class JavaWrapper(Params):
    """
    Utility class to help create wrapper classes from Java/Scala
    implementations of pipeline components.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(JavaWrapper, self).__init__()

    @abstractproperty
    def _java_class(self):
        """
        Fully-qualified class name of the wrapped Java component.
        """
        raise NotImplementedError

    def _create_java_obj(self):
        """
        Creates a new Java object and returns its reference.
        """
        java_obj = _jvm()
        for name in self._java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj()

    def _transfer_params_to_java(self, params, java_obj):
        """
        Transforms the embedded params and additional params to the
        input Java object.
        :param params: additional params (overwriting embedded values)
        :param java_obj: Java object to receive the params
        """
        paramMap = self._merge_params(params)
        for param in self.params():
            if param in paramMap:
                java_obj.set(param.name, paramMap[param])

    def _empty_java_param_map(self):
        """
        Returns an empty Java ParamMap reference.
        """
        return _jvm().org.apache.spark.ml.param.ParamMap()


@inherit_doc
class JavaEstimator(Estimator, JavaWrapper):
    """
    Base class for :py:class:`Estimator`s that wrap Java/Scala
    implementations.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(JavaEstimator, self).__init__()

    @abstractmethod
    def _create_model(self, java_model):
        """
        Creates a model from the input Java model reference.
        """
        raise NotImplementedError

    def _fit_java(self, dataset, params={}):
        """
        Fits a Java model to the input dataset.
        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.SchemaRDD`
        :param params: additional params (overwriting embedded values)
        :return: fitted Java model
        """
        java_obj = self._create_java_obj()
        self._transfer_params_to_java(params, java_obj)
        return java_obj.fit(dataset._jschema_rdd, self._empty_java_param_map())

    def fit(self, dataset, params={}):
        java_model = self._fit_java(dataset, params)
        return self._create_model(java_model)


@inherit_doc
class JavaTransformer(Transformer, JavaWrapper):
    """
    Base class for :py:class:`Transformer`s that wrap Java/Scala
    implementations.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(JavaTransformer, self).__init__()

    def transform(self, dataset, params={}):
        java_obj = self._create_java_obj()
        self._transfer_params_to_java(params, java_obj)
        return SchemaRDD(java_obj.transform(dataset._jschema_rdd, self._empty_java_param_map()),
                         dataset.sql_ctx)
