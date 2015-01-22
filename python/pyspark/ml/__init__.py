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
from pyspark.sql import SchemaRDD, inherit_doc # TODO: move inherit_doc to Spark Core
from pyspark.ml.param import Param, Params
from pyspark.ml.util import Identifiable

__all__ = ["Pipeline", "Transformer", "Estimator", "param", "feature", "classification"]


def _jvm():
    return SparkContext._jvm


def _inherit_doc(cls):
    for name, func in vars(cls).items():
        # only inherit docstring for public functions
        if name.startswith("_"):
            continue
        if not func.__doc__:
            for parent in cls.__bases__:
                parent_func = getattr(parent, name, None)
                if parent_func and getattr(parent_func, "__doc__", None):
                    func.__doc__ = parent_func.__doc__
                    break
    return cls


@inherit_doc
class PipelineStage(Params):
    """
    A stage in a pipeline, either an :py:class:`Estimator` or a
    :py:class:`Transformer`.
    """

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
        map = self._merge_params(params)
        transformers = []
        for stage in self.getStages():
            if isinstance(stage, Transformer):
                transformers.append(stage)
                dataset = stage.transform(dataset, map)
            elif isinstance(stage, Estimator):
                model = stage.fit(dataset, map)
                transformers.append(model)
                dataset = model.transform(dataset, map)
            else:
                raise ValueError(
                    "Cannot recognize a pipeline stage of type %s." % type(stage).__name__)
        return PipelineModel(transformers)


@inherit_doc
class PipelineModel(Transformer):

    def __init__(self, transformers):
        super(PipelineModel, self).__init__()
        self.transformers = transformers

    def transform(self, dataset, params={}):
        map = self._merge_params(params)
        for t in self.transformers:
            dataset = t.transform(dataset, map)
        return dataset


@inherit_doc
class JavaWrapper(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        super(JavaWrapper, self).__init__()

    @abstractproperty
    def _java_class(self):
        raise NotImplementedError

    def _create_java_obj(self):
        java_obj = _jvm()
        for name in self._java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj()


@inherit_doc
class JavaEstimator(Estimator, JavaWrapper):

    __metaclass__ = ABCMeta

    def __init__(self):
        super(JavaEstimator, self).__init__()

    @abstractmethod
    def _create_model(self, java_model):
        raise NotImplementedError

    def _fit_java(self, dataset, params={}):
        java_obj = self._create_java_obj()
        self._transfer_params_to_java(params, java_obj)
        return java_obj.fit(dataset._jschema_rdd, _jvm().org.apache.spark.ml.param.ParamMap())

    def fit(self, dataset, params={}):
        java_model = self._fit_java(dataset, params)
        return self._create_model(java_model)


@inherit_doc
class JavaTransformer(Transformer, JavaWrapper):

    __metaclass__ = ABCMeta

    def __init__(self):
        super(JavaTransformer, self).__init__()

    def transform(self, dataset, params={}):
        java_obj = self._create_java_obj()
        self._transfer_params_to_java(params, java_obj)
        return SchemaRDD(java_obj.transform(dataset._jschema_rdd,
                                            _jvm().org.apache.spark.ml.param.ParamMap()),
                         dataset.sql_ctx)
