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

from pyspark.ml.param import Param, Params
from pyspark.ml.util import keyword_only
from pyspark.mllib.common import inherit_doc


__all__ = ['Estimator', 'Transformer', 'Pipeline', 'PipelineModel']


@inherit_doc
class Estimator(Params):
    """
    Abstract class for estimators that fit models to data.
    """

    __metaclass__ = ABCMeta

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
class Transformer(Params):
    """
    Abstract class for transformers that transform one dataset into
    another.
    """

    __metaclass__ = ABCMeta

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

    @keyword_only
    def __init__(self, stages=[]):
        """
        __init__(self, stages=[])
        """
        super(Pipeline, self).__init__()
        #: Param for pipeline stages.
        self.stages = Param(self, "stages", "pipeline stages")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

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

    @keyword_only
    def setParams(self, stages=[]):
        """
        setParams(self, stages=[])
        Sets params for Pipeline.
        """
        kwargs = self.setParams._input_kwargs
        return self._set_params(**kwargs)

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
                    if i < indexOfLastEstimator:
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
