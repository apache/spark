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
from pyspark.mlv2.base import Estimator, Model, Transformer
from pyspark.ml.param import Param, Params
from pyspark.ml.common import inherit_doc
from pyspark.sql.dataframe import DataFrame


PipelineStage = Union[Estimator, Transformer]

if TYPE_CHECKING:
    from pyspark.ml._typing import ParamMap


@inherit_doc
class Pipeline(Estimator["PipelineModel"]):
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

    .. versionadded:: 3.5.0
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

        .. versionadded:: 3.5.0

        Parameters
        ----------
        value : list
            of :py:class:`pyspark.mlv2.Transformer`
            or :py:class:`pyspark.mlv2.Estimator`

        Returns
        -------
        :py:class:`Pipeline`
            the pipeline instance
        """
        return self._set(stages=value)

    @since("3.5.0")
    def getStages(self) -> List["PipelineStage"]:
        """
        Get pipeline stages.
        """
        return self.getOrDefault(self.stages)

    @keyword_only
    @since("3.5.0")
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

        .. versionadded:: 3.5.0

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


@inherit_doc
class PipelineModel(Model):
    """
    Represents a compiled pipeline with transformers and fitted models.

    .. versionadded:: 3.5.0
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
