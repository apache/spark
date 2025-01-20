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
from typing import Any, Dict, List, Optional, Union, cast, TYPE_CHECKING

import pandas as pd

from pyspark import keyword_only, since
from pyspark.ml.connect.base import Estimator, Model, Transformer
from pyspark.ml.connect.io_utils import (
    ParamsReadWrite,
    MetaAlgorithmReadWrite,
)
from pyspark.ml.param import Param, Params
from pyspark.ml.common import inherit_doc
from pyspark.sql.dataframe import DataFrame


if TYPE_CHECKING:
    from pyspark.ml._typing import ParamMap


class _PipelineReadWrite(MetaAlgorithmReadWrite):
    def _get_child_stages(self) -> List[Any]:
        if isinstance(self, Pipeline):
            return list(self.getStages())
        elif isinstance(self, PipelineModel):
            return list(self.stages)
        else:
            raise ValueError(f"Unknown type {self.__class__}")

    def _get_skip_saving_params(self) -> List[str]:
        """
        Returns params to be skipped when saving metadata.
        """
        return ["stages"]

    def _save_meta_algorithm(self, root_path: str, node_path: List[str]) -> Dict[str, Any]:
        metadata = self._get_metadata_to_save()
        metadata["stages"] = []

        if isinstance(self, Pipeline):
            stages = self.getStages()
        elif isinstance(self, PipelineModel):
            stages = self.stages
        else:
            raise ValueError(f"Unknown type {self.__class__}")

        for stage_index, stage in enumerate(stages):
            stage_node_path = node_path + [f"pipeline_stage_{stage_index}"]
            stage_metadata = stage._save_to_node_path(  # type: ignore[attr-defined]
                root_path, stage_node_path
            )
            metadata["stages"].append(stage_metadata)
        return metadata

    def _load_meta_algorithm(self, root_path: str, node_metadata: Dict[str, Any]) -> None:
        stages = []
        for stage_meta in node_metadata["stages"]:
            stage = ParamsReadWrite._load_instance_from_metadata(stage_meta, root_path)
            stages.append(stage)

        if isinstance(self, Pipeline):
            self.setStages(stages)
        elif isinstance(self, PipelineModel):
            self.stages = stages
        else:
            raise ValueError()


@inherit_doc
class Pipeline(Estimator["PipelineModel"], _PipelineReadWrite):
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

    .. deprecated:: 4.0.0

    Examples
    --------
    >>> from pyspark.ml.connect import Pipeline
    >>> from pyspark.ml.connect.classification import LogisticRegression
    >>> from pyspark.ml.connect.feature import StandardScaler
    >>> scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
    >>> lor = LogisticRegression(maxIter=20, learningRate=0.01)
    >>> pipeline=Pipeline(stages=[scaler, lor])
    >>> dataset = spark.createDataFrame([
    ...     ([1.0, 2.0], 1),
    ...     ([2.0, -1.0], 1),
    ...     ([-3.0, -2.0], 0),
    ...     ([-1.0, -2.0], 0),
    ... ], schema=['features', 'label'])
    >>> pipeline_model = pipeline.fit(dataset)
    >>> transformed_dataset = pipeline_model.transform(dataset)
    >>> transformed_dataset.show()
    +------------+-----+--------------------+----------+--------------------+
    |    features|label|     scaled_features|prediction|         probability|
    +------------+-----+--------------------+----------+--------------------+
    |  [1.0, 2.0]|    1|[0.56373452100212...|         1|[0.02423273026943...|
    | [2.0, -1.0]|    1|[1.01472213780381...|         1|[0.09334788471460...|
    |[-3.0, -2.0]|    0|[-1.2402159462046...|         0|[0.99808156490325...|
    |[-1.0, -2.0]|    0|[-0.3382407126012...|         0|[0.96210002899169...|
    +------------+-----+--------------------+----------+--------------------+
    >>> pipeline_model.saveToLocal("/tmp/pipeline")
    >>> loaded_pipeline_model = PipelineModel.loadFromLocal("/tmp/pipeline")
    """

    stages: Param[List[Params]] = Param(
        Params._dummy(), "stages", "a list of pipeline stages"
    )  # type: ignore[assignment]

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(self, *, stages: Optional[List[Params]] = None):
        """
        __init__(self, \\*, stages=None)
        """
        super(Pipeline, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def setStages(self, value: List[Params]) -> "Pipeline":
        """
        Set pipeline stages.

        .. versionadded:: 3.5.0

        .. deprecated:: 4.0.0

        Parameters
        ----------
        value : list
            of :py:class:`pyspark.ml.connect.Transformer`
            or :py:class:`pyspark.ml.connect.Estimator`

        Returns
        -------
        :py:class:`Pipeline`
            the pipeline instance
        """
        return self._set(stages=value)

    @since("3.5.0")
    def getStages(self) -> List[Params]:
        """
        Get pipeline stages.
        """
        return self.getOrDefault(self.stages)

    @keyword_only
    @since("3.5.0")
    def setParams(self, *, stages: Optional[List[Params]] = None) -> "Pipeline":
        """
        setParams(self, \\*, stages=None)
        Sets params for Pipeline.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _fit(self, dataset: Union[DataFrame, pd.DataFrame]) -> "PipelineModel":
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
                    model = stage.fit(dataset)  # type: ignore[attr-defined]
                    transformers.append(model)
                    if i < indexOfLastEstimator:
                        dataset = model.transform(dataset)
            else:
                transformers.append(cast(Transformer, stage))
        pipeline_model = PipelineModel(transformers)  # type: ignore[arg-type]
        pipeline_model._resetUid(self.uid)
        return pipeline_model

    def copy(self, extra: Optional["ParamMap"] = None) -> "Pipeline":
        """
        Creates a copy of this instance.

        .. versionadded:: 3.5.0

        .. deprecated:: 4.0.0

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
class PipelineModel(Model, _PipelineReadWrite):
    """
    Represents a compiled pipeline with transformers and fitted models.

    .. versionadded:: 3.5.0

    .. deprecated:: 4.0.0
    """

    def __init__(self, stages: Optional[List[Params]] = None):
        super(PipelineModel, self).__init__()
        self.stages = stages  # type: ignore[assignment]

    def _transform(self, dataset: Union[DataFrame, pd.DataFrame]) -> Union[DataFrame, pd.DataFrame]:
        for t in self.stages:
            dataset = t.transform(dataset)
        return dataset

    def copy(self, extra: Optional["ParamMap"] = None) -> "PipelineModel":
        """
        Creates a copy of this instance.

        .. versionadded:: 3.5.0

        .. deprecated:: 4.0.0

        :param extra: extra parameters
        :returns: new instance
        """
        if extra is None:
            extra = dict()
        stages = [stage.copy(extra) for stage in self.stages]
        return PipelineModel(stages)
