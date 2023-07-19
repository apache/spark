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

from pyspark import keyword_only
from pyspark.ml.connect.base import _PredictorParams

from pyspark.ml.param.shared import HasProbabilityCol

from typing import Any, Dict, Union, List, Tuple, Callable, Optional
import numpy as np
import pandas as pd
import math

from pyspark.sql import DataFrame
from pyspark.ml.common import inherit_doc
from pyspark.ml.torch.distributor import TorchDistributor
from pyspark.ml.param.shared import (
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasWeightCol,
    HasSeed,
    HasNumTrainWorkers,
    HasBatchSize,
    HasLearningRate,
    HasMomentum,
)
from pyspark.ml.connect.base import Predictor, PredictionModel
from pyspark.ml.connect.io_utils import ParamsReadWrite, CoreModelReadWrite
from pyspark.sql.functions import lit, count, countDistinct

import torch
import torch.nn as torch_nn


class _LogisticRegressionParams(
    _PredictorParams,
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasWeightCol,
    HasNumTrainWorkers,
    HasBatchSize,
    HasLearningRate,
    HasMomentum,
    HasProbabilityCol,
    HasSeed,
):
    """
    Params for :py:class:`LogisticRegression` and :py:class:`LogisticRegressionModel`.

    .. versionadded:: 3.0.0
    """

    def __init__(self, *args: Any):
        super(_LogisticRegressionParams, self).__init__(*args)
        self._setDefault(
            maxIter=100,
            tol=1e-6,
            batchSize=32,
            learningRate=0.001,
            momentum=0.9,
            seed=0,
        )


def _train_logistic_regression_model_worker_fn(
    num_samples_per_worker: int,
    num_features: int,
    batch_size: int,
    max_iter: int,
    num_classes: int,
    learning_rate: float,
    momentum: float,
    fit_intercept: bool,
    seed: int,
) -> Any:
    from pyspark.ml.torch.distributor import _get_spark_partition_data_loader
    from torch.nn.parallel import DistributedDataParallel as DDP
    import torch.distributed
    import torch.optim as optim

    # TODO: add a setting seed param.
    torch.manual_seed(seed)

    # TODO: support training on GPU
    # TODO: support L1 / L2 regularization
    torch.distributed.init_process_group("gloo")

    linear_model = torch_nn.Linear(
        num_features, num_classes, bias=fit_intercept, dtype=torch.float32
    )
    ddp_model = DDP(linear_model)

    loss_fn = torch_nn.CrossEntropyLoss()

    optimizer = optim.SGD(ddp_model.parameters(), lr=learning_rate, momentum=momentum)
    data_loader = _get_spark_partition_data_loader(
        num_samples_per_worker,
        batch_size,
        num_workers=0,
        prefetch_factor=None,  # type: ignore
    )
    for i in range(max_iter):
        ddp_model.train()

        step_count = 0

        loss_sum = 0.0
        for x, target in data_loader:
            optimizer.zero_grad()
            output = ddp_model(x.to(torch.float32))
            loss = loss_fn(output, target.to(torch.long))
            loss.backward()
            loss_sum += loss.detach().numpy()
            optimizer.step()
            step_count += 1

        # TODO: early stopping
        #  When each epoch ends, computes loss on validation dataset and compare
        #  current epoch validation loss with last epoch validation loss, if
        #  less than provided `tol`, stop training.

        if torch.distributed.get_rank() == 0:
            print(f"Progress: train epoch {i + 1} completes, train loss = {loss_sum / step_count}")

    if torch.distributed.get_rank() == 0:
        return ddp_model.module.state_dict()

    return None


@inherit_doc
class LogisticRegression(
    Predictor["LogisticRegressionModel"], _LogisticRegressionParams, ParamsReadWrite
):
    """
    Logistic regression estimator.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> from pyspark.ml.connect.classification import LogisticRegression, LogisticRegressionModel
    >>> lor = LogisticRegression(maxIter=20, learningRate=0.01)
    >>> dataset = spark.createDataFrame([
    ...     ([1.0, 2.0], 1),
    ...     ([2.0, -1.0], 1),
    ...     ([-3.0, -2.0], 0),
    ...     ([-1.0, -2.0], 0),
    ... ], schema=['features', 'label'])
    >>> lor_model = lor.fit(dataset)
    >>> transformed_dataset = lor_model.transform(dataset)
    >>> transformed_dataset.show()
    +------------+-----+----------+--------------------+
    |    features|label|prediction|         probability|
    +------------+-----+----------+--------------------+
    |  [1.0, 2.0]|    1|         1|[0.02423273026943...|
    | [2.0, -1.0]|    1|         1|[0.09334788471460...|
    |[-3.0, -2.0]|    0|         0|[0.99808156490325...|
    |[-1.0, -2.0]|    0|         0|[0.96210002899169...|
    +------------+-----+----------+--------------------+
    >>> lor_model.saveToLocal("/tmp/lor_model")
    >>> LogisticRegressionModel.loadFromLocal("/tmp/lor_model")
    LogisticRegression_...
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        labelCol: str = "label",
        predictionCol: str = "prediction",
        probabilityCol: str = "probability",
        maxIter: int = 100,
        tol: float = 1e-6,
        numTrainWorkers: int = 1,
        batchSize: int = 32,
        learningRate: float = 0.001,
        momentum: float = 0.9,
        seed: int = 0,
    ):
        """
        __init__(
            self,
            *,
            featuresCol: str = "features",
            labelCol: str = "label",
            predictionCol: str = "prediction",
            probabilityCol: str = "probability",
            maxIter: int = 100,
            tol: float = 1e-6,
            numTrainWorkers: int = 1,
            batchSize: int = 32,
            learningRate: float = 0.001,
            momentum: float = 0.9,
            seed: int = 0,
        )
        """
        super(LogisticRegression, self).__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def _fit(self, dataset: Union[DataFrame, pd.DataFrame]) -> "LogisticRegressionModel":
        if isinstance(dataset, pd.DataFrame):
            # TODO: support pandas dataframe fitting
            raise NotImplementedError("Fitting pandas dataframe is not supported yet.")

        num_train_workers = self.getNumTrainWorkers()
        batch_size = self.getBatchSize()

        # We don't need to persist the dataset because the shuffling result from the repartition
        # has been cached.
        dataset = dataset.select(self.getFeaturesCol(), self.getLabelCol()).repartition(
            num_train_workers
        )

        # TODO: check label values are in range of [0, num_classes)
        num_rows, num_classes = dataset.agg(
            count(lit(1)), countDistinct(self.getLabelCol())
        ).head()  # type: ignore[misc]

        num_batches_per_worker = math.ceil(num_rows / num_train_workers / batch_size)
        num_samples_per_worker = num_batches_per_worker * batch_size

        num_features = len(dataset.select(self.getFeaturesCol()).head()[0])  # type: ignore[index]

        if num_classes < 2:
            raise ValueError("Training dataset distinct labels must >= 2.")

        # TODO: support GPU.
        distributor = TorchDistributor(
            local_mode=False, use_gpu=False, num_processes=num_train_workers
        )
        model_state_dict = distributor._train_on_dataframe(
            _train_logistic_regression_model_worker_fn,
            dataset,
            num_samples_per_worker=num_samples_per_worker,
            num_features=num_features,
            batch_size=batch_size,
            max_iter=self.getMaxIter(),
            num_classes=num_classes,
            learning_rate=self.getLearningRate(),
            momentum=self.getMomentum(),
            fit_intercept=self.getFitIntercept(),
            seed=self.getSeed(),
        )

        dataset.unpersist()

        torch_model = torch_nn.Linear(
            num_features, num_classes, bias=self.getFitIntercept(), dtype=torch.float32
        )
        torch_model.load_state_dict(model_state_dict)

        lor_model = LogisticRegressionModel(
            torch_model, num_features=num_features, num_classes=num_classes
        )
        lor_model._resetUid(self.uid)
        return self._copyValues(lor_model)


@inherit_doc
class LogisticRegressionModel(
    PredictionModel, _LogisticRegressionParams, ParamsReadWrite, CoreModelReadWrite
):
    """
    Model fitted by LogisticRegression.

    .. versionadded:: 3.5.0
    """

    def __init__(
        self,
        torch_model: Any = None,
        num_features: Optional[int] = None,
        num_classes: Optional[int] = None,
    ):
        super().__init__()
        self.torch_model = torch_model
        self.num_features = num_features
        self.num_classes = num_classes

    @property
    def numFeatures(self) -> int:
        return self.num_features  # type: ignore[return-value]

    @property
    def numClasses(self) -> int:
        return self.num_classes  # type: ignore[return-value]

    def _input_columns(self) -> List[str]:
        return [self.getOrDefault(self.featuresCol)]

    def _output_columns(self) -> List[Tuple[str, str]]:
        output_cols = [(self.getOrDefault(self.predictionCol), "bigint")]
        prob_col = self.getOrDefault(self.probabilityCol)
        if prob_col:
            output_cols += [(prob_col, "array<double>")]
        return output_cols

    def _get_transform_fn(self) -> Callable[["pd.Series"], Any]:
        model_state_dict = self.torch_model.state_dict()
        num_features = self.num_features
        num_classes = self.num_classes
        fit_intercept = self.getFitIntercept()

        def transform_fn(input_series: Any) -> Any:
            torch_model = torch_nn.Linear(
                num_features,  # type: ignore[arg-type]
                num_classes,  # type: ignore[arg-type]
                bias=fit_intercept,
                dtype=torch.float32,
            )
            # TODO: Use spark broadast for `model_state_dict`,
            #  it can improve performance when model is large.
            torch_model.load_state_dict(model_state_dict)

            input_array = np.stack(input_series.values)

            with torch.inference_mode():
                result = torch_model(torch.tensor(input_array, dtype=torch.float32))
                predictions = torch.argmax(result, dim=1).numpy()

            if self.getProbabilityCol():
                probabilities = torch.softmax(result, dim=1).numpy()

                return pd.DataFrame(
                    {
                        self.getPredictionCol(): list(predictions),
                        self.getProbabilityCol(): list(probabilities),
                    },
                    index=input_series.index.copy(),
                )
            else:
                return pd.Series(data=list(predictions), index=input_series.index.copy())

        return transform_fn

    def _get_core_model_filename(self) -> str:
        return self.__class__.__name__ + ".torch"

    def _save_core_model(self, path: str) -> None:
        lor_torch_model = torch_nn.Sequential(
            self.torch_model,
            torch_nn.Softmax(dim=1),
        )
        torch.save(lor_torch_model, path)

    def _load_core_model(self, path: str) -> None:
        lor_torch_model = torch.load(path)
        self.torch_model = lor_torch_model[0]

    def _get_extra_metadata(self) -> Dict[str, Any]:
        return {
            "num_features": self.num_features,
            "num_classes": self.num_classes,
        }

    def _load_extra_metadata(self, extra_metadata: Dict[str, Any]) -> None:
        """
        Load extra metadata attribute from extra metadata json object.
        """
        self.num_features = extra_metadata["num_features"]
        self.num_classes = extra_metadata["num_classes"]
