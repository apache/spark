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

from pyspark.mlv2.base import _PredictorParams

from pyspark.ml.param.shared import HasProbabilityCol

from typing import Any, Union
import numpy as np
import pandas as pd
import math
from pyspark.mlv2.base import Estimator, Model

from pyspark.sql import DataFrame

from pyspark.ml.torch.distributor import TorchDistributor
from pyspark.ml.param.shared import (
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasWeightCol,
)
from pyspark.mlv2.common_params import (
    HasNumTrainWorkers,
    HasBatchSize,
    HasLearningRate,
    HasMomentum,
)
from pyspark.sql.functions import lit, count, countDistinct

import torch
import torch.nn as torch_nn
import torch.nn.functional as torch_fn


class _ClassifierParams(_PredictorParams):
    """
    Classifier Params for classification tasks.

    .. versionadded:: 3.5.0
    """

    pass


class _ProbabilisticClassifierParams(HasProbabilityCol, _ClassifierParams):
    """
    Params for :py:class:`ProbabilisticClassifier` and
    :py:class:`ProbabilisticClassificationModel`.

    .. versionadded:: 3.5.0
    """

    pass


class _LogisticRegressionParams(
    _ProbabilisticClassifierParams,
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasWeightCol,
    HasNumTrainWorkers,
    HasBatchSize,
    HasLearningRate,
    HasMomentum,
):
    """
    Params for :py:class:`LogisticRegression` and :py:class:`LogisticRegressionModel`.

    .. versionadded:: 3.0.0
    """

    def __init__(
            self,
            *,
            maxIter=100,
            tol=1e-6,
            numTrainWorkers=1,
            batchSize=32,
            learningRate=0.001,
            momentum=0.9,
    ):
        super(_LogisticRegressionParams, self).__init__()
        self._set(
            maxIter=maxIter,
            tol=tol,
            numTrainWorkers=numTrainWorkers,
            batchSize=batchSize,
            learningRate=learningRate,
            momentum=momentum,
        )


class _LinearNet(torch_nn.Module):
    def __init__(self, num_features, num_labels, bias) -> None:
        super(_LinearNet, self).__init__()
        output_dim = num_labels
        self.fc = torch_nn.Linear(num_features, output_dim, bias=bias, dtype=torch.float32)

    def forward(self, x: Any) -> Any:
        output = self.fc(x)
        return output


def _train_logistic_regression_model_worker_fn(
        num_samples_per_worker,
        num_features,
        batch_size,
        max_iter,
        num_labels,
        learning_rate,
        momentum,
        fit_intercept,
):
    from pyspark.ml.torch.distributor import _get_spark_partition_data_loader
    from torch.nn.parallel import DistributedDataParallel as DDP
    import torch.distributed
    import torch.optim as optim

    # TODO: support training on GPU
    # TODO: support L1 / L2 regularization
    torch.distributed.init_process_group("gloo")

    ddp_model = DDP(_LinearNet(
        num_features=num_features,
        num_labels=num_labels,
        bias=fit_intercept
    ))

    loss_fn = torch_nn.CrossEntropyLoss()

    optimizer = optim.SGD(ddp_model.parameters(), lr=learning_rate, momentum=momentum)
    data_loader = _get_spark_partition_data_loader(num_samples_per_worker, batch_size)
    for i in range(max_iter):
        ddp_model.train()

        step_count = 0
        for x, target in data_loader:
            optimizer.zero_grad()
            output = ddp_model(x.to(torch.float32))
            loss_fn(output, target.to(torch.long)).backward()
            optimizer.step()
            step_count += 1

        print(f"DBG: train loop {i} steps {step_count}\n")

        # TODO: early stopping
        #  When each epoch ends, computes loss on validation dataset and compare
        #  current epoch validation loss with last epoch validation loss, if
        #  less than provided `tol`, stop training.

    if torch.distributed.get_rank() == 0:
        return ddp_model.module.state_dict()

    return None


class LogisticRegression(Estimator["LogisticRegressionModel"], _LogisticRegressionParams):

    def _fit(self, dataset: Union[DataFrame, pd.DataFrame]) -> "LogisticRegressionModel":
        if isinstance(dataset, pd.DataFrame):
            # TODO: support pandas dataframe fitting
            raise NotImplementedError("Fitting pandas dataframe is not supported yet.")

        num_train_workers = self.getNumTrainWorkers()
        batch_size = self.getBatchSize()

        # Q: Shall we persist the shuffled dataset ?
        # shuffling results are already cached
        dataset = (
            dataset
                .select(self.getFeaturesCol(), self.getLabelCol())
                .repartition(num_train_workers)
                .persist()
        )

        # TODO: check label values are in range of [0, label_count)
        num_rows, num_labels = dataset.agg(count(lit(1)), countDistinct(self.getLabelCol())).head()

        num_batches_per_worker = math.ceil(num_rows / num_train_workers / batch_size)
        num_samples_per_worker = num_batches_per_worker * batch_size

        num_features = len(dataset.select(self.getFeaturesCol()).head()[0])

        if num_labels < 2:
            raise ValueError("Training dataset distinct labels must >= 2.")

        # TODO: support GPU.
        distributor = TorchDistributor(local_mode=False, use_gpu=False)
        model_state_dict = distributor._train_on_dataframe(
            _train_logistic_regression_model_worker_fn,
            dataset,
            num_samples_per_worker=num_samples_per_worker,
            num_features=num_features,
            batch_size=batch_size,
            max_iter=self.getMaxIter(),
            num_labels=num_labels,
            learning_rate=self.getLearningRate(),
            momentum=self.getMomentum(),
            fit_intercept=self.getFitIntercept(),
        )

        dataset.unpersist()

        torch_model = _LinearNet(
            num_features=num_features, num_labels=num_labels, bias=self.getFitIntercept()
        )
        torch_model.load_state_dict(model_state_dict)

        lor_model = LogisticRegressionModel(torch_model)
        lor_model._resetUid(self.uid)
        return self._copyValues(lor_model)


class LogisticRegressionModel(Model, _LogisticRegressionParams):

    def __init__(self, torch_model):
        super().__init__()
        self.torch_model = torch_model
