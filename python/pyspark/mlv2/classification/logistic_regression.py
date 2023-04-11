import numpy as np
import math
from pyspark.mlv2.base import Estimator, Model

from pyspark.sql import DataFrame

from pyspark.ml.param import (
    Param,
    Params,
    TypeConverters,
)
from pyspark.ml.torch.distributor import TorchDistributor
from pyspark.mlv2.classification.base import _ProbabilisticClassifierParams
from pyspark.ml.param.shared import (
    HasRegParam,
    HasElasticNetParam,
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasWeightCol,
)
from pyspark.mlv2.common_params import (
    HasUseGPU,
    HasNumTrainWorkers,
    HasBatchSize,
    HasLearningRate,
    HasMomentum,
)
from pyspark.sql.functions import lit, count, countDistinct

import torch
import torch.nn as torch_nn
import torch.nn.functional as torch_fn


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

    def __init__(self, *args: Any):
        super(_LogisticRegressionParams, self).__init__(*args)
        self._setDefault(
            maxIter=100,
            tol=1e-6,
            numTrainWorkers=1,
            numBatchSize=32,
            learning_rate=0.001,
            momentum=0.9,
        )


class _Net(torch_nn.Module):
    def __init__(self, num_features, num_labels, bias) -> None:
        super(_Net, self).__init__()

        if num_labels > 2:
            self.is_multinomial = True
            output_dim = num_labels
        else:
            self.is_multinomial = False
            output_dim = 1

        self.fc = torch_nn.Linear(num_features, output_dim, bias=bias)

    def forward(self, x: Any) -> Any:
        output = self.fc(x)
        if not self.is_multinomial:
            output = torch.sigmoid(output).squeeze()
        return output


def _train_worker_fn(
    num_samples_per_worker,
    num_features,
    batch_size,
    max_iter,
    num_labels,
    learning_rate,
    momentum,
    fit_intercept,
):
    from pyspark.ml.torch.distributor import get_spark_partition_data_loader
    from torch.nn.parallel import DistributedDataParallel as DDP
    import torch.distributed
    import torch.optim as optim

    torch.distributed.init_process_group("gloo")

    ddp_model = DDP(_Net(
        num_features=num_features,
        num_labels=num_labels,
        bias=fit_intercept
    ))

    if num_labels > 2:
        loss_fn = torch_nn.CrossEntropyLoss()
    else:
        loss_fn = torch_nn.BCELoss()

    optimizer = optim.SGD(ddp_model.parameters(), lr=learning_rate, momentum=momentum)
    data_loader = get_spark_partition_data_loader(num_samples_per_worker, batch_size)
    for i in range(max_iter):
        ddp_model.train()
        for x, target in data_loader:
            optimizer.zero_grad()
            output = ddp_model(x)
            loss_fn(output, target).backward()
            optimizer.step()

        # TODO: early stopping
        #  When each epoch ends, computes loss on validation dataset and compare
        #  current epoch validation loss with last epoch validation loss, if
        #  less than provided `tol`, stop training.

    if torch.distributed.get_rank() == 0:
        return ddp_model.module.state_dict()

    return None


class LogisticRegression(Estimator["LogisticRegressionModel"], _LogisticRegressionParams):

    def _fit(self, dataset: DataFrame) -> "LogisticRegressionModel":

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
        model_state_dict = distributor.train_on_dataframe(
            _train_worker_fn,
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

        torch_model = _Net(num_features=num_features, num_labels=num_labels, bias=self.getFitIntercept())
        torch_model.load_state_dict(model_state_dict)

        lor_model = LogisticRegressionModel(torch_model)
        lor_model._resetUid(self.uid)
        return self._copyValues(lor_model)


class LogisticRegressionModel(Model, _LogisticRegressionParams):

    def __init__(self, torch_model):
        self.torch_model = torch_model

