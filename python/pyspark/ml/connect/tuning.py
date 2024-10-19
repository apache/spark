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

from multiprocessing.pool import ThreadPool
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    TYPE_CHECKING,
)

import numpy as np
import pandas as pd

from pyspark import keyword_only, since, inheritable_thread_target
from pyspark.ml.connect import Estimator, Model
from pyspark.ml.connect.base import Evaluator
from pyspark.ml.connect.io_utils import (
    MetaAlgorithmReadWrite,
    ParamsReadWrite,
)
from pyspark.ml.param import Params, Param, TypeConverters
from pyspark.ml.param.shared import HasParallelism, HasSeed
from pyspark.sql.functions import col, lit, rand
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.utils import is_remote


if TYPE_CHECKING:
    from pyspark.ml._typing import ParamMap


class _ValidatorParams(HasSeed):
    """
    Common params for TrainValidationSplit and CrossValidator.
    """

    estimator: Param[Estimator] = Param(
        Params._dummy(), "estimator", "estimator to be cross-validated"
    )
    estimatorParamMaps: Param[List["ParamMap"]] = Param(
        Params._dummy(), "estimatorParamMaps", "estimator param maps"
    )
    evaluator: Param[Evaluator] = Param(
        Params._dummy(),
        "evaluator",
        "evaluator used to select hyper-parameters that maximize the validator metric",
    )

    @since("2.0.0")
    def getEstimator(self) -> Estimator:
        """
        Gets the value of estimator or its default value.
        """
        return self.getOrDefault(self.estimator)

    @since("2.0.0")
    def getEstimatorParamMaps(self) -> List["ParamMap"]:
        """
        Gets the value of estimatorParamMaps or its default value.
        """
        return self.getOrDefault(self.estimatorParamMaps)

    @since("2.0.0")
    def getEvaluator(self) -> Evaluator:
        """
        Gets the value of evaluator or its default value.
        """
        return self.getOrDefault(self.evaluator)


class _CrossValidatorParams(_ValidatorParams):
    """
    Params for :py:class:`CrossValidator` and :py:class:`CrossValidatorModel`.

    .. versionadded:: 3.5.0
    """

    numFolds: Param[int] = Param(
        Params._dummy(),
        "numFolds",
        "number of folds for cross validation",
        typeConverter=TypeConverters.toInt,
    )

    foldCol: Param[str] = Param(
        Params._dummy(),
        "foldCol",
        "Param for the column name of user "
        + "specified fold number. Once this is specified, :py:class:`CrossValidator` "
        + "won't do random k-fold split. Note that this column should be integer type "
        + "with range [0, numFolds) and Spark will throw exception on out-of-range "
        + "fold numbers.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_CrossValidatorParams, self).__init__(*args)
        self._setDefault(numFolds=3, foldCol="")

    @since("1.4.0")
    def getNumFolds(self) -> int:
        """
        Gets the value of numFolds or its default value.
        """
        return self.getOrDefault(self.numFolds)

    @since("3.1.0")
    def getFoldCol(self) -> str:
        """
        Gets the value of foldCol or its default value.
        """
        return self.getOrDefault(self.foldCol)


def _parallelFitTasks(
    estimator: Estimator,
    train: DataFrame,
    evaluator: Evaluator,
    validation: DataFrame,
    epm: Sequence["ParamMap"],
) -> List[Callable[[], Tuple[int, float]]]:
    """
    Creates a list of callables which can be called from different threads to fit and evaluate
    an estimator in parallel. Each callable returns an `(index, metric)` pair.

    Parameters
    ----------
    est : :py:class:`pyspark.ml.baseEstimator`
        he estimator to be fit.
    train : :py:class:`pyspark.sql.DataFrame`
        DataFrame, training data set, used for fitting.
    eva : :py:class:`pyspark.ml.evaluation.Evaluator`
        used to compute `metric`
    validation : :py:class:`pyspark.sql.DataFrame`
        DataFrame, validation data set, used for evaluation.
    epm : :py:class:`collections.abc.Sequence`
        Sequence of ParamMap, params maps to be used during fitting & evaluation.
    collectSubModel : bool
        Whether to collect sub model.

    Returns
    -------
    tuple
        (int, float), an index into `epm` and the associated metric value.
    """

    active_session = SparkSession.getActiveSession()

    if active_session is None:
        raise RuntimeError(
            "An active SparkSession is required for running cross validator fit tasks."
        )

    def get_single_task(index: int, param_map: Any) -> Callable[[], Tuple[int, float]]:
        def single_task() -> Tuple[int, float]:
            if not is_remote():
                # Active session is thread-local variable, in background thread the active session
                # is not set, the following line sets it as the main thread active session.
                active_session._jvm.SparkSession.setActiveSession(  # type: ignore[union-attr]
                    active_session._jsparkSession  # type: ignore[union-attr]
                )

            model = estimator.fit(train, param_map)
            metric = evaluator.evaluate(
                model.transform(validation, param_map)  # type: ignore[union-attr]
            )
            return index, metric

        return single_task

    return [get_single_task(index, param_map) for index, param_map in enumerate(epm)]


class _CrossValidatorReadWrite(MetaAlgorithmReadWrite):
    def _get_skip_saving_params(self) -> List[str]:
        """
        Returns params to be skipped when saving metadata.
        """
        return ["estimator", "estimatorParamMaps", "evaluator"]

    def _save_meta_algorithm(self, root_path: str, node_path: List[str]) -> Dict[str, Any]:
        metadata = self._get_metadata_to_save()
        metadata[
            "estimator"
        ] = self.getEstimator()._save_to_node_path(  # type: ignore[attr-defined]
            root_path, node_path + ["crossvalidator_estimator"]
        )
        metadata[
            "evaluator"
        ] = self.getEvaluator()._save_to_node_path(  # type: ignore[attr-defined]
            root_path, node_path + ["crossvalidator_evaluator"]
        )
        metadata["estimator_param_maps"] = [
            [
                {"parent": param.parent, "name": param.name, "value": value}
                for param, value in param_map.items()
            ]
            for param_map in self.getEstimatorParamMaps()  # type: ignore[attr-defined]
        ]

        if isinstance(self, CrossValidatorModel):
            metadata["avg_metrics"] = self.avgMetrics
            metadata["std_metrics"] = self.stdMetrics

            metadata["best_model"] = self.bestModel._save_to_node_path(
                root_path, node_path + ["crossvalidator_best_model"]
            )
        return metadata

    def _load_meta_algorithm(self, root_path: str, node_metadata: Dict[str, Any]) -> None:
        estimator = ParamsReadWrite._load_instance_from_metadata(
            node_metadata["estimator"], root_path
        )
        self.set(self.estimator, estimator)  # type: ignore[attr-defined]

        evaluator = ParamsReadWrite._load_instance_from_metadata(
            node_metadata["evaluator"], root_path
        )
        self.set(self.evaluator, evaluator)  # type: ignore[attr-defined]

        json_epm = node_metadata["estimator_param_maps"]

        uid_to_instances = MetaAlgorithmReadWrite.get_uid_map(estimator)

        epm = []
        for json_param_map in json_epm:
            param_map = {}
            for json_param in json_param_map:
                est = uid_to_instances[json_param["parent"]]
                param = getattr(est, json_param["name"])
                value = json_param["value"]
                param_map[param] = value
            epm.append(param_map)

        self.set(self.estimatorParamMaps, epm)  # type: ignore[attr-defined]

        if isinstance(self, CrossValidatorModel):
            self.avgMetrics = node_metadata["avg_metrics"]
            self.stdMetrics = node_metadata["std_metrics"]

            self.bestModel = ParamsReadWrite._load_instance_from_metadata(
                node_metadata["best_model"], root_path
            )


class CrossValidator(
    Estimator["CrossValidatorModel"],
    _CrossValidatorParams,
    HasParallelism,
    _CrossValidatorReadWrite,
):
    """
    K-fold cross validation performs model selection by splitting the dataset into a set of
    non-overlapping randomly partitioned folds which are used as separate training and test datasets
    e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs,
    each of which uses 2/3 of the data for training and 1/3 for testing. Each fold is used as the
    test set exactly once.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> from pyspark.ml.connect.tuning import CrossValidator
    >>> from pyspark.ml.connect.classification import LogisticRegression
    >>> from pyspark.ml.connect.evaluation import BinaryClassificationEvaluator
    >>> from pyspark.ml.tuning import ParamGridBuilder
    >>> from sklearn.datasets import load_breast_cancer
    >>> lor = LogisticRegression(maxIter=20, learningRate=0.01)
    >>> ev = BinaryClassificationEvaluator()
    >>> grid = ParamGridBuilder().addGrid(lor.maxIter, [2, 20]).build()
    >>> cv = CrossValidator(estimator=lor, evaluator=ev, estimatorParamMaps=grid)
    >>> sk_dataset = load_breast_cancer()
    >>> train_dataset = spark.createDataFrame(
    ...     zip(sk_dataset.data.tolist(), [int(t) for t in sk_dataset.target]),
    ...     schema="features: array<double>, label: long",
    ... )
    >>> cv_model = cv.fit(train_dataset)
    >>> transformed_dataset = cv_model.transform(train_dataset.limit(10))
    >>> cv_model.avgMetrics
    [0.5527792527167658, 0.8348714668615984]
    >>> cv_model.stdMetrics
    [0.04902833489813031, 0.05247132866444953]
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        estimator: Optional[Estimator] = None,
        estimatorParamMaps: Optional[List["ParamMap"]] = None,
        evaluator: Optional[Evaluator] = None,
        numFolds: int = 3,
        seed: Optional[int] = None,
        parallelism: int = 1,
        foldCol: str = "",
    ) -> None:
        """
        __init__(self, \\*, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                 seed=None, parallelism=1, foldCol="")
        """
        super(CrossValidator, self).__init__()
        self._setDefault(parallelism=1)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    @since("3.5.0")
    def setParams(
        self,
        *,
        estimator: Optional[Estimator] = None,
        estimatorParamMaps: Optional[List["ParamMap"]] = None,
        evaluator: Optional[Evaluator] = None,
        numFolds: int = 3,
        seed: Optional[int] = None,
        parallelism: int = 1,
        foldCol: str = "",
    ) -> "CrossValidator":
        """
        setParams(self, \\*, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                  seed=None, parallelism=1, collectSubModels=False, foldCol=""):
        Sets params for cross validator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("3.5.0")
    def setEstimator(self, value: Estimator) -> "CrossValidator":
        """
        Sets the value of :py:attr:`estimator`.
        """
        return self._set(estimator=value)

    @since("3.5.0")
    def setEstimatorParamMaps(self, value: List["ParamMap"]) -> "CrossValidator":
        """
        Sets the value of :py:attr:`estimatorParamMaps`.
        """
        return self._set(estimatorParamMaps=value)

    @since("3.5.0")
    def setEvaluator(self, value: Evaluator) -> "CrossValidator":
        """
        Sets the value of :py:attr:`evaluator`.
        """
        return self._set(evaluator=value)

    @since("3.5.0")
    def setNumFolds(self, value: int) -> "CrossValidator":
        """
        Sets the value of :py:attr:`numFolds`.
        """
        return self._set(numFolds=value)

    @since("3.5.0")
    def setFoldCol(self, value: str) -> "CrossValidator":
        """
        Sets the value of :py:attr:`foldCol`.
        """
        return self._set(foldCol=value)

    def setSeed(self, value: int) -> "CrossValidator":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def setParallelism(self, value: int) -> "CrossValidator":
        """
        Sets the value of :py:attr:`parallelism`.
        """
        return self._set(parallelism=value)

    def setCollectSubModels(self, value: bool) -> "CrossValidator":
        """
        Sets the value of :py:attr:`collectSubModels`.
        """
        return self._set(collectSubModels=value)

    @staticmethod
    def _gen_avg_and_std_metrics(
        metrics_all: List[List[float]],
    ) -> Tuple[List[float], List[float]]:
        avg_metrics = np.mean(metrics_all, axis=0)
        std_metrics = np.std(metrics_all, axis=0)
        return list(avg_metrics), list(std_metrics)

    def _fit(self, dataset: Union[pd.DataFrame, DataFrame]) -> "CrossValidatorModel":
        if isinstance(dataset, pd.DataFrame):
            # TODO: support pandas dataframe fitting
            raise NotImplementedError("Fitting pandas dataframe is not supported yet.")

        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        eva = self.getOrDefault(self.evaluator)
        nFolds = self.getOrDefault(self.numFolds)
        metrics_all = [[0.0] * numModels for i in range(nFolds)]

        pool = ThreadPool(processes=min(self.getParallelism(), numModels))

        datasets = self._kFold(dataset)
        for i in range(nFolds):
            validation = datasets[i][1].cache()
            train = datasets[i][0].cache()

            tasks = _parallelFitTasks(est, train, eva, validation, epm)
            if not is_remote():
                tasks = list(map(inheritable_thread_target, tasks))

            for j, metric in pool.imap_unordered(lambda f: f(), tasks):
                metrics_all[i][j] = metric

            validation.unpersist()
            train.unpersist()

        metrics, std_metrics = CrossValidator._gen_avg_and_std_metrics(metrics_all)

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)
        bestModel = cast(Model, est.fit(dataset, epm[bestIndex]))
        cv_model = self._copyValues(
            CrossValidatorModel(
                bestModel,
                avgMetrics=metrics,
                stdMetrics=std_metrics,
            )
        )
        cv_model._resetUid(self.uid)
        return cv_model

    def _kFold(self, dataset: DataFrame) -> List[Tuple[DataFrame, DataFrame]]:
        nFolds = self.getOrDefault(self.numFolds)
        foldCol = self.getOrDefault(self.foldCol)

        datasets = []
        if not foldCol:
            # Do random k-fold split.
            seed = self.getOrDefault(self.seed)
            h = 1.0 / nFolds
            randCol = self.uid + "_rand"
            df = dataset.select("*", rand(seed).alias(randCol))
            for i in range(nFolds):
                validateLB = i * h
                validateUB = (i + 1) * h
                condition = (df[randCol] >= validateLB) & (df[randCol] < validateUB)
                validation = df.filter(condition)
                train = df.filter(~condition)
                datasets.append((train, validation))
        else:
            # TODO:
            #  Add verification that foldCol column values are in range [0, nFolds)
            for i in range(nFolds):
                training = dataset.filter(col(foldCol) != lit(i))
                validation = dataset.filter(col(foldCol) == lit(i))
                if training.isEmpty():
                    raise ValueError("The training data at fold %s is empty." % i)
                if validation.isEmpty():
                    raise ValueError("The validation data at fold %s is empty." % i)
                datasets.append((training, validation))

        return datasets

    def copy(self, extra: Optional["ParamMap"] = None) -> "CrossValidator":
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies creates a deep copy of
        the embedded paramMap, and copies the embedded and extra parameters over.


        .. versionadded:: 3.5.0

        Parameters
        ----------
        extra : dict, optional
            Extra parameters to copy to the new instance

        Returns
        -------
        :py:class:`CrossValidator`
            Copy of this instance
        """
        if extra is None:
            extra = dict()
        newCV = Params.copy(self, extra)
        if self.isSet(self.estimator):
            newCV.setEstimator(self.getEstimator().copy(extra))
        # estimatorParamMaps remain the same
        if self.isSet(self.evaluator):
            newCV.setEvaluator(self.getEvaluator().copy(extra))
        return newCV


class CrossValidatorModel(Model, _CrossValidatorParams, _CrossValidatorReadWrite):
    """
    CrossValidatorModel contains the model with the highest average cross-validation
    metric across folds and uses this model to transform input data. CrossValidatorModel
    also tracks the metrics for each param map evaluated.

    .. versionadded:: 3.5.0
    """

    def __init__(
        self,
        bestModel: Optional[Model] = None,
        avgMetrics: Optional[List[float]] = None,
        stdMetrics: Optional[List[float]] = None,
    ) -> None:
        super(CrossValidatorModel, self).__init__()
        #: best model from cross validation
        self.bestModel = bestModel
        #: Average cross-validation metrics for each paramMap in
        #: CrossValidator.estimatorParamMaps, in the corresponding order.
        self.avgMetrics = avgMetrics or []
        #: standard deviation of metrics for each paramMap in
        #: CrossValidator.estimatorParamMaps, in the corresponding order.
        self.stdMetrics = stdMetrics or []

    def _transform(self, dataset: Union[DataFrame, pd.DataFrame]) -> Union[DataFrame, pd.DataFrame]:
        return self.bestModel.transform(dataset)

    def copy(self, extra: Optional["ParamMap"] = None) -> "CrossValidatorModel":
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies the underlying bestModel,
        creates a deep copy of the embedded paramMap, and
        copies the embedded and extra parameters over.
        It does not copy the extra Params into the subModels.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        extra : dict, optional
            Extra parameters to copy to the new instance

        Returns
        -------
        :py:class:`CrossValidatorModel`
            Copy of this instance
        """
        if extra is None:
            extra = dict()
        bestModel = self.bestModel.copy(extra)
        avgMetrics = list(self.avgMetrics)
        stdMetrics = list(self.stdMetrics)

        return self._copyValues(
            CrossValidatorModel(bestModel, avgMetrics=avgMetrics, stdMetrics=stdMetrics),
            extra=extra,
        )
