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
import itertools
import sys
from multiprocessing.pool import ThreadPool

import numpy as np

from pyspark import since, keyword_only
from pyspark.ml import Estimator, Model
from pyspark.ml.common import _py2java, _java2py
from pyspark.ml.param import Params, Param, TypeConverters
from pyspark.ml.param.shared import HasCollectSubModels, HasParallelism, HasSeed
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaParams
from pyspark.sql.functions import rand

__all__ = ['ParamGridBuilder', 'CrossValidator', 'CrossValidatorModel', 'TrainValidationSplit',
           'TrainValidationSplitModel']


def _parallelFitTasks(est, train, eva, validation, epm, collectSubModel):
    """
    Creates a list of callables which can be called from different threads to fit and evaluate
    an estimator in parallel. Each callable returns an `(index, metric)` pair.

    :param est: Estimator, the estimator to be fit.
    :param train: DataFrame, training data set, used for fitting.
    :param eva: Evaluator, used to compute `metric`
    :param validation: DataFrame, validation data set, used for evaluation.
    :param epm: Sequence of ParamMap, params maps to be used during fitting & evaluation.
    :param collectSubModel: Whether to collect sub model.
    :return: (int, float, subModel), an index into `epm` and the associated metric value.
    """
    modelIter = est.fitMultiple(train, epm)

    def singleTask():
        index, model = next(modelIter)
        metric = eva.evaluate(model.transform(validation, epm[index]))
        return index, metric, model if collectSubModel else None

    return [singleTask] * len(epm)


class ParamGridBuilder(object):
    r"""
    Builder for a param grid used in grid search-based model selection.

    >>> from pyspark.ml.classification import LogisticRegression
    >>> lr = LogisticRegression()
    >>> output = ParamGridBuilder() \
    ...     .baseOn({lr.labelCol: 'l'}) \
    ...     .baseOn([lr.predictionCol, 'p']) \
    ...     .addGrid(lr.regParam, [1.0, 2.0]) \
    ...     .addGrid(lr.maxIter, [1, 5]) \
    ...     .build()
    >>> expected = [
    ...     {lr.regParam: 1.0, lr.maxIter: 1, lr.labelCol: 'l', lr.predictionCol: 'p'},
    ...     {lr.regParam: 2.0, lr.maxIter: 1, lr.labelCol: 'l', lr.predictionCol: 'p'},
    ...     {lr.regParam: 1.0, lr.maxIter: 5, lr.labelCol: 'l', lr.predictionCol: 'p'},
    ...     {lr.regParam: 2.0, lr.maxIter: 5, lr.labelCol: 'l', lr.predictionCol: 'p'}]
    >>> len(output) == len(expected)
    True
    >>> all([m in expected for m in output])
    True

    .. versionadded:: 1.4.0
    """

    def __init__(self):
        self._param_grid = {}

    @since("1.4.0")
    def addGrid(self, param, values):
        """
        Sets the given parameters in this grid to fixed values.

        param must be an instance of Param associated with an instance of Params
        (such as Estimator or Transformer).
        """
        if isinstance(param, Param):
            self._param_grid[param] = values
        else:
            raise TypeError("param must be an instance of Param")

        return self

    @since("1.4.0")
    def baseOn(self, *args):
        """
        Sets the given parameters in this grid to fixed values.
        Accepts either a parameter dictionary or a list of (parameter, value) pairs.
        """
        if isinstance(args[0], dict):
            self.baseOn(*args[0].items())
        else:
            for (param, value) in args:
                self.addGrid(param, [value])

        return self

    @since("1.4.0")
    def build(self):
        """
        Builds and returns all combinations of parameters specified
        by the param grid.
        """
        keys = self._param_grid.keys()
        grid_values = self._param_grid.values()

        def to_key_value_pairs(keys, values):
            return [(key, key.typeConverter(value)) for key, value in zip(keys, values)]

        return [dict(to_key_value_pairs(keys, prod)) for prod in itertools.product(*grid_values)]


class _ValidatorParams(HasSeed):
    """
    Common params for TrainValidationSplit and CrossValidator.
    """

    estimator = Param(Params._dummy(), "estimator", "estimator to be cross-validated")
    estimatorParamMaps = Param(Params._dummy(), "estimatorParamMaps", "estimator param maps")
    evaluator = Param(
        Params._dummy(), "evaluator",
        "evaluator used to select hyper-parameters that maximize the validator metric")

    @since("2.0.0")
    def getEstimator(self):
        """
        Gets the value of estimator or its default value.
        """
        return self.getOrDefault(self.estimator)

    @since("2.0.0")
    def getEstimatorParamMaps(self):
        """
        Gets the value of estimatorParamMaps or its default value.
        """
        return self.getOrDefault(self.estimatorParamMaps)

    @since("2.0.0")
    def getEvaluator(self):
        """
        Gets the value of evaluator or its default value.
        """
        return self.getOrDefault(self.evaluator)

    @classmethod
    def _from_java_impl(cls, java_stage):
        """
        Return Python estimator, estimatorParamMaps, and evaluator from a Java ValidatorParams.
        """

        # Load information from java_stage to the instance.
        estimator = JavaParams._from_java(java_stage.getEstimator())
        evaluator = JavaParams._from_java(java_stage.getEvaluator())
        epms = [estimator._transfer_param_map_from_java(epm)
                for epm in java_stage.getEstimatorParamMaps()]
        return estimator, epms, evaluator

    def _to_java_impl(self):
        """
        Return Java estimator, estimatorParamMaps, and evaluator from this Python instance.
        """

        gateway = SparkContext._gateway
        cls = SparkContext._jvm.org.apache.spark.ml.param.ParamMap

        java_epms = gateway.new_array(cls, len(self.getEstimatorParamMaps()))
        for idx, epm in enumerate(self.getEstimatorParamMaps()):
            java_epms[idx] = self.getEstimator()._transfer_param_map_to_java(epm)

        java_estimator = self.getEstimator()._to_java()
        java_evaluator = self.getEvaluator()._to_java()
        return java_estimator, java_epms, java_evaluator


class _CrossValidatorParams(_ValidatorParams):
    """
    Params for :py:class:`CrossValidator` and :py:class:`CrossValidatorModel`.

    .. versionadded:: 3.0.0
    """

    numFolds = Param(Params._dummy(), "numFolds", "number of folds for cross validation",
                     typeConverter=TypeConverters.toInt)

    @since("1.4.0")
    def getNumFolds(self):
        """
        Gets the value of numFolds or its default value.
        """
        return self.getOrDefault(self.numFolds)


class CrossValidator(Estimator, _CrossValidatorParams, HasParallelism, HasCollectSubModels,
                     MLReadable, MLWritable):
    """

    K-fold cross validation performs model selection by splitting the dataset into a set of
    non-overlapping randomly partitioned folds which are used as separate training and test datasets
    e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs,
    each of which uses 2/3 of the data for training and 1/3 for testing. Each fold is used as the
    test set exactly once.


    >>> from pyspark.ml.classification import LogisticRegression
    >>> from pyspark.ml.evaluation import BinaryClassificationEvaluator
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.tuning import CrossValidatorModel
    >>> import tempfile
    >>> dataset = spark.createDataFrame(
    ...     [(Vectors.dense([0.0]), 0.0),
    ...      (Vectors.dense([0.4]), 1.0),
    ...      (Vectors.dense([0.5]), 0.0),
    ...      (Vectors.dense([0.6]), 1.0),
    ...      (Vectors.dense([1.0]), 1.0)] * 10,
    ...     ["features", "label"])
    >>> lr = LogisticRegression()
    >>> grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
    >>> evaluator = BinaryClassificationEvaluator()
    >>> cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator,
    ...     parallelism=2)
    >>> cvModel = cv.fit(dataset)
    >>> cvModel.getNumFolds()
    3
    >>> cvModel.avgMetrics[0]
    0.5
    >>> path = tempfile.mkdtemp()
    >>> model_path = path + "/model"
    >>> cvModel.write().save(model_path)
    >>> cvModelRead = CrossValidatorModel.read().load(model_path)
    >>> cvModelRead.avgMetrics
    [0.5, ...
    >>> evaluator.evaluate(cvModel.transform(dataset))
    0.8333...

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,
                 seed=None, parallelism=1, collectSubModels=False):
        """
        __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                 seed=None, parallelism=1, collectSubModels=False)
        """
        super(CrossValidator, self).__init__()
        self._setDefault(numFolds=3, parallelism=1)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,
                  seed=None, parallelism=1, collectSubModels=False):
        """
        setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                  seed=None, parallelism=1, collectSubModels=False):
        Sets params for cross validator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setEstimator(self, value):
        """
        Sets the value of :py:attr:`estimator`.
        """
        return self._set(estimator=value)

    @since("2.0.0")
    def setEstimatorParamMaps(self, value):
        """
        Sets the value of :py:attr:`estimatorParamMaps`.
        """
        return self._set(estimatorParamMaps=value)

    @since("2.0.0")
    def setEvaluator(self, value):
        """
        Sets the value of :py:attr:`evaluator`.
        """
        return self._set(evaluator=value)

    @since("1.4.0")
    def setNumFolds(self, value):
        """
        Sets the value of :py:attr:`numFolds`.
        """
        return self._set(numFolds=value)

    def setSeed(self, value):
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def setParallelism(self, value):
        """
        Sets the value of :py:attr:`parallelism`.
        """
        return self._set(parallelism=value)

    def setCollectSubModels(self, value):
        """
        Sets the value of :py:attr:`collectSubModels`.
        """
        return self._set(collectSubModels=value)

    def _fit(self, dataset):
        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        eva = self.getOrDefault(self.evaluator)
        nFolds = self.getOrDefault(self.numFolds)
        seed = self.getOrDefault(self.seed)
        h = 1.0 / nFolds
        randCol = self.uid + "_rand"
        df = dataset.select("*", rand(seed).alias(randCol))
        metrics = [0.0] * numModels

        pool = ThreadPool(processes=min(self.getParallelism(), numModels))
        subModels = None
        collectSubModelsParam = self.getCollectSubModels()
        if collectSubModelsParam:
            subModels = [[None for j in range(numModels)] for i in range(nFolds)]

        for i in range(nFolds):
            validateLB = i * h
            validateUB = (i + 1) * h
            condition = (df[randCol] >= validateLB) & (df[randCol] < validateUB)
            validation = df.filter(condition).cache()
            train = df.filter(~condition).cache()

            tasks = _parallelFitTasks(est, train, eva, validation, epm, collectSubModelsParam)
            for j, metric, subModel in pool.imap_unordered(lambda f: f(), tasks):
                metrics[j] += (metric / nFolds)
                if collectSubModelsParam:
                    subModels[i][j] = subModel

            validation.unpersist()
            train.unpersist()

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)
        bestModel = est.fit(dataset, epm[bestIndex])
        return self._copyValues(CrossValidatorModel(bestModel, metrics, subModels))

    @since("1.4.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies creates a deep copy of
        the embedded paramMap, and copies the embedded and extra parameters over.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
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

    @since("2.3.0")
    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        return JavaMLWriter(self)

    @classmethod
    @since("2.3.0")
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaMLReader(cls)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java CrossValidator, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        estimator, epms, evaluator = super(CrossValidator, cls)._from_java_impl(java_stage)
        numFolds = java_stage.getNumFolds()
        seed = java_stage.getSeed()
        parallelism = java_stage.getParallelism()
        collectSubModels = java_stage.getCollectSubModels()
        # Create a new instance of this stage.
        py_stage = cls(estimator=estimator, estimatorParamMaps=epms, evaluator=evaluator,
                       numFolds=numFolds, seed=seed, parallelism=parallelism,
                       collectSubModels=collectSubModels)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java CrossValidator. Used for ML persistence.

        :return: Java object equivalent to this instance.
        """

        estimator, epms, evaluator = super(CrossValidator, self)._to_java_impl()

        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.tuning.CrossValidator", self.uid)
        _java_obj.setEstimatorParamMaps(epms)
        _java_obj.setEvaluator(evaluator)
        _java_obj.setEstimator(estimator)
        _java_obj.setSeed(self.getSeed())
        _java_obj.setNumFolds(self.getNumFolds())
        _java_obj.setParallelism(self.getParallelism())
        _java_obj.setCollectSubModels(self.getCollectSubModels())

        return _java_obj


class CrossValidatorModel(Model, _CrossValidatorParams, MLReadable, MLWritable):
    """

    CrossValidatorModel contains the model with the highest average cross-validation
    metric across folds and uses this model to transform input data. CrossValidatorModel
    also tracks the metrics for each param map evaluated.

    .. versionadded:: 1.4.0
    """

    def __init__(self, bestModel, avgMetrics=[], subModels=None):
        super(CrossValidatorModel, self).__init__()
        #: best model from cross validation
        self.bestModel = bestModel
        #: Average cross-validation metrics for each paramMap in
        #: CrossValidator.estimatorParamMaps, in the corresponding order.
        self.avgMetrics = avgMetrics
        #: sub model list from cross validation
        self.subModels = subModels

    def _transform(self, dataset):
        return self.bestModel.transform(dataset)

    @since("1.4.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies the underlying bestModel,
        creates a deep copy of the embedded paramMap, and
        copies the embedded and extra parameters over.
        It does not copy the extra Params into the subModels.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        bestModel = self.bestModel.copy(extra)
        avgMetrics = self.avgMetrics
        subModels = self.subModels
        return CrossValidatorModel(bestModel, avgMetrics, subModels)

    @since("2.3.0")
    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        return JavaMLWriter(self)

    @classmethod
    @since("2.3.0")
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaMLReader(cls)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java CrossValidatorModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        sc = SparkContext._active_spark_context
        bestModel = JavaParams._from_java(java_stage.bestModel())
        avgMetrics = _java2py(sc, java_stage.avgMetrics())
        estimator, epms, evaluator = super(CrossValidatorModel, cls)._from_java_impl(java_stage)

        py_stage = cls(bestModel=bestModel, avgMetrics=avgMetrics)._set(estimator=estimator)
        py_stage = py_stage._set(estimatorParamMaps=epms)._set(evaluator=evaluator)

        if java_stage.hasSubModels():
            py_stage.subModels = [[JavaParams._from_java(sub_model)
                                   for sub_model in fold_sub_models]
                                  for fold_sub_models in java_stage.subModels()]

        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java CrossValidatorModel. Used for ML persistence.

        :return: Java object equivalent to this instance.
        """

        sc = SparkContext._active_spark_context
        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.tuning.CrossValidatorModel",
                                             self.uid,
                                             self.bestModel._to_java(),
                                             _py2java(sc, self.avgMetrics))
        estimator, epms, evaluator = super(CrossValidatorModel, self)._to_java_impl()

        _java_obj.set("evaluator", evaluator)
        _java_obj.set("estimator", estimator)
        _java_obj.set("estimatorParamMaps", epms)

        if self.subModels is not None:
            java_sub_models = [[sub_model._to_java() for sub_model in fold_sub_models]
                               for fold_sub_models in self.subModels]
            _java_obj.setSubModels(java_sub_models)
        return _java_obj


class _TrainValidationSplitParams(_ValidatorParams):
    """
    Params for :py:class:`TrainValidationSplit` and :py:class:`TrainValidationSplitModel`.

    .. versionadded:: 3.0.0
    """

    trainRatio = Param(Params._dummy(), "trainRatio", "Param for ratio between train and\
     validation data. Must be between 0 and 1.", typeConverter=TypeConverters.toFloat)

    @since("2.0.0")
    def getTrainRatio(self):
        """
        Gets the value of trainRatio or its default value.
        """
        return self.getOrDefault(self.trainRatio)


class TrainValidationSplit(Estimator, _TrainValidationSplitParams, HasParallelism,
                           HasCollectSubModels, MLReadable, MLWritable):
    """
    Validation for hyper-parameter tuning. Randomly splits the input dataset into train and
    validation sets, and uses evaluation metric on the validation set to select the best model.
    Similar to :class:`CrossValidator`, but only splits the set once.

    >>> from pyspark.ml.classification import LogisticRegression
    >>> from pyspark.ml.evaluation import BinaryClassificationEvaluator
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.tuning import TrainValidationSplitModel
    >>> import tempfile
    >>> dataset = spark.createDataFrame(
    ...     [(Vectors.dense([0.0]), 0.0),
    ...      (Vectors.dense([0.4]), 1.0),
    ...      (Vectors.dense([0.5]), 0.0),
    ...      (Vectors.dense([0.6]), 1.0),
    ...      (Vectors.dense([1.0]), 1.0)] * 10,
    ...     ["features", "label"]).repartition(1)
    >>> lr = LogisticRegression()
    >>> grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
    >>> evaluator = BinaryClassificationEvaluator()
    >>> tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator,
    ...     parallelism=1, seed=42)
    >>> tvsModel = tvs.fit(dataset)
    >>> tvsModel.getTrainRatio()
    0.75
    >>> tvsModel.validationMetrics
    [0.5, ...
    >>> path = tempfile.mkdtemp()
    >>> model_path = path + "/model"
    >>> tvsModel.write().save(model_path)
    >>> tvsModelRead = TrainValidationSplitModel.read().load(model_path)
    >>> tvsModelRead.validationMetrics
    [0.5, ...
    >>> evaluator.evaluate(tvsModel.transform(dataset))
    0.833...

    .. versionadded:: 2.0.0
    """

    @keyword_only
    def __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, trainRatio=0.75,
                 parallelism=1, collectSubModels=False, seed=None):
        """
        __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, trainRatio=0.75,\
                 parallelism=1, collectSubModels=False, seed=None)
        """
        super(TrainValidationSplit, self).__init__()
        self._setDefault(trainRatio=0.75, parallelism=1)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("2.0.0")
    @keyword_only
    def setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, trainRatio=0.75,
                  parallelism=1, collectSubModels=False, seed=None):
        """
        setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, trainRatio=0.75,\
                  parallelism=1, collectSubModels=False, seed=None):
        Sets params for the train validation split.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setEstimator(self, value):
        """
        Sets the value of :py:attr:`estimator`.
        """
        return self._set(estimator=value)

    @since("2.0.0")
    def setEstimatorParamMaps(self, value):
        """
        Sets the value of :py:attr:`estimatorParamMaps`.
        """
        return self._set(estimatorParamMaps=value)

    @since("2.0.0")
    def setEvaluator(self, value):
        """
        Sets the value of :py:attr:`evaluator`.
        """
        return self._set(evaluator=value)

    @since("2.0.0")
    def setTrainRatio(self, value):
        """
        Sets the value of :py:attr:`trainRatio`.
        """
        return self._set(trainRatio=value)

    def setSeed(self, value):
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def setParallelism(self, value):
        """
        Sets the value of :py:attr:`parallelism`.
        """
        return self._set(parallelism=value)

    def setCollectSubModels(self, value):
        """
        Sets the value of :py:attr:`collectSubModels`.
        """
        return self._set(collectSubModels=value)

    def _fit(self, dataset):
        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        eva = self.getOrDefault(self.evaluator)
        tRatio = self.getOrDefault(self.trainRatio)
        seed = self.getOrDefault(self.seed)
        randCol = self.uid + "_rand"
        df = dataset.select("*", rand(seed).alias(randCol))
        condition = (df[randCol] >= tRatio)
        validation = df.filter(condition).cache()
        train = df.filter(~condition).cache()

        subModels = None
        collectSubModelsParam = self.getCollectSubModels()
        if collectSubModelsParam:
            subModels = [None for i in range(numModels)]

        tasks = _parallelFitTasks(est, train, eva, validation, epm, collectSubModelsParam)
        pool = ThreadPool(processes=min(self.getParallelism(), numModels))
        metrics = [None] * numModels
        for j, metric, subModel in pool.imap_unordered(lambda f: f(), tasks):
            metrics[j] = metric
            if collectSubModelsParam:
                subModels[j] = subModel

        train.unpersist()
        validation.unpersist()

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)
        bestModel = est.fit(dataset, epm[bestIndex])
        return self._copyValues(TrainValidationSplitModel(bestModel, metrics, subModels))

    @since("2.0.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies creates a deep copy of
        the embedded paramMap, and copies the embedded and extra parameters over.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        newTVS = Params.copy(self, extra)
        if self.isSet(self.estimator):
            newTVS.setEstimator(self.getEstimator().copy(extra))
        # estimatorParamMaps remain the same
        if self.isSet(self.evaluator):
            newTVS.setEvaluator(self.getEvaluator().copy(extra))
        return newTVS

    @since("2.3.0")
    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        return JavaMLWriter(self)

    @classmethod
    @since("2.3.0")
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaMLReader(cls)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java TrainValidationSplit, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        estimator, epms, evaluator = super(TrainValidationSplit, cls)._from_java_impl(java_stage)
        trainRatio = java_stage.getTrainRatio()
        seed = java_stage.getSeed()
        parallelism = java_stage.getParallelism()
        collectSubModels = java_stage.getCollectSubModels()
        # Create a new instance of this stage.
        py_stage = cls(estimator=estimator, estimatorParamMaps=epms, evaluator=evaluator,
                       trainRatio=trainRatio, seed=seed, parallelism=parallelism,
                       collectSubModels=collectSubModels)
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java TrainValidationSplit. Used for ML persistence.
        :return: Java object equivalent to this instance.
        """

        estimator, epms, evaluator = super(TrainValidationSplit, self)._to_java_impl()

        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.tuning.TrainValidationSplit",
                                             self.uid)
        _java_obj.setEstimatorParamMaps(epms)
        _java_obj.setEvaluator(evaluator)
        _java_obj.setEstimator(estimator)
        _java_obj.setTrainRatio(self.getTrainRatio())
        _java_obj.setSeed(self.getSeed())
        _java_obj.setParallelism(self.getParallelism())
        _java_obj.setCollectSubModels(self.getCollectSubModels())
        return _java_obj


class TrainValidationSplitModel(Model, _TrainValidationSplitParams, MLReadable, MLWritable):
    """
    Model from train validation split.

    .. versionadded:: 2.0.0
    """

    def __init__(self, bestModel, validationMetrics=[], subModels=None):
        super(TrainValidationSplitModel, self).__init__()
        #: best model from train validation split
        self.bestModel = bestModel
        #: evaluated validation metrics
        self.validationMetrics = validationMetrics
        #: sub models from train validation split
        self.subModels = subModels

    def _transform(self, dataset):
        return self.bestModel.transform(dataset)

    @since("2.0.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies the underlying bestModel,
        creates a deep copy of the embedded paramMap, and
        copies the embedded and extra parameters over.
        And, this creates a shallow copy of the validationMetrics.
        It does not copy the extra Params into the subModels.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        bestModel = self.bestModel.copy(extra)
        validationMetrics = list(self.validationMetrics)
        subModels = self.subModels
        return TrainValidationSplitModel(bestModel, validationMetrics, subModels)

    @since("2.3.0")
    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        return JavaMLWriter(self)

    @classmethod
    @since("2.3.0")
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaMLReader(cls)

    @classmethod
    def _from_java(cls, java_stage):
        """
        Given a Java TrainValidationSplitModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        # Load information from java_stage to the instance.
        sc = SparkContext._active_spark_context
        bestModel = JavaParams._from_java(java_stage.bestModel())
        validationMetrics = _java2py(sc, java_stage.validationMetrics())
        estimator, epms, evaluator = super(TrainValidationSplitModel,
                                           cls)._from_java_impl(java_stage)
        # Create a new instance of this stage.
        py_stage = cls(bestModel=bestModel,
                       validationMetrics=validationMetrics)._set(estimator=estimator)
        py_stage = py_stage._set(estimatorParamMaps=epms)._set(evaluator=evaluator)

        if java_stage.hasSubModels():
            py_stage.subModels = [JavaParams._from_java(sub_model)
                                  for sub_model in java_stage.subModels()]

        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self):
        """
        Transfer this instance to a Java TrainValidationSplitModel. Used for ML persistence.
        :return: Java object equivalent to this instance.
        """

        sc = SparkContext._active_spark_context
        _java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.tuning.TrainValidationSplitModel",
            self.uid,
            self.bestModel._to_java(),
            _py2java(sc, self.validationMetrics))
        estimator, epms, evaluator = super(TrainValidationSplitModel, self)._to_java_impl()

        _java_obj.set("evaluator", evaluator)
        _java_obj.set("estimator", estimator)
        _java_obj.set("estimatorParamMaps", epms)

        if self.subModels is not None:
            java_sub_models = [sub_model._to_java() for sub_model in self.subModels]
            _java_obj.setSubModels(java_sub_models)

        return _java_obj


if __name__ == "__main__":
    import doctest

    from pyspark.sql import SparkSession
    globs = globals().copy()

    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.tuning tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        sys.exit(-1)
