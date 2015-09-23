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
import numpy as np

from pyspark import since
from pyspark.ml.param import Params, Param
from pyspark.ml import Estimator, Model
from pyspark.ml.util import keyword_only
from pyspark.sql.functions import rand

__all__ = ['ParamGridBuilder', 'CrossValidator', 'CrossValidatorModel']


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
        """
        self._param_grid[param] = values

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
        return [dict(zip(keys, prod)) for prod in itertools.product(*grid_values)]


class CrossValidator(Estimator):
    """
    K-fold cross validation.

    >>> from pyspark.ml.classification import LogisticRegression
    >>> from pyspark.ml.evaluation import BinaryClassificationEvaluator
    >>> from pyspark.mllib.linalg import Vectors
    >>> dataset = sqlContext.createDataFrame(
    ...     [(Vectors.dense([0.0]), 0.0),
    ...      (Vectors.dense([0.4]), 1.0),
    ...      (Vectors.dense([0.5]), 0.0),
    ...      (Vectors.dense([0.6]), 1.0),
    ...      (Vectors.dense([1.0]), 1.0)] * 10,
    ...     ["features", "label"])
    >>> lr = LogisticRegression()
    >>> grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
    >>> evaluator = BinaryClassificationEvaluator()
    >>> cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
    >>> cvModel = cv.fit(dataset)
    >>> evaluator.evaluate(cvModel.transform(dataset))
    0.8333...

    .. versionadded:: 1.4.0
    """

    # a placeholder to make it appear in the generated doc
    estimator = Param(Params._dummy(), "estimator", "estimator to be cross-validated")

    # a placeholder to make it appear in the generated doc
    estimatorParamMaps = Param(Params._dummy(), "estimatorParamMaps", "estimator param maps")

    # a placeholder to make it appear in the generated doc
    evaluator = Param(
        Params._dummy(), "evaluator",
        "evaluator used to select hyper-parameters that maximize the cross-validated metric")

    # a placeholder to make it appear in the generated doc
    numFolds = Param(Params._dummy(), "numFolds", "number of folds for cross validation")

    @keyword_only
    def __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3):
        """
        __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3)
        """
        super(CrossValidator, self).__init__()
        #: param for estimator to be cross-validated
        self.estimator = Param(self, "estimator", "estimator to be cross-validated")
        #: param for estimator param maps
        self.estimatorParamMaps = Param(self, "estimatorParamMaps", "estimator param maps")
        #: param for the evaluator used to select hyper-parameters that
        #: maximize the cross-validated metric
        self.evaluator = Param(
            self, "evaluator",
            "evaluator used to select hyper-parameters that maximize the cross-validated metric")
        #: param for number of folds for cross validation
        self.numFolds = Param(self, "numFolds", "number of folds for cross validation")
        self._setDefault(numFolds=3)
        kwargs = self.__init__._input_kwargs
        self._set(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3):
        """
        setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3):
        Sets params for cross validator.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setEstimator(self, value):
        """
        Sets the value of :py:attr:`estimator`.
        """
        self._paramMap[self.estimator] = value
        return self

    @since("1.4.0")
    def getEstimator(self):
        """
        Gets the value of estimator or its default value.
        """
        return self.getOrDefault(self.estimator)

    @since("1.4.0")
    def setEstimatorParamMaps(self, value):
        """
        Sets the value of :py:attr:`estimatorParamMaps`.
        """
        self._paramMap[self.estimatorParamMaps] = value
        return self

    @since("1.4.0")
    def getEstimatorParamMaps(self):
        """
        Gets the value of estimatorParamMaps or its default value.
        """
        return self.getOrDefault(self.estimatorParamMaps)

    @since("1.4.0")
    def setEvaluator(self, value):
        """
        Sets the value of :py:attr:`evaluator`.
        """
        self._paramMap[self.evaluator] = value
        return self

    @since("1.4.0")
    def getEvaluator(self):
        """
        Gets the value of evaluator or its default value.
        """
        return self.getOrDefault(self.evaluator)

    @since("1.4.0")
    def setNumFolds(self, value):
        """
        Sets the value of :py:attr:`numFolds`.
        """
        self._paramMap[self.numFolds] = value
        return self

    @since("1.4.0")
    def getNumFolds(self):
        """
        Gets the value of numFolds or its default value.
        """
        return self.getOrDefault(self.numFolds)

    def _fit(self, dataset):
        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        eva = self.getOrDefault(self.evaluator)
        nFolds = self.getOrDefault(self.numFolds)
        h = 1.0 / nFolds
        randCol = self.uid + "_rand"
        df = dataset.select("*", rand(0).alias(randCol))
        metrics = np.zeros(numModels)
        for i in range(nFolds):
            validateLB = i * h
            validateUB = (i + 1) * h
            condition = (df[randCol] >= validateLB) & (df[randCol] < validateUB)
            validation = df.filter(condition)
            train = df.filter(~condition)
            for j in range(numModels):
                model = est.fit(train, epm[j])
                # TODO: duplicate evaluator to take extra params from input
                metric = eva.evaluate(model.transform(validation, epm[j]))
                metrics[j] += metric

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)
        bestModel = est.fit(dataset, epm[bestIndex])
        return CrossValidatorModel(bestModel)

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


class CrossValidatorModel(Model):
    """
    Model from k-fold cross validation.

    .. versionadded:: 1.4.0
    """

    def __init__(self, bestModel):
        super(CrossValidatorModel, self).__init__()
        #: best model from cross validation
        self.bestModel = bestModel

    def _transform(self, dataset):
        return self.bestModel.transform(dataset)

    @since("1.4.0")
    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies the underlying bestModel,
        creates a deep copy of the embedded paramMap, and
        copies the embedded and extra parameters over.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        return CrossValidatorModel(self.bestModel.copy(extra))


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.tuning tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
