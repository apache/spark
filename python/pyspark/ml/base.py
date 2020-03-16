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

import copy
import threading

from pyspark import since
from pyspark.ml.param.shared import *
from pyspark.ml.common import inherit_doc
from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, StructType


class _FitMultipleIterator(object):
    """
    Used by default implementation of Estimator.fitMultiple to produce models in a thread safe
    iterator. This class handles the simple case of fitMultiple where each param map should be
    fit independently.

    :param fitSingleModel: Function: (int => Model) which fits an estimator to a dataset.
        `fitSingleModel` may be called up to `numModels` times, with a unique index each time.
        Each call to `fitSingleModel` with an index should return the Model associated with
        that index.
    :param numModel: Number of models this iterator should produce.

    See Estimator.fitMultiple for more info.
    """
    def __init__(self, fitSingleModel, numModels):
        """

        """
        self.fitSingleModel = fitSingleModel
        self.numModel = numModels
        self.counter = 0
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            index = self.counter
            if index >= self.numModel:
                raise StopIteration("No models remaining.")
            self.counter += 1
        return index, self.fitSingleModel(index)

    def next(self):
        """For python2 compatibility."""
        return self.__next__()


@inherit_doc
class Estimator(Params):
    """
    Abstract class for estimators that fit models to data.

    .. versionadded:: 1.3.0
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def _fit(self, dataset):
        """
        Fits a model to the input dataset. This is called by the default implementation of fit.

        :param dataset: input dataset, which is an instance of :py:class:`pyspark.sql.DataFrame`
        :returns: fitted model
        """
        raise NotImplementedError()

    @since("2.3.0")
    def fitMultiple(self, dataset, paramMaps):
        """
        Fits a model to the input dataset for each param map in `paramMaps`.

        :param dataset: input dataset, which is an instance of :py:class:`pyspark.sql.DataFrame`.
        :param paramMaps: A Sequence of param maps.
        :return: A thread safe iterable which contains one model for each param map. Each
                 call to `next(modelIterator)` will return `(index, model)` where model was fit
                 using `paramMaps[index]`. `index` values may not be sequential.
        """
        estimator = self.copy()

        def fitSingleModel(index):
            return estimator.fit(dataset, paramMaps[index])

        return _FitMultipleIterator(fitSingleModel, len(paramMaps))

    @since("1.3.0")
    def fit(self, dataset, params=None):
        """
        Fits a model to the input dataset with optional parameters.

        :param dataset: input dataset, which is an instance of :py:class:`pyspark.sql.DataFrame`
        :param params: an optional param map that overrides embedded params. If a list/tuple of
                       param maps is given, this calls fit on each param map and returns a list of
                       models.
        :returns: fitted model(s)
        """
        if params is None:
            params = dict()
        if isinstance(params, (list, tuple)):
            models = [None] * len(params)
            for index, model in self.fitMultiple(dataset, params):
                models[index] = model
            return models
        elif isinstance(params, dict):
            if params:
                return self.copy(params)._fit(dataset)
            else:
                return self._fit(dataset)
        else:
            raise ValueError("Params must be either a param map or a list/tuple of param maps, "
                             "but got %s." % type(params))


@inherit_doc
class Transformer(Params):
    """
    Abstract class for transformers that transform one dataset into another.

    .. versionadded:: 1.3.0
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def _transform(self, dataset):
        """
        Transforms the input dataset.

        :param dataset: input dataset, which is an instance of :py:class:`pyspark.sql.DataFrame`
        :returns: transformed dataset
        """
        raise NotImplementedError()

    @since("1.3.0")
    def transform(self, dataset, params=None):
        """
        Transforms the input dataset with optional parameters.

        :param dataset: input dataset, which is an instance of :py:class:`pyspark.sql.DataFrame`
        :param params: an optional param map that overrides embedded params.
        :returns: transformed dataset
        """
        if params is None:
            params = dict()
        if isinstance(params, dict):
            if params:
                return self.copy(params)._transform(dataset)
            else:
                return self._transform(dataset)
        else:
            raise ValueError("Params must be a param map but got %s." % type(params))


@inherit_doc
class Model(Transformer):
    """
    Abstract class for models that are fitted by estimators.

    .. versionadded:: 1.4.0
    """

    __metaclass__ = ABCMeta


@inherit_doc
class UnaryTransformer(HasInputCol, HasOutputCol, Transformer):
    """
    Abstract class for transformers that take one input column, apply transformation,
    and output the result as a new column.

    .. versionadded:: 2.3.0
    """

    def setInputCol(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @abstractmethod
    def createTransformFunc(self):
        """
        Creates the transform function using the given param map. The input param map already takes
        account of the embedded param map. So the param values should be determined
        solely by the input param map.
        """
        raise NotImplementedError()

    @abstractmethod
    def outputDataType(self):
        """
        Returns the data type of the output column.
        """
        raise NotImplementedError()

    @abstractmethod
    def validateInputType(self, inputType):
        """
        Validates the input type. Throw an exception if it is invalid.
        """
        raise NotImplementedError()

    def transformSchema(self, schema):
        inputType = schema[self.getInputCol()].dataType
        self.validateInputType(inputType)
        if self.getOutputCol() in schema.names:
            raise ValueError("Output column %s already exists." % self.getOutputCol())
        outputFields = copy.copy(schema.fields)
        outputFields.append(StructField(self.getOutputCol(),
                                        self.outputDataType(),
                                        nullable=False))
        return StructType(outputFields)

    def _transform(self, dataset):
        self.transformSchema(dataset.schema)
        transformUDF = udf(self.createTransformFunc(), self.outputDataType())
        transformedDataset = dataset.withColumn(self.getOutputCol(),
                                                transformUDF(dataset[self.getInputCol()]))
        return transformedDataset
