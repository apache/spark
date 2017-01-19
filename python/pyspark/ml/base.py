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

from pyspark import since
from pyspark.ml.param import Params
from pyspark.ml.common import inherit_doc


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
            return [self.fit(dataset, paramMap) for paramMap in params]
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
