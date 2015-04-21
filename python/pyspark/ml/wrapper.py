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

from abc import ABCMeta

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.ml.param import Params
from pyspark.ml.pipeline import Estimator, Transformer
from pyspark.mllib.common import inherit_doc


def _jvm():
    """
    Returns the JVM view associated with SparkContext. Must be called
    after SparkContext is initialized.
    """
    jvm = SparkContext._jvm
    if jvm:
        return jvm
    else:
        raise AttributeError("Cannot load _jvm from SparkContext. Is SparkContext initialized?")


@inherit_doc
class JavaWrapper(Params):
    """
    Utility class to help create wrapper classes from Java/Scala
    implementations of pipeline components.
    """

    __metaclass__ = ABCMeta

    #: Fully-qualified class name of the wrapped Java component.
    _java_class = None

    def _java_obj(self):
        """
        Returns or creates a Java object.
        """
        java_obj = _jvm()
        for name in self._java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj()

    def _transfer_params_to_java(self, params, java_obj):
        """
        Transforms the embedded params and additional params to the
        input Java object.
        :param params: additional params (overwriting embedded values)
        :param java_obj: Java object to receive the params
        """
        paramMap = self.extractParamMap(params)
        for param in self.params:
            if param in paramMap:
                java_obj.set(param.name, paramMap[param])

    def _empty_java_param_map(self):
        """
        Returns an empty Java ParamMap reference.
        """
        return _jvm().org.apache.spark.ml.param.ParamMap()

    def _create_java_param_map(self, params, java_obj):
        paramMap = self._empty_java_param_map()
        for param, value in params.items():
            if param.parent is self:
                paramMap.put(java_obj.getParam(param.name), value)
        return paramMap


@inherit_doc
class JavaEstimator(Estimator, JavaWrapper):
    """
    Base class for :py:class:`Estimator`s that wrap Java/Scala
    implementations.
    """

    __metaclass__ = ABCMeta

    def _create_model(self, java_model):
        """
        Creates a model from the input Java model reference.
        """
        return JavaModel(java_model)

    def _fit_java(self, dataset, params={}):
        """
        Fits a Java model to the input dataset.
        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.DataFrame`
        :param params: additional params (overwriting embedded values)
        :return: fitted Java model
        """
        java_obj = self._java_obj()
        self._transfer_params_to_java(params, java_obj)
        return java_obj.fit(dataset._jdf, self._empty_java_param_map())

    def fit(self, dataset, params={}):
        java_model = self._fit_java(dataset, params)
        return self._create_model(java_model)


@inherit_doc
class JavaTransformer(Transformer, JavaWrapper):
    """
    Base class for :py:class:`Transformer`s that wrap Java/Scala
    implementations.
    """

    __metaclass__ = ABCMeta

    def transform(self, dataset, params={}):
        java_obj = self._java_obj()
        self._transfer_params_to_java({}, java_obj)
        java_param_map = self._create_java_param_map(params, java_obj)
        return DataFrame(java_obj.transform(dataset._jdf, java_param_map),
                         dataset.sql_ctx)


@inherit_doc
class JavaModel(JavaTransformer):
    """
    Base class for :py:class:`Model`s that wrap Java/Scala
    implementations.
    """

    __metaclass__ = ABCMeta

    def __init__(self, java_model):
        super(JavaTransformer, self).__init__()
        self._java_model = java_model

    def _java_obj(self):
        return self._java_model
