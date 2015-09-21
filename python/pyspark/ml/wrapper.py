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
from pyspark.ml.pipeline import Estimator, Transformer, Model
from pyspark.mllib.common import inherit_doc, _java2py, _py2java


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

    #: The wrapped Java companion object. Subclasses should initialize
    #: it properly. The param values in the Java object should be
    #: synced with the Python wrapper in fit/transform/evaluate/copy.
    _java_obj = None

    @staticmethod
    def _new_java_obj(java_class, *args):
        """
        Construct a new Java object.
        """
        sc = SparkContext._active_spark_context
        java_obj = _jvm()
        for name in java_class.split("."):
            java_obj = getattr(java_obj, name)
        java_args = [_py2java(sc, arg) for arg in args]
        return java_obj(*java_args)

    def _make_java_param_pair(self, param, value):
        """
        Makes a Java parm pair.
        """
        sc = SparkContext._active_spark_context
        param = self._resolveParam(param)
        java_param = self._java_obj.getParam(param.name)
        java_value = _py2java(sc, value)
        return java_param.w(java_value)

    def _transfer_params_to_java(self):
        """
        Transforms the embedded params to the companion Java object.
        """
        paramMap = self.extractParamMap()
        for param in self.params:
            if param in paramMap:
                pair = self._make_java_param_pair(param, paramMap[param])
                self._java_obj.set(pair)

    def _transfer_params_from_java(self):
        """
        Transforms the embedded params from the companion Java object.
        """
        sc = SparkContext._active_spark_context
        for param in self.params:
            if self._java_obj.hasParam(param.name):
                java_param = self._java_obj.getParam(param.name)
                value = _java2py(sc, self._java_obj.getOrDefault(java_param))
                self._paramMap[param] = value

    @staticmethod
    def _empty_java_param_map():
        """
        Returns an empty Java ParamMap reference.
        """
        return _jvm().org.apache.spark.ml.param.ParamMap()


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
        raise NotImplementedError()

    def _fit_java(self, dataset):
        """
        Fits a Java model to the input dataset.

        :param dataset: input dataset, which is an instance of
                        :py:class:`pyspark.sql.DataFrame`
        :param params: additional params (overwriting embedded values)
        :return: fitted Java model
        """
        self._transfer_params_to_java()
        return self._java_obj.fit(dataset._jdf)

    def _fit(self, dataset):
        java_model = self._fit_java(dataset)
        return self._create_model(java_model)


@inherit_doc
class JavaTransformer(Transformer, JavaWrapper):
    """
    Base class for :py:class:`Transformer`s that wrap Java/Scala
    implementations.
    """

    __metaclass__ = ABCMeta

    def _transform(self, dataset):
        self._transfer_params_to_java()
        return DataFrame(self._java_obj.transform(dataset._jdf), dataset.sql_ctx)


@inherit_doc
class JavaModel(Model, JavaTransformer):
    """
    Base class for :py:class:`Model`s that wrap Java/Scala
    implementations. Subclasses should inherit this class before
    param mix-ins, because this sets the UID from the Java model.
    """

    __metaclass__ = ABCMeta

    def __init__(self, java_model):
        """
        Initialize this instance with a Java model object.
        Subclasses should call this constructor, initialize params,
        and then call _transformer_params_from_java.
        """
        super(JavaModel, self).__init__()
        self._java_obj = java_model
        self.uid = java_model.uid()

    def copy(self, extra=None):
        """
        Creates a copy of this instance with the same uid and some
        extra params. This implementation first calls Params.copy and
        then make a copy of the companion Java model with extra params.
        So both the Python wrapper and the Java model get copied.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        that = super(JavaModel, self).copy(extra)
        that._java_obj = self._java_obj.copy(self._empty_java_param_map())
        that._transfer_params_to_java()
        return that

    def _call_java(self, name, *args):
        m = getattr(self._java_obj, name)
        sc = SparkContext._active_spark_context
        java_args = [_py2java(sc, arg) for arg in args]
        return _java2py(sc, m(*java_args))
