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

from py4j.java_collections import JavaArray, JavaList, ListConverter

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.ml import Estimator, Transformer, Model
from pyspark.ml.param import Params
from pyspark.ml.util import _jvm, JavaMLReader
from pyspark.mllib.common import inherit_doc, _java2py, _py2java, callMLlibFunc


def _param_value_py2java(sc, obj):
    from pyspark.ml.param import Params
    if isinstance(obj, Params):
        obj = _stage_py2java(obj)
    elif isinstance(obj, list):
        obj = ListConverter().\
            convert([_param_value_py2java(sc, x) for x in obj], sc._gateway._gateway_client)
        #gateway = SparkContext._gateway
        #java_stages = gateway.new_array(_jvm().java.lang.Object, len(obj))
        #for idx, stage in enumerate(obj):
        #    java_stages[idx] = _param_value_py2java(sc, stage)
        #obj = java_stages
    else:
        obj = _py2java(sc, obj)
    return obj


def _param_value_java2py(sc, r):
    if callMLlibFunc("isInstanceOfParams", r):
        r = _stage_java2py(r)
    elif isinstance(r, (JavaArray, JavaList)):
        r = [_param_value_java2py(sc, x) for x in r]
    else:
        r = _java2py(sc, r)
    return r


def _params_py2java(py_stage, java_stage):
    sc = SparkContext._active_spark_context
    paramMap = py_stage.extractParamMap()
    for param in py_stage.params:
        if param in paramMap:
            py_value = paramMap[param]
            java_param = java_stage.getParam(param.name)
            java_value = _param_value_py2java(sc, py_value)
            java_stage.set(java_param.w(java_value))


def _params_java2py(py_stage, java_stage):
    sc = SparkContext._active_spark_context
    for param in py_stage.params:
        if java_stage.hasParam(param.name):
            java_param = java_stage.getParam(param.name)
            if java_stage.isDefined(java_param):
                java_value = java_stage.getOrDefault(java_param)
                py_value = _param_value_java2py(sc, java_value)
                py_stage._paramMap[param] = py_value


def _stage_py2java(py_stage):
    if isinstance(py_stage, JavaWrapper):
        py_stage._transfer_params_to_java()
        return py_stage._java_obj
    else:
        # TODO: not runnable
        java_stage = JavaWrapper.\
            _new_java_obj(JavaMLReader._java_loader_class(py_stage.__class__), py_stage.uid)
        _params_py2java(py_stage, java_stage)
        return java_stage


def _stage_java2py(java_stage):
    def __get_class(clazz):
        """
        Loads Python class from its name.
        """
        parts = clazz.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m
    stage_name = java_stage.getClass().getName().replace("org.apache.spark", "pyspark")
    # Generate a default new instance from the stage_name class.
    # TODO: not all python classes have the constructor
    py_stage = __get_class(stage_name)()
    # Load information from java_stage to the instance.
    if isinstance(py_stage, JavaWrapper):
        py_stage._java_obj = java_stage
        py_stage._resetUid(java_stage.uid())
        py_stage._transfer_params_from_java()
    else:
        py_stage._resetUid(java_stage.uid())
        _params_java2py(py_stage, java_stage)
    return py_stage


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

    def _transfer_params_to_java(self):
        """
        Transforms the embedded params to the companion Java object.
        """
        _params_py2java(self, self._java_obj)

    def _transfer_params_from_java(self):
        """
        Transforms the embedded params from the companion Java object.
        """
        _params_java2py(self, self._java_obj)

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

    @abstractmethod
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
    implementations. Subclasses should ensure they have the transformer Java object
    available as _java_obj.
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

    def __init__(self, java_model=None):
        """
        Initialize this instance with a Java model object.
        Subclasses should call this constructor, initialize params,
        and then call _transformer_params_from_java.

        This instance can be instantiated without specifying java_model,
        it will be assigned after that, but this scenario only used by
        :py:class:`JavaMLReader` to load models.  This is a bit of a
        hack, but it is easiest since a proper fix would require
        MLReader (in pyspark.ml.util) to depend on these wrappers, but
        these wrappers depend on pyspark.ml.util (both directly and via
        other ML classes).
        """
        super(JavaModel, self).__init__()
        if java_model is not None:
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
        if self._java_obj is not None:
            that._java_obj = self._java_obj.copy(self._empty_java_param_map())
            that._transfer_params_to_java()
        return that

    def _call_java(self, name, *args):
        m = getattr(self._java_obj, name)
        sc = SparkContext._active_spark_context
        java_args = [_py2java(sc, arg) for arg in args]
        return _java2py(sc, m(*java_args))
