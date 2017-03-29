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
import sys
if sys.version >= '3':
    xrange = range

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.ml import Estimator, Transformer, Model
from pyspark.ml.param import Params
from pyspark.ml.util import _jvm
from pyspark.ml.common import inherit_doc, _java2py, _py2java


class JavaWrapper(object):
    """
    Wrapper class for a Java companion object
    """
    def __init__(self, java_obj=None):
        super(JavaWrapper, self).__init__()
        self._java_obj = java_obj

    @classmethod
    def _create_from_java_class(cls, java_class, *args):
        """
        Construct this object from given Java classname and arguments
        """
        java_obj = JavaWrapper._new_java_obj(java_class, *args)
        return cls(java_obj)

    def _call_java(self, name, *args):
        m = getattr(self._java_obj, name)
        sc = SparkContext._active_spark_context
        java_args = [_py2java(sc, arg) for arg in args]
        return _java2py(sc, m(*java_args))

    @staticmethod
    def _new_java_obj(java_class, *args):
        """
        Returns a new Java object.
        """
        sc = SparkContext._active_spark_context
        java_obj = _jvm()
        for name in java_class.split("."):
            java_obj = getattr(java_obj, name)
        java_args = [_py2java(sc, arg) for arg in args]
        return java_obj(*java_args)

    @staticmethod
    def _new_java_array(pylist, java_class):
        """
        Create a Java array of given java_class type. Useful for
        calling a method with a Scala Array from Python with Py4J.

        :param pylist:
          Python list to convert to a Java Array.
        :param java_class:
          Java class to specify the type of Array. Should be in the
          form of sc._gateway.jvm.* (sc is a valid Spark Context).
        :return:
          Java Array of converted pylist.

        Example primitive Java classes:
          - basestring -> sc._gateway.jvm.java.lang.String
          - int -> sc._gateway.jvm.java.lang.Integer
          - float -> sc._gateway.jvm.java.lang.Double
          - bool -> sc._gateway.jvm.java.lang.Boolean
        """
        sc = SparkContext._active_spark_context
        java_array = sc._gateway.new_array(java_class, len(pylist))
        for i in xrange(len(pylist)):
            java_array[i] = pylist[i]
        return java_array


@inherit_doc
class JavaParams(JavaWrapper, Params):
    """
    Utility class to help create wrapper classes from Java/Scala
    implementations of pipeline components.
    """
    #: The param values in the Java object should be
    #: synced with the Python wrapper in fit/transform/evaluate/copy.

    __metaclass__ = ABCMeta

    def __del__(self):
        if SparkContext._active_spark_context:
            SparkContext._active_spark_context._gateway.detach(self._java_obj)

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

    def _transfer_param_map_to_java(self, pyParamMap):
        """
        Transforms a Python ParamMap into a Java ParamMap.
        """
        paramMap = JavaWrapper._new_java_obj("org.apache.spark.ml.param.ParamMap")
        for param in self.params:
            if param in pyParamMap:
                pair = self._make_java_param_pair(param, pyParamMap[param])
                paramMap.put([pair])
        return paramMap

    def _transfer_params_from_java(self):
        """
        Transforms the embedded params from the companion Java object.
        """
        sc = SparkContext._active_spark_context
        for param in self.params:
            if self._java_obj.hasParam(param.name):
                java_param = self._java_obj.getParam(param.name)
                # SPARK-14931: Only check set params back to avoid default params mismatch.
                if self._java_obj.isSet(java_param):
                    value = _java2py(sc, self._java_obj.getOrDefault(java_param))
                    self._set(**{param.name: value})

    def _transfer_param_map_from_java(self, javaParamMap):
        """
        Transforms a Java ParamMap into a Python ParamMap.
        """
        sc = SparkContext._active_spark_context
        paramMap = dict()
        for pair in javaParamMap.toList():
            param = pair.param()
            if self.hasParam(str(param.name())):
                paramMap[self.getParam(param.name())] = _java2py(sc, pair.value())
        return paramMap

    @staticmethod
    def _empty_java_param_map():
        """
        Returns an empty Java ParamMap reference.
        """
        return _jvm().org.apache.spark.ml.param.ParamMap()

    def _to_java(self):
        """
        Transfer this instance's Params to the wrapped Java object, and return the Java object.
        Used for ML persistence.

        Meta-algorithms such as Pipeline should override this method.

        :return: Java object equivalent to this instance.
        """
        self._transfer_params_to_java()
        return self._java_obj

    @staticmethod
    def _from_java(java_stage):
        """
        Given a Java object, create and return a Python wrapper of it.
        Used for ML persistence.

        Meta-algorithms such as Pipeline should override this method as a classmethod.
        """
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
        py_type = __get_class(stage_name)
        if issubclass(py_type, JavaParams):
            # Load information from java_stage to the instance.
            py_stage = py_type()
            py_stage._java_obj = java_stage
            py_stage._resetUid(java_stage.uid())
            py_stage._transfer_params_from_java()
        elif hasattr(py_type, "_from_java"):
            py_stage = py_type._from_java(java_stage)
        else:
            raise NotImplementedError("This Java stage cannot be loaded into Python currently: %r"
                                      % stage_name)
        return py_stage

    def copy(self, extra=None):
        """
        Creates a copy of this instance with the same uid and some
        extra params. This implementation first calls Params.copy and
        then make a copy of the companion Java pipeline component with
        extra params. So both the Python wrapper and the Java pipeline
        component get copied.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        that = super(JavaParams, self).copy(extra)
        if self._java_obj is not None:
            that._java_obj = self._java_obj.copy(self._empty_java_param_map())
            that._transfer_params_to_java()
        return that


@inherit_doc
class JavaEstimator(JavaParams, Estimator):
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
class JavaTransformer(JavaParams, Transformer):
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
class JavaModel(JavaTransformer, Model):
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
        and then call _transfer_params_from_java.

        This instance can be instantiated without specifying java_model,
        it will be assigned after that, but this scenario only used by
        :py:class:`JavaMLReader` to load models.  This is a bit of a
        hack, but it is easiest since a proper fix would require
        MLReader (in pyspark.ml.util) to depend on these wrappers, but
        these wrappers depend on pyspark.ml.util (both directly and via
        other ML classes).
        """
        super(JavaModel, self).__init__(java_model)
        if java_model is not None:
            self._resetUid(java_model.uid())
