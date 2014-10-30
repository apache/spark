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

import py4j.protocol
from py4j.protocol import Py4JJavaError
from py4j.java_gateway import JavaObject
from py4j.java_collections import MapConverter, ListConverter, JavaArray, JavaList

from pyspark import RDD, SparkContext
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer


# Hack for support float('inf') in Py4j
_old_smart_decode = py4j.protocol.smart_decode

_float_str_mapping = {
    'nan': 'NaN',
    'inf': 'Infinity',
    '-inf': '-Infinity',
}


def _new_smart_decode(obj):
    if isinstance(obj, float):
        s = unicode(obj)
        return _float_str_mapping.get(s, s)
    return _old_smart_decode(obj)

py4j.protocol.smart_decode = _new_smart_decode


_picklable_classes = [
    'LinkedList',
    'SparseVector',
    'DenseVector',
    'DenseMatrix',
    'Rating',
    'LabeledPoint',
]


# this will call the MLlib version of pythonToJava()
def _to_java_object_rdd(rdd, cache=False):
    """ Return an JavaRDD of Object by unpickling

    It will convert each Python object into Java object by Pyrolite, whenever the
    RDD is serialized in batch or not.
    """
    rdd = rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
    if cache:
        rdd.cache()
    return rdd.ctx._jvm.SerDe.pythonToJava(rdd._jrdd, True)


def _py2java(sc, obj):
    """ Convert Python object into Java """
    if isinstance(obj, RDD):
        obj = _to_java_object_rdd(obj)
    elif isinstance(obj, SparkContext):
        obj = obj._jsc
    elif isinstance(obj, dict):
        obj = MapConverter().convert(obj, sc._gateway._gateway_client)
    elif isinstance(obj, (list, tuple)):
        obj = ListConverter().convert(obj, sc._gateway._gateway_client)
    elif isinstance(obj, JavaObject):
        pass
    elif isinstance(obj, (int, long, float, bool, basestring)):
        pass
    else:
        bytes = bytearray(PickleSerializer().dumps(obj))
        obj = sc._jvm.SerDe.loads(bytes)
    return obj


def _java2py(sc, r):
    if isinstance(r, JavaObject):
        clsName = r.getClass().getSimpleName()
        # convert RDD into JavaRDD
        if clsName != 'JavaRDD' and clsName.endswith("RDD"):
            r = r.toJavaRDD()
            clsName = 'JavaRDD'

        if clsName == 'JavaRDD':
            jrdd = sc._jvm.SerDe.javaToPython(r)
            return RDD(jrdd, sc, AutoBatchedSerializer(PickleSerializer()))

        elif isinstance(r, (JavaArray, JavaList)) or clsName in _picklable_classes:
            r = sc._jvm.SerDe.dumps(r)

    if isinstance(r, bytearray):
        r = PickleSerializer().loads(str(r))
    return r


def callJavaFunc(sc, func, *args):
    """ Call Java Function """
    args = [_py2java(sc, a) for a in args]
    return _java2py(sc, func(*args))


def callMLlibFunc(name, *args):
    """ Call API in PythonMLLibAPI """
    sc = SparkContext._active_spark_context
    api = getattr(sc._jvm.PythonMLLibAPI(), name)
    return callJavaFunc(sc, api, *args)


class JavaModelWrapper(object):
    """
    Wrapper for the model in JVM
    """
    def __init__(self, java_model):
        self._sc = SparkContext._active_spark_context
        self._java_model = java_model

    def __del__(self):
        self._sc._gateway.detach(self._java_model)

    def call(self, name, *a):
        """Call method of java_model"""
        return callJavaFunc(self._sc, getattr(self._java_model, name), *a)
