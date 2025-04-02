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

from typing import Any, Callable, TYPE_CHECKING

from pyspark.util import is_remote_only
from pyspark.serializers import CPickleSerializer, AutoBatchedSerializer
from pyspark.sql import DataFrame, SparkSession

if TYPE_CHECKING:
    import py4j.protocol
    from py4j.java_gateway import JavaObject

    import pyspark.core.context
    from pyspark.core.rdd import RDD
    from pyspark.core.context import SparkContext
    from pyspark.ml._typing import C, JavaObjectOrPickleDump


if not is_remote_only():
    import py4j

    # Hack for support float('inf') in Py4j
    _old_smart_decode = py4j.protocol.smart_decode

_float_str_mapping = {
    "nan": "NaN",
    "inf": "Infinity",
    "-inf": "-Infinity",
}


def _new_smart_decode(obj: Any) -> str:
    if isinstance(obj, float):
        s = str(obj)
        return _float_str_mapping.get(s, s)
    return _old_smart_decode(obj)


if not is_remote_only():
    import py4j

    py4j.protocol.smart_decode = _new_smart_decode


_picklable_classes = [
    "SparseVector",
    "DenseVector",
    "SparseMatrix",
    "DenseMatrix",
]


# this will call the ML version of pythonToJava()
def _to_java_object_rdd(rdd: "RDD") -> "JavaObject":
    """Return an JavaRDD of Object by unpickling

    It will convert each Python object into Java object by Pickle, whenever the
    RDD is serialized in batch or not.
    """
    rdd = rdd._reserialize(AutoBatchedSerializer(CPickleSerializer()))
    assert rdd.ctx._jvm is not None
    return getattr(rdd.ctx._jvm, "org.apache.spark.ml.python.MLSerDe").pythonToJava(rdd._jrdd, True)


def _py2java(sc: "SparkContext", obj: Any) -> "JavaObject":
    """Convert Python object into Java"""
    from py4j.java_gateway import JavaObject
    from pyspark.core.rdd import RDD
    from pyspark.core.context import SparkContext

    if isinstance(obj, RDD):
        obj = _to_java_object_rdd(obj)
    elif isinstance(obj, DataFrame):
        obj = obj._jdf
    elif isinstance(obj, SparkContext):
        obj = obj._jsc
    elif isinstance(obj, list):
        obj = [_py2java(sc, x) for x in obj]
    elif isinstance(obj, JavaObject):
        pass
    elif isinstance(obj, (int, float, bool, bytes, str)):
        pass
    else:
        data = bytearray(CPickleSerializer().dumps(obj))
        assert sc._jvm is not None
        obj = getattr(sc._jvm, "org.apache.spark.ml.python.MLSerDe").loads(data)
    return obj


def _java2py(sc: "SparkContext", r: "JavaObjectOrPickleDump", encoding: str = "bytes") -> Any:
    from py4j.protocol import Py4JJavaError
    from py4j.java_gateway import JavaObject
    from py4j.java_collections import JavaArray, JavaList

    if isinstance(r, JavaObject):
        clsName = r.getClass().getSimpleName()
        # convert RDD into JavaRDD
        if clsName != "JavaRDD" and clsName.endswith("RDD"):
            r = r.toJavaRDD()
            clsName = "JavaRDD"

        assert sc._jvm is not None

        if clsName == "JavaRDD":
            jrdd = getattr(sc._jvm, "org.apache.spark.ml.python.MLSerDe").javaToPython(r)
            return RDD(jrdd, sc)

        if clsName == "Dataset":
            return DataFrame(r, SparkSession._getActiveSessionOrCreate())

        if clsName in _picklable_classes:
            r = getattr(sc._jvm, "org.apache.spark.ml.python.MLSerDe").dumps(r)
        elif isinstance(r, (JavaArray, JavaList)):
            try:
                r = getattr(sc._jvm, "org.apache.spark.ml.python.MLSerDe").dumps(r)
            except Py4JJavaError:
                pass  # not picklable

    if isinstance(r, (bytearray, bytes)):
        r = CPickleSerializer().loads(bytes(r), encoding=encoding)
    return r


def callJavaFunc(
    sc: "pyspark.core.context.SparkContext",
    func: Callable[..., "JavaObjectOrPickleDump"],
    *args: Any,
) -> "JavaObjectOrPickleDump":
    """Call Java Function"""
    java_args = [_py2java(sc, a) for a in args]
    return _java2py(sc, func(*java_args))


def inherit_doc(cls: "C") -> "C":
    """
    A decorator that makes a class inherit documentation from its parents.
    """
    for name, func in vars(cls).items():
        # only inherit docstring for public functions
        if name.startswith("_"):
            continue
        if not func.__doc__:
            for parent in cls.__bases__:
                parent_func = getattr(parent, name, None)
                if parent_func and getattr(parent_func, "__doc__", None):
                    func.__doc__ = parent_func.__doc__
                    break
    return cls
