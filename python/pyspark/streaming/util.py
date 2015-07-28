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

import time
from datetime import datetime
import traceback

from pyspark import SparkContext, RDD


class TransformFunction(object):
    """
    This class wraps a function RDD[X] -> RDD[Y] that was passed to
    DStream.transform(), allowing it to be called from Java via Py4J's
    callback server.

    Java calls this function with a sequence of JavaRDDs and this function
    returns a single JavaRDD pointer back to Java.
    """
    _emptyRDD = None

    def __init__(self, ctx, func, *deserializers):
        self.ctx = ctx
        self.func = func
        self.deserializers = deserializers
        self._rdd_wrapper = lambda jrdd, ctx, ser: RDD(jrdd, ctx, ser)

    def rdd_wrapper(self, func):
        self._rdd_wrapper = func
        return self

    def call(self, milliseconds, jrdds):
        try:
            if self.ctx is None:
                self.ctx = SparkContext._active_spark_context
            if not self.ctx or not self.ctx._jsc:
                # stopped
                return

            # extend deserializers with the first one
            sers = self.deserializers
            if len(sers) < len(jrdds):
                sers += (sers[0],) * (len(jrdds) - len(sers))

            rdds = [self._rdd_wrapper(jrdd, self.ctx, ser) if jrdd else None
                    for jrdd, ser in zip(jrdds, sers)]
            t = datetime.fromtimestamp(milliseconds / 1000.0)
            r = self.func(t, *rdds)
            if r:
                return r._jrdd
        except Exception:
            traceback.print_exc()

    def __repr__(self):
        return "TransformFunction(%s)" % self.func

    class Java:
        implements = ['org.apache.spark.streaming.api.python.PythonTransformFunction']


class TransformFunctionSerializer(object):
    """
    This class implements a serializer for PythonTransformFunction Java
    objects.

    This is necessary because the Java PythonTransformFunction objects are
    actually Py4J references to Python objects and thus are not directly
    serializable. When Java needs to serialize a PythonTransformFunction,
    it uses this class to invoke Python, which returns the serialized function
    as a byte array.
    """
    def __init__(self, ctx, serializer, gateway=None):
        self.ctx = ctx
        self.serializer = serializer
        self.gateway = gateway or self.ctx._gateway
        self.gateway.jvm.PythonDStream.registerSerializer(self)

    def dumps(self, id):
        try:
            func = self.gateway.gateway_property.pool[id]
            return bytearray(self.serializer.dumps((func.func, func.deserializers)))
        except Exception:
            traceback.print_exc()

    def loads(self, data):
        try:
            f, deserializers = self.serializer.loads(bytes(data))
            return TransformFunction(self.ctx, f, *deserializers)
        except Exception:
            traceback.print_exc()

    def __repr__(self):
        return "TransformFunctionSerializer(%s)" % self.serializer

    class Java:
        implements = ['org.apache.spark.streaming.api.python.PythonTransformFunctionSerializer']


def rddToFileName(prefix, suffix, timestamp):
    """
    Return string prefix-time(.suffix)

    >>> rddToFileName("spark", None, 12345678910)
    'spark-12345678910'
    >>> rddToFileName("spark", "tmp", 12345678910)
    'spark-12345678910.tmp'
    """
    if isinstance(timestamp, datetime):
        seconds = time.mktime(timestamp.timetuple())
        timestamp = int(seconds * 1000) + timestamp.microsecond // 1000
    if suffix is None:
        return prefix + "-" + str(timestamp)
    else:
        return prefix + "-" + str(timestamp) + "." + suffix


if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        exit(-1)
