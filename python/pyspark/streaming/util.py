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
import sys

from py4j.java_gateway import is_instance_of

from pyspark import SparkContext, RDD


class TransformFunction:
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
        self.rdd_wrap_func = lambda jrdd, ctx, ser: RDD(jrdd, ctx, ser)
        self.failure = None

    def rdd_wrapper(self, func):
        self.rdd_wrap_func = func
        return self

    def call(self, milliseconds, jrdds):
        # Clear the failure
        self.failure = None
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

            rdds = [
                self.rdd_wrap_func(jrdd, self.ctx, ser) if jrdd else None
                for jrdd, ser in zip(jrdds, sers)
            ]
            t = datetime.fromtimestamp(milliseconds / 1000.0)
            r = self.func(t, *rdds)
            if r:
                # Here, we work around to ensure `_jrdd` is `JavaRDD` by wrapping it by `map`.
                # org.apache.spark.streaming.api.python.PythonTransformFunction requires to return
                # `JavaRDD`; however, this could be `JavaPairRDD` by some APIs, for example, `zip`.
                # See SPARK-17756.
                if is_instance_of(self.ctx._gateway, r._jrdd, "org.apache.spark.api.java.JavaRDD"):
                    return r._jrdd
                else:
                    return r.map(lambda x: x)._jrdd
        except BaseException:
            self.failure = traceback.format_exc()

    def getLastFailure(self):
        return self.failure

    def __repr__(self):
        return "TransformFunction(%s)" % self.func

    class Java:
        implements = ["org.apache.spark.streaming.api.python.PythonTransformFunction"]


class TransformFunctionSerializer:
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
        self.failure = None

    def dumps(self, id):
        # Clear the failure
        self.failure = None
        try:
            func = self.gateway.gateway_property.pool[id]
            return bytearray(
                self.serializer.dumps((func.func, func.rdd_wrap_func, func.deserializers))
            )
        except BaseException:
            self.failure = traceback.format_exc()

    def loads(self, data):
        # Clear the failure
        self.failure = None
        try:
            f, wrap_func, deserializers = self.serializer.loads(bytes(data))
            return TransformFunction(self.ctx, f, *deserializers).rdd_wrapper(wrap_func)
        except BaseException:
            self.failure = traceback.format_exc()

    def getLastFailure(self):
        return self.failure

    def __repr__(self):
        return "TransformFunctionSerializer(%s)" % self.serializer

    class Java:
        implements = ["org.apache.spark.streaming.api.python.PythonTransformFunctionSerializer"]


def rddToFileName(prefix, suffix, timestamp):
    """
    Return string prefix-time(.suffix)

    Examples
    --------
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
        sys.exit(-1)
