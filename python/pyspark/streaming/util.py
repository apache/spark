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

from datetime import datetime
import traceback

from pyspark import SparkContext, RDD


class TransformFunction(object):
    """
    This class is for py4j callback.
    """
    _emptyRDD = None

    def __init__(self, ctx, func, *deserializers):
        self.ctx = ctx
        self.func = func
        self.deserializers = deserializers

    @property
    def emptyRDD(self):
        if self._emptyRDD is None and self.ctx:
            self._emptyRDD = self.ctx.parallelize([]).cache()
        return self._emptyRDD

    def call(self, milliseconds, jrdds):
        try:
            if self.ctx is None:
                self.ctx = SparkContext._active_spark_context

            # extend deserializers with the first one
            sers = self.deserializers
            if len(sers) < len(jrdds):
                sers += (sers[0],) * (len(jrdds) - len(sers))

            rdds = [RDD(jrdd, self.ctx, ser) if jrdd else self.emptyRDD
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
    def __init__(self, ctx, serializer, gateway=None):
        self.ctx = ctx
        self.serializer = serializer
        self.gateway = gateway or self.ctx._gateway

    def dumps(self, id):
        try:
            func = self.gateway.gateway_property.pool[id]
            return bytearray(self.serializer.dumps((func.func, func.deserializers)))
        except Exception:
            traceback.print_exc()

    def loads(self, bytes):
        try:
            f, deserializers = self.serializer.loads(str(bytes))
            return TransformFunction(self.ctx, f, *deserializers)
        except Exception:
            traceback.print_exc()

    def __repr__(self):
        return "TransformFunctionSerializer(%s)" % self.serializer

    class Java:
        implements = ['org.apache.spark.streaming.api.python.PythonTransformFunctionSerializer']


def rddToFileName(prefix, suffix, time):
    """
    Return string prefix-time(.suffix)

    >>> rddToFileName("spark", None, 12345678910)
    'spark-12345678910'
    >>> rddToFileName("spark", "tmp", 12345678910)
    'spark-12345678910.tmp'

    """
    if suffix is None:
        return prefix + "-" + str(time)
    else:
        return prefix + "-" + str(time) + "." + suffix


if __name__ == "__main__":
    import doctest
    doctest.testmod()
