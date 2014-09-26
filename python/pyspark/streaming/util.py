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

from pyspark.rdd import RDD


class RDDFunction(object):
    """
    This class is for py4j callback. This class is related with
    org.apache.spark.streaming.api.python.PythonRDDFunction.
    """
    def __init__(self, ctx, func, jrdd_deserializer):
        self.ctx = ctx
        self.func = func
        self.deserializer = jrdd_deserializer

    def call(self, jrdd, milliseconds):
        try:
            rdd = RDD(jrdd, self.ctx, self.deserializer)
            r = self.func(rdd, milliseconds)
            if r:
                return r._jrdd
        except:
            import traceback
            traceback.print_exc()

    def __repr__(self):
        return "RDDFunction(%s, %s)" % (str(self.deserializer), str(self.func))

    class Java:
        implements = ['org.apache.spark.streaming.api.python.PythonRDDFunction']


class RDDFunction2(object):
    """
    This class is for py4j callback. This class is related with
    org.apache.spark.streaming.api.python.PythonRDDFunction2.
    """
    def __init__(self, ctx, func, jrdd_deserializer):
        self.ctx = ctx
        self.func = func
        self.deserializer = jrdd_deserializer

    def call(self, jrdd, jrdd2, milliseconds):
        try:
            rdd = RDD(jrdd, self.ctx, self.deserializer) if jrdd else None
            other = RDD(jrdd2, self.ctx, self.deserializer) if jrdd2 else None
            r = self.func(rdd, other, milliseconds)
            if r:
                return r._jrdd
        except:
            import traceback
            traceback.print_exc()

    def __repr__(self):
        return "RDDFunction(%s, %s)" % (str(self.deserializer), str(self.func))

    class Java:
        implements = ['org.apache.spark.streaming.api.python.PythonRDDFunction2']


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
