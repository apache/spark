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


class RDDFunction():
    """
    This class is for py4j callback. This class is related with
    org.apache.spark.streaming.api.python.PythonRDDFunction.
    """
    def __init__(self, ctx, jrdd_deserializer, func):
        self.ctx = ctx
        self.deserializer = jrdd_deserializer
        self.func = func

    def call(self, jrdd, time):
        # Wrap JavaRDD into python's RDD class
        rdd = RDD(jrdd, self.ctx, self.deserializer)
        # Call user defined RDD function
        self.func(rdd, time)

    def __str__(self):
        return "%s, %s" % (str(self.deserializer), str(self.func))

    class Java:
        implements = ['org.apache.spark.streaming.api.python.PythonRDDFunction']


def msDurationToString(ms):
    """
    Returns a human-readable string representing a duration such as "35ms"

    >> msDurationToString(10)
    '10 ms'
    >>> msDurationToString(1000)
    '1.0 s'
    >>> msDurationToString(60000)
    '1.0 m'
    >>> msDurationToString(3600000)
    '1.00 h'
    """
    second = 1000
    minute = 60 * second
    hour = 60 * minute

    if ms < second:
        return "%d ms" % ms
    elif ms < minute:
        return "%.1f s" % (float(ms) / second)
    elif ms < hour:
        return "%.1f m" % (float(ms) / minute)
    else:
        return "%.2f h" % (float(ms) / hour)


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
