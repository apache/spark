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
    This class is for py4j callback.
    """
    def __init__(self, ctx, func, *deserializers):
        self.ctx = ctx
        self.func = func
        self.deserializers = deserializers
        emptyRDD = getattr(self.ctx, "_emptyRDD", None)
        if emptyRDD is None:
            self.ctx._emptyRDD = emptyRDD = self.ctx.parallelize([]).cache()
        self.emptyRDD = emptyRDD

    def call(self, milliseconds, jrdds):
        try:
            # extend deserializers with the first one
            sers = self.deserializers
            if len(sers) < len(jrdds):
                sers += (sers[0],) * (len(jrdds) - len(sers))

            rdds = [RDD(jrdd, self.ctx, ser) if jrdd else self.emptyRDD
                    for jrdd, ser in zip(jrdds, sers)]
            r = self.func(milliseconds, *rdds)
            if r:
                return r._jrdd
        except Exception:
            import traceback
            traceback.print_exc()

    def __repr__(self):
        return "RDDFunction(%s)" % (str(self.func))

    class Java:
        implements = ['org.apache.spark.streaming.api.python.PythonRDDFunction']


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
