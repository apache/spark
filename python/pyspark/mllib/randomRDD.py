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
from pyspark.mllib._common import _deserialize_double
from pyspark.serializers import NoOpSerializer


class RandomRDDGenerators:
    """
    Generator methods for creating RDDs comprised of i.i.d samples from
    some distribution.
    """

    @staticmethod
    def uniformRDD(sc, size, numPartitions=None, seed=None):
        """
        Generates an RDD comprised of i.i.d samples from the
        uniform distribution on [0.0, 1.0].

        >>> RandomRDDGenerators.uniformRDD(sc, 10, 1, 0).collect()

        """
        if not numPartitions:
            if not seed:
                jrdd = sc._jvm.PythonMLLibAPI().uniformRDD(sc._jsc, size, numPartitions, seed)
            else:
                jrdd = sc._jvm.PythonMLLibAPI().uniformRDD(sc._jsc, size, numPartitions)
        else:
            jrdd = sc._jvm.PythonMLLibAPI().uniformRDD(sc._jsc, size)

        uniform =  RDD(jrdd, sc, NoOpSerializer())
        return uniform.map(lambda bytes: _deserialize_double(bytearray(bytes)))


def _test():
    import doctest
    from pyspark.context import SparkContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    globs['sc'] = SparkContext('local[2]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
