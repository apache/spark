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

from pyspark import SparkContext
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc, inherit_doc

__all__ = ['FPGrowth', 'FPGrowthModel']


@inherit_doc
class FPGrowthModel(JavaModelWrapper):

    """A FP-Growth model for mining frequent itemsets using the Parallel FP-Growth algorithm.

    >>> r1 = ["r","z","h","k","p"]
    >>> r2 = ["z","y","x","w","v","u","t","s"]
    >>> r3 = ["s","x","o","n","r"]
    >>> r4 = ["x","z","y","m","t","s","q","e"]
    >>> r5 = ["z"]
    >>> r6 = ["x","z","y","r","q","t","p"]
    >>> rdd = sc.parallelize([r1,r2,r3,r4,r5,r6], 2)
    >>> model = FPGrowth.train(rdd, 0.5, 2)
    >>> result = model.freqItemsets().collect()
    >>> expected = [([u"s"], 3), ([u"z"], 5), ([u"x"], 4), ([u"t"], 3), ([u"y"], 3), ([u"r"],3),
    ... ([u"x", u"z"], 3), ([u"y", u"t"], 3), ([u"t", u"x"], 3), ([u"s",u"x"], 3),
    ... ([u"y", u"x"], 3), ([u"y", u"z"], 3), ([u"t", u"z"], 3), ([u"y", u"x", u"z"], 3),
    ... ([u"t", u"x", u"z"], 3), ([u"y", u"t", u"z"], 3), ([u"y", u"t", u"x"], 3),
    ... ([u"y", u"t", u"x", u"z"], 3)]
    >>> diff1 = [x for x in result if x not in expected]
    >>> len(diff1)
    0
    >>> diff2 = [x for x in expected if x not in result]
    >>> len(diff2)
    0
    """
    def freqItemsets(self):
        return self.call("getFreqItemsets")


class FPGrowth(object):

    @classmethod
    def train(cls, data, minSupport=0.3, numPartitions=-1):
        """
        Computes an FP-Growth model that contains frequent itemsets.
        :param data:            The input data set, each element contains a transaction.
        :param minSupport:      The minimal support level (default: `0.3`).
        :param numPartitions:   The number of partitions used by parallel FP-growth
                                (default: same as input data).
        """
        model = callMLlibFunc("trainFPGrowthModel", data, float(minSupport), int(numPartitions))
        return FPGrowthModel(model)


def _test():
    import doctest
    import pyspark.mllib.fpm
    globs = pyspark.mllib.fpm.__dict__.copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest')
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
