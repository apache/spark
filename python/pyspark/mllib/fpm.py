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

import numpy
from numpy import array
from collections import namedtuple

from pyspark import SparkContext
from pyspark.rdd import ignore_unicode_prefix
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc, inherit_doc

__all__ = ['FPGrowth', 'FPGrowthModel']


@inherit_doc
@ignore_unicode_prefix
class FPGrowthModel(JavaModelWrapper):

    """
    .. note:: Experimental

    A FP-Growth model for mining frequent itemsets
    using the Parallel FP-Growth algorithm.

    >>> data = [["a", "b", "c"], ["a", "b", "d", "e"], ["a", "c", "e"], ["a", "c", "f"]]
    >>> rdd = sc.parallelize(data, 2)
    >>> model = FPGrowth.train(rdd, 0.6, 2)
    >>> sorted(model.freqItemsets().collect())
    [FreqItemset(items=[u'a'], freq=4), FreqItemset(items=[u'c'], freq=3), ...
    """

    def freqItemsets(self):
        """
        Returns the frequent itemsets of this model.
        """
        return self.call("getFreqItemsets").map(lambda x: (FPGrowth.FreqItemset(x[0], x[1])))


class FPGrowth(object):
    """
    .. note:: Experimental

    A Parallel FP-growth algorithm to mine frequent itemsets.
    """

    @classmethod
    def train(cls, data, minSupport=0.3, numPartitions=-1):
        """
        Computes an FP-Growth model that contains frequent itemsets.

        :param data: The input data set, each element contains a
            transaction.
        :param minSupport: The minimal support level (default: `0.3`).
        :param numPartitions: The number of partitions used by
            parallel FP-growth (default: same as input data).
        """
        model = callMLlibFunc("trainFPGrowthModel", data, float(minSupport), int(numPartitions))
        return FPGrowthModel(model)

    class FreqItemset(namedtuple("FreqItemset", ["items", "freq"])):
        """
        Represents an (items, freq) tuple.
        """


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
