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

import sys

if sys.version > '3':
    xrange = range

import numpy as np
try:
    import xmlrunner
except ImportError:
    xmlrunner = None
if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.mllib.common import callMLlibFunc
from pyspark.rdd import RDD


class KernelDensity(object):
    """
    .. note:: Experimental

    Estimate probability density at required points given a RDD of samples
    from the population.

    >>> kd = KernelDensity()
    >>> sample = sc.parallelize([0.0, 1.0])
    >>> kd.setSample(sample)
    >>> kd.estimate([0.0, 1.0])
    array([ 0.3204565,  0.3204565])
    """
    def __init__(self):
        self._bandwidth = 1.0
        self._sample = None

    def setBandwidth(self, bandwidth):
        """Set bandwidth of each sample. Defaults to 1.0"""
        self._bandwidth = bandwidth

    def setSample(self, sample):
        """Set sample points from the population. Should be a RDD"""
        if not isinstance(sample, RDD):
            raise TypeError("samples should be a RDD, received %s" % type(sample))
        self._sample = sample

    def estimate(self, points):
        """Estimate the probability density at points"""
        points = list(points)
        densities = callMLlibFunc(
            "estimateKernelDensity", self._sample, self._bandwidth, points)
        return np.asarray(densities)


def _test():
    import doctest
    from pyspark import SparkContext
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    t = doctest.DocTestSuite(globs=globs, optionflags=doctest.ELLIPSIS)
    if xmlrunner:
        result = xmlrunner.XMLTestRunner(output='target/test-reports',
                                         verbosity=3).run(t)
    else:
        result = unittest.TextTestRunner(verbosity=3).run(t)
    globs['sc'].stop()
    if not result.wasSuccessful():
        exit(-1)


if __name__ == "__main__":
    _test()
