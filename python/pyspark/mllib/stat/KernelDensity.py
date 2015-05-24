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

from pyspark.mllib.common import callMLlibFunc

class KernelDensity(object):
    """
    .. note:: Experimental

    Estimate probabiltiy density at required points given a sample from the
    population.
    """
    def __init__(self):
        self._bandwidth = 1.0
        self._sample = 0

    def setBandwidth(self, bandwidth):
        self._bandwidth = bandwidth

    def setSample(self, sample):
        self._sample = sample

    def estimate(self, points):
        points = list(points)
        densities = callMLlibFunc(
            "estimateKernelDensity", self._sample, self._bandwidth, points) 
        return np.asarray(densities)
