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

from typing import Iterable, Optional

import numpy as np
from numpy import ndarray

from pyspark.mllib.common import callMLlibFunc
from pyspark.rdd import RDD


class KernelDensity(object):
    """
    Estimate probability density at required points given an RDD of samples
    from the population.

    Examples
    --------
    >>> kd = KernelDensity()
    >>> sample = sc.parallelize([0.0, 1.0])
    >>> kd.setSample(sample)
    >>> kd.estimate([0.0, 1.0])
    array([ 0.12938758,  0.12938758])
    """

    def __init__(self) -> None:
        self._bandwidth: float = 1.0
        self._sample: Optional[RDD[float]] = None

    def setBandwidth(self, bandwidth: float) -> None:
        """Set bandwidth of each sample. Defaults to 1.0"""
        self._bandwidth = bandwidth

    def setSample(self, sample: "RDD[float]") -> None:
        """Set sample points from the population. Should be a RDD"""
        if not isinstance(sample, RDD):
            raise TypeError("samples should be a RDD, received %s" % type(sample))
        self._sample = sample

    def estimate(self, points: Iterable[float]) -> ndarray:
        """Estimate the probability density at points"""
        points = list(points)
        densities = callMLlibFunc("estimateKernelDensity", self._sample, self._bandwidth, points)
        return np.asarray(densities)
