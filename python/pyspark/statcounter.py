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

# This file is ported from spark/util/StatCounter.scala

import copy
import math
from typing import Dict, Iterable, Optional

try:
    from numpy import maximum, minimum, sqrt
except ImportError:
    maximum = max  # type: ignore[assignment]
    minimum = min  # type: ignore[assignment]
    sqrt = math.sqrt  # type: ignore[assignment]


class StatCounter(object):
    def __init__(self, values: Optional[Iterable[float]] = None):
        if values is None:
            values = list()
        self.n = 0  # Running count of our values
        self.mu = 0.0  # Running mean of our values
        self.m2 = 0.0  # Running variance numerator (sum of (x - mean)^2)
        self.maxValue = float("-inf")
        self.minValue = float("inf")

        for v in values:
            self.merge(v)

    # Add a value into this StatCounter, updating the internal statistics.
    def merge(self, value: float) -> "StatCounter":
        delta = value - self.mu
        self.n += 1
        self.mu += delta / self.n
        self.m2 += delta * (value - self.mu)
        self.maxValue = maximum(self.maxValue, value)
        self.minValue = minimum(self.minValue, value)

        return self

    # Merge another StatCounter into this one, adding up the internal statistics.
    def mergeStats(self, other: "StatCounter") -> "StatCounter":
        if not isinstance(other, StatCounter):
            raise TypeError("Can only merge StatCounter but got %s" % type(other))

        if other is self:  # reference equality holds
            self.mergeStats(other.copy())  # Avoid overwriting fields in a weird order
        else:
            if self.n == 0:
                self.mu = other.mu
                self.m2 = other.m2
                self.n = other.n
                self.maxValue = other.maxValue
                self.minValue = other.minValue

            elif other.n != 0:
                delta = other.mu - self.mu
                if other.n * 10 < self.n:
                    self.mu = self.mu + (delta * other.n) / (self.n + other.n)
                elif self.n * 10 < other.n:
                    self.mu = other.mu - (delta * self.n) / (self.n + other.n)
                else:
                    self.mu = (self.mu * self.n + other.mu * other.n) / (self.n + other.n)

                self.maxValue = maximum(self.maxValue, other.maxValue)
                self.minValue = minimum(self.minValue, other.minValue)

                self.m2 += other.m2 + (delta * delta * self.n * other.n) / (self.n + other.n)
                self.n += other.n
        return self

    # Clone this StatCounter
    def copy(self) -> "StatCounter":
        return copy.deepcopy(self)

    def count(self) -> int:
        return int(self.n)

    def mean(self) -> float:
        return self.mu

    def sum(self) -> float:
        return self.n * self.mu

    def min(self) -> float:
        return self.minValue

    def max(self) -> float:
        return self.maxValue

    # Return the variance of the values.
    def variance(self) -> float:
        if self.n == 0:
            return float("nan")
        else:
            return self.m2 / self.n

    #
    # Return the sample variance, which corrects for bias in estimating the variance by dividing
    # by N-1 instead of N.
    #
    def sampleVariance(self) -> float:
        if self.n <= 1:
            return float("nan")
        else:
            return self.m2 / (self.n - 1)

    # Return the standard deviation of the values.
    def stdev(self) -> float:
        return sqrt(self.variance())

    #
    # Return the sample standard deviation of the values, which corrects for bias in estimating the
    # variance by dividing by N-1 instead of N.
    #
    def sampleStdev(self) -> float:
        return sqrt(self.sampleVariance())

    def asDict(self, sample: bool = False) -> Dict[str, float]:
        """Returns the :class:`StatCounter` members as a ``dict``.

        Examples
        --------
        >>> sc.parallelize([1., 2., 3., 4.]).stats().asDict()
        {'count': 4L,
         'max': 4.0,
         'mean': 2.5,
         'min': 1.0,
         'stdev': 1.2909944487358056,
         'sum': 10.0,
         'variance': 1.6666666666666667}
        """
        return {
            "count": self.count(),
            "mean": self.mean(),
            "sum": self.sum(),
            "min": self.min(),
            "max": self.max(),
            "stdev": self.stdev() if sample else self.sampleStdev(),
            "variance": self.variance() if sample else self.sampleVariance(),
        }

    def __repr__(self) -> str:
        return "(count: %s, mean: %s, stdev: %s, max: %s, min: %s)" % (
            self.count(),
            self.mean(),
            self.stdev(),
            self.max(),
            self.min(),
        )
