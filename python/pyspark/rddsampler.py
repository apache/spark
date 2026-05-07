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
import random
import math
from typing import Generic, Hashable, Iterable, Iterator, Optional, TypeVar


T = TypeVar("T")
K = TypeVar("K", bound=Hashable)


class RDDSamplerBase:
    def __init__(self, withReplacement: bool, seed: Optional[int] = None) -> None:
        self._seed = seed if seed is not None else random.randint(0, sys.maxsize)
        self._withReplacement = withReplacement
        self._random: Optional[random.Random] = None

    def initRandomGenerator(self, split: int) -> None:
        self._random = random.Random(self._seed ^ split)

        # mixing because the initial seeds are close to each other
        for _ in range(10):
            self._random.randint(0, 1)

    def getUniformSample(self) -> float:
        assert self._random is not None
        return self._random.random()

    def getPoissonSample(self, mean: float) -> int:
        # Using Knuth's algorithm described in
        # http://en.wikipedia.org/wiki/Poisson_distribution
        assert self._random is not None
        if mean < 20.0:
            # one exp and k+1 random calls
            lda = math.exp(-mean)
            p = self._random.random()
            k = 0
            while p > lda:
                k += 1
                p *= self._random.random()
        else:
            # switch to the log domain, k+1 expovariate (random + log) calls
            p = self._random.expovariate(mean)
            k = 0
            while p < 1.0:
                k += 1
                p += self._random.expovariate(mean)
        return k

    def func(self, split: int, iterator: Iterable[T]) -> Iterator[T]:
        raise NotImplementedError


class RDDSampler(RDDSamplerBase):
    def __init__(self, withReplacement: bool, fraction: float, seed: Optional[int] = None) -> None:
        RDDSamplerBase.__init__(self, withReplacement, seed)
        self._fraction = fraction

    def func(self, split: int, iterator: Iterable[T]) -> Iterator[T]:
        self.initRandomGenerator(split)
        if self._withReplacement:
            for obj in iterator:
                # For large datasets, the expected number of occurrences of each element in
                # a sample with replacement is Poisson(frac). We use that to get a count for
                # each element.
                count = self.getPoissonSample(self._fraction)
                for _ in range(0, count):
                    yield obj
        else:
            for obj in iterator:
                if self.getUniformSample() < self._fraction:
                    yield obj


class RDDRangeSampler(RDDSamplerBase):
    def __init__(self, lowerBound: float, upperBound: float, seed: Optional[int] = None) -> None:
        RDDSamplerBase.__init__(self, False, seed)
        self._lowerBound = lowerBound
        self._upperBound = upperBound

    def func(self, split: int, iterator: Iterable[T]) -> Iterator[T]:
        self.initRandomGenerator(split)
        for obj in iterator:
            if self._lowerBound <= self.getUniformSample() < self._upperBound:
                yield obj


class RDDStratifiedSampler(RDDSamplerBase, Generic[K]):
    def __init__(
        self, withReplacement: bool, fractions: dict[K, float], seed: Optional[int] = None
    ) -> None:
        RDDSamplerBase.__init__(self, withReplacement, seed)
        self._fractions = fractions

    def func(self, split: int, iterator: Iterable[tuple[K, T]]) -> Iterator[tuple[K, T]]:  # type: ignore[override]
        self.initRandomGenerator(split)
        if self._withReplacement:
            for key, val in iterator:
                # For large datasets, the expected number of occurrences of each element in
                # a sample with replacement is Poisson(frac). We use that to get a count for
                # each element.
                count = self.getPoissonSample(self._fractions[key])
                for _ in range(0, count):
                    yield key, val
        else:
            for key, val in iterator:
                if self.getUniformSample() < self._fractions[key]:
                    yield key, val
