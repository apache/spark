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


class RDDSamplerBase(object):

    def __init__(self, withReplacement, seed=None):
        try:
            import numpy
            self._use_numpy = True
        except ImportError:
            print >> sys.stderr, (
                "NumPy does not appear to be installed. "
                "Falling back to default random generator for sampling.")
            self._use_numpy = False

        self._seed = seed if seed is not None else random.randint(0, 2 ** 32 - 1)
        self._withReplacement = withReplacement
        self._random = None
        self._split = None
        self._rand_initialized = False

    def initRandomGenerator(self, split):
        if self._use_numpy:
            import numpy
            self._random = numpy.random.RandomState(self._seed ^ split)
        else:
            self._random = random.Random(self._seed ^ split)

        # mixing because the initial seeds are close to each other
        for _ in xrange(10):
            self._random.randint(0, 1)

        self._split = split
        self._rand_initialized = True

    def getUniformSample(self, split):
        if not self._rand_initialized or split != self._split:
            self.initRandomGenerator(split)

        if self._use_numpy:
            return self._random.random_sample()
        else:
            return self._random.uniform(0.0, 1.0)

    def getPoissonSample(self, split, mean):
        if not self._rand_initialized or split != self._split:
            self.initRandomGenerator(split)

        if self._use_numpy:
            return self._random.poisson(mean)
        else:
            # here we simulate drawing numbers n_i ~ Poisson(lambda = 1/mean) by
            # drawing a sequence of numbers delta_j ~ Exp(mean)
            num_arrivals = 1
            cur_time = 0.0

            cur_time += self._random.expovariate(mean)

            if cur_time > 1.0:
                return 0

            while(cur_time <= 1.0):
                cur_time += self._random.expovariate(mean)
                num_arrivals += 1

            return (num_arrivals - 1)

    def shuffle(self, vals):
        if self._random is None:
            self.initRandomGenerator(0)  # this should only ever called on the master so
            # the split does not matter

        if self._use_numpy:
            self._random.shuffle(vals)
        else:
            self._random.shuffle(vals, self._random.random)


class RDDSampler(RDDSamplerBase):

    def __init__(self, withReplacement, fraction, seed=None):
        RDDSamplerBase.__init__(self, withReplacement, seed)
        self._fraction = fraction

    def func(self, split, iterator):
        if self._withReplacement:
            for obj in iterator:
                # For large datasets, the expected number of occurrences of each element in
                # a sample with replacement is Poisson(frac). We use that to get a count for
                # each element.
                count = self.getPoissonSample(split, mean=self._fraction)
                for _ in range(0, count):
                    yield obj
        else:
            for obj in iterator:
                if self.getUniformSample(split) <= self._fraction:
                    yield obj


class RDDStratifiedSampler(RDDSamplerBase):

    def __init__(self, withReplacement, fractions, seed=None):
        RDDSamplerBase.__init__(self, withReplacement, seed)
        self._fractions = fractions

    def func(self, split, iterator):
        if self._withReplacement:
            for key, val in iterator:
                # For large datasets, the expected number of occurrences of each element in
                # a sample with replacement is Poisson(frac). We use that to get a count for
                # each element.
                count = self.getPoissonSample(split, mean=self._fractions[key])
                for _ in range(0, count):
                    yield key, val
        else:
            for key, val in iterator:
                if self.getUniformSample(split) <= self._fractions[key]:
                    yield key, val
