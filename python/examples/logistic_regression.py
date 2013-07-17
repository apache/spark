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

"""
This example requires numpy (http://www.numpy.org/)
"""
from collections import namedtuple
from math import exp
from os.path import realpath
import sys

import numpy as np
from pyspark import SparkContext


N = 100000  # Number of data points
D = 10  # Number of dimensions
R = 0.7   # Scaling factor
ITERATIONS = 5
np.random.seed(42)


DataPoint = namedtuple("DataPoint", ['x', 'y'])
from lr import DataPoint  # So that DataPoint is properly serialized


def generateData():
    def generatePoint(i):
        y = -1 if i % 2 == 0 else 1
        x = np.random.normal(size=D) + (y * R)
        return DataPoint(x, y)
    return [generatePoint(i) for i in range(N)]


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print >> sys.stderr, \
            "Usage: PythonLR <master> [<slices>]"
        exit(-1)
    sc = SparkContext(sys.argv[1], "PythonLR", pyFiles=[realpath(__file__)])
    slices = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    points = sc.parallelize(generateData(), slices).cache()

    # Initialize w to a random value
    w = 2 * np.random.ranf(size=D) - 1
    print "Initial w: " + str(w)

    def add(x, y):
        x += y
        return x

    for i in range(1, ITERATIONS + 1):
        print "On iteration %i" % i

        gradient = points.map(lambda p:
            (1.0 / (1.0 + exp(-p.y * np.dot(w, p.x)))) * p.y * p.x
        ).reduce(add)
        w -= gradient

    print "Final w: " + str(w)
