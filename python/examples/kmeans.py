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
import sys

import numpy as np
from pyspark import SparkContext


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print >> sys.stderr, "Usage: kmeans <master> <file> <k> <convergeDist>"
        exit(-1)
    sc = SparkContext(sys.argv[1], "PythonKMeans")
    lines = sc.textFile(sys.argv[2])
    data = lines.map(parseVector).cache()
    K = int(sys.argv[3])
    convergeDist = float(sys.argv[4])

    # TODO: change this after we port takeSample()
    #kPoints = data.takeSample(False, K, 34)
    kPoints = data.take(K)
    tempDist = 1.0

    while tempDist > convergeDist:
        closest = data.map(
            lambda p : (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(
            lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
        newPoints = pointStats.map(
            lambda (x, (y, z)): (x, y / z)).collect()

        tempDist = sum(np.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)

        for (x, y) in newPoints:
            kPoints[x] = y

    print "Final centers: " + str(kPoints)
