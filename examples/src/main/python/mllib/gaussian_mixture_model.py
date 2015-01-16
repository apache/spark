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
A Gaussian Mixture Model clustering program using MLlib.

This example requires NumPy (http://www.numpy.org/).
"""

import sys
import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import GaussianMixtureEM


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: gaussian_mixture_model <input_file> <k> <convergenceTol> "
        exit(-1)
    sc = SparkContext(appName="GMM")
    lines = sc.textFile(sys.argv[1])
    data = lines.map(parseVector)
    k = int(sys.argv[2])
    convergenceTol = float(sys.argv[3])
    model = GaussianMixtureEM.train(data, k, convergenceTol)
    for i in range(k):
        print ("weight = ", model.weight[i], "mu = ", model.mu[i],
               "sigma = ", model.sigma[i].toArray())
    print model.predictLabels(data).collect()
    sc.stop()
