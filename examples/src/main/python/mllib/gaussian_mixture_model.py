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
"""

import random
import argparse
import numpy as np

from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import GaussianMixture


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


if __name__ == "__main__":
    """
    Parameters
    ----------
    :param inputFile:        Input file path which contains data points
    :param k:                Number of mixture components
    :param convergenceTol:   Convergence threshold. Default to 1e-3
    :param maxIterations:    Number of EM iterations to perform. Default to 100
    :param seed:             Random seed
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('inputFile', help='Input File')
    parser.add_argument('k', type=int, help='Number of clusters')
    parser.add_argument('--convergenceTol', default=1e-3, type=float, help='convergence threshold')
    parser.add_argument('--maxIterations', default=100, type=int, help='Number of iterations')
    parser.add_argument('--seed', default=random.getrandbits(19),
                        type=int, help='Random seed')
    args = parser.parse_args()

    conf = SparkConf().setAppName("GMM")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(args.inputFile)
    data = lines.map(parseVector)
    model = GaussianMixture.train(data, args.k, args.convergenceTol,
                                  args.maxIterations, args.seed)
    for i in range(args.k):
        print(("weight = ", model.weights[i], "mu = ", model.gaussians[i].mu,
               "sigma = ", model.gaussians[i].sigma.toArray()))
    print("\n")
    print(("The membership value of each vector to all mixture components (first 100): ",
           model.predictSoft(data).take(100)))
    print("\n")
    print(("Cluster labels (first 100): ", model.predict(data).take(100)))
    sc.stop()
