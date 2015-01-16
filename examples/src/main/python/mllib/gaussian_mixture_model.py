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
import random
import argparse
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import GaussianMixtureEM


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


if __name__ == "__main__":
    """
    Parameters
    ----------
    input_file : path of the file which contains data points
    k : Number of mixture components
    convergenceTol : convergence_threshold.Default to 1e-3
    seed : random seed
    n_iter : Number of EM iterations to perform. Default to 100
    """
    conf = SparkConf().setAppName("GMM")
    sc = SparkContext(conf=conf)

    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='input file')
    parser.add_argument('k', type=int, help='num_of_clusters')
    parser.add_argument('--ct', default=1e-3, type=float, help='convergence_threshold')
    parser.add_argument('--seed', default=random.getrandbits(19),
                        type=long, help='num_of_iterations')
    parser.add_argument('--n_iter', default=100, type=int, help='num_of_iterations')
    args = parser.parse_args()

    lines = sc.textFile(args.input_file)
    data = lines.map(parseVector)
    model = GaussianMixtureEM.train(data, args.k, args.ct, args.seed, args.n_iter)
    for i in range(args.k):
        print ("weight = ", model.weight[i], "mu = ", model.mu[i],
               "sigma = ", model.sigma[i].toArray())
    print model.predictLabels(data).collect()
    sc.stop()
