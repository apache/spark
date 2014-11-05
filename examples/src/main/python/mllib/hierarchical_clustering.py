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
A hierarchical clustering program using MLlib.

This example requires NumPy, SciPy and matplotlib.
"""

import numpy
import os
import sys

import numpy
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import dendrogram

from pyspark.mllib.clustering import HierarchicalClustering
from pyspark import SparkContext


def parseVector(line):
    return numpy.array([float(x) for x in line.split(',')])


def usage():
    print >> sys.stderr, \
        "Usage: hierarchical_clustering [CSV filepath] [# clusters] [threshold]\n"
    exit(1)

if __name__ == "__main__":
    if not len(sys.argv) == 1 and not len(sys.argv) == 4:
        usage()

    sc = SparkContext(appName="HierarchicalClustering")

    # Parse arguments.
    data_path = 'data/mllib/sample_hierarchical_data.csv'
    k = 20
    if len(sys.argv) == 4:
        data_path = sys.argv[1]
        k = int(sys.argv[2])
        threshold = float(sys.argv[3])
    if not os.path.isfile(data_path):
        sc.stop()
        usage()

    # Load data.
    lines = sc.textFile(data_path)
    data = lines.map(parseVector)

    # Train a model
    model = HierarchicalClustering.train(data, k)
    merge_list = model.to_merge_list()
    print "Cluster Centers: " + str(model.clusterCenters)
    dendrogram(merge_list)
    plt.show()

    # Cut the trained cluster tree
    new_model = model.cut(threshold)
    merge_list = new_model.to_merge_list()
    print "Cut Cluster Centers: " + str(new_model.clusterCenters)
    dendrogram(merge_list)
    plt.show()

    sc.stop()
