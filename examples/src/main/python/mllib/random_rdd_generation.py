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
Randomly generated RDDs.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.mllib.random import RandomRDDs


if __name__ == "__main__":
    if len(sys.argv) not in [1, 2]:
        print("Usage: random_rdd_generation", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonRandomRDDGeneration")

    numExamples = 10000  # number of examples to generate
    fraction = 0.1  # fraction of data to sample

    # Example: RandomRDDs.normalRDD
    normalRDD = RandomRDDs.normalRDD(sc, numExamples)
    print('Generated RDD of %d examples sampled from the standard normal distribution'
          % normalRDD.count())
    print('  First 5 samples:')
    for sample in normalRDD.take(5):
        print('    ' + str(sample))
    print()

    # Example: RandomRDDs.normalVectorRDD
    normalVectorRDD = RandomRDDs.normalVectorRDD(sc, numRows=numExamples, numCols=2)
    print('Generated RDD of %d examples of length-2 vectors.' % normalVectorRDD.count())
    print('  First 5 samples:')
    for sample in normalVectorRDD.take(5):
        print('    ' + str(sample))
    print()

    sc.stop()
