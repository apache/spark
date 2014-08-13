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
Randomly generated and sampled RDDs.
"""

import sys

from pyspark import SparkContext
from pyspark.mllib.random import RandomRDDGenerators
from pyspark.mllib.util import MLUtils



if __name__ == "__main__":
    if len(sys.argv) not in [1, 2]:
        print >> sys.stderr, "Usage: random_and_sampled_rdds <libsvm data file>"
        exit(-1)
    if len(sys.argv) == 2:
        datapath = sys.argv[1]
    else:
        datapath = 'data/mllib/sample_binary_classification_data.txt'

    sc = SparkContext(appName="PythonRandomAndSampledRDDs")

    points = MLUtils.loadLibSVMFile(sc, datapath)

    numExamples = 10000 # number of examples to generate
    fraction = 0.1 # fraction of data to sample

    # Example: RandomRDDGenerators
    normalRDD = RandomRDDGenerators.normalRDD(sc, numExamples)
    print 'Generated RDD of %d examples sampled from the standard normal distribution'\
        % normalRDD.count()
    normalVectorRDD = RandomRDDGenerators.normalVectorRDD(sc, numRows = numExamples, numCols = 2)
    print 'Generated RDD of %d examples of length-2 vectors.' % normalVectorRDD.count()

    print

    # Example: RDD.sample() and RDD.takeSample()
    expectedSampleSize = int(numExamples * fraction)
    print 'Sampling RDD using fraction %g.  Expected sample size = %d.' \
        % (fraction, expectedSampleSize)
    sampledRDD = normalRDD.sample(withReplacement = True, fraction = fraction)
    print '  RDD.sample(): sample has %d examples' % sampledRDD.count()
    sampledArray = normalRDD.takeSample(withReplacement = True, num = expectedSampleSize)
    print '  RDD.takeSample(): sample has %d examples' % len(sampledArray)

    print

    # Example: RDD.sampleByKey()
    examples = MLUtils.loadLibSVMFile(sc, datapath)
    sizeA = examples.count()
    print 'Loaded data with %d examples from file: %s' % (sizeA, datapath)
    keyedRDD = examples.map(lambda lp: (int(lp.label), lp.features))
    print '  Keyed data using label (Int) as key ==> Orig'
    #  Count examples per label in original data.
    keyCountsA = keyedRDD.countByKey()
    #  Subsample, and count examples per label in sampled data.
    fractions = {}
    for k in keyCountsA.keys():
        fractions[k] = fraction
    sampledByKeyRDD = keyedRDD.sampleByKey(withReplacement = True, fractions = fractions)
    keyCountsB = sampledByKeyRDD.countByKey()
    sizeB = sum(keyCountsB.values())
    print '  Sampled %d examples using approximate stratified sampling (by label). ==> Sample' \
        % sizeB
    print '   \tFractions of examples with key'
    print 'Key\tOrig\tSample'
    for k in sorted(keyCountsA.keys()):
        print '%d\t%g\t%g' % (k, keyCountsA[k] / float(sizeA), keyCountsB[k] / float(sizeB))

    sc.stop()
