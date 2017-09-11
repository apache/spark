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

# $example on$
from pyspark.mllib.evaluation import MultilabelMetrics
# $example off$
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="MultiLabelMetricsExample")
    # $example on$
    scoreAndLabels = sc.parallelize([
        ([0.0, 1.0], [0.0, 2.0]),
        ([0.0, 2.0], [0.0, 1.0]),
        ([], [0.0]),
        ([2.0], [2.0]),
        ([2.0, 0.0], [2.0, 0.0]),
        ([0.0, 1.0, 2.0], [0.0, 1.0]),
        ([1.0], [1.0, 2.0])])

    # Instantiate metrics object
    metrics = MultilabelMetrics(scoreAndLabels)

    # Summary stats
    print("Recall = %s" % metrics.recall())
    print("Precision = %s" % metrics.precision())
    print("F1 measure = %s" % metrics.f1Measure())
    print("Accuracy = %s" % metrics.accuracy)

    # Individual label stats
    labels = scoreAndLabels.flatMap(lambda x: x[1]).distinct().collect()
    for label in labels:
        print("Class %s precision = %s" % (label, metrics.precision(label)))
        print("Class %s recall = %s" % (label, metrics.recall(label)))
        print("Class %s F1 Measure = %s" % (label, metrics.f1Measure(label)))

    # Micro stats
    print("Micro precision = %s" % metrics.microPrecision)
    print("Micro recall = %s" % metrics.microRecall)
    print("Micro F1 measure = %s" % metrics.microF1Measure)

    # Hamming loss
    print("Hamming loss = %s" % metrics.hammingLoss)

    # Subset accuracy
    print("Subset accuracy = %s" % metrics.subsetAccuracy)
    # $example off$
