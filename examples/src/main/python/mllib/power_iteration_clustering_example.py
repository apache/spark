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

from pyspark import SparkContext
# $example on$
from pyspark.mllib.clustering import PowerIterationClustering, PowerIterationClusteringModel
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PowerIterationClusteringExample")  # SparkContext

    # $example on$
    # Load and parse the data
    data = sc.textFile("data/mllib/pic_data.txt")
    similarities = data.map(lambda line: tuple([float(x) for x in line.split(' ')]))

    # Cluster the data into two classes using PowerIterationClustering
    model = PowerIterationClustering.train(similarities, 2, 10)

    model.assignments().foreach(lambda x: print(str(x.id) + " -> " + str(x.cluster)))

    # Save and load model
    model.save(sc, "target/org/apache/spark/PythonPowerIterationClusteringExample/PICModel")
    sameModel = PowerIterationClusteringModel\
        .load(sc, "target/org/apache/spark/PythonPowerIterationClusteringExample/PICModel")
    # $example off$

    sc.stop()
