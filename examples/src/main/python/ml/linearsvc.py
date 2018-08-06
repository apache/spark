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

from __future__ import print_function

# $example on$
from pyspark.ml.classification import LinearSVC
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("linearSVC Example")\
        .getOrCreate()

    # $example on$
    # Load training data
    training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    lsvc = LinearSVC(maxIter=10, regParam=0.1)

    # Fit the model
    lsvcModel = lsvc.fit(training)

    # Print the coefficients and intercept for linear SVC
    print("Coefficients: " + str(lsvcModel.coefficients))
    print("Intercept: " + str(lsvcModel.intercept))

    # $example off$

    spark.stop()
