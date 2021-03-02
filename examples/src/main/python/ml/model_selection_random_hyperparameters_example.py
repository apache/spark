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
This example uses random hyperparameters to perform model selection.
Run with:

  bin/spark-submit examples/src/main/python/ml/model_selection_random_hyperparameters_example.py
"""
# $example on$
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamRandomBuilder, CrossValidator
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("TrainValidationSplit") \
        .getOrCreate()

    # $example on$
    data = spark.read.format("libsvm") \
        .load("data/mllib/sample_linear_regression_data.txt")

    lr = LinearRegression(maxIter=10)

    # We sample the regularization parameter logarithmically over the range [0.01, 1.0].
    # This means that values around 0.01, 0.1 and 1.0 are roughly equally likely.
    # Note that both parameters must be greater than zero as otherwise we'll get an infinity.
    # We sample the the ElasticNet mixing parameter uniformly over the range [0, 1]
    # Note that in real life, you'd choose more than the 5 samples we see below.
    hyperparameters = ParamRandomBuilder() \
        .addLog10Random(lr.regParam, 0.01, 1.0, 5) \
        .addRandom(lr.elasticNetParam, 0.0, 1.0, 5) \
        .addGrid(lr.fitIntercept, [False, True]) \
        .build()

    cv = CrossValidator(estimator=lr,
                        estimatorParamMaps=hyperparameters,
                        evaluator=RegressionEvaluator(),
                        numFolds=2)

    model = cv.fit(data)
    bestModel = model.bestModel
    print("Optimal model has regParam = {}, elasticNetParam = {}, fitIntercept = {}"
          .format(bestModel.getRegParam(), bestModel.getElasticNetParam(),
                  bestModel.getFitIntercept()))

    # $example off$
    spark.stop()
