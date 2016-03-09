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

from pyspark import SparkContext
from pyspark.sql import SQLContext

# $example on$
import math

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="ALSExample")
    sqlContext = SQLContext(sc)

    # $example on$
    lines = sc.textFile("data/mllib/als/sample_movielens_ratings.txt")
    parts = lines.map(lambda l: l.split("::"))
    ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                         rating=float(p[2]), timestamp=long(p[3])))
    ratings = sqlContext.createDataFrame(ratingsRDD)
    (training, test) = ratings.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    rawPredictions = model.transform(test)
    predictions = rawPredictions\
        .withColumn("rating", rawPredictions.rating.cast("double"))\
        .withColumn("prediction", rawPredictions.prediction.cast("double"))
    evaluator =\
        RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))
    # $example off$
    sc.stop()
