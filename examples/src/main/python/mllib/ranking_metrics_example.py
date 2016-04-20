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
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
# $example off$
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="Ranking Metrics Example")

    # Several of the methods available in scala are currently missing from pyspark
    # $example on$
    # Read in the ratings data
    lines = sc.textFile("data/mllib/sample_movielens_data.txt")

    def parseLine(line):
        fields = line.split("::")
        return Rating(int(fields[0]), int(fields[1]), float(fields[2]) - 2.5)
    ratings = lines.map(lambda r: parseLine(r))

    # Train a model on to predict user-product ratings
    model = ALS.train(ratings, 10, 10, 0.01)

    # Get predicted ratings on all existing user-product pairs
    testData = ratings.map(lambda p: (p.user, p.product))
    predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating))

    ratingsTuple = ratings.map(lambda r: ((r.user, r.product), r.rating))
    scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])

    # Instantiate regression metrics to compare predicted and actual ratings
    metrics = RegressionMetrics(scoreAndLabels)

    # Root mean squared error
    print("RMSE = %s" % metrics.rootMeanSquaredError)

    # R-squared
    print("R-squared = %s" % metrics.r2)
    # $example off$
