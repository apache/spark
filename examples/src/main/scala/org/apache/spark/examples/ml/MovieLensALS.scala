/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.ml

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row

/**
 * An example app for ALS on MovieLens data (http://grouplens.org/datasets/movielens/).
 * Run with
 * {{{
 * bin/run-example org.apache.spark.examples.ml.MovieLensALS
 * }}}
 * A synthetic dataset in MovieLens format can be found at `data/mllib/sample_movielens_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object MovieLensALS {

  case class Rating(user: Int, item: Int, rating: Float)

  object Rating {
    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      assert(fields.size >= 3)
      // We use parseInt/parseFloat directly because toInt/toFloat generates extra assembly code.
      val user = java.lang.Integer.parseInt(fields(0))
      val item = java.lang.Integer.parseInt(fields(1))
      val rating = java.lang.Float.parseFloat(fields(2))
      Rating(user, item, rating)
    }
  }

  case class Params(
      input: String = null,
      maxIter: Int = 10,
      regParam: Double = 0.1,
      rank: Int = 10,
      numUserBlocks: Int = 10,
      numItemBlocks: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("MovieLensALS") {
      head("MovieLensALS: an example app for ALS on MovieLens data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("maxIter")
        .text(s"max number of iterations, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks}")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numItemBlocks")
        .text(s"number of item blocks, default: ${defaultParams.numItemBlocks}")
        .action((x, c) => c.copy(numItemBlocks = x))
      arg[String]("<input>")
        .required()
        .text("input paths to a MovieLens dataset of ratings")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.ml.MovieLensALS \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --rank 5 --numIterations 20 --lambda 1.0 \
          |  data/mllib/sample_movielens_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"MovieLensALS with $params")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val ratings = sc.textFile(params.input).map(Rating.parseRating).cache()

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.item).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    val splits = ratings.randomSplit(Array(0.8, 0.2), 0L)
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    ratings.unpersist(blocking = false)

    val als = new ALS()
      .setRank(params.rank)
      .setMaxIter(params.maxIter)
      .setRegParam(params.regParam)
      .setNumUserBlocks(params.numUserBlocks)
      .setNumItemBlocks(params.numItemBlocks)

    val model = als.fit(training)

    val mse = model.transform(test)
      .select('rating, 'prediction)
      .flatMap { case Row(rating: Float, prediction: Float) =>
        val err = rating.toDouble - prediction
        val err2 = err * err
        if (err2.isNaN) {
          Iterator.empty
        } else {
          Iterator.single(err2)
        }
      }.mean()
    val rmse = math.sqrt(mse)

    println(s"Test RMSE = $rmse.")

    sc.stop()
  }
}
