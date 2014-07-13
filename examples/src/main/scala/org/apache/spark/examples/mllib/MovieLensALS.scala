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

package org.apache.spark.examples.mllib

import scala.collection.mutable

import com.esotericsoftware.kryo.Kryo
import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}

/**
 * An example app for ALS on MovieLens data (http://grouplens.org/datasets/movielens/).
 * Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.MovieLensALS
 * }}}
 * A synthetic dataset in MovieLens format can be found at `data/mllib/sample_movielens_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object MovieLensALS {

  class ALSRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Rating])
      kryo.register(classOf[mutable.BitSet])
    }
  }

  case class Params(
      input: String = null,
      kryo: Boolean = false,
      numIterations: Int = 20,
      lambda: Double = 1.0,
      rank: Int = 10,
      numUserBlocks: Int = -1,
      numProductBlocks: Int = -1,
      implicitPrefs: Boolean = false)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("MovieLensALS") {
      head("MovieLensALS: an example app for ALS on MovieLens data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Unit]("kryo")
        .text("use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks} (auto)")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numProductBlocks")
        .text(s"number of product blocks, default: ${defaultParams.numProductBlocks} (auto)")
        .action((x, c) => c.copy(numProductBlocks = x))
      opt[Unit]("implicitPrefs")
        .text("use implicit preference")
        .action((_, c) => c.copy(implicitPrefs = true))
      arg[String]("<input>")
        .required()
        .text("input paths to a MovieLens dataset of ratings")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.MovieLensALS \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --rank 5 --numIterations 20 --lambda 1.0 --kryo \
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
    if (params.kryo) {
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[ALSRegistrator].getName)
        .set("spark.kryoserializer.buffer.mb", "8")
    }
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val ratings = sc.textFile(params.input).map { line =>
      val fields = line.split("::")
      if (params.implicitPrefs) {
        /*
         * MovieLens ratings are on a scale of 1-5:
         * 5: Must see
         * 4: Will enjoy
         * 3: It's okay
         * 2: Fairly bad
         * 1: Awful
         * So we should not recommend a movie if the predicted rating is less than 3.
         * To map ratings to confidence scores, we use
         * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
         * entries are generally between It's okay and Fairly bad.
         * The semantics of 0 in this expanded world of non-positive weights
         * are "the same as never having interacted at all".
         */
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
      } else {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    }.cache()

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    val splits = ratings.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = if (params.implicitPrefs) {
      /*
       * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
       * Negative values means "confident that the prediction should be 0".
       * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
       * the confidence. The error is the difference between prediction and either 1 or 0,
       * depending on whether r is positive or negative.
       */
      splits(1).map(x => Rating(x.user, x.product, if (x.rating > 0) 1.0 else 0.0))
    } else {
      splits(1)
    }.cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    ratings.unpersist(blocking = false)

    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(training)

    val rmse = computeRmse(model, test, params.implicitPrefs)

    println(s"Test RMSE = $rmse.")

    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean) = {

    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
}
